//! Input ingestion helper for the FlowLog compiler.
//!
//! This module generates Rust code that:
//! - Declares `(handle, collection)` pairs for each EDB relation.
//! - Creates *mutable* bindings for handles so we can call `update(...)`.
//! - Ingests EDB facts from CSV-like files (arity > 0).
//! - Ingests compile-time boolean facts (from the parsed program).
//! - Closes all handles to signal end-of-input.
//!
//! Sharding strategy (to distribute ingestion across workers):
//! - If the first column is an integer: shard by `first % peers`.
//! - If the first column is a string: shard by a stable 64-bit FNV-1a hash of the bytes.

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::path::Path;

use super::Compiler;
use common::ExecutionMode;
use parser::{ConstType, DataType, Relation};
use profiler::{with_profiler, Profiler};

impl Compiler {
    /// Generate per-EDB declarations as `(handle, collection)` pairs:
    ///
    /// ```ignore
    /// let (h_<rel>, <rel>) = scope.new_collection::<_, Diff>();
    /// ```
    pub(super) fn gen_input_decls(&mut self, profiler: &mut Option<Profiler>) -> Vec<TokenStream> {
        let normalize = self.dedup_collection();

        let edbs = self.program.edbs();
        if edbs.is_empty() {
            return Vec::new();
        }

        self.imports.mark_input();

        // Record enter inpus block if profiler is enabled
        with_profiler(profiler, |profiler| {
            profiler.update_input_block();
        });

        edbs.iter()
            .map(|rel| {
                let handle = format_ident!("h{}", rel.name());
                let coll = format_ident!("{}", rel.name());

                // Record input EDB operator and dedup operator in profiler if enabled
                with_profiler(profiler, |profiler| {
                    profiler.input_edb_operator(rel.name().to_string(), coll.to_string());
                    profiler.input_dedup_operator(
                        rel.name().to_string(),
                        coll.to_string(),
                        coll.to_string(),
                    );
                });

                quote! {
                    let (#handle, #coll) = scope.new_collection::<_, Diff>();
                    let #coll = #coll #normalize;
                }
            })
            .collect()
    }

    /// Build a *single* mutable handle binding pattern for one or more handles.
    ///
    /// We intentionally shadow the original handles returned from `new_collection(...)`
    /// so downstream code can uniformly work with mutable handles:
    /// - 0 inputs: emits a harmless binding and returns `()`.
    /// - 1 input:  `let mut hR = worker.dataflow(...);` and returns `hR`.
    /// - N inputs: `let (mut hA, mut hB, ...) = worker.dataflow(...);` and returns `(hA, hB, ...)`.
    ///
    /// In **incremental** mode, we additionally bind/return a `probe` handle as the last element.
    pub(super) fn build_handle_binding(&self) -> (TokenStream, TokenStream) {
        let edb_names = self.program.edb_names();

        match self.config.mode() {
            ExecutionMode::Batch => match edb_names.len() {
                0 => (quote! { _handles }, quote! { () }),
                1 => {
                    let h = format_ident!("h{}", edb_names[0]);
                    (quote! { mut #h }, quote! { #h })
                }
                _ => {
                    let hs: Vec<_> = edb_names.iter().map(|n| format_ident!("h{}", n)).collect();
                    let lhs: Vec<_> = hs.iter().map(|h| quote! { mut #h }).collect();
                    (quote! { ( #(#lhs),* ) }, quote! { ( #(#hs),* ) })
                }
            },

            ExecutionMode::Incremental => {
                let probe = format_ident!("probe");

                match edb_names.len() {
                    0 => {
                        // `worker.dataflow` will return `(probe,)` in incremental mode.
                        (quote! { ( #probe, ) }, quote! { #probe })
                    }
                    1 => {
                        let h = format_ident!("h{}", edb_names[0]);
                        (quote! { ( #h, #probe ) }, quote! { ( #h, #probe ) })
                    }
                    _ => {
                        let hs: Vec<_> =
                            edb_names.iter().map(|n| format_ident!("h{}", n)).collect();
                        let lhs: Vec<_> = hs.iter().map(|h| quote! { #h }).collect();
                        (
                            quote! { ( #(#lhs),*, #probe ) },
                            quote! { ( #(#hs),*, #probe ) },
                        )
                    }
                }
            }
        }
    }

    /// Generate ingestion code for every EDB relation:
    /// - CSV ingestion for arity > 0
    /// - boolean fact ingestion (if present)
    pub(super) fn gen_ingest_stmts(&mut self) -> Vec<TokenStream> {
        let mut stmts = Vec::new();
        for rel in self.program.edbs() {
            if rel.arity() > 0 {
                self.imports.mark_std_file();
                self.imports.mark_std_buf_io();
                self.imports.mark_semiring_one();
            }

            self.imports.mark_semiring_one();

            let csv_ingest = self.gen_csv_ingest_stmt(rel);
            let bool_ingest = self.gen_bool_fact_ingest_stmt(rel);
            stmts.push(quote! {
                #csv_ingest
                #bool_ingest
            });
        }

        stmts
    }

    /// Generate CSV ingestion for a single relation.
    ///
    /// Notes:
    /// - Nullary relations do not read from CSV.
    /// - Records are sharded by the first field to spread ingestion work.
    fn gen_csv_ingest_stmt(&self, rel: &Relation) -> TokenStream {
        let arity = rel.arity();

        // Nullary relations do not read from files; they are handled via boolean facts (if any).
        if arity == 0 {
            return quote! {};
        }

        let handle = format_ident!("h{}", rel.name());

        // Resolve input path: `<fact_dir>/<file_name>` if configured, otherwise `file_name`.
        let file_name = rel.input_file_name();
        let path = self
            .config
            .fact_dir()
            .map(|dir| {
                Path::new(dir)
                    .join(&file_name)
                    .to_string_lossy()
                    .into_owned()
            })
            .unwrap_or_else(|| file_name);

        // Delimiter is expected to be a 1-byte separator (e.g., ',' or '\t').
        // We materialize it once to avoid repeatedly indexing `.as_bytes()[0]`.
        let delimiter = rel.input_delimiter();

        let data_types = rel.data_type();
        debug_assert_eq!(
            data_types.len(),
            arity,
            "relation arity must match number of attribute data types",
        );

        // Field identifiers: f0, f1, ..., f{arity-1}.
        let field_idents: Vec<Ident> = (0..arity).map(|i| format_ident!("f{}", i)).collect();

        // Parse fields f1..f{arity-1} (the first field is parsed in the sharding logic).
        let parse_rest_stmts: Vec<TokenStream> = field_idents
            .iter()
            .zip(data_types.iter())
            .skip(1)
            .map(|(ident, dt)| match *dt {
                DataType::Int32 => quote! {
                    let #ident: i32 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i32>().ok()?;
                },
                DataType::Int64 => quote! {
                    let #ident: i64 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i64>().ok()?;
                },
                DataType::String => quote! {
                    let #ident: String = std::str::from_utf8(tuple.next()?).ok()?.to_string();
                },
            })
            .collect();

        // Sharding / "should this worker ingest this row?" logic.
        let first_key = field_idents[0].clone();
        let (should_send_stmt, materialize_first_field): (TokenStream, TokenStream) =
            match data_types[0] {
                DataType::Int32 => (
                    quote! {
                        let #first_key: i32 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i32>().ok()?;
                        let should_send = ((#first_key as usize) % peers) == index;
                    },
                    // Integer 32 bits first field is already parsed; nothing more to do.
                    quote! {},
                ),
                DataType::Int64 => (
                    quote! {
                        let #first_key: i64 = std::str::from_utf8(tuple.next()?).ok()?.parse::<i64>().ok()?;
                        let should_send = ((#first_key as usize) % peers) == index;
                    },
                    // Integer 64 bits first field is already parsed; nothing more to do.
                    quote! {},
                ),
                DataType::String => (
                    quote! {
                        let __f0_bytes = tuple.next()?;
                        let should_send = {
                            // 64-bit FNV-1a hash for stable worker assignment on string keys.
                            let mut hash: u64 = 0xcbf29ce484222325;
                            for &b in __f0_bytes {
                                hash ^= b as u64;
                                hash = hash.wrapping_mul(0x100000001b3);
                            }
                            ((hash as usize) % peers) == index
                        };
                    },
                    // Materialize the String only when this worker keeps the row.
                    quote! {
                        let #first_key: String = std::str::from_utf8(__f0_bytes).ok()?.to_string();
                    },
                ),
            };

        // Element expression for `.update(...)`.
        let elem_expr = if arity == 1 {
            let x0 = field_idents[0].clone();
            quote! { ( #x0, ) }
        } else {
            quote! { ( #(#field_idents),* ) }
        };

        quote! {
            {
                let mut reader = BufReader::new(
                    File::open(#path)
                        .unwrap_or_else(|e| panic!("failed to open {}: {}", #path, e))
                );
                let delim: u8 = #delimiter.as_bytes()[0];

                // Reuse a single buffer across all lines to avoid per-line heap allocation.
                let mut buf = Vec::with_capacity(256);
                let mut __line_no = 0;
                while reader.read_until(b'\n', &mut buf)
                    .unwrap_or_else(|e| panic!("I/O error reading {} at line {}: {}", #path, __line_no, e)) > 0
                {
                    __line_no += 1;
                    // Strip trailing newline (and carriage return for CRLF files).
                    if buf.last() == Some(&b'\n') { buf.pop(); }
                    if buf.last() == Some(&b'\r') { buf.pop(); }

                    if !buf.is_empty() {
                        let row = (|| -> Option<_> {
                            let mut tuple = buf.split(|&bt| bt == delim);

                            #should_send_stmt
                            if !should_send {
                                return None;
                            }

                            #materialize_first_field
                            #(#parse_rest_stmts)*

                            Some(#elem_expr)
                        })();
                        if let Some(row) = row {
                            #handle.update(row, SEMIRING_ONE);
                        }
                    }
                    buf.clear();
                }
            }
        }
    }

    /// Generate ingestion code for boolean facts associated with `rel`.
    ///
    /// This is used for facts provided directly in the program (not via CSV files),
    /// typically for nullary relations or small compile-time datasets.
    fn gen_bool_fact_ingest_stmt(&self, rel: &Relation) -> TokenStream {
        let handle = format_ident!("h{}", rel.name());
        let rel_name = rel.name().to_string();

        let Some(facts) = self.program.bool_facts().get(&rel_name) else {
            return quote! {};
        };

        // Build literal tuples for all `true` facts only.
        let tuples: Vec<TokenStream> = facts
            .iter()
            .filter(|(_, b)| *b)
            .map(|(vals, _)| {
                let elems: Vec<TokenStream> = vals
                    .iter()
                    .map(|c| match c {
                        ConstType::Int32(i) => quote! { #i },
                        ConstType::Int64(i) => quote! { #i },
                        ConstType::Text(s) => quote! { #s.to_string() },
                    })
                    .collect();

                if elems.len() == 1 {
                    let e0 = &elems[0];
                    quote! { ( #e0, ) }
                } else {
                    quote! { ( #(#elems),* ) }
                }
            })
            .collect();

        if tuples.is_empty() {
            quote! {}
        } else {
            quote! {
                for row in [ #(#tuples),* ] {
                    #handle.update(row, SEMIRING_ONE);
                }
            }
        }
    }

    /// Generate code to close all input handles (signals end-of-input to the dataflow).
    ///
    /// We also print a per-relation "loaded" message on worker 0.
    pub fn gen_close_stmts(&self) -> Vec<TokenStream> {
        self.program
            .edb_names()
            .iter()
            .map(|name| {
                let handle = format_ident!("h{}", name);
                quote! {
                    #handle.close();
                    if index == 0 {
                        println!("{:?}:\tData loaded for {}", timer.elapsed(), #name);
                    }
                }
            })
            .collect()
    }
}
