//! Input ingestion helper for the FlowLog compiler.
//!
//! This module generates Rust code that:
//! - Declares `(handle, collection)` pairs for each EDB relation.
//! - Creates *mutable* bindings for handles so we can call `update(...)`.
//! - Ingests EDB facts from CSV-like files (arity > 0).
//! - Ingests compile-time boolean facts (from the parsed program).
//! - Closes all handles to signal end-of-input.
//!
//! Ingestion strategy (byte-range parallel reading):
//! Each worker reads only its ~1/N byte slice of each facts file, avoiding
//! redundant I/O. Non-first workers skip the partial first line at their
//! seek offset.
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

        if self.config.str_intern_enabled()
            && edbs
                .iter()
                .any(|rel| rel.data_type().contains(&DataType::String))
        {
            self.imports.mark_string_intern();
        }

        // Record enter inputs block if profiler is enabled
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
                self.imports.mark_memchr();
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

    /// Generate CSV ingestion for a single relation using byte-range parallel reading.
    ///
    /// Each worker reads only its ~1/N byte slice of the file. Non-first workers
    /// skip the partial first line at their seek offset. All parsed rows are fed
    /// into the input handle.
    ///
    /// Notes:
    /// - Nullary relations do not read from CSV.
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
        // Unescape common escape sequences so that e.g. `\t` in the .dl file
        // produces an actual tab byte (0x09) in the generated Rust code.
        let delimiter = match rel.input_delimiter() {
            "\\t" => "\t",
            "\\n" => "\n",
            other => other,
        };

        let data_types = rel.data_type();
        debug_assert_eq!(
            data_types.len(),
            arity,
            "relation arity must match number of attribute data types",
        );

        // Field identifiers: f0, f1, ..., f{arity-1}.
        let field_idents: Vec<Ident> = (0..arity).map(|i| format_ident!("f{}", i)).collect();

        // Parse all fields uniformly (byte-range handles partitioning).
        let parse_stmts: Vec<TokenStream> = field_idents
            .iter()
            .enumerate()
            .zip(data_types.iter())
            .map(|((i, ident), dt)| {
                if i == 0 {
                    // First field: find the first delimiter.
                    match *dt {
                        DataType::Int32 => quote! {
                            let mut __delims = memchr_iter(delim, &buf);
                            let __first_end = __delims.next().unwrap_or(buf.len());
                            let #ident: i32 = std::str::from_utf8(&buf[..__first_end]).ok()?.parse::<i32>().ok()?;
                            let mut __start = __first_end + 1;
                        },
                        DataType::Int64 => quote! {
                            let mut __delims = memchr_iter(delim, &buf);
                            let __first_end = __delims.next().unwrap_or(buf.len());
                            let #ident: i64 = std::str::from_utf8(&buf[..__first_end]).ok()?.parse::<i64>().ok()?;
                            let mut __start = __first_end + 1;
                        },
                        DataType::String => {
                            if self.imports.needs_string_intern() {
                                quote! {
                                    let mut __delims = memchr_iter(delim, &buf);
                                    let __first_end = __delims.next().unwrap_or(buf.len());
                                    let #ident: Spur = intern(std::str::from_utf8(&buf[..__first_end]).ok()?);
                                    let mut __start = __first_end + 1;
                                }
                            } else {
                                quote! {
                                    let mut __delims = memchr_iter(delim, &buf);
                                    let __first_end = __delims.next().unwrap_or(buf.len());
                                    let #ident: String = std::str::from_utf8(&buf[..__first_end]).ok()?.to_string();
                                    let mut __start = __first_end + 1;
                                }
                            }
                        }
                        DataType::Bool => quote! {
                            let mut __delims = memchr_iter(delim, &buf);
                            let __first_end = __delims.next().unwrap_or(buf.len());
                            let #ident: bool = std::str::from_utf8(&buf[..__first_end]).ok()?.parse::<bool>().ok()?;
                            let mut __start = __first_end + 1;
                        },
                    }
                } else {
                    // Subsequent fields.
                    match *dt {
                        DataType::Int32 => quote! {
                            let __end = __delims.next().unwrap_or(buf.len());
                            let #ident: i32 = std::str::from_utf8(&buf[__start..__end]).ok()?.parse::<i32>().ok()?;
                            __start = __end + 1;
                        },
                        DataType::Int64 => quote! {
                            let __end = __delims.next().unwrap_or(buf.len());
                            let #ident: i64 = std::str::from_utf8(&buf[__start..__end]).ok()?.parse::<i64>().ok()?;
                            __start = __end + 1;
                        },
                        DataType::String => {
                            if self.imports.needs_string_intern() {
                                quote! {
                                    let __end = __delims.next().unwrap_or(buf.len());
                                    let #ident: Spur = intern(std::str::from_utf8(&buf[__start..__end]).ok()?);
                                    __start = __end + 1;
                                }
                            } else {
                                quote! {
                                    let __end = __delims.next().unwrap_or(buf.len());
                                    let #ident: String = std::str::from_utf8(&buf[__start..__end]).ok()?.to_string();
                                    __start = __end + 1;
                                }
                            }
                        }
                        DataType::Bool => quote! {
                            let __end = __delims.next().unwrap_or(buf.len());
                            let #ident: bool = std::str::from_utf8(&buf[__start..__end]).ok()?.parse::<bool>().ok()?;
                            __start = __end + 1;
                        },
                    }
                }
            })
            .collect();

        // Element expression for `.update(...)`.
        let elem_expr = if arity == 1 {
            let x0 = field_idents[0].clone();
            quote! { ( #x0, ) }
        } else {
            quote! { ( #(#field_idents),* ) }
        };

        quote! {
            {
                let (mut __reader, __byte_budget) = __byte_range_reader(#path, index, peers);
                let delim: u8 = #delimiter.as_bytes()[0];

                // Reuse a single buffer across all lines to avoid per-line heap allocation.
                let mut buf = Vec::with_capacity(256);
                let mut __bytes_consumed: u64 = 0;
                while __bytes_consumed < __byte_budget {
                    buf.clear();
                    if __reader.read_until(b'\n', &mut buf)
                        .unwrap_or_else(|e| panic!("I/O error reading {}: {}", #path, e)) == 0
                    { break; }
                    __bytes_consumed += buf.len() as u64;

                    // Strip trailing newline (and carriage return for CRLF files).
                    if buf.last() == Some(&b'\n') { buf.pop(); }
                    if buf.last() == Some(&b'\r') { buf.pop(); }

                    if !buf.is_empty() {
                        let row = (|| -> Option<_> {
                            #(#parse_stmts)*

                            Some(#elem_expr)
                        })();
                        if let Some(row) = row {
                            #handle.update(row, SEMIRING_ONE);
                        }
                    }
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
                        ConstType::Text(s) => {
                            if self.imports.needs_string_intern() {
                                quote! { intern(#s) }
                            } else {
                                quote! { #s.to_string() }
                            }
                        }
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

    /// Generate the `__byte_range_reader` helper function for byte-range parallel CSV reading.
    ///
    /// Each worker calls it to open its byte slice of a CSV file:
    ///   1. Divide the file into N equal byte ranges.
    ///   2. Seek to the start of this worker's range.
    ///   3. Non-first workers skip the partial first line (the previous worker reads it fully).
    ///   4. Return (reader, bytes_to_read) so the caller can read lines within its budget.
    pub(crate) fn gen_byte_range_reader(&self) -> TokenStream {
        if !matches!(self.config.mode(), ExecutionMode::Batch) {
            return quote! {};
        }

        // Only emit when there are EDB relations with arity > 0 (i.e., CSV ingestion needed).
        if !self.program.edbs().iter().any(|rel| rel.arity() > 0) {
            return quote! {};
        }

        quote! {
            /// Open a byte-range slice of a CSV file for parallel reading.
            ///
            /// Each worker reads approximately `1/peers` of the file. Non-first workers
            /// skip the partial line at their seek offset so no line is double-read or missed.
            ///
            /// Returns `(reader, bytes_to_read)`:
            /// - `reader`: a `BufReader` positioned at the first complete line in this range.
            /// - `bytes_to_read`: the byte budget for this worker (read lines until exhausted).
            fn __byte_range_reader(
                path: &str,
                index: usize,
                peers: usize,
            ) -> (BufReader<File>, u64) {
                let mut file = File::open(path)
                    .unwrap_or_else(|e| panic!("failed to open {}: {}", path, e));
                let file_size = file
                    .metadata()
                    .unwrap_or_else(|e| panic!("failed to get metadata for {}: {}", path, e))
                    .len();

                let chunk = file_size / peers as u64;
                let start = chunk * index as u64;
                let end = if index == peers - 1 {
                    file_size
                } else {
                    chunk * (index + 1) as u64
                };

                // Guard: if this worker has nothing to read (tiny file, many workers).
                if start >= end {
                    file.seek(SeekFrom::End(0))
                        .unwrap_or_else(|e| panic!("failed to seek in {}: {}", path, e));
                    return (BufReader::new(file), 0);
                }

                if index > 0 && start > 0 {
                    // Peek the byte immediately before our range to decide whether
                    // we landed on a line boundary or mid-line.
                    file.seek(SeekFrom::Start(start - 1))
                        .unwrap_or_else(|e| panic!("failed to seek in {}: {}", path, e));
                    let mut reader = BufReader::new(file);
                    let mut peek = [0u8; 1];
                    reader.read_exact(&mut peek)
                        .unwrap_or_else(|e| panic!("failed to read boundary byte in {}: {}", path, e));

                    if peek[0] == b'\n' {
                        // Exactly on a line boundary — reader is already at `start`, ready to go.
                        return (reader, end - start);
                    }
                    // Mid-line: skip the rest of this partial line.
                    let mut discard = Vec::new();
                    let skipped = reader
                        .read_until(b'\n', &mut discard)
                        .unwrap_or_else(|e| {
                            panic!("failed to skip partial line in {}: {}", path, e)
                        });
                    return (reader, (end - start).saturating_sub(skipped as u64));
                }

                // Worker 0 (or start == 0 for tiny files): read from the beginning.
                let reader = BufReader::new(file);
                (reader, end - start)
            }
        }
    }
}
