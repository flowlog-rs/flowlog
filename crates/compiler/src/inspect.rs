//! Inspector codegen helpers for FlowLog compiler.
//!
//! This module generates `TokenStream`s that attach lightweight “inspectors” to
//! collections produced by the FlowLog compiler/runtime. The inspectors are
//! intended for debugging and evaluation:
//! - print tuples or sizes to stderr
//! - write tuple values to per-worker partition files
//! - merge partition files into a single output (typically run by worker 0)
//!
//! In incremental mode, tuple inspectors consolidate per-timestamp updates first
//! so stdout and output files show the net delta for each commit rather than the
//! full transient recursive update stream.

use crate::Compiler;

use parser::{DataType, Relation};
use profiler::{with_profiler, Profiler};

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{Index, LitStr};

impl Compiler {
    /// Incremental outputs are easier to consume as net deltas per commit.
    #[inline]
    fn maybe_consolidate_incremental_output(&self) -> TokenStream {
        if self.config.is_incremental() {
            quote! { .consolidate() }
        } else {
            quote! {}
        }
    }

    /// Generate code that prints the *cardinality* of a collection at each update.
    ///
    /// Strategy:
    /// - normalize the collection so each logical record contributes `SEMIRING_ONE`
    /// - convert to the underlying `(data, time, diff)` stream
    /// - map everything to the single key `()` so all updates consolidate into one
    /// - consolidate and inspect the resulting multiplicity (the size)
    pub(crate) fn gen_size_inspector(
        &self,
        var: &Ident,
        name: &str,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let prefix = name.to_string();

        let maybe_probe = if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        };

        // Record inspect size operator in profiler if enabled
        with_profiler(profiler, |profiler| {
            profiler.inspect_size_operator(prefix.clone(), prefix.clone());
        });

        if self.config.is_datalog_batch() {
            quote! {{
                #var.clone()
                    // Consolidate merges Present + Present = Present (pure dedup).
                    .consolidate()
                    // Drop data; keep time, emit `diff = 1` in DD's inner representation.
                    .inner
                    .flat_map(move |(_, t, _)| std::iter::once(((), t.clone(), 1_i32)))
                    // Back to a collection so we can consolidate.
                    .as_collection()
                    .map(|_| ())
                    .consolidate()
                    .inspect(|(_data, time, size)| eprintln!("[size][{}] t={:?} size={:?}", #prefix, time, size))
                    #maybe_probe;
            }}
        } else {
            let dedup = Self::threshold_i32();
            quote! {{
                #var.clone()
                    #dedup
                    .inner
                    .flat_map(move |(_, t, d)| std::iter::once(((), t.clone(), d)))
                    .as_collection()
                    .map(|_| ())
                    .consolidate()
                    .inspect(|(_data, time, diff)| eprintln!("[size][{}] t={:?} size_diff={:?}", #prefix, time, diff))
                    #maybe_probe;
            }}
        }
    }

    /// Generate code that prints tuple updates to stderr for debugging.
    ///
    /// In incremental mode, the printed stream is consolidated first so each
    /// commit shows only its net delta.
    ///
    /// If `arity == 0`, print `True` instead of `()`, matching common Datalog
    /// conventions for 0-arity relations.
    pub(crate) fn gen_print_inspector(
        &self,
        var: &Ident,
        name: &str,
        arity: usize,
        data_types: &[DataType],
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let prefix = name.to_string();

        let maybe_probe = if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        };

        let maybe_consolidate = self.maybe_consolidate_incremental_output();

        // Record inspect content terminal operator in profiler if enabled
        with_profiler(profiler, |profiler| {
            profiler.inspect_content_terminal_operator(prefix.clone(), prefix.clone());
        });

        if arity == 0 {
            quote! {{
                #var
                    #maybe_consolidate
                    .inspect(|(_data, time, _diff)| {
                        eprintln!("[tuple][{}]  t={:?}  True", #prefix, time)
                    })
                    #maybe_probe;
            }}
        } else {
            let field_displays: Vec<TokenStream> = data_types
                .iter()
                .enumerate()
                .map(|(i, dt)| {
                    let idx = Index::from(i);
                    match dt {
                        DataType::String if self.imports.needs_string_intern() => {
                            quote! { resolve(data.#idx) }
                        }
                        _ => quote! { data.#idx },
                    }
                })
                .collect();
            let fmt_str = (0..arity).map(|_| "{:?}").collect::<Vec<_>>().join(", ");
            let fmt_full = format!(
                "[tuple][{}]  t={{:?}}  data=({})  diff={{:+?}}",
                prefix, fmt_str
            );
            let fmt_lit = LitStr::new(&fmt_full, Span::call_site());
            quote! {{
                #var
                    #maybe_consolidate
                    .inspect(|(data, time, diff)| {
                        eprintln!(#fmt_lit, time #(, #field_displays )*, diff);
                    })
                    #maybe_probe;
            }}
        }
    }

    /// Generate code that writes tuple updates to a per-worker file.
    ///
    /// It creates `<parent_dir>/<relation_name><index>` where `index` is the worker id.
    /// Each tuple is appended as one line:
    /// - arity 0: `True`
    /// - arity >0: comma-separated values using `{}` formatting
    ///
    /// In incremental mode, writes the net delta for each commit after
    /// consolidating transient recursive updates.
    pub(crate) fn gen_write_inspector(
        &self,
        var: &Ident,
        name: &str,
        parent_dir: &str,
        idb: &Relation,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let base_dir = parent_dir.to_string();
        let rel_name = name.to_string();

        let maybe_probe = if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        };

        let maybe_consolidate = self.maybe_consolidate_incremental_output();

        // Record inspect content file operator in profiler if enabled
        with_profiler(profiler, |profiler| {
            profiler.inspect_content_file_operator(rel_name.clone(), rel_name.clone());
        });

        let data_accessors: Vec<TokenStream> = (0..idb.arity())
            .map(|i| {
                let idx = Index::from(i);
                match idb.data_type().get(i) {
                    Some(DataType::String) if self.imports.needs_string_intern() => {
                        quote! { resolve(data.#idx) }
                    }
                    _ => quote! { data.#idx },
                }
            })
            .collect();

        // Generate the inspect pattern and write statement based on mode and arity.
        //
        // Batch modes (Standard and Extended) output only positive facts — the diff
        // column is omitted. Incremental modes include the diff column for change
        // tracking.
        let (inspect_pattern, write_stmt) = if self.config.is_batch() {
            if idb.arity() == 0 {
                (
                    quote! { (_data, _time, _diff) },
                    quote! { writeln!(&mut file, "True").expect("write failed"); },
                )
            } else {
                let fmt = vec!["{}"; idb.arity()].join(idb.output_delimiter());
                let fmt = LitStr::new(&fmt, Span::call_site());
                (
                    quote! { (data, _time, _diff) },
                    quote! {
                        writeln!(&mut file, #fmt #(, #data_accessors )*).expect("write failed");
                    },
                )
            }
        } else {
            // Incremental: include diff column for change tracking.
            if idb.arity() == 0 {
                (
                    quote! { (_data, _time, _diff) },
                    quote! { writeln!(&mut file, "True").expect("write failed"); },
                )
            } else {
                let mut parts = vec!["{}"; idb.arity()];
                parts.push("{:+}");
                let fmt = parts.join(idb.output_delimiter());
                let fmt = LitStr::new(&fmt, Span::call_site());
                (
                    quote! { (data, _time, diff) },
                    quote! {
                        writeln!(&mut file, #fmt #(, #data_accessors )*, diff)
                            .expect("write failed");
                    },
                )
            }
        };

        quote! {{
            let base_dir = #base_dir;
            let rel_name = #rel_name;
            let path = format!("{}/{}{}", base_dir, rel_name, index);

            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", path, e));

            #var
                #maybe_consolidate
                .inspect(move |#inspect_pattern| {
                    use std::io::Write as _;
                    #write_stmt
                })
                #maybe_probe;
        }}
    }

    /// Generate code that merges per-worker partition files into one output file.
    ///
    /// Reads `<base_path>/<name><wid>` for `wid in 0..peers` and writes the
    /// concatenation into one merged output.
    ///
    /// Batch mode writes `<base_path>/<name>` and deletes the part files.
    /// Incremental mode writes `<base_path>/<name>_t<time_stamp>` and clears the
    /// part files for reuse on the next commit.
    ///
    /// Intended usage:
    /// - place after fixpoint
    /// - guard it so only worker 0 performs the merge (e.g., `if index == 0 { ... }`)
    pub(crate) fn gen_merge_partitions(&self, name: &str, base_path: &str) -> TokenStream {
        let base_dir = base_path.to_string();
        let rel_name = name.to_string();

        let write_merged_file = if self.config.is_incremental() {
            quote! {
                let out_path = format!("{}/{}_t{}", base_dir, rel_name, time_stamp);
                if let Err(e) = std::fs::write(&out_path, &merged) {
                    eprintln!("[merge] failed to write {}: {}", out_path, e);
                }
            }
        } else {
            quote! {
                let out_path = format!("{}/{}", base_dir, rel_name);
                if let Err(e) = std::fs::write(&out_path, &merged) {
                    eprintln!("[merge] failed to write {}: {}", out_path, e);
                }
            }
        };

        let cleanup_partitions = if self.config.is_incremental() {
            quote! {
                // Keep partition files but clear contents (truncate) for next epoch.
                for wid in 0..peers {
                    let p = format!("{}/{}{}", base_dir, rel_name, wid);
                    if let Err(e) = std::fs::write(&p, "") {
                        eprintln!("[merge] failed to clear {}: {}", p, e);
                    }
                }
            }
        } else {
            gen_delete_partitions(name, base_path)
        };

        quote! {{
            let base_dir = #base_dir;
            let rel_name = #rel_name;

            // Merge `<base_dir>/<rel_name><wid>` into one output.
            let merged = (0..peers)
                .filter_map(|wid| {
                    std::fs::read_to_string(format!("{}/{}{}", base_dir, rel_name, wid)).ok()
                })
                .collect::<String>();

            #write_merged_file
            #cleanup_partitions
        }}
    }
}

/// Generate code that deletes per-worker partition files `<base>/<name><wid>`.
pub(crate) fn gen_delete_partitions(name: &str, base_path: &str) -> TokenStream {
    let base_dir = base_path.to_string();
    let rel_name = name.to_string();

    quote! {{
        for wid in 0..peers {
            let _ = std::fs::remove_file(format!("{}/{}{}", #base_dir, #rel_name, wid));
        }
    }}
}
