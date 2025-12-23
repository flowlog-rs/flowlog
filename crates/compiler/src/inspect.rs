//! Inspector codegen helpers for FlowLog compiler.
//!
//! This module generates `TokenStream`s that attach lightweight “inspectors” to
//! collections produced by the FlowLog compiler/runtime. The inspectors are
//! intended for debugging and evaluation:
//! - print tuples or sizes to stderr
//! - write tuple values to per-worker partition files
//! - merge partition files into a single output (typically run by worker 0)

use crate::Compiler;

use common::ExecutionMode;

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{Index, LitStr};

impl Compiler {
    /// Generate code that prints the *cardinality* of a collection at each update.
    ///
    /// Strategy:
    /// - normalize the collection so each logical record contributes `SEMIRING_ONE`
    /// - convert to the underlying `(data, time, diff)` stream
    /// - map everything to the single key `()` so all updates consolidate into one
    /// - consolidate and inspect the resulting multiplicity (the size)
    pub(crate) fn gen_size_inspector(&self, var: &Ident, name: &str) -> TokenStream {
        let prefix = name.to_string();

        let maybe_probe = match self.config.mode() {
            ExecutionMode::Incremental => quote! { .probe_with(&mut probe) },
            ExecutionMode::Batch => quote! {},
        };

        quote! {{
            #var
                // Ensure each distinct record has weight `SEMIRING_ONE` under our semiring.
                .threshold_semigroup(move |_, _, old| old.is_none().then_some(SEMIRING_ONE))
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
    }

    /// Generate code that prints each tuple update (data only) at stderr for debugging.
    ///
    /// If `arity == 0`, print `True` instead of `()`, matching common Datalog
    /// conventions for 0-arity relations.
    pub(crate) fn gen_print_inspector(&self, var: &Ident, name: &str, arity: usize) -> TokenStream {
        let prefix = name.to_string();

        let maybe_probe = match self.config.mode() {
            ExecutionMode::Incremental => quote! { .probe_with(&mut probe) },
            ExecutionMode::Batch => quote! {},
        };

        if arity == 0 {
            quote! {{
                #var.inspect(|(_data, time, _diff)| {
                    eprintln!("[tuple][{}]  t={:?}  True", #prefix, time)
                })
                #maybe_probe;
            }}
        } else {
            quote! {{
                #var.inspect(|(data, time, diff)| {
                    eprintln!("[tuple][{}]  t={:?}  data={:?}  diff={:+?}", #prefix, time, data, diff)
                })
                #maybe_probe;
            }}
        }
    }

    /// Generate code that writes the data part of each update to a per-worker file.
    ///
    /// It creates `<parent_dir>/<relation_name><index>` where `index` is the worker id.
    /// Each tuple is appended as one line:
    /// - arity 0: `True`
    /// - arity >0: comma-separated values using `{}` formatting
    pub(crate) fn gen_write_inspector(
        &self,
        var: &Ident,
        name: &str,
        parent_dir: &str,
        arity: usize,
    ) -> TokenStream {
        let base_dir = parent_dir.to_string();
        let rel_name = name.to_string();

        let maybe_probe = match self.config.mode() {
            ExecutionMode::Incremental => quote! { .probe_with(&mut probe) },
            ExecutionMode::Batch => quote! {},
        };

        let data_accessors: Vec<TokenStream> = (0..arity)
            .map(|i| {
                let idx = Index::from(i);
                quote! { data.#idx }
            })
            .collect();

        // Generate the write statement. In incremental mode, append `diff` at the end.
        let write_stmt = match (self.config.mode(), arity) {
            (_, 0) => quote! {
                writeln!(&mut file, "True").expect("write failed");
            },
            (ExecutionMode::Batch, _) => {
                let fmt = vec!["{}"; arity].join(",");
                let fmt = LitStr::new(&fmt, Span::call_site());
                quote! {
                    writeln!(&mut file, #fmt #(, #data_accessors )*).expect("write failed");
                }
            }
            (ExecutionMode::Incremental, _) => {
                // tuple fields + ",{:+}" for diff at the end
                let mut parts = vec!["{}"; arity];
                parts.push("{:+}");
                let fmt = parts.join("  ");
                let fmt = LitStr::new(&fmt, Span::call_site());
                quote! {
                    writeln!(&mut file, #fmt #(, #data_accessors )*, diff).expect("write failed");
                }
            }
        };

        quote! {{
            let base_dir = #base_dir;
            let rel_name = #rel_name;
            let path = format!("{}/{}{}", base_dir, rel_name, index);

            let mut file = std::fs::File::create(&path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", path, e));

            #var.inspect(move |(data, _time, diff)| {
                use std::io::Write as _;
                #write_stmt
            })
            #maybe_probe;
        }}
    }

    /// Generate code that merges per-worker partition files into one output file.
    ///
    /// Reads `<base_path>/<name><wid>` for `wid in 0..peers` and writes the
    /// concatenation into `<base_path>/<name>`, then deletes the part files.
    ///
    /// Intended usage:
    /// - place after fixpoint
    /// - guard it so only worker 0 performs the merge (e.g., `if index == 0 { ... }`)
    pub(crate) fn gen_merge_partitions(&self, name: &str, base_path: &str) -> TokenStream {
        let base_dir = base_path.to_string();
        let rel_name = name.to_string();

        let write_merged_file = match self.config.mode() {
            ExecutionMode::Incremental => quote! {
                let out_path = format!("{}/{}_t{}", base_dir, rel_name, time_stamp);
                if let Err(e) = std::fs::write(&out_path, &merged) {
                    eprintln!("[merge] failed to write {}: {}", out_path, e);
                }
            },
            ExecutionMode::Batch => quote! {
                let out_path = format!("{}/{}", base_dir, rel_name);
                if let Err(e) = std::fs::write(&out_path, &merged) {
                    eprintln!("[merge] failed to write {}: {}", out_path, e);
                }
            },
        };

        let cleanup_partitions = match self.config.mode() {
            ExecutionMode::Incremental => quote! {
                // Keep partition files but clear contents (truncate) for next epoch.
                for wid in 0..peers {
                    let p = format!("{}/{}{}", base_dir, rel_name, wid);
                    if let Err(e) = std::fs::write(&p, "") {
                        eprintln!("[merge] failed to clear {}: {}", p, e);
                    }
                }
            },
            ExecutionMode::Batch => gen_delete_partitions(name, base_path),
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
