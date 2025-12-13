//! Inspector codegen helpers for FlowLog compiler.
//!
//! This module generates `TokenStream`s that attach lightweight “inspectors” to
//! collections produced by the FlowLog compiler/runtime. The inspectors are
//! intended for debugging and evaluation:
//! - print tuples or sizes to stderr
//! - write tuple values to per-worker partition files
//! - merge partition files into a single output (typically run by worker 0)

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{Index, LitStr};

/// Generate code that prints the *cardinality* of a collection at each update.
///
/// Strategy:
/// - normalize the collection so each logical record contributes `SEMIRING_ONE`
/// - convert to the underlying `(data, time, diff)` stream
/// - map everything to the single key `()` so all updates consolidate into one
/// - consolidate and inspect the resulting multiplicity (the size)
pub fn gen_size_inspector(var: &Ident, name: &str) -> TokenStream {
    let prefix = name.to_string();

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
            .inspect(|(_data, _time, size)| eprintln!("[size] [{}] {:?}", #prefix, size));
    }}
}

/// Generate code that prints each tuple update (data only) at stderr for debugging.
///
/// If `arity == 0`, print `True` instead of `()`, matching common Datalog
/// conventions for 0-arity relations.
pub fn gen_print_inspector(var: &Ident, name: &str, arity: usize) -> TokenStream {
    let prefix = name.to_string();

    if arity == 0 {
        quote! {{
            #var.inspect(|(_data, _time, _diff)| {
                eprintln!("[tuple] [{}] True", #prefix)
            });
        }}
    } else {
        quote! {{
            #var.inspect(|(data, _time, _diff)| {
                eprintln!("[tuple] [{}] {:?}", #prefix, data)
            });
        }}
    }
}

/// Generate code that writes the data part of each update to a per-worker file.
///
/// It creates `<parent_dir>/<relation_name><index>` where `index` is the worker id.
/// Each tuple is appended as one line:
/// - arity 0: `True`
/// - arity >0: comma-separated values using `{}` formatting
pub fn gen_write_inspector(var: &Ident, name: &str, parent_dir: &str, arity: usize) -> TokenStream {
    let base_dir = parent_dir.to_string();
    let rel_name = name.to_string();

    let data_accessors: Vec<TokenStream> = (0..arity)
        .map(|i| {
            let idx = Index::from(i);
            quote! { data.#idx }
        })
        .collect();

    let write_stmt = if arity == 0 {
        quote! {
            writeln!(&mut file, "True").expect("write failed");
        }
    } else {
        let fmt = vec!["{}"; arity].join(",");
        let fmt = LitStr::new(&fmt, Span::call_site());
        quote! {
            writeln!(&mut file, #fmt #(, #data_accessors )*).expect("write failed");
        }
    };

    quote! {{
        let base_dir = #base_dir;
        let rel_name = #rel_name;
        let path = format!("{}/{}{}", base_dir, rel_name, index);

        let mut file = std::fs::File::create(&path)
            .unwrap_or_else(|e| panic!("failed to create {}: {}", path, e));

        #var.inspect(move |(data, _time, _diff)| {
            use std::io::Write as _;
            #write_stmt
        });
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
pub fn gen_merge_partitions(name: &str, base_path: &str) -> TokenStream {
    let base_dir = base_path.to_string();
    let rel_name = name.to_string();

    quote! {{
        let base_dir = #base_dir;
        let rel_name = #rel_name;

        // Merge `<base_dir>/<rel_name><wid>` into `<base_dir>/<rel_name>`.
        let merged = (0..peers)
            .filter_map(|wid| {
                std::fs::read_to_string(format!("{}/{}{}", base_dir, rel_name, wid)).ok()
            })
            .collect::<String>();

        if let Err(e) = std::fs::write(format!("{}/{}", base_dir, rel_name), merged) {
            eprintln!("[merge] failed to write {}/{}: {}", base_dir, rel_name, e);
        }

        for wid in 0..peers {
            let _ = std::fs::remove_file(format!("{}/{}{}", base_dir, rel_name, wid));
        }
    }}
}
