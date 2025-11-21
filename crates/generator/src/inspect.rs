use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{Index, LitStr};

/// Generate a TokenStream that inspects the cardinality of a differential dataflow
/// collection identified by `var`. The output prints a single consolidated record
/// whose weight corresponds to the number of elements in the collection at that point.
pub fn gen_size_inspector(var: &Ident, name: &str) -> TokenStream {
    let prefix = format!("{}:", name);

    // We map all records to unit `()` so they consolidate to one key whose
    // multiplicity equals the current collection size. We then inspect it.
    quote! {
        {
            // Consolidate to a single key carrying the total multiplicity
            // and print the resulting update.
            #var
                .map(|_| ())
                .consolidate()
                .inspect(|(_data, _time, size)| eprintln!("[size] [{}] {:?}", #prefix, size));
        }
    }
}

/// Generate a TokenStream that prints each update of a collection with its time and diff.
/// Useful for debugging tuple contents.
pub fn gen_print_inspector(var: &Ident, name: &str) -> TokenStream {
    let prefix = format!("{}:", name);
    quote! {
        {
            #var
                .inspect(|(data, _time, _diff)| eprintln!("[tuple] [{}] {:?}", #prefix, data));
        }
    }
}

/// Generate a TokenStream that writes the data part of each update to the given file path.
/// Opens the file once and appends each tuple as a line using Debug formatting.
pub fn gen_write_inspector(var: &Ident, name: &str, parent_dir: &str, arity: usize) -> TokenStream {
    let dir = parent_dir.to_string();
    let relation_name = name.to_string();
    let data_accessors: Vec<TokenStream> = (0..arity)
        .map(|idx| {
            let index = Index::from(idx);
            quote! { data.#index }
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
    quote! {
        {
            let base_path = #dir;
            let name = #relation_name;
            let path = format!("{}/{}{}", base_path, name, index);
            let mut file = std::fs::File::create(&path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", path, e));
            #var
                .inspect(move |(data, _time, _diff)| {
                    use std::io::Write as _;
                    #write_stmt
                });
        }
    }
}

/// Generate a TokenStream that merges per-worker partition files into a single file.
/// It reads `<base_path><wid>` for all workers and writes the concatenation to `<base_path>`.
/// Place the generated code after fixpoint and guard it with `if index == 0 { ... }`.
pub fn gen_merge_partitions(name: &str, base_path: &str) -> TokenStream {
    let base = base_path.to_string();
    let relation_name = name.to_string();
    quote! {
        {
            let base_path = #base;
            let name = #relation_name;
            // Merge `<base_path><wid>` into `<base_path>` and remove parts.
            let merged = (0..peers)
                .filter_map(|wid| std::fs::read_to_string(format!("{}/{}{}", base_path, name, wid)).ok())
                .collect::<String>();

            if let Err(e) = std::fs::write(format!("{}/{}", base_path, name), merged) {
                eprintln!("[merge] failed to write {}: {}", base_path, e);
            }

            for wid in 0..peers {
                let _ = std::fs::remove_file(format!("{}/{}{}", base_path, name, wid));
            }
        }
    }
}
