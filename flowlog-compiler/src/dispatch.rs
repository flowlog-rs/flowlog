//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container. Each command selects a loader;
//! runtime loaders handle decoding and partitioning.

use flowlog_parser::InputSource;
use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::io::input;

/// Emits name-based command dispatch on the generated `Inputs` container.
pub(crate) fn gen_dispatch(program: &Program) -> TokenStream {
    let mut put_arms = Vec::new();
    let mut file_arms = Vec::new();
    let mut relation_names = Vec::new();
    for relation in program.edbs() {
        let name = relation.name();
        let field = format_ident!("in_{}", name);
        let method = if relation.arity() == 0 {
            format_ident!("load_flag")
        } else {
            format_ident!("load_put")
        };
        relation_names.push(name);
        put_arms.push(quote! { #name => Some(self.#field.#method(text, ordinal, diff)), });
        let load = input::gen_load(
            relation,
            quote! { self.#field },
            quote! { path },
            quote! { diff },
        );
        let load = match relation.input() {
            Some(InputSource::Sqlite { .. }) => {
                let relation_name = relation.raw_name();
                quote! {{
                    let result = #load;
                    if let Err(error) = &result {
                        eprintln!("[relation][{}] {} in {}", #relation_name, error, path.display());
                        std::process::exit(1);
                    }
                    result
                }}
            }
            Some(InputSource::File { .. } | InputSource::Command { .. }) | None => load,
        };
        file_arms.push(quote! { #name => Some(#load), });
    }

    quote! {
        impl Inputs {
            pub fn names() -> &'static [&'static str] { &[#(#relation_names),*] }

            pub fn load_put(
                &mut self,
                name: &str,
                text: &str,
                ordinal: usize,
                diff: Diff,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match name.to_ascii_lowercase().as_str() {
                    #(#put_arms)*
                    _ => None,
                }
            }

            pub fn load_file(
                &mut self,
                name: &str,
                path: &std::path::Path,
                diff: Diff,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match name.to_ascii_lowercase().as_str() {
                    #(#file_arms)*
                    _ => None,
                }
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::fs;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;

    use super::*;

    #[test]
    fn commands_route_to_each_relations_loader() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("program.dl");
        fs::write(
            &path,
            r#"
            .decl Edge(id: int32)
            .input Edge(delimiter=",", header="true")
            .decl Flag()
            .input Flag(IO="command")
            .decl Out(id: int32)
            Out(x) :- Edge(x), Flag().
            .output Out
            "#,
        )
        .expect("program");
        let program = flowlog_parser::parse(
            path.to_str().expect("path"),
            &[],
            &mut SourceMap::default(),
            &mut Config::default(),
        )
        .expect("parse");
        let generated = gen_dispatch(&program).to_string();
        for expected in [
            quote! {
                match name.to_ascii_lowercase().as_str() {
                    "edge" => Some(self.in_edge.load_put(text, ordinal, diff)),
                    "flag" => Some(self.in_flag.load_flag(text, ordinal, diff)),
                    _ => None,
                }
            },
            quote! {
                match name.to_ascii_lowercase().as_str() {
                    "edge" => Some(self.in_edge.load_file(path, diff)),
                    "flag" => Some(self.in_flag.load_file(path, diff)),
                    _ => None,
                }
            },
            quote! { &["edge", "flag"] },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }
}
