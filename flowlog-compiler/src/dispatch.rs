//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container. Each command selects a loader;
//! runtime loaders handle decoding and partitioning. A static relation
//! refuses every command: its input closes after the initial load.

use flowlog_build::input_field_ident;
use flowlog_parser::InputSource;
use flowlog_parser::Mutability;
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
        relation_names.push(name);
        let (put_arm, file_arm) = match relation.input_mutability() {
            Mutability::Static => {
                let raw_name = relation.raw_name();
                let refuse = quote! {
                    Some(Err(::flowlog_runtime::RuntimeError::StaticRelation { relation: #raw_name }))
                };
                (quote! { #name => #refuse, }, quote! { #name => #refuse, })
            }
            Mutability::Mutable => {
                let field = input_field_ident(name);
                let method = if relation.arity() == 0 {
                    format_ident!("load_flag")
                } else {
                    format_ident!("load_put")
                };
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
                (
                    quote! { #name => Some(self.#field.#method(text, ordinal, diff)), },
                    quote! { #name => Some(#load), },
                )
            }
        };
        put_arms.push(put_arm);
        file_arms.push(file_arm);
    }

    quote! {
        impl Inputs {
            pub fn names() -> &'static [&'static str] { &[#(#relation_names),*] }

            pub fn load_put(
                &mut self,
                name: &str,
                text: &str,
                ordinal: usize,
                diff: ::flowlog_runtime::txn::Diff,
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
                diff: ::flowlog_runtime::txn::Diff,
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

    fn generate(source: &str) -> String {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("program.dl");
        fs::write(&path, source).expect("program");
        let program = flowlog_parser::parse(
            path.to_str().expect("path"),
            &[],
            &mut SourceMap::default(),
            &mut Config::default(),
        )
        .expect("parse");
        gen_dispatch(&program).to_string()
    }

    #[test]
    fn commands_route_to_each_mutable_relations_loader() {
        let generated = generate(
            r#"
            .decl Edge(id: int32) mutable
            .input Edge(delimiter=",", header="true")
            .decl Flag() mutable
            .input Flag(IO="command")
            .decl Out(id: int32)
            Out(x) :- Edge(x), Flag().
            .output Out
            "#,
        );
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

    /// A static relation stays in the prompt's names, but every command on
    /// it is refused rather than routed to its closed loader.
    #[test]
    fn commands_on_a_static_relation_are_refused() {
        let generated = generate(
            r#"
            .decl Edge(id: int32)
            .input Edge
            .decl Out(id: int32)
            Out(x) :- Edge(x).
            .output Out
            "#,
        );
        let refused = quote! {
            "edge" => Some(Err(::flowlog_runtime::RuntimeError::StaticRelation { relation: "Edge" })),
        };
        assert_eq!(
            generated.matches(&refused.to_string()).count(),
            2,
            "{generated}"
        );
        assert!(!generated.contains("in_edge"), "{generated}");
        assert!(
            generated.contains(&quote! { &["edge"] }.to_string()),
            "{generated}"
        );
    }
}
