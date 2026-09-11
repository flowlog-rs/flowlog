//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container. Each command selects a loader and its
//! source options; runtime loaders handle decoding and partitioning.

use flowlog_parser::InputSource;
use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

/// Emits name-based command dispatch on the generated `Inputs` container.
pub(crate) fn gen_dispatch(program: &Program) -> TokenStream {
    let mut puts = Vec::new();
    let mut files = Vec::new();
    let mut names = Vec::new();
    for relation in program.edbs() {
        let name = relation.name();
        let field = format_ident!("in_{}", name);
        let delimiter = relation
            .input()
            .and_then(InputSource::delim)
            .unwrap_or(b'\t');
        let has_header = relation.input().is_some_and(InputSource::has_header);
        let method = if relation.arity() == 0 {
            format_ident!("load_flag")
        } else {
            format_ident!("load_put")
        };
        names.push(name);
        puts.push(quote! { #name => Some(self.#field.#method(text, ordinal, #delimiter, diff)), });
        files.push(quote! {
            #name => Some(self.#field.load_file(path, #delimiter, #has_header, diff)),
        });
    }

    quote! {
        impl Inputs {
            pub fn names() -> &'static [&'static str] { &[#(#names),*] }

            pub fn load_put(
                &mut self,
                name: &str,
                text: &str,
                ordinal: usize,
                diff: Diff,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match name.to_ascii_lowercase().as_str() {
                    #(#puts)*
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
                    #(#files)*
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
    fn commands_route_to_loaders_with_each_relations_input_options() {
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
                    "edge" => Some(self.in_edge.load_put(text, ordinal, 44u8, diff)),
                    "flag" => Some(self.in_flag.load_flag(text, ordinal, 9u8, diff)),
                    _ => None,
                }
            },
            quote! {
                match name.to_ascii_lowercase().as_str() {
                    "edge" => Some(self.in_edge.load_file(path, 44u8, true, diff)),
                    "flag" => Some(self.in_flag.load_file(path, 9u8, false, diff)),
                    _ => None,
                }
            },
            quote! { &["edge", "flag"] },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }
}
