//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container: `insert` and `delete`, each taking the
//! rows a command names. A relation's arm loads them at the weight its
//! mutability gives the command; runtime loaders handle decoding and
//! partitioning. A static relation refuses every command: its input closes
//! after the initial load.

use flowlog_codegen::input_field_ident;
use flowlog_parser::InputSource;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input;

/// Emits name-based command dispatch on the generated `Inputs` container.
pub(crate) fn gen_dispatch(program: &Program) -> TokenStream {
    let mut insert_arms = Vec::new();
    let mut delete_arms = Vec::new();
    let mut relation_names = Vec::new();
    for relation in program.edbs() {
        let name = relation.name();
        relation_names.push(name);
        let (insert, delete) = match relation.input_mutability() {
            Mutability::Static => {
                let raw_name = relation.raw_name();
                let refuse = quote! {
                    Some(Err(::flowlog_runtime::RuntimeError::StaticRelation { relation: #raw_name }))
                };
                (refuse.clone(), refuse)
            }
            Mutability::Mutable => {
                let field = input_field_ident(name);
                (
                    gen_load_rows(relation, &field, quote! { 1 }),
                    gen_load_rows(relation, &field, quote! { -1 }),
                )
            }
        };
        insert_arms.push(quote! { #name => #insert, });
        delete_arms.push(quote! { #name => #delete, });
    }

    quote! {
        impl Inputs {
            pub fn names() -> &'static [&'static str] { &[#(#relation_names),*] }

            pub fn insert(
                &mut self,
                name: &str,
                rows: &::flowlog_runtime::txn::Rows,
                ordinal: usize,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match name.to_ascii_lowercase().as_str() {
                    #(#insert_arms)*
                    _ => None,
                }
            }

            pub fn delete(
                &mut self,
                name: &str,
                rows: &::flowlog_runtime::txn::Rows,
                ordinal: usize,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match name.to_ascii_lowercase().as_str() {
                    #(#delete_arms)*
                    _ => None,
                }
            }
        }
    }
}

/// Returns the load of `rows` into `relation` through its loader `field`
/// at weight `diff`: a tuple through the put loader, a file through the
/// source's file loader. A SQLite source that fails ends the process: its
/// rows cannot be partially applied.
fn gen_load_rows(relation: &Relation, field: &Ident, diff: TokenStream) -> TokenStream {
    let file = input::gen_load(
        relation,
        quote! { self.#field },
        quote! { path },
        diff.clone(),
    );
    let file = match relation.input() {
        Some(InputSource::Sqlite { .. }) => {
            let relation_name = relation.raw_name();
            quote! {{
                let result = #file;
                if let Err(error) = &result {
                    eprintln!("[relation][{}] {} in {}", #relation_name, error, path.display());
                    std::process::exit(1);
                }
                result
            }}
        }
        Some(InputSource::File { .. } | InputSource::Command { .. }) | None => file,
    };
    quote! {
        Some(match rows {
            ::flowlog_runtime::txn::Rows::Tuple(text) => self.#field.load_put(text, ordinal, #diff),
            ::flowlog_runtime::txn::Rows::File(path) => #file,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;

    use super::*;

    fn generate(source: &str) -> String {
        gen_dispatch(&program(source)).to_string()
    }

    /// Each command loads a mutable relation's rows at the command's
    /// weight: a tuple through the put loader, a file through the file
    /// loader.
    #[test]
    fn commands_load_each_mutable_relations_rows_at_their_weight() {
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
                "edge" => Some(match rows {
                    ::flowlog_runtime::txn::Rows::Tuple(text) => self.in_edge.load_put(text, ordinal, 1),
                    ::flowlog_runtime::txn::Rows::File(path) => self.in_edge.load_file(path, 1),
                }),
            },
            quote! {
                "flag" => Some(match rows {
                    ::flowlog_runtime::txn::Rows::Tuple(text) => self.in_flag.load_put(text, ordinal, -1),
                    ::flowlog_runtime::txn::Rows::File(path) => self.in_flag.load_file(path, -1),
                }),
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
