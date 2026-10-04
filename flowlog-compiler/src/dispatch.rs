//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container: `apply` takes one transaction
//! update and routes it by relation and verb. A relation's arm loads the
//! rows at the weight its mutability gives the verb; runtime loaders
//! handle decoding and partitioning. A static relation refuses every
//! update: its input closes after the initial load. An append relation
//! refuses a deletion.

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
    let mut arms = Vec::new();
    let mut relation_names = Vec::new();
    for relation in program.edbs() {
        let name = relation.name();
        relation_names.push(name);
        let raw_name = relation.raw_name();
        let field = input_field_ident(name);
        // Each arm binds the rows only when it loads them, so a refusing
        // arm leaves nothing unread.
        match relation.input_mutability() {
            Mutability::Static => arms.push(quote! {
                (#name, _) => Some(Err(::flowlog_runtime::RuntimeError::StaticRelation {
                    relation: #raw_name
                })),
            }),
            Mutability::Append => {
                let insert =
                    gen_load_rows(relation, &field, quote! { ::flowlog_runtime::diff::Append });
                arms.push(quote! {
                    (#name, ::flowlog_runtime::txn::TxnOp::Insert { rows, .. }) => #insert,
                    (#name, ::flowlog_runtime::txn::TxnOp::Delete { .. }) => {
                        Some(Err(::flowlog_runtime::RuntimeError::AppendRelation {
                            relation: #raw_name
                        }))
                    }
                });
            }
            Mutability::Mutable => {
                let insert = gen_load_rows(relation, &field, quote! { 1 });
                let delete = gen_load_rows(relation, &field, quote! { -1 });
                arms.push(quote! {
                    (#name, ::flowlog_runtime::txn::TxnOp::Insert { rows, .. }) => #insert,
                    (#name, ::flowlog_runtime::txn::TxnOp::Delete { rows, .. }) => #delete,
                });
            }
        }
    }

    quote! {
        impl Inputs {
            pub fn names() -> &'static [&'static str] { &[#(#relation_names),*] }

            pub fn apply(
                &mut self,
                op: &::flowlog_runtime::txn::TxnOp,
                ordinal: usize,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match (op.rel().to_ascii_lowercase().as_str(), op) {
                    #(#arms)*
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

    /// An insert loads a mutable relation's rows at `1` and a delete at
    /// `-1`: a tuple through the put loader, a file through the file
    /// loader.
    #[test]
    fn updates_load_a_mutable_relations_rows_at_the_verbs_weight() {
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
                ("edge", ::flowlog_runtime::txn::TxnOp::Insert { rows, .. }) => Some(match rows {
                    ::flowlog_runtime::txn::Rows::Tuple(text) => self.in_edge.load_put(text, ordinal, 1),
                    ::flowlog_runtime::txn::Rows::File(path) => self.in_edge.load_file(path, 1),
                }),
            },
            quote! {
                ("flag", ::flowlog_runtime::txn::TxnOp::Delete { rows, .. }) => Some(match rows {
                    ::flowlog_runtime::txn::Rows::Tuple(text) => self.in_flag.load_put(text, ordinal, -1),
                    ::flowlog_runtime::txn::Rows::File(path) => self.in_flag.load_file(path, -1),
                }),
            },
            quote! { &["edge", "flag"] },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    /// An insert into an append relation loads its rows with presence, a
    /// tuple and a file alike; a deletion is refused without reading them.
    #[test]
    fn an_append_relation_inserts_with_presence_and_refuses_a_deletion() {
        let generated = generate(
            r#"
            .decl Edge(id: int32) append
            .input Edge(delimiter=",")
            .decl Out(id: int32)
            Out(x) :- Edge(x).
            .output Out
            "#,
        );
        for expected in [
            quote! {
                ("edge", ::flowlog_runtime::txn::TxnOp::Insert { rows, .. }) => Some(match rows {
                    ::flowlog_runtime::txn::Rows::Tuple(text) =>
                        self.in_edge.load_put(text, ordinal, ::flowlog_runtime::diff::Append),
                    ::flowlog_runtime::txn::Rows::File(path) =>
                        self.in_edge.load_file(path, ::flowlog_runtime::diff::Append),
                }),
            },
            quote! {
                ("edge", ::flowlog_runtime::txn::TxnOp::Delete { .. }) => {
                    Some(Err(::flowlog_runtime::RuntimeError::AppendRelation { relation: "Edge" }))
                }
            },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    /// A static relation stays in the prompt's names, but every update on
    /// it is refused rather than routed to its closed loader.
    #[test]
    fn updates_on_a_static_relation_are_refused() {
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
            ("edge", _) => Some(Err(::flowlog_runtime::RuntimeError::StaticRelation { relation: "Edge" })),
        };
        assert!(generated.contains(&refused.to_string()), "{generated}");
        assert!(!generated.contains("in_edge"), "{generated}");
        assert!(
            generated.contains(&quote! { &["edge"] }.to_string()),
            "{generated}"
        );
    }
}
