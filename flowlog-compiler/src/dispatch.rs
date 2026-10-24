//! Incremental command dispatch.
//!
//! [`gen_dispatch`] adds name-based routing and prompt relation names to
//! the generated `Inputs` container. `check` says whether a relation
//! accepts an update at all, for the prompt to refuse one before it is
//! queued: a static relation refuses every update, since its input closes
//! after the initial load, and an append relation refuses a deletion.
//! `apply` takes one accepted update and routes it by relation and verb;
//! a relation's arm loads the rows at the weight its mutability gives the
//! verb, and runtime loaders handle decoding and partitioning. Both
//! matches come from one classification of each relation, so they cannot
//! disagree.

use flowlog_codegen::input_field_ident;
use flowlog_parser::Mutability;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::io::input;

/// Emits name-based command dispatch on the generated `Inputs` container.
pub(crate) fn gen_dispatch(program: &Program) -> TokenStream {
    let mut checks = Vec::new();
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
            Mutability::Static => {
                let refused = quote! {
                    Some(Err(::flowlog_runtime::RuntimeError::StaticRelation {
                        relation: #raw_name
                    }))
                };
                checks.push(quote! { (#name, _) => #refused, });
                arms.push(quote! { (#name, _) => #refused, });
            }
            Mutability::Append => {
                let refused = quote! {
                    Some(Err(::flowlog_runtime::RuntimeError::AppendRelation {
                        relation: #raw_name
                    }))
                };
                let insert =
                    gen_load_rows(relation, &field, quote! { ::flowlog_runtime::diff::Append });
                checks.push(quote! {
                    (#name, ::flowlog_runtime::txn::TxnOp::Insert { .. }) => Some(Ok(())),
                    (#name, ::flowlog_runtime::txn::TxnOp::Delete { .. }) => #refused,
                });
                arms.push(quote! {
                    (#name, ::flowlog_runtime::txn::TxnOp::Insert { rows, .. }) => #insert,
                    (#name, ::flowlog_runtime::txn::TxnOp::Delete { .. }) => #refused,
                });
            }
            Mutability::Mutable => {
                let insert = gen_load_rows(relation, &field, quote! { 1 });
                let delete = gen_load_rows(relation, &field, quote! { -1 });
                checks.push(quote! { (#name, _) => Some(Ok(())), });
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

            pub fn check(
                op: &::flowlog_runtime::txn::TxnOp,
            ) -> Option<Result<(), ::flowlog_runtime::RuntimeError>> {
                match (op.rel().to_ascii_lowercase().as_str(), op) {
                    #(#checks)*
                    _ => None,
                }
            }

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
/// source's file loader.
fn gen_load_rows(relation: &Relation, field: &Ident, diff: TokenStream) -> TokenStream {
    let file = input::gen_load(
        relation,
        quote! { self.#field },
        quote! { path },
        diff.clone(),
    );
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

    /// The prompt's check and the workers' apply classify an update alike:
    /// a static relation refuses every update, an append relation a
    /// deletion, a mutable relation nothing.
    #[test]
    fn check_refuses_what_its_relation_does_not_accept() {
        let generated = generate(
            r#"
            .decl Fixed(id: int32)
            .input Fixed(delimiter=",")
            .decl Grow(id: int32) append
            .input Grow(IO="command")
            .decl Edge(id: int32) mutable
            .input Edge(IO="command")
            .decl Out(id: int32)
            Out(x) :- Fixed(x), Grow(x), Edge(x).
            .output Out
            "#,
        );
        for expected in [
            quote! {
                ("fixed", _) => Some(Err(::flowlog_runtime::RuntimeError::StaticRelation {
                    relation: "Fixed"
                })),
            },
            quote! {
                ("grow", ::flowlog_runtime::txn::TxnOp::Insert { .. }) => Some(Ok(())),
                ("grow", ::flowlog_runtime::txn::TxnOp::Delete { .. }) =>
                    Some(Err(::flowlog_runtime::RuntimeError::AppendRelation { relation: "Grow" })),
            },
            quote! { ("edge", _) => Some(Ok(())), },
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
                ("edge", ::flowlog_runtime::txn::TxnOp::Delete { .. }) =>
                    Some(Err(::flowlog_runtime::RuntimeError::AppendRelation { relation: "Edge" })),
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
