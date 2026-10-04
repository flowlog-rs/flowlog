//! Inputs: each EDB's `(handle, collection)` pair inside the dataflow scope
//! and the handles the dataflow returns to its driver, as an [`Input`]; and
//! the `Inputs` container of the loaders the driver feeds those handles
//! through.

use flowlog_parser::Mutability;
use flowlog_parser::Relation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::input_field_ident;
use crate::input_handle_ident;
use crate::relation_marker_ident;
use crate::ty::data::internal_tuple_tokens;
use crate::ty::diff::weight_tokens;

/// Returns the `Inputs` container: one loader field per input, the
/// constructor that wraps each input session in its loader, and the
/// lifecycle methods, each forwarding to the loaders it applies to.
///
/// Each loader carries its relation's declared weight, and every lifecycle
/// method past the inline facts drives the loaders of one kind. A static
/// input is loaded once: the driver ends it with `close_static` before the
/// first advance. A dynamic input, append or mutable, advances each epoch
/// and closes with `close_dynamic` when the driver quits; the epoch
/// methods exist only when some input is dynamic, since only a transaction
/// driver calls them.
pub(super) fn gen_inputs_container(edbs: &[&Relation], string_intern: bool) -> TokenStream {
    let mut fields = Vec::new();
    let mut parameters = Vec::new();
    let mut initializers = Vec::new();
    let mut inline = Vec::new();
    let mut advance = Vec::new();
    let mut flush = Vec::new();
    let mut close_static = Vec::new();
    let mut close_dynamic = Vec::new();
    for relation in edbs {
        let marker = relation_marker_ident(relation.name());
        let field = input_field_ident(relation.name());
        let handle = input_handle_ident(relation.name());
        let tuple = internal_tuple_tokens(&relation.data_type(), string_intern);
        let mutability = relation.input_mutability();
        let weight = weight_tokens(mutability);
        fields.push(quote! {
            pub #field: ::flowlog_runtime::io::input::Loader<#marker, Ts, #weight>
        });
        parameters.push(quote! {
            #handle: ::flowlog_runtime::differential_dataflow::input::InputSession<Ts, #tuple, #weight>
        });
        initializers.push(quote! {
            #field: ::flowlog_runtime::io::input::Loader::new(#handle, peers, index, uses_ord)?
        });
        inline.push(quote! { self.#field.inline_facts(::flowlog_runtime::diff::Unit::one()); });
        match mutability {
            Mutability::Static => close_static.push(quote! { self.#field.close(); }),
            Mutability::Append | Mutability::Mutable => {
                advance.push(quote! { self.#field.advance_to(t); });
                flush.push(quote! { self.#field.flush(); });
                close_dynamic.push(quote! { self.#field.close(); });
            }
        }
    }

    // The epoch methods belong to the transaction driver, which exists only
    // when some input is dynamic; a single-run driver never calls them.
    let epoch_methods = (!advance.is_empty()).then(|| {
        quote! {
            pub fn advance_dynamic_to(&mut self, t: Ts) { #(#advance)* }
            pub fn flush_dynamic(&mut self) { #(#flush)* }
            pub fn close_dynamic(&mut self) { #(#close_dynamic)* }
        }
    });

    // Worker helpers need the whole set of differently typed loaders. A
    // container keeps their parameter lists independent of relation count,
    // at the cost of thin forwarding methods in generated code.
    quote! {
        pub(crate) struct Inputs {
            #(#fields,)*
        }

        impl Inputs {
            pub fn new(
                #(#parameters,)*
                peers: usize,
                index: usize,
                uses_ord: bool,
            ) -> Result<Self, ::flowlog_runtime::RuntimeError> {
                Ok(Self { #(#initializers,)* })
            }

            pub fn apply_inline_all(&mut self) { #(#inline)* }
            pub fn close_static(&mut self) { #(#close_static)* }
            #epoch_methods
        }
    }
}

/// The inputs' fragments, both inside the skeleton's `dataflow`.
#[derive(Debug, Default)]
pub(crate) struct Input {
    /// Each EDB's `(handle, collection)` pair at the weight of its declared
    /// mutability, read as a set so a repeated fact cannot raise a
    /// multiplicity; preceded by the `Input` trait import they call. Empty
    /// when the program has no EDB.
    pub declarations: TokenStream,
    /// Each EDB's handle, in declaration order: the dataflow returns them
    /// to its driver, which feeds them through the `Inputs` container.
    pub handles: Vec<Ident>,
}

impl Codegen {
    /// Returns the inputs' fragments.
    pub(crate) fn gen_input(&mut self, plan_graph: &mut Option<PlanGraph>) -> Input {
        let edbs = self.program.edbs();
        if edbs.is_empty() {
            return Input::default();
        }

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.update_input_block();
        });

        let str_intern = self.config.str_intern_enabled();
        let mut handles = Vec::with_capacity(edbs.len());
        let declarations = edbs.iter().map(|rel| {
            let handle = input_handle_ident(rel.name());
            handles.push(handle.clone());
            // The collection binding comes from the global ident map,
            // never re-derived from the name, so it always matches the
            // ident every downstream flow resolves via fingerprint.
            let coll = self.find_global_ident(rel.fingerprint());

            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.input_edb_operator(rel.raw_name().to_string(), coll.to_string());
                plan_graph.input_dedup_operator(
                    rel.raw_name().to_string(),
                    coll.to_string(),
                    coll.to_string(),
                    rel.input_mutability(),
                );
            });

            let ty = internal_tuple_tokens(&rel.data_type(), str_intern);
            let weight = weight_tokens(rel.input_mutability());

            quote! {
                let (#handle, #coll) = scope.new_collection::<#ty, #weight>();
                let #coll = ::flowlog_runtime::operators::flowlog_input_dedup(#coll);
            }
        });
        let declarations = quote! {
            use ::flowlog_runtime::differential_dataflow::input::Input;
            #(#declarations)*
        };
        Input {
            declarations,
            handles,
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::rendered;

    /// Renders the `Inputs` container of `source`'s program.
    fn generate(source: &str, string_intern: bool) -> String {
        rendered(gen_inputs_container(&program(source).edbs(), string_intern))
    }

    /// The `Input` trait comes into scope with the first declaration, each
    /// collection carries its relation's declared weight, and each is read
    /// as a set.
    // Cases: mutability, weight.
    #[rstest]
    #[case::static_input("", quote! { ::flowlog_runtime::diff::Static })]
    #[case::append_input(" append", quote! { ::flowlog_runtime::diff::Append })]
    #[case::mutable_input(" mutable", quote! { ::flowlog_runtime::diff::Mutable })]
    fn an_input_declares_a_set_collection_at_its_weight(
        #[case] mutability: &str,
        #[case] weight: TokenStream,
    ) {
        let mut codegen = codegen(&format!(
            ".decl A(x: int32){mutability}\n.input A\n.output A\n"
        ));
        let expected = quote! {
            use ::flowlog_runtime::differential_dataflow::input::Input;
            let (ha, rel_0_a) = scope.new_collection::<(i32,), #weight>();
            let rel_0_a = ::flowlog_runtime::operators::flowlog_input_dedup(rel_0_a);
        };
        assert_eq!(
            codegen.gen_input(&mut None).declarations.to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn no_input_declares_nothing() {
        let input = codegen("").gen_input(&mut None);
        assert!(input.declarations.is_empty());
        assert!(input.handles.is_empty());
    }

    // Cases: program, handles.
    #[rstest]
    #[case::one_input(".decl A(x: int32)\n.input A\n.output A", &["ha"])]
    #[case::two_inputs(".decl B(x: int32)\n.input B\n.decl A(x: int32)\n.input A\n.output A\n.output B", &["hb", "ha"])]
    fn the_handles_are_every_input_in_declaration_order(
        #[case] source: &str,
        #[case] expected: &[&str],
    ) {
        let handles = codegen(source).gen_input(&mut None).handles;
        let handles: Vec<String> = handles.iter().map(ToString::to_string).collect();
        assert_eq!(handles, expected);
    }

    /// A loader and its session carry the relation's declared weight.
    #[rstest]
    #[case::undeclared("", quote! { ::flowlog_runtime::diff::Static })]
    #[case::static_input(" static", quote! { ::flowlog_runtime::diff::Static })]
    #[case::append_input(" append", quote! { ::flowlog_runtime::diff::Append })]
    #[case::mutable_input(" mutable", quote! { ::flowlog_runtime::diff::Mutable })]
    fn input_fields_bind_runtime_loaders_to_their_sessions(
        #[case] mutability: &str,
        #[case] weight: TokenStream,
    ) {
        let generated = generate(
            &format!(".decl Edge(id: int32){mutability}\n.input Edge\n.output Edge\n"),
            false,
        );
        for expected in [
            quote! { pub in_edge: ::flowlog_runtime::io::input::Loader<Reledge, Ts, #weight> },
            quote! { hedge: ::flowlog_runtime::differential_dataflow::input::InputSession<Ts, (i32,), #weight> },
            quote! { in_edge: ::flowlog_runtime::io::input::Loader::new(hedge, peers, index, uses_ord)? },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    /// Inline facts reach every input; the epoch methods and `close_dynamic`
    /// reach the append and mutable ones, and `close_static` only the
    /// static ones.
    #[test]
    fn lifecycle_methods_forward_to_the_inputs_of_their_mutability() {
        let generated = generate(
            r#"
            .decl Edge(id: int32) mutable
            .input Edge
            .decl Seen(id: int32) append
            .input Seen
            .decl Node(name: string)
            .input Node
            .decl Out(id: int32)
            Out(x) :- Edge(x), Seen(x), Node("a").
            .output Out
            "#,
            false,
        );
        for expected in [
            quote! {
                pub fn apply_inline_all(&mut self) {
                    self.in_edge.inline_facts(::flowlog_runtime::diff::Unit::one());
                    self.in_seen.inline_facts(::flowlog_runtime::diff::Unit::one());
                    self.in_node.inline_facts(::flowlog_runtime::diff::Unit::one());
                }
            },
            quote! {
                pub fn advance_dynamic_to(&mut self, t: Ts) {
                    self.in_edge.advance_to(t);
                    self.in_seen.advance_to(t);
                }
            },
            quote! {
                pub fn flush_dynamic(&mut self) {
                    self.in_edge.flush();
                    self.in_seen.flush();
                }
            },
            quote! {
                pub fn close_static(&mut self) {
                    self.in_node.close();
                }
            },
            quote! {
                pub fn close_dynamic(&mut self) {
                    self.in_edge.close();
                    self.in_seen.close();
                }
            },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    #[test]
    fn no_inputs_generate_an_empty_container_with_no_op_lifecycle_methods() {
        let generated = generate(".decl Z(x: int32)\n.output Z", false);
        for expected in [
            quote! { pub(crate) struct Inputs {} },
            quote! { Ok(Self {}) },
            quote! { pub fn apply_inline_all(&mut self) {} },
            quote! { pub fn close_static(&mut self) {} },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    /// A single-run driver never advances or closes a dynamic input, so a
    /// program without one gets no epoch methods to leave unused.
    #[test]
    fn a_program_without_a_dynamic_input_has_no_epoch_methods() {
        let generated = generate(
            ".decl Node(name: string)\n.input Node\n.decl Out(name: string)\nOut(x) :- Node(x).\n.output Out\n",
            false,
        );
        assert!(generated.contains("fn close_static"), "{generated}");
        for absent in ["advance_dynamic_to", "flush_dynamic", "close_dynamic"] {
            assert!(!generated.contains(absent), "{generated}");
        }
    }
}
