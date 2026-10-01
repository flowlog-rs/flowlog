//! Inputs: the `Inputs` container of the loaders a driver feeds, each EDB's
//! `(handle, collection)` pair inside the dataflow scope, and the handles
//! the dataflow returns to its driver.

use flowlog_parser::Mutability;
use flowlog_parser::Relation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::input_field_ident;
use crate::input_handle_ident;
use crate::relation_marker_ident;
use crate::tuple_tokens;
use crate::ty::data::internal_tuple_tokens;
use crate::ty::diff::weight_tokens;

/// Returns the `Inputs` container: one loader field per input, the
/// constructor that wraps each input session in its loader, and the
/// lifecycle methods, each forwarding to the loaders it applies to.
///
/// Each loader carries its relation's declared weight, and every lifecycle
/// method past the inline facts drives the loaders of one mutability. A
/// static input is loaded once: the driver ends it with `close_static`
/// before the first advance. A mutable one advances each epoch and closes
/// with `close_mutable` when the driver quits.
pub(super) fn gen_inputs_container(edbs: &[&Relation], string_intern: bool) -> TokenStream {
    let mut fields = Vec::new();
    let mut parameters = Vec::new();
    let mut initializers = Vec::new();
    let mut inline = Vec::new();
    let mut advance = Vec::new();
    let mut flush = Vec::new();
    let mut close_static = Vec::new();
    let mut close_mutable = Vec::new();
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
            Mutability::Mutable => {
                advance.push(quote! { self.#field.advance_to(t); });
                flush.push(quote! { self.#field.flush(); });
                close_mutable.push(quote! { self.#field.close(); });
            }
        }
    }

    // Worker helpers need the whole set of differently typed loaders. A
    // container keeps their parameter lists independent of relation count,
    // at the cost of thin forwarding methods in generated code.
    quote! {
        pub(crate) struct Inputs {
            #(#fields,)*
        }

        #[allow(dead_code, unused_variables)]
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
            pub fn advance_mutable_to(&mut self, t: Ts) { #(#advance)* }
            pub fn flush_mutable(&mut self) { #(#flush)* }
            pub fn close_static(&mut self) { #(#close_static)* }
            pub fn close_mutable(&mut self) { #(#close_mutable)* }
        }
    }
}

impl Codegen {
    /// Returns each EDB's input declaration: a `(handle, collection)` pair
    /// at the weight of the relation's declared mutability, deduplicated so
    /// a repeated fact cannot raise a multiplicity. Brings the `Input` trait
    /// the declarations call into scope; empty when the program has no EDB.
    pub(crate) fn gen_inputs(&mut self, plan_graph: &mut Option<PlanGraph>) -> TokenStream {
        let edbs = self.program.edbs();
        if edbs.is_empty() {
            return quote! {};
        }

        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.update_input_block();
        });

        let str_intern = self.config.str_intern_enabled();
        let declarations = edbs.iter().map(|rel| {
            let handle = input_handle_ident(rel.name());
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
                let #coll = ::flowlog_runtime::operators::flowlog_dedup(#coll);
            }
        });
        quote! {
            use ::flowlog_runtime::differential_dataflow::input::Input;
            #(#declarations)*
        }
    }

    /// Returns the tuple of handles the dataflow closure returns and its
    /// caller binds, both spelled the same: every EDB's handle in name
    /// order, then `probe` for an incremental engine.
    pub(crate) fn gen_handles(&self) -> TokenStream {
        let handles = self
            .program
            .edb_names()
            .into_iter()
            .map(|name| input_handle_ident(&name))
            .chain(
                self.program
                    .is_incremental()
                    .then(|| format_ident!("probe")),
            );
        tuple_tokens(handles.map(|h| quote! { #h }))
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    /// Renders the `Inputs` container of `source`'s program.
    fn generate(source: &str, string_intern: bool) -> String {
        let tokens = gen_inputs_container(&program(source).edbs(), string_intern);
        syn::parse2::<syn::File>(tokens.clone()).expect("valid Rust syntax");
        tokens.to_string()
    }

    /// The `Input` trait comes into scope with the first declaration, and
    /// each collection carries its relation's declared weight.
    // Cases: mutability, weight.
    #[rstest]
    #[case::static_input("", quote! { ::flowlog_runtime::diff::Static })]
    #[case::mutable_input(" mutable", quote! { ::flowlog_runtime::diff::Mutable })]
    fn an_input_declares_a_deduplicated_collection_at_its_weight(
        #[case] mutability: &str,
        #[case] weight: TokenStream,
    ) {
        let mut codegen = codegen(&format!(".decl A(x: int32){mutability}\n.input A\n"));
        let expected = quote! {
            use ::flowlog_runtime::differential_dataflow::input::Input;
            let (ha, rel_0_a) = scope.new_collection::<(i32,), #weight>();
            let rel_0_a = ::flowlog_runtime::operators::flowlog_dedup(rel_0_a);
        };
        assert_eq!(
            codegen.gen_inputs(&mut None).to_string(),
            expected.to_string()
        );
    }

    #[test]
    fn no_input_declares_nothing() {
        assert!(codegen("").gen_inputs(&mut None).is_empty());
    }

    // Cases: program, handles.
    #[rstest]
    #[case::no_input("", quote! { () })]
    #[case::one_input(".decl A(x: int32)\n.input A", quote! { (ha,) })]
    #[case::two_inputs(".decl B(x: int32)\n.input B\n.decl A(x: int32)\n.input A", quote! { (ha, hb) })]
    #[case::incremental(".decl A(x: int32) mutable\n.input A", quote! { (ha, probe) })]
    fn the_handles_are_every_input_by_name_then_the_probe(
        #[case] source: &str,
        #[case] expected: TokenStream,
    ) {
        let handles = codegen(source).gen_handles();
        assert_eq!(handles.to_string(), expected.to_string());
    }

    /// A loader and its session carry the relation's declared weight.
    #[rstest]
    #[case::undeclared("", quote! { ::flowlog_runtime::diff::Static })]
    #[case::static_input(" static", quote! { ::flowlog_runtime::diff::Static })]
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

    /// Inline facts reach every input; the epoch methods and `close_mutable`
    /// reach only the mutable ones, and `close_static` only the static ones.
    #[test]
    fn lifecycle_methods_forward_to_the_inputs_of_their_mutability() {
        let generated = generate(
            r#"
            .decl Edge(id: int32) mutable
            .input Edge
            .decl Node(name: string)
            .input Node
            .decl Out(id: int32)
            Out(x) :- Edge(x), Node("a").
            .output Out
            "#,
            false,
        );
        for expected in [
            quote! {
                pub fn apply_inline_all(&mut self) {
                    self.in_edge.inline_facts(::flowlog_runtime::diff::Unit::one());
                    self.in_node.inline_facts(::flowlog_runtime::diff::Unit::one());
                }
            },
            quote! {
                pub fn advance_mutable_to(&mut self, t: Ts) {
                    self.in_edge.advance_to(t);
                }
            },
            quote! {
                pub fn flush_mutable(&mut self) {
                    self.in_edge.flush();
                }
            },
            quote! {
                pub fn close_static(&mut self) {
                    self.in_node.close();
                }
            },
            quote! {
                pub fn close_mutable(&mut self) {
                    self.in_edge.close();
                }
            },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    #[test]
    fn no_inputs_generate_an_empty_container_with_no_op_lifecycle_methods() {
        let generated = generate("", false);
        for expected in [
            quote! { pub(crate) struct Inputs {} },
            quote! { Ok(Self {}) },
            quote! { pub fn apply_inline_all(&mut self) {} },
            quote! { pub fn advance_mutable_to(&mut self, t: Ts) {} },
            quote! { pub fn flush_mutable(&mut self) {} },
            quote! { pub fn close_static(&mut self) {} },
            quote! { pub fn close_mutable(&mut self) {} },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }
}
