//! Relation declarations: [`gen_relations`] returns each relation's runtime
//! `Relation` implementation and the worker-local `Inputs` container that
//! owns the inputs' typed loaders and forwards lifecycle calls to them.
//! Source loading and command dispatch live elsewhere.

use flowlog_parser::DataType;
use flowlog_parser::InputSource;
use flowlog_parser::Mutability;
use flowlog_parser::OrderKey;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::CodegenError;
use crate::const_to_token;
use crate::data_type_tokens;
use crate::input_field_ident;
use crate::input_handle_ident;
use crate::relation_marker_ident;
use crate::tuple_tokens;
use crate::ty::diff::weight_tokens;

/// Returns a `Relation` declaration for every input and output, then the
/// `Inputs` container of the inputs' loaders.
///
/// Each loader carries its relation's declared weight, and every lifecycle
/// method past the inline facts drives the loaders of one mutability. A
/// static input is loaded once: the driver ends it with `close_static`
/// before the first advance. A mutable one advances each epoch and closes
/// with `close_mutable` when the driver quits.
///
/// The generated code expects its parent module to supply `Ts` and the
/// tuple representation imports; interned facts use the runtime's string
/// pool.
pub fn gen_relations(program: &Program, string_intern: bool) -> Result<TokenStream, CodegenError> {
    let edbs = program.edbs();
    let outputs = program.idbs();
    let declarations = edbs
        .iter()
        .copied()
        .chain(outputs.into_iter().filter(|output| {
            !edbs
                .iter()
                .any(|input| input.fingerprint() == output.fingerprint())
        }))
        .map(|relation| gen_declaration(program, relation, string_intern))
        .collect::<Result<Vec<_>, _>>()?;
    let inputs = gen_inputs(&edbs, string_intern);
    Ok(quote! {
        use super::*;
        #(#declarations)*
        #inputs
    })
}

/// Returns a relation's marker type `Rel<name>` and its `Relation`
/// implementation: name, arity, tuple type, the text settings its input and
/// output declare, its inline facts, and its output ordering.
fn gen_declaration(
    program: &Program,
    relation: &Relation,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let marker = relation_marker_ident(relation.name());
    let name = relation.raw_name();
    let arity = relation.arity();
    let tuple = data_type_tokens(&relation.data_type(), string_intern);
    let input_delimiter = relation
        .input()
        .and_then(|source| source.delim())
        .map(|delimiter| {
            quote! { const INPUT_DELIMITER: u8 = #delimiter; }
        });
    let input_has_header = relation
        .input()
        .is_some_and(InputSource::has_header)
        .then(|| quote! { const INPUT_HAS_HEADER: bool = true; });
    let output_delimiter = relation
        .output_sink()
        .and_then(|sink| sink.delim())
        .map(|delimiter| {
            quote! { const OUTPUT_DELIMITER: u8 = #delimiter; }
        });
    let facts = match program.facts().get(relation.name()) {
        Some(rows) if !rows.is_empty() => {
            let tuples = rows
                .iter()
                .map(|row| {
                    Ok(tuple_tokens(
                        row.columns
                            .iter()
                            .map(|value| const_to_token(value, string_intern))
                            .collect::<Result<Vec<_>, _>>()?,
                    ))
                })
                .collect::<Result<Vec<_>, CodegenError>>()?;
            quote! {
                fn facts() -> impl IntoIterator<Item = Self::Tuple> {
                    [#(#tuples),*]
                }
            }
        }
        Some(_) | None => quote! {},
    };
    let ordering = relation.output_sink().and_then(|sink| {
        sink.order_by().map(|keys| {
            let compare = gen_compare(keys, string_intern);
            let limit = match sink.limit() {
                Some(limit) => quote! { Some(#limit) },
                None => quote! { None },
            };
            quote! {
                const ORDERED: bool = true;
                const LIMIT: Option<usize> = #limit;
                #compare
            }
        })
    });
    Ok(quote! {
        #[allow(non_camel_case_types)]
        pub(crate) struct #marker;

        impl ::flowlog_runtime::io::Relation for #marker {
            const NAME: &'static str = #name;
            const ARITY: usize = #arity;
            type Tuple = #tuple;
            #input_delimiter
            #input_has_header
            #output_delimiter
            #facts
            #ordering
        }
    })
}

/// Returns the `Inputs` container: one loader field per input, the
/// constructor that wraps each input session in its loader, and the
/// lifecycle methods, each forwarding to the loaders it applies to.
fn gen_inputs(edbs: &[&Relation], string_intern: bool) -> TokenStream {
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
        let tuple = data_type_tokens(&relation.data_type(), string_intern);
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

/// Returns the `compare` function that orders tuples by each key of `spec`
/// in turn, ascending or descending; an interned string compares by its
/// text.
fn gen_compare(spec: &[OrderKey], string_intern: bool) -> TokenStream {
    let comparisons = spec.iter().map(|(col_idx, data_type, ascending)| {
        let index = Index::from(*col_idx);
        let a = quote! { a.#index };
        let b = quote! { b.#index };
        let (a_expr, b_expr) = if string_intern {
            (
                resolve_string_leaves(&a, data_type),
                resolve_string_leaves(&b, data_type),
            )
        } else {
            (a, b)
        };
        let cmp_expr = if *ascending {
            quote! { #a_expr.cmp(&#b_expr) }
        } else {
            quote! { #b_expr.cmp(&#a_expr) }
        };
        quote! {
            let cmp = #cmp_expr;
            if cmp != std::cmp::Ordering::Equal { return cmp; }
        }
    });
    quote! {
        fn compare(a: &Self::Tuple, b: &Self::Tuple) -> std::cmp::Ordering {
            #(#comparisons)*
            std::cmp::Ordering::Equal
        }
    }
}

/// Returns `access` with every interned string leaf resolved to text
/// through the runtime's output snapshot, keeping any tuple nesting; other
/// leaves pass through unchanged.
fn resolve_string_leaves(access: &TokenStream, data_type: &DataType) -> TokenStream {
    match data_type {
        DataType::String => quote! { ::flowlog_runtime::intern::resolve_out(#access) },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Index::from(j);
                resolve_string_leaves(&quote! { (#access).#jdx }, fdt)
            });
            quote! { ( #(#elems,)* ) }
        }
        DataType::IntLit
        | DataType::FloatLit
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Bool => access.clone(),
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
    use rstest::rstest;

    use super::*;

    fn generate(source: &str, string_intern: bool) -> String {
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
        let tokens = gen_relations(&program, string_intern).expect("generate relations");
        syn::parse2::<syn::File>(tokens.clone()).expect("valid Rust syntax");
        tokens.to_string()
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
        assert!(!generated.contains("fn facts"));
        assert!(!generated.contains("INPUT_HAS_HEADER"), "{generated}");
    }

    #[test]
    fn relation_text_settings_are_declared_independently() {
        let generated = generate(
            ".decl Edge(id: int32)\n\
             .input Edge(delimiter=\",\", header=\"true\")\n\
             .output Edge(delimiter=\"|\")\n",
            false,
        );
        for expected in [
            quote! { const INPUT_DELIMITER: u8 = 44u8; },
            quote! { const INPUT_HAS_HEADER: bool = true; },
            quote! { const OUTPUT_DELIMITER: u8 = 124u8; },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    #[test]
    fn inline_counted_relations_keep_default_text_settings() {
        let generated = generate(
            ".decl Counted(id: int32)\nCounted(1).\n.printsize Counted\n",
            false,
        );
        assert!(!generated.contains("INPUT_DELIMITER"), "{generated}");
        assert!(!generated.contains("INPUT_HAS_HEADER"), "{generated}");
        assert!(!generated.contains("OUTPUT_DELIMITER"), "{generated}");
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

    #[test]
    fn inline_facts_are_declared_in_source_order() {
        let generated = generate(".decl R(x: int32)\nR(2).\nR(1).\n.output R\n", false);
        let expected = quote! {
            fn facts() -> impl IntoIterator<Item = Self::Tuple> {
                [(2,), (1,)]
            }
        };
        assert!(generated.contains(&expected.to_string()), "{generated}");
    }

    #[test]
    fn a_nullary_fact_declares_a_presence() {
        let generated = generate(".decl Flag()\nFlag().\n.output Flag\n", false);
        let expected = quote! {
            fn facts() -> impl IntoIterator<Item = Self::Tuple> {
                [()]
            }
        };
        assert!(generated.contains(&expected.to_string()), "{generated}");
    }

    /// Keys compare in order, a `DESC` key with its sides swapped, and an
    /// interned string by its text.
    #[test]
    fn an_ordered_output_compares_its_keys_in_turn() {
        let generated = generate(
            ".decl R(id: int32, name: string)\n\
             R(1, \"a\").\n\
             .output R(order_by=\"name DESC, id\", limit=\"3\")\n",
            true,
        );
        let expected = quote! {
            const ORDERED: bool = true;
            const LIMIT: Option<usize> = Some(3usize);
            fn compare(a: &Self::Tuple, b: &Self::Tuple) -> std::cmp::Ordering {
                let cmp = ::flowlog_runtime::intern::resolve_out(b.1)
                    .cmp(&::flowlog_runtime::intern::resolve_out(a.1));
                if cmp != std::cmp::Ordering::Equal { return cmp; }
                let cmp = a.0.cmp(&b.0);
                if cmp != std::cmp::Ordering::Equal { return cmp; }
                std::cmp::Ordering::Equal
            }
        };
        assert!(generated.contains(&expected.to_string()), "{generated}");
    }
}
