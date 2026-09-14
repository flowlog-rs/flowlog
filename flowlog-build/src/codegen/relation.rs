//! Generated relation declarations and worker-local loader ownership.
//!
//! [`gen_relations`] emits runtime `Relation` implementations and an
//! `Inputs` container that groups typed loaders and forwards lifecycle
//! calls. Source loading and command dispatch are outside this module.

use flowlog_parser::DataType;
use flowlog_parser::InputSource;
use flowlog_parser::OrderKey;
use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Index;

use super::CodegenError;
use super::const_to_token;
use super::data_type_tokens;
use super::tuple_tokens;

/// Emits I/O relation declarations and a worker-local `Inputs` container.
///
/// The parent module supplies `Ts`, `Diff`, `SEMIRING_ONE`, and tuple
/// representation imports. Interned facts use the runtime's string pool.
pub fn gen_relations(program: &Program, string_intern: bool) -> Result<TokenStream, CodegenError> {
    let edbs = program.edbs();
    let mut declarations = Vec::new();
    let mut fields = Vec::new();
    let mut parameters = Vec::new();
    let mut initializers = Vec::new();
    let mut inline = Vec::new();
    let mut advance = Vec::new();
    let mut flush = Vec::new();
    let mut close = Vec::new();

    let outputs = program.idbs();
    let relations = edbs
        .iter()
        .copied()
        .chain(outputs.into_iter().filter(|output| {
            !edbs
                .iter()
                .any(|input| input.fingerprint() == output.fingerprint())
        }));
    for relation in relations {
        let marker = format_ident!("Rel{}", relation.name());
        let field = format_ident!("in_{}", relation.name());
        let handle = format_ident!("h{}", relation.name());
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
        let output_delimiter =
            relation
                .output_sink()
                .and_then(|sink| sink.delim())
                .map(|delimiter| {
                    quote! { const OUTPUT_DELIMITER: u8 = #delimiter; }
                });
        let facts = match program.facts().get(relation.name()) {
            Some(rows) if !rows.is_empty() => {
                let tuples = if arity == 0 {
                    vec![quote! { () }]
                } else {
                    rows.iter()
                        .map(|row| {
                            row.columns
                                .iter()
                                .map(|value| const_to_token(value, string_intern))
                                .collect::<Result<Vec<_>, _>>()
                                .map(tuple_tokens)
                        })
                        .collect::<Result<Vec<_>, _>>()?
                };
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
        declarations.push(quote! {
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
        });
        if !edbs
            .iter()
            .any(|input| input.fingerprint() == relation.fingerprint())
        {
            continue;
        }
        fields.push(quote! {
            pub #field: ::flowlog_runtime::io::input::Loader<#marker, Ts, Diff>
        });
        parameters.push(quote! {
            #handle: ::flowlog_runtime::differential_dataflow::input::InputSession<Ts, #tuple, Diff>
        });
        initializers.push(quote! {
            #field: ::flowlog_runtime::io::input::Loader::new(#handle, peers, index, uses_ord)?
        });
        inline.push(quote! { self.#field.inline_facts(SEMIRING_ONE); });
        advance.push(quote! { self.#field.advance_to(t); });
        flush.push(quote! { self.#field.flush(); });
        close.push(quote! { self.#field.close(); });
    }

    // Worker helpers need the whole set of differently typed loaders. A
    // container keeps their parameter lists independent of relation count,
    // at the cost of thin forwarding methods in generated code.
    Ok(quote! {
        use super::*;
        #(#declarations)*

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
            pub fn advance_to_all(&mut self, t: Ts) { #(#advance)* }
            pub fn flush_all(&mut self) { #(#flush)* }
            pub fn close_all(&mut self) { #(#close)* }
        }
    })
}

/// Emits comparisons of relation tuples, resolving interned string leaves.
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

/// Resolves interned string leaves through the runtime's output snapshot.
/// Tuple nesting is preserved; non-string leaves pass through unchanged.
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
        let tokens = gen_relations(&program, false).expect("generate inputs");
        syn::parse2::<syn::File>(tokens.clone()).expect("valid Rust syntax");
        tokens.to_string()
    }

    #[test]
    fn input_fields_bind_runtime_loaders_to_their_sessions() {
        let generated = generate(".decl Edge(id: int32)\n.input Edge\n.output Edge\n");
        for expected in [
            quote! { pub in_edge: ::flowlog_runtime::io::input::Loader<Reledge, Ts, Diff> },
            quote! { hedge: ::flowlog_runtime::differential_dataflow::input::InputSession<Ts, (i32,), Diff> },
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
        let generated = generate(".decl Counted(id: int32)\nCounted(1).\n.printsize Counted\n");
        assert!(!generated.contains("INPUT_DELIMITER"), "{generated}");
        assert!(!generated.contains("INPUT_HAS_HEADER"), "{generated}");
        assert!(!generated.contains("OUTPUT_DELIMITER"), "{generated}");
    }

    #[test]
    fn lifecycle_methods_forward_to_every_input() {
        let generated = generate(
            r#"
            .decl Edge(id: int32)
            .input Edge
            .decl Node(name: string)
            .input Node
            .decl Out(id: int32)
            Out(x) :- Edge(x), Node("a").
            .output Out
            "#,
        );
        for expected in [
            quote! {
                pub(crate) struct Inputs {
                    pub in_edge: ::flowlog_runtime::io::input::Loader<Reledge, Ts, Diff>,
                    pub in_node: ::flowlog_runtime::io::input::Loader<Relnode, Ts, Diff>,
                }
            },
            quote! {
                pub fn apply_inline_all(&mut self) {
                    self.in_edge.inline_facts(SEMIRING_ONE);
                    self.in_node.inline_facts(SEMIRING_ONE);
                }
            },
            quote! {
                pub fn advance_to_all(&mut self, t: Ts) {
                    self.in_edge.advance_to(t);
                    self.in_node.advance_to(t);
                }
            },
            quote! {
                pub fn flush_all(&mut self) {
                    self.in_edge.flush();
                    self.in_node.flush();
                }
            },
            quote! {
                pub fn close_all(&mut self) {
                    self.in_edge.close();
                    self.in_node.close();
                }
            },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    #[test]
    fn no_inputs_generate_an_empty_container_with_no_op_lifecycle_methods() {
        let generated = generate("");
        for expected in [
            quote! { pub(crate) struct Inputs {} },
            quote! { Ok(Self {}) },
            quote! { pub fn apply_inline_all(&mut self) {} },
            quote! { pub fn advance_to_all(&mut self, t: Ts) {} },
            quote! { pub fn flush_all(&mut self) {} },
            quote! { pub fn close_all(&mut self) {} },
        ] {
            assert!(generated.contains(&expected.to_string()), "{generated}");
        }
    }

    #[test]
    fn repeated_nullary_facts_declare_one_presence() {
        let generated = generate(".decl Flag()\nFlag().\nFlag().\n.output Flag\n");
        let expected = quote! {
            fn facts() -> impl IntoIterator<Item = Self::Tuple> {
                [()]
            }
        };
        assert!(generated.contains(&expected.to_string()), "{generated}");
    }
}
