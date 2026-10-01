//! Relation declarations: each relation's runtime `Relation`
//! implementation, the identity its loader and its writer both read.
//! Loading, the input collections, and the output emitters live in the
//! sibling modules.

use flowlog_parser::DataType;
use flowlog_parser::InputSource;
use flowlog_parser::OrderKey;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::CodegenError;
use crate::const_to_token;
use crate::internal_tuple_tokens;
use crate::relation_marker_ident;
use crate::tuple_tokens;

/// Returns a relation's marker type `Rel<name>` and its `Relation`
/// implementation: name, arity, tuple type, the text settings its input and
/// output declare, its inline facts, and its output ordering.
pub(super) fn gen_declaration(
    program: &Program,
    relation: &Relation,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let marker = relation_marker_ident(relation.name());
    let name = relation.raw_name();
    let arity = relation.arity();
    let tuple = internal_tuple_tokens(&relation.data_type(), string_intern);
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
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;

    fn generate(source: &str, string_intern: bool) -> String {
        let program = program(source);
        let declarations = program
            .relations()
            .iter()
            .map(|relation| gen_declaration(&program, relation, string_intern))
            .collect::<Result<Vec<_>, _>>()
            .expect("generate declarations");
        let tokens = quote! { #(#declarations)* };
        syn::parse2::<syn::File>(tokens.clone()).expect("valid Rust syntax");
        tokens.to_string()
    }

    /// `string` lowers to the interner's key when interning is on, and a
    /// file input carries the parser's default delimiter, a tab.
    #[test]
    fn a_declaration_names_the_relation_and_its_tuple() {
        let generated = generate(".decl R(id: int32, name: string)\n.input R\n", true);
        let expected = quote! {
            #[allow(non_camel_case_types)]
            pub(crate) struct Relr;

            impl ::flowlog_runtime::io::Relation for Relr {
                const NAME: &'static str = "R";
                const ARITY: usize = 2usize;
                type Tuple = (i32, ::flowlog_runtime::lasso::Spur);
                const INPUT_DELIMITER: u8 = 9u8;
            }
        };
        assert_eq!(generated, expected.to_string());
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

    #[test]
    fn a_plain_relation_declares_no_facts_and_no_ordering() {
        let generated = generate(".decl Edge(id: int32)\n.input Edge\n.output Edge\n", false);
        assert!(!generated.contains("fn facts"), "{generated}");
        assert!(!generated.contains("ORDERED"), "{generated}");
        assert!(!generated.contains("fn compare"), "{generated}");
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
    /// interned string by its text; the limit is `None` unless declared.
    // Cases: limit clause, LIMIT.
    #[rstest]
    #[case::limited(", limit=\"3\"", quote! { Some(3usize) })]
    #[case::unlimited("", quote! { None })]
    fn an_ordered_output_compares_its_keys_in_turn(
        #[case] limit: &str,
        #[case] expected_limit: TokenStream,
    ) {
        let generated = generate(
            &format!(
                ".decl R(id: int32, name: string)\n\
                 R(1, \"a\").\n\
                 .output R(order_by=\"name DESC, id\"{limit})\n"
            ),
            true,
        );
        let expected = quote! {
            const ORDERED: bool = true;
            const LIMIT: Option<usize> = #expected_limit;
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
