//! Relation declarations: each relation's runtime `Relation`
//! implementation, the identity its loader and its writer both read.
//! Loading, the input collections, and the output emitters live in the
//! sibling modules.

use flowlog_parser::DataType;
use flowlog_parser::InputSource;
use flowlog_parser::OrderKey;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::CodegenError;
use crate::const_to_token;
use crate::internal_tuple_tokens;
use crate::relation_marker_ident;
use crate::row_tokens;
use crate::ty::data::column_is_copy;
use crate::ty::data::internal_kv_tokens;
use crate::ty::data::row_is_copy;

/// Returns a relation's marker type `Rel<name>` and its `Relation`
/// implementation: name, arity, declared and stored row types, the text
/// settings its input and output declare, its inline facts, and its output
/// ordering. `layout` is where the plan outputs each declared column, if
/// the relation is output.
///
/// # Errors
///
/// Returns an internal error for an inline fact codegen cannot write, or a
/// `layout` that does not place each declared column in its own slot.
pub(super) fn gen_relation_declaration(
    program: &Program,
    relation: &Relation,
    layout: Option<&[TransformationArgument]>,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let marker = relation_marker_ident(relation.name());
    let name = relation.raw_name();
    let arity = relation.arity();
    let tuple = internal_tuple_tokens(&relation.data_type(), string_intern);
    let storage = gen_storage(relation, layout, string_intern)?;
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
                    Ok(row_tokens(
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
            #storage
            #input_delimiter
            #input_has_header
            #output_delimiter
            #facts
            #ordering
        }
    })
}

/// Returns the relation's `Stored` row type and the `restore` that turns a
/// stored row back into its declared row. `layout` places each declared
/// column in a key or value slot; with no layout, or one that keeps the
/// declared row, the relation stores its `Tuple` itself.
///
/// # Errors
///
/// Returns an internal error if `layout` names a join slot, has the wrong
/// length, or does not give each declared column its own slot.
fn gen_storage(
    relation: &Relation,
    layout: Option<&[TransformationArgument]>,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let types = relation.data_type();
    // A short layout is not the declared row: it falls through to the
    // length check below.
    let keeps_declared_row = |layout: &[TransformationArgument]| {
        layout.len() == types.len()
            && layout
                .iter()
                .enumerate()
                .all(|(column, slot)| *slot == TransformationArgument::KV((false, column)))
    };
    let Some(layout) = layout.filter(|layout| !keeps_declared_row(layout)) else {
        let copy = if row_is_copy(&types, string_intern) {
            quote! { *row }
        } else {
            quote! { row.clone() }
        };
        return Ok(quote! {
            type Stored = Self::Tuple;
            fn restore(row: &Self::Stored) -> Self::Tuple {
                #copy
            }
        });
    };
    let invalid = || {
        CodegenError::internal(format!(
            "output layout {layout:?} of `{}` does not place each of its {} columns \
             in its own key or value slot",
            relation.raw_name(),
            relation.arity(),
        ))
    };
    if layout.len() != types.len() {
        return Err(invalid());
    }
    let key_len = layout
        .iter()
        .filter(|slot| matches!(slot, TransformationArgument::KV((true, _))))
        .count();
    let mut key: Vec<Option<DataType>> = vec![None; key_len];
    let mut value: Vec<Option<DataType>> = vec![None; layout.len() - key_len];
    let mut fields = Vec::with_capacity(layout.len());
    for (data_type, slot) in types.iter().zip(layout) {
        let TransformationArgument::KV((is_key, index)) = *slot else {
            return Err(invalid());
        };
        let side = if is_key { &mut key } else { &mut value };
        let Some(cell) = side.get_mut(index).filter(|cell| cell.is_none()) else {
            return Err(invalid());
        };
        *cell = Some(data_type.clone());
        // A literal `row.0.` would lex its `0.` as a float.
        let position = Index::from(index);
        // A collection keyed on every column holds its key alone, as
        // `keyed_output` writes it, so like an unkeyed one its row is flat.
        let access = match (key_len, is_key) {
            (0, _) => quote! { row.#position },
            (_, true) if key_len == layout.len() => quote! { row.#position },
            (_, true) => {
                let key_side = Index::from(0);
                quote! { row.#key_side.#position }
            }
            (_, false) => {
                let value_side = Index::from(1);
                quote! { row.#value_side.#position }
            }
        };
        fields.push(if column_is_copy(data_type, string_intern) {
            access
        } else {
            quote! { #access.clone() }
        });
    }
    let filled = |side: Vec<Option<DataType>>| {
        side.into_iter()
            .collect::<Option<Vec<_>>>()
            .expect("each column took a slot of its own, and the slots number the columns")
    };
    let stored = internal_kv_tokens(&(filled(key), filled(value)), string_intern);
    let declared = row_tokens(fields);
    Ok(quote! {
        type Stored = #stored;
        fn restore(row: &Self::Stored) -> Self::Tuple {
            #declared
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
        DataType::String => quote! { ::flowlog_runtime::intern::resolve(#access) },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Index::from(j);
                resolve_string_leaves(&quote! { (#access).#jdx }, fdt)
            });
            row_tokens(elems)
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
    use crate::test_harness::rendered;

    /// Renders every relation's declaration in `source`'s program.
    fn generate(source: &str, string_intern: bool) -> String {
        let program = program(source);
        let declarations = program
            .relations()
            .iter()
            .map(|relation| gen_relation_declaration(&program, relation, None, string_intern))
            .collect::<Result<Vec<_>, _>>()
            .expect("generate declarations");
        rendered(quote! { #(#declarations)* })
    }

    /// `string` lowers to the interner's key when interning is on, and a
    /// file input carries the parser's default delimiter, a tab.
    #[test]
    fn a_declaration_names_the_relation_and_its_tuple() {
        let generated = generate(
            ".decl R(id: int32, name: string)\n.input R\n.printsize R\n",
            true,
        );
        let expected = quote! {
            #[allow(non_camel_case_types)]
            pub(crate) struct Relr;

            impl ::flowlog_runtime::io::Relation for Relr {
                const NAME: &'static str = "R";
                const ARITY: usize = 2usize;
                type Tuple = (i32, ::flowlog_runtime::intern::Symbol);
                type Stored = Self::Tuple;
                fn restore(row: &Self::Stored) -> Self::Tuple {
                    *row
                }
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
                [(2i32,), (1i32,)]
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
                let cmp = ::flowlog_runtime::intern::resolve(b.1)
                    .cmp(&::flowlog_runtime::intern::resolve(a.1));
                if cmp != std::cmp::Ordering::Equal { return cmp; }
                let cmp = a.0.cmp(&b.0);
                if cmp != std::cmp::Ordering::Equal { return cmp; }
                std::cmp::Ordering::Equal
            }
        };
        assert!(generated.contains(&expected.to_string()), "{generated}");
    }

    // --- Storage ---

    fn kv(is_key: bool, index: usize) -> TransformationArgument {
        TransformationArgument::KV((is_key, index))
    }

    /// Renders the storage `layout` gives `R(a: int32, b: string, c: int64)`
    /// with strings held by value.
    fn storage(layout: Option<&[TransformationArgument]>) -> Result<String, CodegenError> {
        let program = program(".decl R(a: int32, b: string, c: int64)\n.input R\n.output R\n");
        gen_storage(&program.relations()[0], layout, false).map(|tokens| tokens.to_string())
    }

    /// A relation that is not output, or is output as its declared row,
    /// stores its declared row.
    // Cases: layout.
    #[rstest]
    #[case::not_output(None)]
    #[case::declared_row(Some(vec![kv(false, 0), kv(false, 1), kv(false, 2)]))]
    fn a_declared_row_is_stored_as_is(#[case] layout: Option<Vec<TransformationArgument>>) {
        let expected = quote! {
            type Stored = Self::Tuple;
            fn restore(row: &Self::Stored) -> Self::Tuple {
                row.clone()
            }
        };
        assert_eq!(storage(layout.as_deref()).unwrap(), expected.to_string());
    }

    /// A declared row of `Copy` columns is copied out rather than cloned.
    #[test]
    fn a_copy_declared_row_is_copied_out() {
        let program = program(".decl R(a: int32, b: string)\n.input R\n.output R\n");
        let interned = gen_storage(&program.relations()[0], None, true).unwrap();
        let expected = quote! {
            type Stored = Self::Tuple;
            fn restore(row: &Self::Stored) -> Self::Tuple {
                *row
            }
        };
        assert_eq!(interned.to_string(), expected.to_string());
    }

    /// The stored row follows the layout, and `restore` reads each
    /// declared column back from its slot, cloning only what is not `Copy`.
    // Cases: layout, stored type, declared row read back.
    #[rstest]
    #[case::permuted_row(
        vec![kv(false, 1), kv(false, 2), kv(false, 0)],
        quote! { (i64, i32, String) },
        quote! { (row.1, row.2.clone(), row.0) }
    )]
    #[case::keyed(
        vec![kv(false, 1), kv(true, 0), kv(false, 0)],
        quote! { ((String,), (i64, i32)) },
        {
            // `quote!` would read `row.1.1` as the float `1.1`.
            let (zero, one) = (Index::from(0), Index::from(1));
            quote! { (row.#one.#one, row.#zero.#zero.clone(), row.#one.#zero) }
        }
    )]
    #[case::all_key(
        vec![kv(true, 1), kv(true, 2), kv(true, 0)],
        quote! { (i64, i32, String) },
        quote! { (row.1, row.2.clone(), row.0) }
    )]
    fn a_reordered_row_is_stored_as_laid_out(
        #[case] layout: Vec<TransformationArgument>,
        #[case] stored: TokenStream,
        #[case] declared: TokenStream,
    ) {
        let expected = quote! {
            type Stored = #stored;
            fn restore(row: &Self::Stored) -> Self::Tuple {
                #declared
            }
        };
        assert_eq!(storage(Some(&layout)).unwrap(), expected.to_string());
    }

    // Cases: layout that does not give each column its own slot.
    #[rstest]
    #[case::empty(Vec::new())]
    #[case::short(vec![kv(false, 1), kv(false, 0)])]
    #[case::short_declared_prefix(vec![kv(false, 0)])]
    #[case::shared_slot(vec![kv(false, 0), kv(false, 0), kv(false, 1)])]
    #[case::gap(vec![kv(false, 0), kv(false, 3), kv(false, 1)])]
    #[case::join_slot(vec![
        kv(false, 0),
        kv(false, 1),
        TransformationArgument::Jn((false, false, 2)),
    ])]
    fn an_invalid_output_layout_is_an_internal_error(#[case] layout: Vec<TransformationArgument>) {
        let error = storage(Some(&layout)).expect_err("not a slot per column");
        assert!(matches!(&error, CodegenError::Internal(_)), "{error}");
        assert!(
            error
                .to_string()
                .contains("does not place each of its 3 columns in its own key or value slot"),
            "{error}"
        );
    }
}
