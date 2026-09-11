//! Library-mode Rust bindings.
//!
//! Relation names determine the generated `rel` tuple aliases and result
//! fields. Validation rejects names that Rust cannot represent or that
//! collide after conversion. Output expressions translate engine values
//! back to those user-facing tuple types; input conversion stays in the
//! runtime loaders.

use std::collections::HashMap;
use std::io;

use flowlog_parser::DataType;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;
use syn::Index;

use crate::build::BuildError;
use crate::codegen::tuple_tokens;
use crate::codegen::user_tuple_tokens;

// =============================================================================
// Rust names
// =============================================================================

/// Prefixes loader fields so relation names cannot become Rust keywords.
pub(super) fn inputs_field_ident(rel: &Relation) -> Ident {
    format_ident!("in_{}", rel.name())
}

/// Preserves the canonical relation name as a results field.
///
/// Requires [`validate_api_surface`] to have accepted the program.
pub(super) fn results_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}", rel.name())
}

/// Gives a `.printsize` results field its `<name>_size` suffix.
pub(super) fn printsize_field_ident(rel: &Relation) -> Ident {
    format_ident!("{}_size", rel.name())
}

/// Converts the canonical relation name into a PascalCase tuple alias.
///
/// Requires [`validate_api_surface`] to have accepted the program.
pub(super) fn user_tuple_ident(rel: &Relation) -> Ident {
    format_ident!("{}", pascal_case(rel.name()))
}

fn pascal_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut capitalize = true;
    for c in name.chars() {
        if c == '_' || c == '-' {
            capitalize = true;
            continue;
        }
        if capitalize {
            out.extend(c.to_uppercase());
            capitalize = false;
        } else {
            out.push(c);
        }
    }
    out
}

// =============================================================================
// Tuple aliases
// =============================================================================

/// Emits `rel` aliases for non-nullary input and output relations.
///
/// Aliases use ordinary Rust field types, independent of engine storage.
/// Requires [`validate_api_surface`] to have accepted the program.
pub(super) fn gen_public_rel_module(program: &Program) -> TokenStream {
    let aliases = collect_user_rels(program).into_iter().map(|rel| {
        let ident = user_tuple_ident(rel);
        let tuple_ty = user_tuple_tokens(&rel.data_type());
        quote! { pub type #ident = #tuple_ty; }
    });

    quote! {
        pub mod rel {
            #(#aliases)*
        }
    }
}

/// Includes each non-nullary input or output relation once, inputs first.
fn collect_user_rels(program: &Program) -> Vec<&Relation> {
    let mut seen: Vec<&Relation> = Vec::new();
    for rel in program.edbs().into_iter().chain(program.output_idbs()) {
        if rel.arity() == 0 {
            continue;
        }
        if !seen.iter().any(|r| r.name() == rel.name()) {
            seen.push(rel);
        }
    }
    seen
}

// =============================================================================
// Name validation
// =============================================================================

/// Rejects invalid or colliding Rust names in the generated library API.
///
/// Result fields preserve canonical relation names; tuple aliases use
/// PascalCase. Neither escapes Rust keywords. Output and `.printsize`
/// fields share one namespace, while tuple aliases have their own.
pub(super) fn validate_api_surface(program: &Program) -> Result<(), BuildError> {
    // Both directives add fields to the same result struct, so a size
    // field must also be checked against ordinary output fields.
    let mut fields: HashMap<String, String> = HashMap::new();
    for rel in program.output_idbs() {
        ensure_plain_ident(rel.name(), rel.raw_name(), "a results field")?;
        ensure_unique(
            &mut fields,
            rel.name().to_string(),
            rel.raw_name(),
            "results field",
        )?;
    }
    for rel in program.printsize_idbs() {
        let field = printsize_field_ident(rel).to_string();
        ensure_unique(&mut fields, field, rel.raw_name(), "results field")?;
    }

    let mut aliases: HashMap<String, String> = HashMap::new();
    for rel in collect_user_rels(program) {
        let stem = pascal_case(rel.name());
        ensure_plain_ident(&stem, rel.raw_name(), "a `rel::` type alias")?;
        ensure_unique(&mut aliases, stem, rel.raw_name(), "`rel::` type alias")?;
    }

    Ok(())
}

/// Rejects names that cannot be emitted as plain Rust identifiers.
fn ensure_plain_ident(name: &str, raw_name: &str, what: &str) -> Result<(), BuildError> {
    // Let syn define identifier validity instead of maintaining a separate
    // keyword list that could disagree with the generated Rust parser.
    if syn::parse_str::<syn::Ident>(name).is_err() {
        return Err(BuildError::from(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "relation `{raw_name}` cannot be exposed through the library API: \
                 `{name}` is not usable as {what} (it is a Rust keyword): rename \
                 the relation, or drop its `.output`/`.printsize` directive"
            ),
        )));
    }
    Ok(())
}

/// Records a generated name, rejecting a collision with a prior owner.
fn ensure_unique(
    owners: &mut HashMap<String, String>,
    ident: String,
    raw_name: &str,
    what: &str,
) -> Result<(), BuildError> {
    if let Some(prev) = owners.insert(ident.clone(), raw_name.to_string()) {
        return Err(BuildError::from(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "relations `{prev}` and `{raw_name}` would both surface as the \
                 {what} `{ident}` in the generated library API: rename one of them"
            ),
        )));
    }
    Ok(())
}

// =============================================================================
// Output conversion
// =============================================================================

/// Emits an expression converting an engine field to its user-facing value.
///
/// Floats lose their ordering wrappers and interned strings become owned
/// strings. Tuple fields are converted recursively; other values pass
/// through unchanged. Interned strings resolve through the runtime pool.
pub(super) fn tuple_to_user_expr(
    dt: &DataType,
    string_intern: bool,
    src: TokenStream,
) -> TokenStream {
    match dt {
        DataType::Float32 | DataType::Float64 => quote! { (#src).into_inner() },
        DataType::String if string_intern => {
            quote! { ::flowlog_runtime::intern::resolve_out(#src).to_string() }
        }
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(i, f)| {
                let idx = Index::from(i);
                tuple_to_user_expr(f, string_intern, quote! { #src.#idx })
            });
            tuple_tokens(elems)
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
        | DataType::String
        | DataType::Bool => src,
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

    fn program(source: &str) -> Program {
        // Program has no public constructor; parsing is the smallest
        // available entry for constructing these binding test inputs.
        let dir = tempfile::tempdir().expect("temporary directory");
        let path = dir.path().join("program.dl");
        fs::write(&path, source).expect("program source");
        flowlog_parser::parse(
            path.to_str().expect("program path"),
            &[],
            &mut SourceMap::default(),
            &mut Config::default(),
        )
        .expect("valid program")
    }

    #[rstest]
    #[case("Edge", ["in_edge", "edge", "edge_size", "Edge"])]
    #[case("method_lookup", ["in_method_lookup", "method_lookup", "method_lookup_size", "MethodLookup"])]
    #[case("type_", ["in_type_", "type_", "type__size", "Type"])]
    #[case("crate_", ["in_crate_", "crate_", "crate__size", "Crate"])]
    fn relation_names_define_rust_bindings(#[case] name: &str, #[case] expected: [&str; 4]) {
        let program = program(&format!(
            ".decl {name}(x: int32)\n.input {name}\n.output {name}\n.printsize {name}\n"
        ));
        validate_api_surface(&program).expect("valid bindings");
        let rel = &program.relations()[0];
        assert_eq!(
            [
                inputs_field_ident(rel).to_string(),
                results_field_ident(rel).to_string(),
                printsize_field_ident(rel).to_string(),
                user_tuple_ident(rel).to_string(),
            ],
            expected,
        );
    }

    #[rstest]
    #[case("type")]
    #[case("match")]
    #[case("in")]
    #[case("loop")]
    #[case("self")]
    #[case("crate")]
    #[case("super")]
    #[case("yield")]
    #[case("try")]
    fn keyword_result_fields_are_rejected(#[case] name: &str) {
        let program = program(&format!(
            ".decl {name}(x: int32)\n.input {name}\n.output {name}\n"
        ));
        let BuildError::Io(error) = validate_api_surface(&program).expect_err("keyword field");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error.to_string(),
            format!(
                "relation `{name}` cannot be exposed through the library API: \
                 `{name}` is not usable as a results field (it is a Rust keyword): rename \
                 the relation, or drop its `.output`/`.printsize` directive"
            ),
        );
    }

    #[rstest]
    #[case(
        ".decl self(x: int32)\n.input self\n.printsize self\n",
        "relation `self` cannot be exposed through the library API: \
         `Self` is not usable as a `rel::` type alias (it is a Rust keyword): rename \
         the relation, or drop its `.output`/`.printsize` directive"
    )]
    #[case(
        ".decl foo_bar(x: int32)\n.input foo_bar\n.output foo_bar\n\
         .decl foo__bar(x: int32)\n.input foo__bar\n.output foo__bar\n",
        "relations `foo_bar` and `foo__bar` would both surface as the \
         `rel::` type alias `FooBar` in the generated library API: rename one of them"
    )]
    #[case(
        ".decl x(v: int32)\n.input x\n.printsize x\n\
         .decl x_size(v: int32)\n.input x_size\n.output x_size\n",
        "relations `x_size` and `x` would both surface as the \
         results field `x_size` in the generated library API: rename one of them"
    )]
    fn converted_name_conflicts_are_rejected(#[case] source: &str, #[case] expected: &str) {
        let program = program(source);
        let BuildError::Io(error) = validate_api_surface(&program).expect_err("invalid binding");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), expected);
    }

    #[test]
    fn keyword_input_names_are_valid_with_prefixed_or_suffixed_fields() {
        let program = program(".decl Type(x: int32)\n.input Type\n.printsize Type\n");
        validate_api_surface(&program).expect("valid bindings");
        let rel = &program.relations()[0];
        assert_eq!(inputs_field_ident(rel).to_string(), "in_type");
        assert_eq!(printsize_field_ident(rel).to_string(), "type_size");
        assert_eq!(user_tuple_ident(rel).to_string(), "Type");
    }

    #[rstest]
    #[case(
        ".decl Edge(id: int32, label: string, score: f64)\n.input Edge\n.output Edge\n",
        quote! { pub mod rel { pub type Edge = (i32, String, f64); } }
    )]
    #[case(
        ".decl Enabled()\n.input Enabled\n.output Enabled\n",
        quote! { pub mod rel {} }
    )]
    #[case(
        ".decl Seed(id: int32)\n.input Seed\n\
         .decl Result(id: int32)\nResult(x) :- Seed(x).\n.output Result\n",
        quote! { pub mod rel { pub type Seed = (i32,); pub type Result = (i32,); } }
    )]
    #[case(
        ".decl Counted(id: int32)\nCounted(1).\n\
         .decl Summary(id: int32)\nSummary(x) :- Counted(x).\n.printsize Summary\n",
        quote! { pub mod rel { pub type Counted = (i32,); } }
    )]
    fn tuple_aliases_cover_input_and_output_values_once(
        #[case] source: &str,
        #[case] expected: TokenStream,
    ) {
        let program = program(source);
        validate_api_surface(&program).expect("valid bindings");
        assert_eq!(
            gen_public_rel_module(&program).to_string(),
            expected.to_string()
        );
    }

    #[rstest]
    #[case(DataType::Float32, false, quote! { (row).into_inner() })]
    #[case(DataType::Float64, true, quote! { (row).into_inner() })]
    #[case(DataType::String, true, quote! { ::flowlog_runtime::intern::resolve_out(row).to_string() })]
    #[case(DataType::String, false, quote! { row })]
    #[case(DataType::Int32, true, quote! { row })]
    #[case(DataType::Bool, false, quote! { row })]
    #[case(
        DataType::FixedTuple(vec![
            DataType::Int32,
            DataType::FixedTuple(vec![DataType::String, DataType::Float64]),
        ]),
        true,
        quote! {
            (row . 0, (
                ::flowlog_runtime::intern::resolve_out(row . 1 . 0).to_string(),
                (row . 1 . 1).into_inner()
            ))
        }
    )]
    fn output_fields_convert_to_user_values(
        #[case] dt: DataType,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        assert_eq!(
            tuple_to_user_expr(&dt, string_intern, quote! { row }).to_string(),
            expected.to_string(),
        );
    }
}
