//! Constants: a literal as the Rust token of its column type, shared by
//! terms, constraints, and inline facts.

use std::any::type_name;
use std::str::FromStr;

use flowlog_parser::Constant;
use flowlog_parser::DataType;
use proc_macro2::Literal;
use proc_macro2::TokenStream;
use quote::ToTokens;
use quote::quote;

use crate::CodegenError;

/// Returns the Rust expression for one constant, parsed from its spelling
/// at its pinned type. A number is an unsuffixed literal, so Rust infers its
/// width from the enclosing tuple type; a float is wrapped in `OrderedFloat`;
/// a string is interned when `string_intern` is set.
///
/// Returns an internal error for an unpinned literal family, a tuple, or a
/// spelling that does not parse at its type: the typechecker pins every
/// literal to a scalar type its spelling fits.
pub(crate) fn const_to_token(
    constant: &Constant,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let text = constant.text();
    Ok(match constant.ty() {
        DataType::Int8 => Literal::i8_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::Int16 => Literal::i16_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::Int32 => Literal::i32_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::Int64 => Literal::i64_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::UInt8 => Literal::u8_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::UInt16 => Literal::u16_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::UInt32 => Literal::u32_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::UInt64 => Literal::u64_unsuffixed(parse(constant)?).into_token_stream(),
        DataType::Float32 => {
            let lit = Literal::f32_unsuffixed(parse(constant)?);
            quote! { OrderedFloat(#lit) }
        }
        DataType::Float64 => {
            let lit = Literal::f64_unsuffixed(parse(constant)?);
            quote! { OrderedFloat(#lit) }
        }
        DataType::String => {
            if string_intern {
                quote! { ::flowlog_runtime::intern::intern(#text) }
            } else {
                quote! { #text.to_string() }
            }
        }
        DataType::Bool => {
            let b = match text {
                "True" => true,
                "False" => false,
                _ => {
                    return Err(CodegenError::internal(format!(
                        "boolean constant `{text}` is neither `True` nor `False`"
                    )));
                }
            };
            quote! { #b }
        }
        DataType::IntLit | DataType::FloatLit => {
            return Err(CodegenError::internal(format!(
                "polymorphic literal {constant:?} reached codegen; \
                 typechecker should have pinned it"
            )));
        }
        DataType::FixedTuple(_) => {
            return Err(CodegenError::internal(format!(
                "tuple-typed constant `{text}` cannot appear as a literal"
            )));
        }
    })
}

/// Returns a constant's spelling parsed as `T`.
fn parse<T: FromStr>(constant: &Constant) -> Result<T, CodegenError> {
    let text = constant.text();
    text.parse().map_err(|_| {
        CodegenError::internal(format!(
            "constant `{text}` does not parse as {}",
            type_name::<T>()
        ))
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Cases: type, spelling, string_intern, expression.
    #[rstest]
    #[case::int(DataType::Int32, "7", false, quote! { 7 })]
    #[case::negative_int(DataType::Int64, "-7", false, quote! { -7 })]
    #[case::unsigned(DataType::UInt8, "255", false, quote! { 255 })]
    #[case::float(DataType::Float64, "1.5", false, quote! { OrderedFloat(1.5) })]
    #[case::whole_float(DataType::Float32, "2", false, quote! { OrderedFloat(2.0) })]
    #[case::string(DataType::String, "a", false, quote! { "a".to_string() })]
    #[case::interned_string(
        DataType::String,
        "a",
        true,
        quote! { ::flowlog_runtime::intern::intern("a") }
    )]
    #[case::true_bool(DataType::Bool, "True", false, quote! { true })]
    #[case::false_bool(DataType::Bool, "False", false, quote! { false })]
    fn a_constant_lowers_to_its_typed_literal(
        #[case] ty: DataType,
        #[case] text: &str,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        let token = const_to_token(&Constant::new(ty, text), string_intern).expect("pinned");
        assert_eq!(token.to_string(), expected.to_string());
    }
}
