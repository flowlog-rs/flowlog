//! Built-in functions: the engine's Souffle-style intrinsics, lowered inline.

use flowlog_parser::BuiltinOperator;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::TokenStream;
use quote::quote;

use crate::CodeGen;
use crate::CodegenError;
use crate::expr::term::as_str;

impl CodeGen {
    /// Returns a built-in call's expression, each operator lowered to its own
    /// inline Rust template rather than a call into `udf::`. A string result
    /// is interned when `string_intern` is set.
    pub(super) fn builtin_to_token<F>(
        &mut self,
        op: BuiltinOperator,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        match op {
            BuiltinOperator::Strlen => {
                // Char count, not byte count: Souffle semantics.
                let [s] = self.value_operands(op, args, string_intern, resolve_var)?;
                let s = as_str(&s, string_intern);
                Ok(quote! { ((#s).chars().count() as i32) })
            }
            BuiltinOperator::Substr => {
                let [s, start, len] = self.value_operands(op, args, string_intern, resolve_var)?;
                let s = as_str(&s, string_intern);
                Ok(emit_string(
                    quote! {
                        (#s).chars().skip((#start) as usize).take((#len) as usize).collect::<String>()
                    },
                    string_intern,
                ))
            }
            BuiltinOperator::Ord => {
                // Typecheck requires `--str-intern` for `ord`, so the argument
                // is an interned key, and its `u32` serves as the opaque
                // per-symbol id.
                debug_assert!(string_intern);
                let [s] = self.value_operands(op, args, string_intern, resolve_var)?;
                Ok(quote! { ((#s).into_inner().get() as i32) })
            }
            BuiltinOperator::ToString => {
                let [n] = self.value_operands(op, args, string_intern, resolve_var)?;
                Ok(emit_string(quote! { (#n).to_string() }, string_intern))
            }
            BuiltinOperator::ToNumber => {
                // 0 on parse failure keeps the function total; Souffle
                // leaves that case unspecified.
                let [s] = self.value_operands(op, args, string_intern, resolve_var)?;
                let s = as_str(&s, string_intern);
                Ok(quote! { ((#s).parse::<i32>().unwrap_or(0)) })
            }
            BuiltinOperator::Cat => {
                // `cat` formats its arguments, so they lower to display text
                // rather than values. After typecheck each is a string and
                // so a single factor: only `cat` itself builds a compound
                // string, and it is a factor.
                let [left, right] = operands(op, args)?;
                debug_assert!(
                    left.rest.is_empty() && right.rest.is_empty(),
                    "cat() arg is a single factor after typecheck"
                );
                let left = self.factor_to_display_token(&left.init, string_intern, resolve_var)?;
                let right =
                    self.factor_to_display_token(&right.init, string_intern, resolve_var)?;
                Ok(emit_string(
                    quote! { format!("{}{}", #left, #right) },
                    string_intern,
                ))
            }
        }
    }

    /// Returns a built-in's `N` arguments lowered as values.
    fn value_operands<F, const N: usize>(
        &mut self,
        op: BuiltinOperator,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<[TokenStream; N], CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        let args: &[ArithmeticArgument; N] = operands(op, args)?;
        let mut values: [TokenStream; N] = std::array::from_fn(|_| TokenStream::new());
        for (value, arg) in values.iter_mut().zip(args) {
            *value = self.build_arithmetic_expr(arg, string_intern, resolve_var)?;
        }
        Ok(values)
    }
}

/// Returns a built-in's `N` operands, or an internal error when the call
/// does not have exactly `N`; the parser enforces each built-in's arity.
fn operands<const N: usize>(
    op: BuiltinOperator,
    operands: &[ArithmeticArgument],
) -> Result<&[ArithmeticArgument; N], CodegenError> {
    operands.try_into().map_err(|_| {
        CodegenError::internal(format!("{op} takes {N} arguments, got {}", operands.len()))
    })
}

/// Returns an owned `String` expression as a string value: interned when
/// `string_intern` is set, otherwise unchanged.
fn emit_string(owned: TokenStream, string_intern: bool) -> TokenStream {
    if string_intern {
        quote! { ::flowlog_runtime::intern::intern(&#owned) }
    } else {
        owned
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::TransformationArgument::KV;
    use rstest::rstest;
    use syn::Index;

    use super::*;

    /// A code generator over an empty program; parsing is the only way to
    /// build a `Program`.
    fn codegen() -> CodeGen {
        let file = tempfile::NamedTempFile::new().expect("tempfile");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("empty program parses");
        CodeGen::new(config, program)
    }

    /// The value column `v.<idx>`, as a whole argument.
    fn value(idx: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(KV((false, idx))),
            rest: Vec::new(),
        }
    }

    fn lower(op: BuiltinOperator, args: &[ArithmeticArgument], string_intern: bool) -> String {
        codegen()
            .builtin_to_token(op, args, string_intern, &|arg| match arg {
                KV((false, idx)) => {
                    let i = Index::from(*idx);
                    Ok(quote! { v.#i.clone() })
                }
                other => Err(CodegenError::internal(format!("unexpected {other:?}"))),
            })
            .expect("built-in call")
            .to_string()
    }

    // Cases: built-in, string_intern, expression.
    #[rstest]
    #[case::strlen(
        BuiltinOperator::Strlen,
        false,
        quote! { (((v.0.clone()).as_str()).chars().count() as i32) }
    )]
    #[case::interned_strlen(
        BuiltinOperator::Strlen,
        true,
        quote! { ((::flowlog_runtime::intern::resolve(v.0.clone())).chars().count() as i32) }
    )]
    #[case::ord(
        BuiltinOperator::Ord,
        true,
        quote! { ((v.0.clone()).into_inner().get() as i32) }
    )]
    #[case::to_string(BuiltinOperator::ToString, false, quote! { (v.0.clone()).to_string() })]
    #[case::interned_to_string(
        BuiltinOperator::ToString,
        true,
        quote! { ::flowlog_runtime::intern::intern(&(v.0.clone()).to_string()) }
    )]
    #[case::to_number(
        BuiltinOperator::ToNumber,
        false,
        quote! { (((v.0.clone()).as_str()).parse::<i32>().unwrap_or(0)) }
    )]
    fn a_unary_built_in_lowers_to_its_template(
        #[case] op: BuiltinOperator,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        assert_eq!(lower(op, &[value(0)], string_intern), expected.to_string());
    }

    #[test]
    fn substr_slices_by_character() {
        assert_eq!(
            lower(
                BuiltinOperator::Substr,
                &[value(0), value(1), value(2)],
                false
            ),
            quote! {
                ((v.0.clone()).as_str())
                    .chars()
                    .skip((v.1.clone()) as usize)
                    .take((v.2.clone()) as usize)
                    .collect::<String>()
            }
            .to_string()
        );
    }

    /// `cat` formats both sides' text in one `format!` and interns the
    /// result.
    #[test]
    fn an_interned_cat_formats_text_and_interns_the_result() {
        let literal = ArithmeticArgument {
            init: FactorArgument::Const(Constant::new(DataType::String, "-")),
            rest: Vec::new(),
        };
        assert_eq!(
            lower(BuiltinOperator::Cat, &[value(0), literal], true),
            quote! {
                ::flowlog_runtime::intern::intern(&format!(
                    "{}{}",
                    ::flowlog_runtime::intern::resolve(v.0.clone()),
                    "-"
                ))
            }
            .to_string()
        );
    }
}
