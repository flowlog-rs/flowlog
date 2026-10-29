//! Built-in functions: the engine's Souffle-style intrinsics, lowered inline.

use flowlog_parser::BuiltinOperator;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::Literal;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::term::as_str;

impl Codegen {
    /// Returns a built-in call's expression, each operator lowered to its own
    /// inline Rust template rather than a call into `udf::`. A string result
    /// is interned when `string_intern` is set. A built-in that
    /// [`lowers_to_cast`] is returned without parentheses.
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
                Ok(quote! { #s.chars().count() as i32 })
            }
            BuiltinOperator::Substr => {
                let [s, start, len] = operands(op, args)?;
                let s = self.arithmetic_to_token(s, string_intern, resolve_var)?;
                let s = as_str(&s, string_intern);
                // `skip(0)` is a no-op clippy denies.
                let skip = (index_literal(start) != Some(0))
                    .then(|| self.index_operand(start, string_intern, resolve_var))
                    .transpose()?
                    .map(|start| quote! { .skip(#start) });
                let len = self.index_operand(len, string_intern, resolve_var)?;
                Ok(emit_string(
                    quote! { #s.chars() #skip .take(#len).collect::<String>() },
                    string_intern,
                ))
            }
            BuiltinOperator::Ord => {
                // Typecheck requires `--str-intern` for `ord`, so the argument
                // is an interned key, and its `u32` serves as the opaque
                // per-symbol id.
                debug_assert!(string_intern);
                let [s] = self.value_operands(op, args, string_intern, resolve_var)?;
                Ok(quote! { #s.into_inner().get() as i32 })
            }
            BuiltinOperator::ToString => {
                let [n] = operands(op, args)?;
                let value = self.arithmetic_to_token(n, string_intern, resolve_var)?;
                // `.to_string()` would bind to an expression's last factor,
                // and does not lex after an integer literal; any other
                // factor is one term.
                let grouped = !n.rest.is_empty()
                    || matches!(&n.init, FactorArgument::Const(c) if c.ty().is_integer());
                let receiver = if grouped {
                    quote! { (#value) }
                } else {
                    value
                };
                Ok(emit_string(quote! { #receiver.to_string() }, string_intern))
            }
            BuiltinOperator::ToNumber => {
                // 0 on parse failure keeps the function total; Souffle
                // leaves that case unspecified.
                let [s] = self.value_operands(op, args, string_intern, resolve_var)?;
                let s = as_str(&s, string_intern);
                Ok(quote! { #s.parse::<i32>().unwrap_or(0) })
            }
            BuiltinOperator::Cat => {
                // `cat` formats its arguments, so they lower to display text
                // rather than values, and a nested `cat` contributes its
                // parts to the same `format!`.
                let mut parts = Vec::new();
                for arg in operands::<2>(op, args)? {
                    self.cat_parts(arg, string_intern, resolve_var, &mut parts)?;
                }
                let template = "{}".repeat(parts.len());
                Ok(emit_string(
                    quote! { format!(#template, #(#parts),*) },
                    string_intern,
                ))
            }
        }
    }

    /// Appends the display text of a `cat` argument to `parts`: a nested
    /// `cat`'s arguments one by one, any other factor as one part.
    fn cat_parts<F>(
        &mut self,
        arg: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
        parts: &mut Vec<TokenStream>,
    ) -> Result<(), CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        // After typecheck each argument is a string and so a single factor:
        // only `cat` itself builds a compound string, and it is a factor.
        debug_assert!(
            arg.rest.is_empty(),
            "cat() arg is a single factor after typecheck"
        );
        match &arg.init {
            FactorArgument::Builtin {
                op: BuiltinOperator::Cat,
                args,
            } => {
                for inner in args {
                    self.cat_parts(inner, string_intern, resolve_var, parts)?;
                }
            }
            factor => {
                parts.push(self.factor_to_display_token(factor, string_intern, resolve_var)?)
            }
        }
        Ok(())
    }

    /// Returns `arg` as a `usize` index: an integer literal keeps its value
    /// with the suffix, anything else is cast whole, in one pair of
    /// parentheses.
    fn index_operand<F>(
        &mut self,
        arg: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        if let Some(index) = index_literal(arg) {
            let literal = Literal::usize_suffixed(index);
            return Ok(quote! { #literal });
        }
        // Lowered as an argument, a group or a `strlen` keeps none of the
        // parentheses it carries as an operand, so the pair around the
        // `as usize` is the only one: two trip clippy's `double_parens` in a
        // library crate.
        let value = self.arithmetic_to_argument(arg, string_intern, resolve_var)?;
        Ok(quote! { (#value) as usize })
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
            *value = self.arithmetic_to_token(arg, string_intern, resolve_var)?;
        }
        Ok(values)
    }
}

/// Returns `true` for a built-in that lowers to a cast, `x as i32`. As an
/// operand it needs parentheses: a method call binds tighter than `as`,
/// and a `<` after it opens generic arguments.
pub(super) fn lowers_to_cast(op: BuiltinOperator) -> bool {
    match op {
        BuiltinOperator::Strlen | BuiltinOperator::Ord => true,
        BuiltinOperator::Substr
        | BuiltinOperator::ToString
        | BuiltinOperator::ToNumber
        | BuiltinOperator::Cat => false,
    }
}

/// Returns `arg`'s value if it is a bare non-negative integer literal.
fn index_literal(arg: &ArithmeticArgument) -> Option<usize> {
    match &arg.init {
        FactorArgument::Const(c) if arg.rest.is_empty() => c.text().parse().ok(),
        _ => None,
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
    use flowlog_parser::ArithmeticOperator;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::TransformationArgument::KV;
    use rstest::rstest;
    use syn::Index;

    use super::*;
    use crate::test_harness::codegen;

    /// The value column `v.<idx>`, as a whole argument.
    fn value(idx: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(KV((false, idx))),
            rest: Vec::new(),
        }
    }

    fn lower(op: BuiltinOperator, args: &[ArithmeticArgument], string_intern: bool) -> String {
        codegen("")
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
        quote! { v.0.clone().as_str().chars().count() as i32 }
    )]
    #[case::interned_strlen(
        BuiltinOperator::Strlen,
        true,
        quote! { ::flowlog_runtime::intern::resolve(v.0.clone()).chars().count() as i32 }
    )]
    #[case::ord(
        BuiltinOperator::Ord,
        true,
        quote! { v.0.clone().into_inner().get() as i32 }
    )]
    #[case::to_string(BuiltinOperator::ToString, false, quote! { v.0.clone().to_string() })]
    #[case::interned_to_string(
        BuiltinOperator::ToString,
        true,
        quote! { ::flowlog_runtime::intern::intern(&v.0.clone().to_string()) }
    )]
    #[case::to_number(
        BuiltinOperator::ToNumber,
        false,
        quote! { v.0.clone().as_str().parse::<i32>().unwrap_or(0) }
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
                v.0.clone().as_str()
                    .chars()
                    .skip((v.1.clone()) as usize)
                    .take((v.2.clone()) as usize)
                    .collect::<String>()
            }
            .to_string()
        );
    }

    /// A literal index is a `usize` literal, a computed one a cast, and a
    /// zero start skips nothing.
    // Cases: start literal, lowering.
    #[rstest]
    #[case::zero_start(
        "0",
        quote! { v.0.clone().as_str().chars().take(2usize).collect::<String>() }
    )]
    #[case::literal_start(
        "1",
        quote! { v.0.clone().as_str().chars().skip(1usize).take(2usize).collect::<String>() }
    )]
    fn substr_literal_indices_need_no_cast(#[case] start: &str, #[case] expected: TokenStream) {
        let literal = |n: &str| ArithmeticArgument {
            init: FactorArgument::Const(Constant::new(DataType::Int32, n)),
            rest: Vec::new(),
        };
        assert_eq!(
            lower(
                BuiltinOperator::Substr,
                &[value(0), literal(start), literal("2")],
                false
            ),
            expected.to_string()
        );
    }

    /// A group or a `strlen` as an index converts to `usize` whole, in one
    /// pair of parentheses, not inside the pair it carries as an operand.
    // Cases: start, skip.
    #[rstest]
    #[case::cast(
        ArithmeticArgument {
            init: FactorArgument::Builtin {
                op: BuiltinOperator::Strlen,
                args: vec![value(1)],
            },
            rest: Vec::new(),
        },
        quote! { .skip((v.1.clone().as_str().chars().count() as i32) as usize) }
    )]
    #[case::group(
        ArithmeticArgument {
            init: FactorArgument::Group(Box::new(ArithmeticArgument {
                init: FactorArgument::Var(KV((false, 1))),
                rest: vec![(ArithmeticOperator::BitAnd, FactorArgument::Var(KV((false, 2))))],
            })),
            rest: Vec::new(),
        },
        quote! { .skip((v.1.clone() & v.2.clone()) as usize) }
    )]
    fn a_compound_substr_index_is_cast_in_one_pair_of_parentheses(
        #[case] start: ArithmeticArgument,
        #[case] skip: TokenStream,
    ) {
        let lowered = lower(BuiltinOperator::Substr, &[value(0), start, value(2)], false);
        assert!(lowered.contains(&skip.to_string()), "{lowered}");
    }

    #[test]
    fn a_nested_cat_formats_in_one_call() {
        let inner = ArithmeticArgument {
            init: FactorArgument::Builtin {
                op: BuiltinOperator::Cat,
                args: vec![
                    ArithmeticArgument {
                        init: FactorArgument::Const(Constant::new(DataType::String, " ")),
                        rest: Vec::new(),
                    },
                    value(1),
                ],
            },
            rest: Vec::new(),
        };
        assert_eq!(
            lower(BuiltinOperator::Cat, &[value(0), inner], false),
            quote! { format!("{}{}{}", v.0.clone(), " ", v.1.clone()) }.to_string()
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
