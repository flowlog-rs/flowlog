//! Arithmetic terms: a rule argument's factors and operators as a Rust
//! expression. [`CodeGen::build_arithmetic_expr`] lowers any argument, given
//! how a variable lowers; the row, key-value, and join builders at the
//! bottom fix that for each closure shape.
//! [`CodeGen::factor_to_display_token`] lowers a `cat` argument to text.

use flowlog_parser::ArithmeticOperator;
use flowlog_parser::DataType;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::CodeGen;
use crate::CodegenError;
use crate::expr::term::constant::const_to_token;
use crate::tuple_tokens;

/// Returns `true` for the operators [`arithmetic_step`] emits as a call
/// rather than an infix expression.
fn is_call_form(op: &ArithmeticOperator) -> bool {
    match op {
        ArithmeticOperator::Power
        | ArithmeticOperator::ShiftLeft
        | ArithmeticOperator::ShiftRight
        | ArithmeticOperator::ShiftRightUnsigned => true,
        ArithmeticOperator::Plus
        | ArithmeticOperator::Minus
        | ArithmeticOperator::Multiply
        | ArithmeticOperator::Divide
        | ArithmeticOperator::Modulo
        | ArithmeticOperator::BitAnd
        | ArithmeticOperator::BitOr
        | ArithmeticOperator::BitXor => false,
    }
}

/// Returns `tokens`, the lowering of `expr`, as one operand: parenthesized,
/// unless `expr` ends in a call, which is already one term and whose
/// parentheses in argument position trip Rust's `unused_parens` lint.
fn as_operand(expr: &ArithmeticArgument, tokens: TokenStream) -> TokenStream {
    if expr.rest().last().is_some_and(|(op, _)| is_call_form(op)) {
        tokens
    } else {
        quote! { ( #tokens ) }
    }
}

/// Returns `lhs op rhs`. Rust's infix operators carry FlowLog's meaning for
/// every operator but the shifts and `^`, which call the runtime's `arith`
/// functions so the shift-count masking and exponent rules live in one
/// place.
fn arithmetic_step(op: &ArithmeticOperator, lhs: TokenStream, rhs: TokenStream) -> TokenStream {
    match op {
        ArithmeticOperator::Plus => quote! { #lhs + #rhs },
        ArithmeticOperator::Minus => quote! { #lhs - #rhs },
        ArithmeticOperator::Multiply => quote! { #lhs * #rhs },
        ArithmeticOperator::Divide => quote! { #lhs / #rhs },
        ArithmeticOperator::Modulo => quote! { #lhs % #rhs },
        ArithmeticOperator::BitAnd => quote! { #lhs & #rhs },
        ArithmeticOperator::BitOr => quote! { #lhs | #rhs },
        ArithmeticOperator::BitXor => quote! { #lhs ^ #rhs },
        ArithmeticOperator::Power => quote! { ::flowlog_runtime::arith::pow(#lhs, #rhs) },
        ArithmeticOperator::ShiftLeft => quote! { ::flowlog_runtime::arith::bshl(#lhs, #rhs) },
        ArithmeticOperator::ShiftRight => quote! { ::flowlog_runtime::arith::bshr(#lhs, #rhs) },
        ArithmeticOperator::ShiftRightUnsigned => {
            quote! { ::flowlog_runtime::arith::bshru(#lhs, #rhs) }
        }
    }
}

impl CodeGen {
    /// Returns an argument's expression, folding its steps left to right and
    /// lowering each variable it reads through `resolve_var`.
    pub(super) fn build_arithmetic_expr<F>(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        // Every infix step but the last is parenthesized, so Rust's operator
        // precedence cannot reorder the fold. The last stays bare, as
        // parentheses around a whole function argument trip Rust's
        // `unused_parens` lint; a call step is already one term.
        let rest = expr.rest();
        let mut result = self.factor_to_token(expr.init(), string_intern, resolve_var)?;
        for (i, (op, factor)) in rest.iter().enumerate() {
            let factor_token = self.factor_to_token(factor, string_intern, resolve_var)?;
            let step = arithmetic_step(op, result, factor_token);
            result = if i < rest.len() - 1 && !is_call_form(op) {
                quote! { ( #step ) }
            } else {
                step
            };
        }
        Ok(result)
    }

    /// Returns a factor as a value expression.
    fn factor_to_token<F>(
        &mut self,
        factor: &FactorArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        match factor {
            FactorArgument::Var(arg) => resolve_var(arg),
            FactorArgument::Const(c) => const_to_token(c, string_intern),
            FactorArgument::FnCall { name, args } => {
                self.fncall_to_token(name, args, string_intern, resolve_var)
            }
            FactorArgument::Builtin { op, args } => {
                self.builtin_to_token(*op, args, string_intern, resolve_var)
            }
            FactorArgument::Group(a) => {
                let inner = self.build_arithmetic_expr(a, string_intern, resolve_var)?;
                Ok(as_operand(a, inner))
            }
            FactorArgument::Tuple { fields } => {
                let field_toks = fields
                    .iter()
                    .map(|f| self.build_arithmetic_expr(f, string_intern, resolve_var))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(tuple_tokens(field_toks))
            }
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.build_arithmetic_expr(tuple, string_intern, resolve_var)?;
                let idx = Index::from(*index);
                Ok(quote! { (#rec).#idx })
            }
        }
    }

    /// Returns a factor of a `cat` (string concatenation) argument as text
    /// `format!` can display: an interned string resolves to its text.
    /// Typecheck makes every `cat` argument a string.
    pub(super) fn factor_to_display_token<F>(
        &mut self,
        factor: &FactorArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        match factor {
            FactorArgument::Var(arg) => {
                let var_token = resolve_var(arg)?;
                Ok(if string_intern {
                    quote! { ::flowlog_runtime::intern::resolve(#var_token) }
                } else {
                    var_token
                })
            }
            FactorArgument::Const(c) => {
                // A string literal is already text, so it skips the
                // intern-then-resolve round trip.
                if c.ty() == &DataType::String {
                    let s = c.text();
                    Ok(quote! { #s })
                } else {
                    const_to_token(c, string_intern)
                }
            }
            FactorArgument::FnCall { name, args } => {
                // Formatting needs the string contents, not the interned key.
                let call = self.fncall_to_token(name, args, string_intern, resolve_var)?;
                Ok(if string_intern {
                    quote! { ::flowlog_runtime::intern::resolve(#call) }
                } else {
                    call
                })
            }
            FactorArgument::Builtin { op, args } => {
                let call = self.builtin_to_token(*op, args, string_intern, resolve_var)?;
                // A built-in inside a `cat` returns a string, which is an
                // interned key in intern mode; resolve it to text.
                Ok(if string_intern {
                    quote! { ::flowlog_runtime::intern::resolve(#call) }
                } else {
                    call
                })
            }
            FactorArgument::Group(a) => {
                // Grammar guarantees a `Group` is multi-term, hence numeric
                // (string concat is `cat`): no display resolution needed.
                let inner = self.build_arithmetic_expr(a, string_intern, resolve_var)?;
                Ok(as_operand(a, inner))
            }
            // A projected field in a `cat` is a string; its interned key
            // resolves to text as the `Var` arm's does.
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.build_arithmetic_expr(tuple, string_intern, resolve_var)?;
                let idx = Index::from(*index);
                let proj = quote! { (#rec).#idx };
                Ok(if string_intern {
                    quote! { ::flowlog_runtime::intern::resolve(#proj) }
                } else {
                    proj
                })
            }
            // A whole tuple is not a string, so typecheck rejects it in a
            // `cat`; the value lowering keeps the match total (unreached).
            FactorArgument::Tuple { .. } => {
                self.factor_to_token(factor, string_intern, resolve_var)
            }
        }
    }

    /// Returns an argument's arithmetic expression inside a row closure,
    /// its variables read from the row pattern's `fields`.
    pub(crate) fn build_row_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        fields: &[Ident],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((_, idx)) => {
                let ident = fields.get(*idx).ok_or_else(|| {
                    CodegenError::internal(format!(
                        "row index {idx} out of bounds (row arity {})",
                        fields.len()
                    ))
                })?;
                Ok(quote! { #ident.clone() })
            }
            TransformationArgument::Jn(_) => Err(CodegenError::internal(format!(
                "join argument {arg:?} in a row expression"
            ))),
        })
    }

    /// Returns an argument's arithmetic expression inside a key-value
    /// closure, its variables read from the `(k, v)` parameters.
    ///
    /// Also accepts join arguments, reading only their `is_key`: an
    /// antijoin is planned as a join, so its arguments are `Jn`, but its
    /// closure sees only the surviving side's `(k, v)`. That is sound
    /// because an antijoin's output takes its values from the surviving
    /// side alone, and a key is the same on both sides.
    pub(crate) fn build_kv_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((is_key, idx))
            | TransformationArgument::Jn((_, is_key, idx)) => {
                let i = Index::from(*idx);
                Ok(if *is_key {
                    quote! { k.#i.clone() }
                } else {
                    quote! { v.#i.clone() }
                })
            }
        })
    }

    /// Returns an argument's arithmetic expression inside a join closure,
    /// its variables read from the `(k, lv, rv)` bindings.
    pub(crate) fn build_join_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::Jn((is_left, is_key, idx)) => {
                let i = Index::from(*idx);
                let side = match (is_left, is_key) {
                    (_, true) => quote! { k },
                    (true, false) => quote! { lv },
                    (false, false) => quote! { rv },
                };
                // Join parameters are references; an expression yields an
                // owned value.
                Ok(quote! { #side.#i.clone() })
            }
            TransformationArgument::KV(_) => Err(CodegenError::internal(format!(
                "key-value argument {arg:?} in a join expression"
            ))),
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_parser::Constant;
    use flowlog_planner::planner::TransformationArgument::Jn;
    use flowlog_planner::planner::TransformationArgument::KV;
    use quote::format_ident;
    use rstest::rstest;

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

    /// The value column `v.<idx>` of a key-value closure.
    fn value(idx: usize) -> FactorArgument {
        FactorArgument::Var(KV((false, idx)))
    }

    fn expr(
        init: FactorArgument,
        rest: Vec<(ArithmeticOperator, FactorArgument)>,
    ) -> ArithmeticArgument {
        ArithmeticArgument { init, rest }
    }

    /// Lowers `expr` in a key-value closure.
    fn kv_tokens(expr: &ArithmeticArgument) -> String {
        codegen()
            .build_kv_args_arithmetic_expr(expr, false)
            .expect("kv expression")
            .to_string()
    }

    // --- Variables in each closure shape ---

    #[test]
    fn a_row_variable_reads_its_pattern_field() {
        let fields = [
            format_ident!("x0"),
            format_ident!("_x1"),
            format_ident!("x2"),
        ];
        let tokens = codegen()
            .build_row_args_arithmetic_expr(&expr(value(2), Vec::new()), &fields, false)
            .expect("row variable");
        assert_eq!(tokens.to_string(), quote! { x2.clone() }.to_string());
    }

    // Cases: variable, expression.
    #[rstest]
    #[case(KV((true, 0)), quote! { k.0.clone() })]
    #[case(KV((false, 1)), quote! { v.1.clone() })]
    #[case::antijoin_key(Jn((true, true, 0)), quote! { k.0.clone() })]
    #[case::antijoin_value(Jn((false, false, 1)), quote! { v.1.clone() })]
    fn a_kv_variable_reads_its_side(
        #[case] arg: TransformationArgument,
        #[case] expected: TokenStream,
    ) {
        let arg = expr(FactorArgument::Var(arg), Vec::new());
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    // Cases: variable (is_left, is_key, index), expression.
    #[rstest]
    #[case(Jn((true, true, 0)), quote! { k.0.clone() })]
    #[case(Jn((false, true, 0)), quote! { k.0.clone() })]
    #[case(Jn((true, false, 1)), quote! { lv.1.clone() })]
    #[case(Jn((false, false, 2)), quote! { rv.2.clone() })]
    fn a_join_variable_reads_its_side(
        #[case] arg: TransformationArgument,
        #[case] expected: TokenStream,
    ) {
        let tokens = codegen()
            .build_join_args_arithmetic_expr(&expr(FactorArgument::Var(arg), Vec::new()), false)
            .expect("join variable");
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    // --- Operators and the fold ---

    // Cases: operator, `v.0 op v.1`.
    #[rstest]
    #[case(ArithmeticOperator::Plus, quote! { v.0.clone() + v.1.clone() })]
    #[case(ArithmeticOperator::Minus, quote! { v.0.clone() - v.1.clone() })]
    #[case(ArithmeticOperator::Multiply, quote! { v.0.clone() * v.1.clone() })]
    #[case(ArithmeticOperator::Divide, quote! { v.0.clone() / v.1.clone() })]
    #[case(ArithmeticOperator::Modulo, quote! { v.0.clone() % v.1.clone() })]
    #[case(ArithmeticOperator::BitAnd, quote! { v.0.clone() & v.1.clone() })]
    #[case(ArithmeticOperator::BitOr, quote! { v.0.clone() | v.1.clone() })]
    #[case(ArithmeticOperator::BitXor, quote! { v.0.clone() ^ v.1.clone() })]
    #[case(
        ArithmeticOperator::Power,
        quote! { ::flowlog_runtime::arith::pow(v.0.clone(), v.1.clone()) }
    )]
    #[case(
        ArithmeticOperator::ShiftLeft,
        quote! { ::flowlog_runtime::arith::bshl(v.0.clone(), v.1.clone()) }
    )]
    #[case(
        ArithmeticOperator::ShiftRight,
        quote! { ::flowlog_runtime::arith::bshr(v.0.clone(), v.1.clone()) }
    )]
    #[case(
        ArithmeticOperator::ShiftRightUnsigned,
        quote! { ::flowlog_runtime::arith::bshru(v.0.clone(), v.1.clone()) }
    )]
    fn each_operator_lowers_to_its_rust_form(
        #[case] op: ArithmeticOperator,
        #[case] expected: TokenStream,
    ) {
        let arg = expr(value(0), vec![(op, value(1))]);
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    /// Only a step followed by another is parenthesized, and a call step
    /// needs no parentheses at all.
    // Cases: operators of `v.0 op v.1 op v.2`, expression.
    #[rstest]
    #[case(
        [ArithmeticOperator::Minus, ArithmeticOperator::Minus],
        quote! { (v.0.clone() - v.1.clone()) - v.2.clone() }
    )]
    #[case(
        [ArithmeticOperator::Power, ArithmeticOperator::Plus],
        quote! { ::flowlog_runtime::arith::pow(v.0.clone(), v.1.clone()) + v.2.clone() }
    )]
    fn the_fold_keeps_its_left_to_right_order(
        #[case] ops: [ArithmeticOperator; 2],
        #[case] expected: TokenStream,
    ) {
        let [first, second] = ops;
        let arg = expr(value(0), vec![(first, value(1)), (second, value(2))]);
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    // Cases: operator inside `(v.0 op v.1) * v.2`, expression.
    #[rstest]
    #[case(
        ArithmeticOperator::Plus,
        quote! { (v.0.clone() + v.1.clone()) * v.2.clone() }
    )]
    #[case(
        ArithmeticOperator::Power,
        quote! { ::flowlog_runtime::arith::pow(v.0.clone(), v.1.clone()) * v.2.clone() }
    )]
    fn a_group_is_parenthesized_unless_it_ends_in_a_call(
        #[case] op: ArithmeticOperator,
        #[case] expected: TokenStream,
    ) {
        let group = FactorArgument::Group(Box::new(expr(value(0), vec![(op, value(1))])));
        let arg = expr(group, vec![(ArithmeticOperator::Multiply, value(2))]);
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    #[test]
    fn a_tuple_projection_reads_the_field() {
        let proj = FactorArgument::TupleProj {
            tuple: Box::new(expr(value(0), Vec::new())),
            index: 1,
        };
        assert_eq!(
            kv_tokens(&expr(proj, Vec::new())),
            quote! { (v.0.clone()).1 }.to_string()
        );
    }

    // --- Display lowering for `cat` ---

    // Cases: factor, string_intern, text.
    #[rstest]
    #[case::variable(value(0), false, quote! { v.0.clone() })]
    #[case::interned_variable(
        value(0),
        true,
        quote! { ::flowlog_runtime::intern::resolve(v.0.clone()) }
    )]
    #[case::interned_literal(
        FactorArgument::Const(Constant::new(DataType::String, "a")),
        true,
        quote! { "a" }
    )]
    #[case::interned_projection(
        FactorArgument::TupleProj {
            tuple: Box::new(expr(value(0), Vec::new())),
            index: 1,
        },
        true,
        quote! { ::flowlog_runtime::intern::resolve((v.0.clone()).1) }
    )]
    fn a_cat_factor_lowers_to_text(
        #[case] factor: FactorArgument,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        let tokens = codegen()
            .factor_to_display_token(&factor, string_intern, &|arg| match arg {
                KV((false, idx)) => {
                    let i = Index::from(*idx);
                    Ok(quote! { v.#i.clone() })
                }
                other => Err(CodegenError::internal(format!("unexpected {other:?}"))),
            })
            .expect("cat factor");
        assert_eq!(tokens.to_string(), expected.to_string());
    }
}
