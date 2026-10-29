//! Arithmetic terms: a rule argument's factors and operators as a Rust
//! expression. [`Codegen::arithmetic_to_token`] lowers any argument, given
//! how a variable lowers; the row, key-value, and join builders at the
//! bottom fix that for each closure shape.
//! [`Codegen::factor_to_display_token`] lowers a `cat` argument to text.

use flowlog_parser::ArithmeticOperator;
use flowlog_parser::DataType;
use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::FactorArgument;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::term::builtin::lowers_to_cast;
use crate::expr::term::constant::const_to_token;
use crate::tuple_tokens;
use crate::ty::data::KvTypes;
use crate::ty::data::column_is_copy;
use crate::ty::data::slot_type;

/// Returns `true` for the operators [`arithmetic_step`] emits as a call
/// rather than an infix expression.
fn is_call_form(op: &ArithmeticOperator) -> bool {
    match op {
        ArithmeticOperator::Plus
        | ArithmeticOperator::Minus
        | ArithmeticOperator::Multiply
        | ArithmeticOperator::Divide
        | ArithmeticOperator::Modulo
        | ArithmeticOperator::Power
        | ArithmeticOperator::ShiftLeft
        | ArithmeticOperator::ShiftRight
        | ArithmeticOperator::ShiftRightUnsigned => true,
        ArithmeticOperator::BitAnd | ArithmeticOperator::BitOr | ArithmeticOperator::BitXor => {
            false
        }
    }
}

/// Returns `binding`, which holds slot `idx` of `types`' keys (`is_key`) or
/// values, as an owned value: itself for a `Copy` column, its clone
/// otherwise.
fn read_slot(
    binding: TokenStream,
    types: &KvTypes,
    is_key: bool,
    idx: usize,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    let column = slot_type(types, is_key, idx)?;
    Ok(if column_is_copy(column, string_intern) {
        binding
    } else {
        quote! { #binding.clone() }
    })
}

/// Returns field `index` of `tuple`, lowered to `rec`.
fn tuple_field(tuple: &ArithmeticArgument, rec: TokenStream, index: usize) -> TokenStream {
    let idx = Index::from(index);
    // Field access binds tighter than any operator, so only a multi-step
    // expression needs grouping.
    if tuple.rest().is_empty() {
        quote! { #rec.#idx }
    } else {
        quote! { (#rec).#idx }
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
/// the bitwise ones alone; every other operator calls the runtime's `arith`
/// functions, where its integer and float meanings live.
fn arithmetic_step(op: &ArithmeticOperator, lhs: TokenStream, rhs: TokenStream) -> TokenStream {
    match op {
        ArithmeticOperator::Plus => quote! { ::flowlog_runtime::arith::add(#lhs, #rhs) },
        ArithmeticOperator::Minus => quote! { ::flowlog_runtime::arith::sub(#lhs, #rhs) },
        ArithmeticOperator::Multiply => quote! { ::flowlog_runtime::arith::mul(#lhs, #rhs) },
        ArithmeticOperator::Divide => quote! { ::flowlog_runtime::arith::div(#lhs, #rhs) },
        ArithmeticOperator::Modulo => quote! { ::flowlog_runtime::arith::rem(#lhs, #rhs) },
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

impl Codegen {
    /// Returns an argument's expression, folding its steps left to right and
    /// lowering each variable it reads through `resolve_var`.
    pub(super) fn arithmetic_to_token<F>(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        // An infix step followed by another infix step is parenthesized, so
        // Rust's operator precedence cannot reorder the fold. A call step is
        // one term, and its operands are whole arguments, which take no
        // parentheses: Rust's `unused_parens` lint rejects them.
        let rest = expr.rest();
        let infix_at = |i: usize| rest.get(i).is_some_and(|(op, _)| !is_call_form(op));
        let mut result = if rest.first().is_some_and(|(op, _)| is_call_form(op)) {
            self.factor_to_argument(expr.init(), string_intern, resolve_var)?
        } else {
            self.factor_to_token(expr.init(), string_intern, resolve_var)?
        };
        for (i, (op, factor)) in rest.iter().enumerate() {
            let factor_token = if is_call_form(op) {
                self.factor_to_argument(factor, string_intern, resolve_var)?
            } else {
                self.factor_to_token(factor, string_intern, resolve_var)?
            };
            let step = arithmetic_step(op, result, factor_token);
            result = if infix_at(i) && infix_at(i + 1) {
                quote! { ( #step ) }
            } else {
                step
            };
        }
        Ok(result)
    }

    /// Returns an argument's expression for a position its context already
    /// delimits, a function argument or the inside of parentheses the
    /// caller adds: [`Self::arithmetic_to_token`]'s, except that a lone group
    /// or cast keeps none of the parentheses it carries as an operand.
    pub(super) fn arithmetic_to_argument<F>(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        if expr.rest().is_empty() {
            self.factor_to_argument(expr.init(), string_intern, resolve_var)
        } else {
            self.arithmetic_to_token(expr, string_intern, resolve_var)
        }
    }

    /// Returns a factor as a whole function argument: a group's expression
    /// and a cast bare, any other factor as [`Self::factor_to_token`] does.
    fn factor_to_argument<F>(
        &mut self,
        factor: &FactorArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        match factor {
            FactorArgument::Group(a) => self.arithmetic_to_argument(a, string_intern, resolve_var),
            FactorArgument::Builtin { op, args } => {
                self.builtin_to_token(*op, args, string_intern, resolve_var)
            }
            FactorArgument::Var(_)
            | FactorArgument::Const(_)
            | FactorArgument::FnCall { .. }
            | FactorArgument::Tuple { .. }
            | FactorArgument::TupleProj { .. } => {
                self.factor_to_token(factor, string_intern, resolve_var)
            }
        }
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
                let call = self.builtin_to_token(*op, args, string_intern, resolve_var)?;
                Ok(if lowers_to_cast(*op) {
                    quote! { (#call) }
                } else {
                    call
                })
            }
            FactorArgument::Group(a) => {
                let inner = self.arithmetic_to_token(a, string_intern, resolve_var)?;
                Ok(as_operand(a, inner))
            }
            FactorArgument::Tuple { fields } => {
                let field_toks = fields
                    .iter()
                    .map(|f| self.arithmetic_to_token(f, string_intern, resolve_var))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(tuple_tokens(field_toks))
            }
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.arithmetic_to_token(tuple, string_intern, resolve_var)?;
                Ok(tuple_field(tuple, rec, *index))
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
                let inner = self.arithmetic_to_token(a, string_intern, resolve_var)?;
                Ok(as_operand(a, inner))
            }
            // A projected field in a `cat` is a string; its interned key
            // resolves to text as the `Var` arm's does.
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.arithmetic_to_token(tuple, string_intern, resolve_var)?;
                let proj = tuple_field(tuple, rec, *index);
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
    /// its variables read from the row pattern's `fields`, whose columns
    /// are `input_type`'s values.
    pub(crate) fn row_arithmetic(
        &mut self,
        expr: &ArithmeticArgument,
        fields: &[Ident],
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        self.arithmetic_to_token(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((_, idx)) => {
                let ident = fields.get(*idx).ok_or_else(|| {
                    CodegenError::internal(format!(
                        "row index {idx} out of bounds (row arity {})",
                        fields.len()
                    ))
                })?;
                read_slot(quote! { #ident }, input_type, false, *idx, string_intern)
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
    pub(crate) fn kv_arithmetic(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        input_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        self.arithmetic_to_token(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((is_key, idx))
            | TransformationArgument::Jn((_, is_key, idx)) => {
                let i = Index::from(*idx);
                let side = if *is_key {
                    quote! { k }
                } else {
                    quote! { v }
                };
                read_slot(
                    quote! { #side.#i },
                    input_type,
                    *is_key,
                    *idx,
                    string_intern,
                )
            }
        })
    }

    /// Returns an argument's arithmetic expression inside a join closure,
    /// its variables read from the `(k, lv, rv)` bindings: the key and the
    /// left values typed by `left_type`, the right values by `right_type`.
    pub(crate) fn join_arithmetic(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        left_type: &KvTypes,
        right_type: &KvTypes,
    ) -> Result<TokenStream, CodegenError> {
        self.arithmetic_to_token(expr, string_intern, &|arg| match arg {
            TransformationArgument::Jn((is_left, is_key, idx)) => {
                let i = Index::from(*idx);
                let (side, types) = match (is_left, is_key) {
                    (_, true) => (quote! { k }, left_type),
                    (true, false) => (quote! { lv }, left_type),
                    (false, false) => (quote! { rv }, right_type),
                };
                // Join parameters are references; an expression yields an
                // owned value, copied or cloned out.
                read_slot(quote! { #side.#i }, types, *is_key, *idx, string_intern)
            }
            TransformationArgument::KV(_) => Err(CodegenError::internal(format!(
                "key-value argument {arg:?} in a join expression"
            ))),
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::BuiltinOperator;
    use flowlog_parser::Constant;
    use flowlog_planner::planner::TransformationArgument::Jn;
    use flowlog_planner::planner::TransformationArgument::KV;
    use quote::format_ident;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

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

    /// `strlen` of the value column `v.<idx>`.
    fn strlen(idx: usize) -> FactorArgument {
        FactorArgument::Builtin {
            op: BuiltinOperator::Strlen,
            args: vec![expr(value(idx), Vec::new())],
        }
    }

    /// Lowers `expr` in a key-value closure over one `int32` key and three
    /// `int32` values.
    fn kv_tokens(expr: &ArithmeticArgument) -> String {
        let input_type: KvTypes = (vec![DataType::Int32], vec![DataType::Int32; 3]);
        codegen("")
            .kv_arithmetic(expr, false, &input_type)
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
        let input_type: KvTypes = (
            Vec::new(),
            vec![DataType::Int32, DataType::Int32, DataType::String],
        );
        let tokens = codegen("")
            .row_arithmetic(&expr(value(2), Vec::new()), &fields, false, &input_type)
            .expect("row variable");
        assert_eq!(tokens.to_string(), quote! { x2.clone() }.to_string());
    }

    // Cases: column type, string interning, expression.
    #[rstest]
    #[case::integer(DataType::Int32, false, quote! { v.0 })]
    #[case::string(DataType::String, false, quote! { v.0.clone() })]
    #[case::interned_string(DataType::String, true, quote! { v.0 })]
    #[case::tuple_with_string(
        DataType::FixedTuple(vec![DataType::Int32, DataType::String]),
        false,
        quote! { v.0.clone() }
    )]
    fn a_variable_is_cloned_only_when_its_column_is_not_copy(
        #[case] column: DataType,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        let input_type: KvTypes = (Vec::new(), vec![column]);
        let tokens = codegen("")
            .kv_arithmetic(&expr(value(0), Vec::new()), string_intern, &input_type)
            .expect("kv variable");
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    // Cases: variable, expression.
    #[rstest]
    #[case(KV((true, 0)), quote! { k.0 })]
    #[case(KV((false, 1)), quote! { v.1 })]
    #[case::antijoin_key(Jn((true, true, 0)), quote! { k.0 })]
    #[case::antijoin_value(Jn((false, false, 1)), quote! { v.1 })]
    fn a_kv_variable_reads_its_side(
        #[case] arg: TransformationArgument,
        #[case] expected: TokenStream,
    ) {
        let arg = expr(FactorArgument::Var(arg), Vec::new());
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    // Cases: variable (is_left, is_key, index), expression.
    #[rstest]
    #[case(Jn((true, true, 0)), quote! { k.0 })]
    #[case(Jn((false, true, 0)), quote! { k.0 })]
    #[case(Jn((true, false, 1)), quote! { lv.1 })]
    #[case(Jn((false, false, 2)), quote! { rv.2 })]
    fn a_join_variable_reads_its_side(
        #[case] arg: TransformationArgument,
        #[case] expected: TokenStream,
    ) {
        let left_type: KvTypes = (vec![DataType::Int32], vec![DataType::Int32; 2]);
        let right_type: KvTypes = (vec![DataType::Int32], vec![DataType::Int32; 3]);
        let tokens = codegen("")
            .join_arithmetic(
                &expr(FactorArgument::Var(arg), Vec::new()),
                false,
                &left_type,
                &right_type,
            )
            .expect("join variable");
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    // --- Operators and the fold ---

    // Cases: operator, `v.0 op v.1`.
    #[rstest]
    #[case(
        ArithmeticOperator::Plus,
        quote! { ::flowlog_runtime::arith::add(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::Minus,
        quote! { ::flowlog_runtime::arith::sub(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::Multiply,
        quote! { ::flowlog_runtime::arith::mul(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::Divide,
        quote! { ::flowlog_runtime::arith::div(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::Modulo,
        quote! { ::flowlog_runtime::arith::rem(v.0, v.1) }
    )]
    #[case(ArithmeticOperator::BitAnd, quote! { v.0 & v.1 })]
    #[case(ArithmeticOperator::BitOr, quote! { v.0 | v.1 })]
    #[case(ArithmeticOperator::BitXor, quote! { v.0 ^ v.1 })]
    #[case(
        ArithmeticOperator::Power,
        quote! { ::flowlog_runtime::arith::pow(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::ShiftLeft,
        quote! { ::flowlog_runtime::arith::bshl(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::ShiftRight,
        quote! { ::flowlog_runtime::arith::bshr(v.0, v.1) }
    )]
    #[case(
        ArithmeticOperator::ShiftRightUnsigned,
        quote! { ::flowlog_runtime::arith::bshru(v.0, v.1) }
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
        [ArithmeticOperator::BitXor, ArithmeticOperator::BitXor],
        quote! { (v.0 ^ v.1) ^ v.2 }
    )]
    #[case(
        [ArithmeticOperator::Power, ArithmeticOperator::BitOr],
        quote! { ::flowlog_runtime::arith::pow(v.0, v.1) | v.2 }
    )]
    fn the_fold_keeps_its_left_to_right_order(
        #[case] ops: [ArithmeticOperator; 2],
        #[case] expected: TokenStream,
    ) {
        let [first, second] = ops;
        let arg = expr(value(0), vec![(first, value(1)), (second, value(2))]);
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    // Cases: operator inside `(v.0 op v.1) & v.2`, expression.
    #[rstest]
    #[case(
        ArithmeticOperator::BitOr,
        quote! { (v.0 | v.1) & v.2 }
    )]
    #[case(
        ArithmeticOperator::Power,
        quote! { ::flowlog_runtime::arith::pow(v.0, v.1) & v.2 }
    )]
    fn a_group_is_parenthesized_unless_it_ends_in_a_call(
        #[case] op: ArithmeticOperator,
        #[case] expected: TokenStream,
    ) {
        let group = FactorArgument::Group(Box::new(expr(value(0), vec![(op, value(1))])));
        let arg = expr(group, vec![(ArithmeticOperator::BitAnd, value(2))]);
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    /// A cast or a group needs parentheses as an operand but takes none as
    /// a whole call argument, where Rust's `unused_parens` lint rejects
    /// them. A lone cast stays parenthesized for the context it lands in.
    // Cases: expression, lowering.
    #[rstest]
    #[case::cast_as_argument(
        expr(strlen(0), vec![(ArithmeticOperator::Plus, value(1))]),
        quote! { ::flowlog_runtime::arith::add(v.0.as_str().chars().count() as i32, v.1) }
    )]
    #[case::cast_as_operand(
        expr(strlen(0), vec![(ArithmeticOperator::BitAnd, value(1))]),
        quote! { (v.0.as_str().chars().count() as i32) & v.1 }
    )]
    #[case::lone_cast(expr(strlen(0), Vec::new()), quote! { (v.0.as_str().chars().count() as i32) })]
    #[case::group_as_argument(
        expr(
            FactorArgument::Group(Box::new(expr(value(0), vec![(ArithmeticOperator::BitOr, value(1))]))),
            vec![(ArithmeticOperator::Plus, value(2))]
        ),
        quote! { ::flowlog_runtime::arith::add(v.0 | v.1, v.2) }
    )]
    #[case::infix_step_as_argument(
        expr(
            value(0),
            vec![(ArithmeticOperator::BitOr, value(1)), (ArithmeticOperator::Plus, value(2))]
        ),
        quote! { ::flowlog_runtime::arith::add(v.0 | v.1, v.2) }
    )]
    fn a_call_argument_takes_no_parentheses(
        #[case] arg: ArithmeticArgument,
        #[case] expected: TokenStream,
    ) {
        assert_eq!(kv_tokens(&arg), expected.to_string());
    }

    #[test]
    fn a_tuple_projection_reads_the_field() {
        let proj = FactorArgument::TupleProj {
            tuple: Box::new(expr(value(0), Vec::new())),
            index: 1,
        };
        let input_type: KvTypes = (
            Vec::new(),
            vec![DataType::FixedTuple(vec![
                DataType::Int32,
                DataType::String,
            ])],
        );
        let tokens = codegen("")
            .kv_arithmetic(&expr(proj, Vec::new()), false, &input_type)
            .expect("tuple projection");
        assert_eq!(tokens.to_string(), quote! { v.0.clone().1 }.to_string());
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
        quote! { ::flowlog_runtime::intern::resolve(v.0.clone().1) }
    )]
    fn a_cat_factor_lowers_to_text(
        #[case] factor: FactorArgument,
        #[case] string_intern: bool,
        #[case] expected: TokenStream,
    ) {
        let tokens = codegen("")
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
