//! Argument / predicate / expression codegen.
//!
//! Lowers the planner's `ArithmeticArgument` / `ComparisonExprArgument` /
//! `FnCallPredicateArgument` / `Constraints` into the Rust token streams
//! embedded in closures, predicates, and key-value builders across every
//! flow operator (row / KV / join-core).

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::Index;

use crate::parser::{ArithmeticOperator, BuiltinOperator, ComparisonOperator, ConstType, DataType};
use crate::planner::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument,
    FnCallPredicateArgument, TransformationArgument,
};

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::tuple_tokens;

// ==================================================
// Row pattern + field identifiers for RowToX transformations
// ==================================================

/// Build a row destructuring pattern `(x0, x1, …)` and the matching field
/// idents. Unused slots are prefixed with `_` so the borrow checker and the
/// unused-variable lint stay quiet.
pub(super) fn row_pattern_and_fields(
    arity: usize,
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
    constraints: &Constraints,
) -> (TokenStream, Vec<Ident>) {
    if arity == 0 {
        return (quote! { () }, Vec::new());
    }

    let used = compute_row_params_tokens(
        arity,
        key_args,
        value_args,
        compares,
        fn_call_preds,
        constraints,
    );

    let fields: Vec<Ident> = (0..arity)
        .map(|idx| {
            if used[idx] {
                format_ident!("x{}", idx)
            } else {
                format_ident!("_x{}", idx)
            }
        })
        .collect();

    let pat = tuple_tokens(fields.iter().map(|f| quote! { #f }));
    (pat, fields)
}

// ==================================================
// Tuple builder utilities
// ==================================================

impl CodeGen {
    /// Row → KV: tuple-shaped expression for the produced key or value,
    /// drawing source fields by index from the row destructuring pattern.
    pub(super) fn build_key_val_from_row_args(
        &mut self,
        args: &[ArithmeticArgument],
        fields: &[Ident],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.build_row_args_arithmetic_expr(arg, fields, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }

    /// KV → KV: tuple-shaped expression drawing from the current `(k, v)`
    /// closure bindings.
    pub(super) fn build_key_val_from_kv_args(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_kv_args_arithmetic_expr(a, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }

    /// Join → KV: tuple-shaped expression drawing from a `join_core`
    /// closure's `(k, lv, rv)` bindings.
    pub(super) fn build_key_val_from_join_args(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_join_args_arithmetic_expr(a, string_intern))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }
}

// ==================================================
// Closure parameter utilities
// ==================================================

/// Per-index usage mask for a row of `arity` slots — `true` where any of
/// the given argument lists references that slot. Feeds
/// [`row_pattern_and_fields`] so unused slots become `_x<i>`.
fn compute_row_params_tokens(
    arity: usize,
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
    constraints: &Constraints,
) -> Vec<bool> {
    let mut used = vec![false; arity];

    let mut mark = |arg: &TransformationArgument| {
        if let TransformationArgument::KV((_, idx)) = arg
            && let Some(slot) = used.get_mut(*idx)
        {
            *slot = true;
        }
    };

    let mut inspect = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark(trans_arg);
        }
    };

    for expr in key_args.iter().chain(value_args.iter()) {
        inspect(expr);
    }
    for cmp in compares {
        inspect(cmp.left());
        inspect(cmp.right());
    }
    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect(arg);
        }
    }
    for (arg, _) in constraints.constant_eq_constraints().as_ref().iter() {
        mark(arg);
    }
    for (left, right) in constraints.variable_eq_constraints().as_ref().iter() {
        mark(left);
        mark(right);
    }

    used
}

/// Emit `name` when `used`, otherwise `_name` — so unused closure
/// parameters don't trigger the `unused_variables` lint in generated code.
fn param_ident(used: bool, name: &str) -> TokenStream {
    let id = format_ident!("{}{}", if used { "" } else { "_" }, name);
    quote! { #id }
}

/// `join_core` closure idents `(k, lv, rv)`.
pub(super) fn compute_join_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
) -> (TokenStream, TokenStream, TokenStream) {
    let (mut use_k, mut use_lv, mut use_rv) = (false, false, false);

    let mut mark = |arg: &TransformationArgument| {
        if let TransformationArgument::Jn((is_left, is_key, _)) = arg {
            if *is_key {
                use_k = true;
            } else if *is_left {
                use_lv = true;
            } else {
                use_rv = true;
            }
        }
    };

    let mut inspect = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark(trans_arg);
        }
    };

    for a in key_args.iter().chain(value_args.iter()) {
        inspect(a);
    }
    for cmp in compares {
        inspect(cmp.left());
        inspect(cmp.right());
    }
    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect(arg);
        }
    }

    (
        param_ident(use_k, "k"),
        param_ident(use_lv, "lv"),
        param_ident(use_rv, "rv"),
    )
}

/// KV closure idents `(k, v)` for `flat_map` / `flat_map_ref`.
pub(super) fn compute_kv_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
    constraints: Option<&Constraints>,
) -> (TokenStream, TokenStream) {
    let (mut use_k, mut use_v) = (false, false);

    let mut mark = |arg: &TransformationArgument| {
        let is_key = match arg {
            TransformationArgument::KV((is_key, _))
            | TransformationArgument::Jn((_, is_key, _)) => *is_key,
        };
        if is_key {
            use_k = true;
        } else {
            use_v = true;
        }
    };

    let mut inspect = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark(trans_arg);
        }
    };

    for arg in key_args.iter().chain(value_args.iter()) {
        inspect(arg);
    }
    for cmp in compares {
        inspect(cmp.left());
        inspect(cmp.right());
    }
    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect(arg);
        }
    }
    if let Some(cons) = constraints {
        for (arg, _) in cons.constant_eq_constraints().as_ref().iter() {
            mark(arg);
        }
        for (left, right) in cons.variable_eq_constraints().as_ref().iter() {
            mark(left);
            mark(right);
        }
    }

    (param_ident(use_k, "k"), param_ident(use_v, "v"))
}

// ==================================================
// Comparison expression predicate builders
// ==================================================

fn comparison_op_tokens(op: &ComparisonOperator) -> TokenStream {
    match op {
        ComparisonOperator::Equal => quote! { == },
        ComparisonOperator::NotEqual => quote! { != },
        ComparisonOperator::GreaterThan => quote! { > },
        ComparisonOperator::GreaterEqualThan => quote! { >= },
        ComparisonOperator::LessThan => quote! { < },
        ComparisonOperator::LessEqualThan => quote! { <= },
    }
}

impl CodeGen {
    /// KV-closure comparison predicate, combined with `&&`.
    pub(super) fn build_kv_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        input_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Result<Option<TokenStream>, CodegenError> {
        if comps.is_empty() {
            return Ok(None);
        }
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l = self.build_kv_args_arithmetic_expr(c.left(), string_intern)?;
                let r = self.build_kv_args_arithmetic_expr(c.right(), string_intern)?;
                let op = comparison_op_tokens(c.operator());
                Ok(
                    if string_intern
                        && c.operator().is_inequality()
                        && self.infer_expr_type(c.left(), input_type, None)? == DataType::String
                        && self.infer_expr_type(c.right(), input_type, None)? == DataType::String
                    {
                        self.features.mark_string_resolve();
                        quote! { resolve(#l) #op resolve(#r) }
                    } else {
                        quote! { (#l) #op (#r) }
                    },
                )
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }

    /// `join_core`-closure comparison predicate, combined with `&&`. The join
    /// operand builder always clones (`k`/`lv`/`rv` are references).
    pub(super) fn build_join_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Result<Option<TokenStream>, CodegenError> {
        if comps.is_empty() {
            return Ok(None);
        }
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l = self.build_join_args_arithmetic_expr(c.left(), string_intern)?;
                let r = self.build_join_args_arithmetic_expr(c.right(), string_intern)?;
                let op = comparison_op_tokens(c.operator());
                Ok(
                    if string_intern
                        && c.operator().is_inequality()
                        && self.infer_expr_type(c.left(), left_type, Some(right_type))?
                            == DataType::String
                        && self.infer_expr_type(c.right(), left_type, Some(right_type))?
                            == DataType::String
                    {
                        self.features.mark_string_resolve();
                        quote! { resolve(#l) #op resolve(#r) }
                    } else {
                        quote! { (#l) #op (#r) }
                    },
                )
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }

    /// Row-closure comparison predicate, combined with `&&`.
    pub(super) fn build_row_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        row_fields: &[Ident],
        string_intern: bool,
        input_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Result<Option<TokenStream>, CodegenError> {
        if comps.is_empty() {
            return Ok(None);
        }
        // Operands are always cloned (see `build_kv_compare_predicate`): keeps
        // tuple-literal operands move-safe and the three compare paths uniform;
        // free for `Copy`/interned leaves.
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l = self.build_row_args_arithmetic_expr(c.left(), row_fields, string_intern)?;
                let r = self.build_row_args_arithmetic_expr(c.right(), row_fields, string_intern)?;
                let op = comparison_op_tokens(c.operator());
                Ok(
                    if string_intern
                        && c.operator().is_inequality()
                        && self.infer_expr_type(c.left(), input_type, None)? == DataType::String
                        && self.infer_expr_type(c.right(), input_type, None)? == DataType::String
                    {
                        self.features.mark_string_resolve();
                        quote! { resolve(#l) #op resolve(#r) }
                    } else {
                        quote! { (#l) #op (#r) }
                    },
                )
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }
}

// ==================================================
// FnCall predicate builders
// ==================================================

impl CodeGen {
    /// KV-closure UDF-call predicate, combined with `&&`.
    pub(super) fn build_kv_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        string_intern: bool,
    ) -> Result<Option<TokenStream>, CodegenError> {
        if fn_calls.is_empty() {
            return Ok(None);
        }
        let parts: Vec<TokenStream> = fn_calls
            .iter()
            .map(|fc| {
                let fn_name = format_ident!("{}", fc.name());
                let param_types = self.udf_param_types(fc.name());
                if string_intern && param_types.contains(&DataType::String) {
                    self.features.mark_string_resolve();
                }
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .enumerate()
                    .map(|(i, a)| {
                        let token = self.build_kv_args_arithmetic_expr(a, string_intern)?;
                        Ok(wrap_udf_arg(
                            token,
                            param_type_at(&param_types, i),
                            string_intern,
                        ))
                    })
                    .collect::<Result<_, CodegenError>>()?;
                Ok(if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                })
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }

    /// `join_core`-closure UDF-call predicate, combined with `&&`.
    pub(super) fn build_join_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        string_intern: bool,
    ) -> Result<Option<TokenStream>, CodegenError> {
        if fn_calls.is_empty() {
            return Ok(None);
        }
        let parts: Vec<TokenStream> = fn_calls
            .iter()
            .map(|fc| {
                let fn_name = format_ident!("{}", fc.name());
                let param_types = self.udf_param_types(fc.name());
                if string_intern && param_types.contains(&DataType::String) {
                    self.features.mark_string_resolve();
                }
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .enumerate()
                    .map(|(i, a)| {
                        let token = self.build_join_args_arithmetic_expr(a, string_intern)?;
                        Ok(wrap_udf_arg(
                            token,
                            param_type_at(&param_types, i),
                            string_intern,
                        ))
                    })
                    .collect::<Result<_, CodegenError>>()?;
                Ok(if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                })
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }

    /// Row-closure UDF-call predicate, combined with `&&`.
    pub(super) fn build_row_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        row_fields: &[Ident],
        string_intern: bool,
    ) -> Result<Option<TokenStream>, CodegenError> {
        if fn_calls.is_empty() {
            return Ok(None);
        }
        let parts: Vec<TokenStream> = fn_calls
            .iter()
            .map(|fc| {
                let fn_name = format_ident!("{}", fc.name());
                let param_types = self.udf_param_types(fc.name());
                if string_intern && param_types.contains(&DataType::String) {
                    self.features.mark_string_resolve();
                }
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .enumerate()
                    .map(|(i, a)| {
                        let token =
                            self.build_row_args_arithmetic_expr(a, row_fields, string_intern)?;
                        Ok(wrap_udf_arg(
                            token,
                            param_type_at(&param_types, i),
                            string_intern,
                        ))
                    })
                    .collect::<Result<_, CodegenError>>()?;
                Ok(if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                })
            })
            .collect::<Result<_, CodegenError>>()?;

        Ok(Some(quote! { #( #parts )&&* }))
    }
}

// ==================================================
// Constraint predicate builders
// ==================================================

/// Combined `==` predicate for KV const-eq and var-eq constraints.
pub(super) fn build_kv_constraints_predicate(
    constraints: &Constraints,
    string_intern: bool,
) -> Result<Option<TokenStream>, CodegenError> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_kv_expr(arg)?;
            let rhs = const_to_token(c, string_intern)?;
            Ok(quote! { #lhs == #rhs })
        })
        .collect::<Result<_, CodegenError>>()?;

    let var_parts = constraints
        .variable_eq_constraints()
        .as_ref()
        .iter()
        .map(|(l, r)| {
            let lhs = trans_arg_to_kv_expr(l)?;
            let rhs = trans_arg_to_kv_expr(r)?;
            Ok(quote! { #lhs == #rhs })
        })
        .collect::<Result<Vec<_>, CodegenError>>()?;
    parts.extend(var_parts);

    Ok((!parts.is_empty()).then(|| quote! { #( #parts )&&* }))
}

/// Combined `==` predicate for row const-eq and var-eq constraints.
pub(super) fn build_row_constraints_predicate(
    constraints: &Constraints,
    row_fields: &[Ident],
    string_intern: bool,
) -> Result<Option<TokenStream>, CodegenError> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_row_expr(arg, row_fields)?;
            let rhs = const_to_token(c, string_intern)?;
            Ok(quote! { #lhs == #rhs })
        })
        .collect::<Result<_, CodegenError>>()?;

    let var_parts = constraints
        .variable_eq_constraints()
        .as_ref()
        .iter()
        .map(|(l, r)| {
            let lhs = trans_arg_to_row_expr(l, row_fields)?;
            let rhs = trans_arg_to_row_expr(r, row_fields)?;
            Ok(quote! { #lhs == #rhs })
        })
        .collect::<Result<Vec<_>, CodegenError>>()?;
    parts.extend(var_parts);

    Ok((!parts.is_empty()).then(|| quote! { #( #parts )&&* }))
}

// ==================================================
// Constraint helpers
// ==================================================

/// Lower a parsed constant to the internal tuple-slot expression —
/// wraps floats in `OrderedFloat`, interns strings when enabled.
///
/// Every numeric variant emits an unsuffixed literal so Rust's own
/// inference picks the matching width from the enclosing tuple type.
/// Returns `CodegenError::internal` for polymorphic `Int` / `Float` — the
/// typechecker should have pinned those to a concrete width first.
pub fn const_to_token(
    constant: &ConstType,
    string_intern: bool,
) -> Result<TokenStream, CodegenError> {
    Ok(match constant {
        ConstType::Int(_) | ConstType::Float(_) => {
            return Err(CodegenError::internal(format!(
                "polymorphic literal {constant:?} reached codegen; \
                 typechecker should have pinned it"
            )));
        }
        ConstType::Int8(n) => {
            let lit = proc_macro2::Literal::i8_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::Int16(n) => {
            let lit = proc_macro2::Literal::i16_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::Int32(n) => {
            let lit = proc_macro2::Literal::i32_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::Int64(n) => {
            let lit = proc_macro2::Literal::i64_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::UInt8(n) => {
            let lit = proc_macro2::Literal::u8_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::UInt16(n) => {
            let lit = proc_macro2::Literal::u16_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::UInt32(n) => {
            let lit = proc_macro2::Literal::u32_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::UInt64(n) => {
            let lit = proc_macro2::Literal::u64_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::Float32(v) => {
            let lit = proc_macro2::Literal::f32_unsuffixed(v.into_inner());
            quote! { OrderedFloat(#lit) }
        }
        ConstType::Float64(v) => {
            let lit = proc_macro2::Literal::f64_unsuffixed(v.into_inner());
            quote! { OrderedFloat(#lit) }
        }
        ConstType::Text(s) => {
            if string_intern {
                quote! { intern(#s) }
            } else {
                quote! { #s.to_string() }
            }
        }
        ConstType::Bool(b) => quote! { #b },
    })
}

fn trans_arg_to_kv_expr(arg: &TransformationArgument) -> Result<TokenStream, CodegenError> {
    match arg {
        TransformationArgument::KV((is_key, idx)) => {
            let i = Index::from(*idx);
            Ok(if *is_key {
                quote! { k.#i }
            } else {
                quote! { v.#i }
            })
        }
        _ => Err(CodegenError::internal(format!(
            "non-KV transformation argument ({arg:?}) in KV-constraint builder"
        ))),
    }
}

fn trans_arg_to_row_expr(
    arg: &TransformationArgument,
    fields: &[Ident],
) -> Result<TokenStream, CodegenError> {
    match arg {
        TransformationArgument::KV((_, idx)) => {
            let ident = fields.get(*idx).ok_or_else(|| {
                CodegenError::internal(format!(
                    "row index {idx} out of bounds (row arity {})",
                    fields.len()
                ))
            })?;
            Ok(quote! { #ident })
        }
        _ => Err(CodegenError::internal(format!(
            "non-KV transformation argument ({arg:?}) in row-constraint builder"
        ))),
    }
}

// ==================================================
// Predicate composition utilities
// ==================================================

/// Fold the non-`None` predicates with `&&`.
pub(super) fn combine_predicates(preds: Vec<Option<TokenStream>>) -> Option<TokenStream> {
    preds
        .into_iter()
        .flatten()
        .reduce(|a, b| quote! { (#a) && (#b) })
}

// ==================================================
// Arithmetic expression helpers
// ==================================================
fn numeric_arithmetic_op_tokens(op: &ArithmeticOperator) -> TokenStream {
    match op {
        ArithmeticOperator::Plus => quote! { + },
        ArithmeticOperator::Minus => quote! { - },
        ArithmeticOperator::Multiply => quote! { * },
        ArithmeticOperator::Divide => quote! { / },
        ArithmeticOperator::Modulo => quote! { % },
    }
}

/// Build a batched `cat` (string concatenation) from a list of display-ready
/// factor token streams.  Every factor must already expand to something that
/// implements `Display` (i.e. `&str` / `String`, *not* a raw `Spur`).
///
/// When `string_intern` is true the result is re-interned.
fn build_cat_batch(factors: Vec<TokenStream>, string_intern: bool) -> TokenStream {
    debug_assert!(factors.len() >= 2, "cat requires at least 2 factors");
    let fmt_str = "{}".repeat(factors.len());
    if string_intern {
        quote! { intern(&format!(#fmt_str, #(#factors),*)) }
    } else {
        quote! { format!(#fmt_str, #(#factors),*) }
    }
}

/// Generic arithmetic expression builder.
///
/// The only thing that varies across row / kv / join contexts is how a
/// `TransformationArgument` is lowered to a `TokenStream`.
impl CodeGen {
    fn build_arithmetic_expr<F>(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        // Numeric fold: left-to-right, parenthesising intermediate steps
        // only — the outermost expression stays bare to avoid Rust's
        // `unused_parens` lint when it's used as a function argument.
        let rest = expr.rest();
        let mut result = self.factor_to_token(expr.init(), string_intern, resolve_var)?;
        for (i, (op, factor)) in rest.iter().enumerate() {
            let op_token = numeric_arithmetic_op_tokens(op);
            let factor_token = self.factor_to_token(factor, string_intern, resolve_var)?;
            result = if i < rest.len() - 1 {
                quote! { ( #result #op_token #factor_token ) }
            } else {
                quote! { #result #op_token #factor_token }
            };
        }
        Ok(result)
    }

    /// Generate a UDF call token stream: `udf::fn_name(arg1.clone(), arg2.clone(), ...)`.
    ///
    /// UDFs take ownership of their arguments. We clone each arg to avoid
    /// invalidating variables that may be used elsewhere in the same closure
    /// (e.g., in the output tuple or another predicate). Clone on Copy types is free.
    ///
    /// Under `--str-intern`, FlowLog string columns are passed around as
    /// interned `Spur` handles, but user UDFs are written in plain Rust and
    /// declare their string parameters/returns as `String`. We bridge the
    /// boundary the same way `builtin_op_to_token` does for built-ins:
    /// `Spur` args are wrapped with `resolve(...).to_string()`, and a
    /// `String` return is re-interned with `intern(&...)`.
    fn fncall_to_token<F>(
        &mut self,
        name: &str,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        let fn_ident = format_ident!("{}", name);

        let param_types = self.udf_param_types(name);
        let arg_tokens: Vec<TokenStream> = args
            .iter()
            .enumerate()
            .map(|(i, a)| {
                let token = self.build_arithmetic_expr(a, string_intern, resolve_var)?;
                Ok(wrap_udf_arg(
                    token,
                    param_type_at(&param_types, i),
                    string_intern,
                ))
            })
            .collect::<Result<_, CodegenError>>()?;

        let returns_string = self
            .udf_return_type(name)
            .is_some_and(|t| t == DataType::String);

        if string_intern && param_types.contains(&DataType::String) {
            self.features.mark_string_resolve();
        }
        if string_intern && returns_string {
            self.features.mark_string_intern();
        }

        self.features.mark_udf();
        let call = quote! { udf::#fn_ident(#(#arg_tokens),*) };
        Ok(if string_intern && returns_string {
            quote! { intern(&#call) }
        } else {
            call
        })
    }

    /// Convert a factor to a token stream for numeric/general expressions.
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
                Ok(quote! { ( #inner ) })
            }
            // Tuple construct → Rust tuple literal `(a, b)` (singleton `(a,)`).
            FactorArgument::Tuple { fields } => {
                let field_toks = fields
                    .iter()
                    .map(|f| self.build_arithmetic_expr(f, string_intern, resolve_var))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(tuple_tokens(field_toks))
            }
            // Tuple projection → Rust tuple field access `(tuple).i`.
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.build_arithmetic_expr(tuple, string_intern, resolve_var)?;
                let idx = Index::from(*index);
                Ok(quote! { (#rec).#idx })
            }
        }
    }

    /// Convert a factor to a display-ready token stream for cat (string concatenation).
    fn factor_to_display_token<F>(
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
                    self.features.mark_string_resolve();
                    quote! { resolve(#var_token) }
                } else {
                    var_token
                })
            }
            FactorArgument::Const(c) => match c {
                // String literals are already displayable – emit them
                // directly without interning first.
                ConstType::Text(s) => Ok(quote! { #s }),
                _ => const_to_token(c, string_intern),
            },
            FactorArgument::FnCall { name, args } => {
                // UDF returns Spur when string_intern is on — resolve for display.
                let call = self.fncall_to_token(name, args, string_intern, resolve_var)?;
                Ok(if string_intern {
                    self.features.mark_string_resolve();
                    quote! { resolve(#call) }
                } else {
                    call
                })
            }
            FactorArgument::Builtin { op, args } => {
                let call = self.builtin_to_token(*op, args, string_intern, resolve_var)?;
                // String-returning builtins emit a Spur in intern mode;
                // resolve for display in a cat / format! context.
                Ok(if string_intern && op.ret_type() == DataType::String {
                    self.features.mark_string_resolve();
                    quote! { resolve(#call) }
                } else {
                    call
                })
            }
            FactorArgument::Group(a) => {
                // Grammar guarantees a `Group` is multi-term, hence numeric
                // (string concat is `cat`) — no display resolution needed.
                let inner = self.build_arithmetic_expr(a, string_intern, resolve_var)?;
                Ok(quote! { ( #inner ) })
            }
            // A projected field reaching a `cat`/display context is a string
            // (typecheck-enforced); resolve the interned `Spur` to display text,
            // exactly as the `Var` arm does for a bound string variable.
            FactorArgument::TupleProj { tuple, index } => {
                let rec = self.build_arithmetic_expr(tuple, string_intern, resolve_var)?;
                let idx = Index::from(*index);
                let proj = quote! { (#rec).#idx };
                Ok(if string_intern {
                    self.features.mark_string_resolve();
                    quote! { resolve(#proj) }
                } else {
                    proj
                })
            }
            // A whole tuple is not a string, so the typechecker rejects it in a
            // `cat` context; reuse the value lowering to stay total (unreached).
            FactorArgument::Tuple { .. } => {
                self.factor_to_token(factor, string_intern, resolve_var)
            }
        }
    }

    /// Inline lowering for an engine built-in (Soufflé-style intrinsic).
    /// Each op emits its own Rust template; no `udf::` indirection.
    fn builtin_to_token<F>(
        &mut self,
        op: BuiltinOperator,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> Result<TokenStream, CodegenError>
    where
        F: Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
    {
        // `cat(a, b)` wants display-ready tokens (the inner factors
        // must lower to something `Display`-able for `format!`).
        // Other built-ins want raw value tokens — handled below.
        if matches!(op, BuiltinOperator::Cat) {
            debug_assert_eq!(args.len(), 2, "cat() arity is enforced at parse time");
            // After typecheck, a string-typed `cat` arg has no
            // arithmetic `rest`; only Cat could produce a compound
            // string, and Cat is itself a built-in factor.
            let factors: Vec<TokenStream> = args
                .iter()
                .map(|a| {
                    debug_assert!(
                        a.rest.is_empty(),
                        "cat() arg is a single factor after typecheck"
                    );
                    self.factor_to_display_token(&a.init, string_intern, resolve_var)
                })
                .collect::<Result<_, _>>()?;
            return Ok(build_cat_batch(factors, string_intern));
        }

        let raw: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_arithmetic_expr(a, string_intern, resolve_var))
            .collect::<Result<_, _>>()?;

        // Avoid marking `resolve` for ops that never read a string param
        // (e.g. `to_string(n: int)`), otherwise an unused helper leaks
        // into the generated binary.
        if string_intern && op.param_types().contains(&DataType::String) {
            self.features.mark_string_resolve();
        }

        let read_str = |t: &TokenStream| -> TokenStream {
            if string_intern {
                quote! { resolve(#t) }
            } else {
                quote! { (#t).as_str() }
            }
        };
        let emit_string = |body: TokenStream| -> TokenStream {
            if string_intern {
                quote! { intern(&#body) }
            } else {
                body
            }
        };

        match op {
            BuiltinOperator::Strlen => {
                // Char count, not byte count — Soufflé semantics.
                let s = read_str(&raw[0]);
                Ok(quote! { ((#s).chars().count() as i32) })
            }
            BuiltinOperator::Substr => {
                let s = read_str(&raw[0]);
                let start = &raw[1];
                let len = &raw[2];
                Ok(emit_string(quote! {
                    (#s).chars().skip((#start) as usize).take((#len) as usize).collect::<String>()
                }))
            }
            BuiltinOperator::Ord => {
                // Typechecker enforces `--str-intern` for `ord`, so the
                // arg is a `Spur` here and its u32 key serves as the
                // opaque per-symbol id.
                debug_assert!(string_intern);
                let s = &raw[0];
                Ok(quote! { ((#s).into_inner().get() as i32) })
            }
            BuiltinOperator::Contains => {
                let needle = read_str(&raw[0]);
                let hay = read_str(&raw[1]);
                Ok(quote! { ((#hay).contains(#needle)) })
            }
            BuiltinOperator::Match => {
                // Soufflé `match(pattern, s)` is a *full* match, so anchor
                // with `^(?:…)$` (the `regex` crate searches by default).
                // A malformed pattern yields `false` rather than aborting.
                // `regex` resolves via the `flowlog_runtime` re-export in
                // both binary and library modes (runtime ≥ 0.2.3).
                let hay = read_str(&raw[1]);
                // Literal pattern (the common case): anchor at codegen time
                // and compile once per call site via a `LazyLock` static.
                if let FactorArgument::Const(ConstType::Text(p)) = &args[0].init
                    && args[0].rest.is_empty()
                {
                    let anchored = format!("^(?:{p})$");
                    return Ok(quote! {{
                        static RE: ::std::sync::LazyLock<
                            Option<::flowlog_runtime::regex::Regex>,
                        > = ::std::sync::LazyLock::new(|| {
                            ::flowlog_runtime::regex::Regex::new(#anchored).ok()
                        });
                        RE.as_ref().is_some_and(|re| re.is_match(#hay))
                    }});
                }
                // Computed pattern: compile per evaluation.
                let pat = read_str(&raw[0]);
                Ok(quote! {
                    ::flowlog_runtime::regex::Regex::new(&format!("^(?:{})$", #pat))
                        .map_or(false, |re| re.is_match(#hay))
                })
            }
            BuiltinOperator::ToString => {
                let n = &raw[0];
                Ok(emit_string(quote! { (#n).to_string() }))
            }
            BuiltinOperator::ToNumber => {
                // Return 0 on parse failure to stay total — Soufflé
                // doesn't pin a behaviour for this case.
                let s = read_str(&raw[0]);
                Ok(quote! { ((#s).parse::<i32>().unwrap_or(0)) })
            }
            // `Cat` is dispatched up-front via `factor_to_display_token`
            // and never reaches this match.
            BuiltinOperator::Cat => unreachable!("cat handled early in builtin_to_token"),
        }
    }

    fn build_row_args_arithmetic_expr(
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
            _ => Err(CodegenError::internal(
                "non-KV argument in row arithmetic builder",
            )),
        })
    }

    fn build_kv_args_arithmetic_expr(
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

    fn build_join_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::Jn((is_left, is_key, idx)) => {
                let i = Index::from(*idx);
                // `k`, `lv`, `rv` are references in join_core; clone to get owned values.
                let ident = if *is_key {
                    Ident::new("k", Span::call_site())
                } else if *is_left {
                    Ident::new("lv", Span::call_site())
                } else {
                    Ident::new("rv", Span::call_site())
                };
                Ok(quote! { #ident.#i.clone() })
            }
            _ => Err(CodegenError::internal(
                "non-Jn argument in join arithmetic builder",
            )),
        })
    }
}

// ==================================================
// UDF signature helpers (shared by predicate and expression UDF lowering)
// ==================================================

impl CodeGen {
    /// Look up the declared parameter types of `.extern fn <name>(...)`.
    /// Returns an empty Vec if the name doesn't match any extern decl —
    /// this should be unreachable after the parser's UDF reclassification
    /// pass, but we tolerate it to keep codegen total.
    pub(super) fn udf_param_types(&self, name: &str) -> Vec<DataType> {
        self.program
            .udfs()
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.params().iter().map(|p| p.data_type().clone()).collect())
            .unwrap_or_default()
    }

    /// Look up the declared return type of `.extern fn <name>(...) -> T`.
    /// `None` if the name doesn't match (same caveat as above).
    pub(super) fn udf_return_type(&self, name: &str) -> Option<DataType> {
        self.program
            .udfs()
            .iter()
            .find(|f| f.name() == name)
            .map(|f| f.ret_type())
    }
}

/// Param-type lookup with graceful out-of-bounds. Variadic-style mismatches
/// shouldn't happen post-typecheck, but a panic here would be surprising.
fn param_type_at(types: &[DataType], idx: usize) -> Option<DataType> {
    types.get(idx).cloned()
}

/// Wrap a UDF argument token so it matches the declared param type at the
/// Rust call boundary. When interning is on and the param is `:string`, the
/// arg expression yields a `Spur` and must be resolved + owned into a
/// `String`. For every other case the existing `.clone()` is sufficient.
fn wrap_udf_arg(
    token: TokenStream,
    param_type: Option<DataType>,
    string_intern: bool,
) -> TokenStream {
    if string_intern && param_type == Some(DataType::String) {
        quote! { resolve((#token).clone()).to_string() }
    } else {
        quote! { (#token).clone() }
    }
}
