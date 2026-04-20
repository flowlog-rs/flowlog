//! Argument / predicate / expression codegen.
//!
//! Lowers the planner's `ArithmeticArgument` / `ComparisonExprArgument` /
//! `FnCallPredicateArgument` / `Constraints` into the Rust token streams
//! embedded in closures, predicates, and key-value builders across every
//! flow operator (row / KV / join-core).

use std::cell::RefCell;
use std::collections::HashMap;

use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::Index;

use parser::{ArithmeticOperator, ComparisonOperator, ConstType, DataType};
use planner::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument,
    FnCallPredicateArgument, TransformationArgument,
};

use crate::codegen::error::CodegenError;
use crate::codegen::ty::data::tuple_tokens;
use crate::codegen::CodeGen;

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
// Multi-use variable detection
// ==================================================

/// Row field → total reference count across the argument lists. Callers
/// decrement as they emit uses and skip the final `.clone()` when count
/// hits 1 (no future uses).
pub(super) fn row_use_counts(arg_lists: &[&[ArithmeticArgument]]) -> HashMap<usize, usize> {
    arg_lists
        .iter()
        .flat_map(|args| args.iter())
        .flat_map(|arg| arg.transformation_arguments())
        .filter_map(|ta| match ta {
            TransformationArgument::KV((_, idx)) => Some(*idx),
            _ => None,
        })
        .fold(HashMap::new(), |mut acc, idx| {
            *acc.entry(idx).or_insert(0) += 1;
            acc
        })
}

/// KV slot (`is_key`, idx) → total reference count, used identically to
/// [`row_use_counts`] to elide the last-use clone.
pub(super) fn kv_use_counts(arg_lists: &[&[ArithmeticArgument]]) -> HashMap<(bool, usize), usize> {
    arg_lists
        .iter()
        .flat_map(|args| args.iter())
        .flat_map(|arg| arg.transformation_arguments())
        .map(|ta| match ta {
            TransformationArgument::KV((is_key, idx))
            | TransformationArgument::Jn((_, is_key, idx)) => (*is_key, *idx),
        })
        .fold(HashMap::new(), |mut acc, key| {
            *acc.entry(key).or_insert(0) += 1;
            acc
        })
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
        remaining: Option<&RefCell<HashMap<usize, usize>>>,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.build_row_args_arithmetic_expr(arg, fields, string_intern, remaining))
            .collect::<Result<_, _>>()?;
        Ok(tuple_tokens(parts))
    }

    /// KV → KV: tuple-shaped expression drawing from the current `(k, v)`
    /// closure bindings.
    pub(super) fn build_key_val_from_kv_args(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
        remaining: Option<&RefCell<HashMap<(bool, usize), usize>>>,
    ) -> Result<TokenStream, CodegenError> {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_kv_args_arithmetic_expr(a, string_intern, remaining))
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
        if let TransformationArgument::KV((_, idx)) = arg {
            if let Some(slot) = used.get_mut(*idx) {
                *slot = true;
            }
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
                let l = self.build_kv_args_arithmetic_expr(c.left(), string_intern, None)?;
                let r = self.build_kv_args_arithmetic_expr(c.right(), string_intern, None)?;
                let op = comparison_op_tokens(c.operator());
                Ok(
                    if string_intern
                        && c.operator().is_inequality()
                        && self.infer_expr_type(c.left(), input_type, None)
                            == Some(DataType::String)
                        && self.infer_expr_type(c.right(), input_type, None)
                            == Some(DataType::String)
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

    /// `join_core`-closure comparison predicate, combined with `&&`.
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
                        && self.infer_expr_type(c.left(), left_type, Some(right_type))
                            == Some(DataType::String)
                        && self.infer_expr_type(c.right(), left_type, Some(right_type))
                            == Some(DataType::String)
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
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l =
                    self.build_row_args_arithmetic_expr(c.left(), row_fields, string_intern, None)?;
                let r = self.build_row_args_arithmetic_expr(
                    c.right(),
                    row_fields,
                    string_intern,
                    None,
                )?;
                let op = comparison_op_tokens(c.operator());
                Ok(
                    if string_intern
                        && c.operator().is_inequality()
                        && self.infer_expr_type(c.left(), input_type, None)
                            == Some(DataType::String)
                        && self.infer_expr_type(c.right(), input_type, None)
                            == Some(DataType::String)
                    {
                        self.features.mark_string_resolve();
                        quote! { resolve(#l) #op resolve(#r) }
                    } else {
                        quote! { #l #op #r }
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
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .map(|a| self.build_kv_args_arithmetic_expr(a, string_intern, None))
                    .collect::<Result<_, _>>()?;
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
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .map(|a| self.build_join_args_arithmetic_expr(a, string_intern))
                    .collect::<Result<_, _>>()?;
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
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .map(|a| {
                        self.build_row_args_arithmetic_expr(a, row_fields, string_intern, None)
                    })
                    .collect::<Result<_, _>>()?;
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
            let rhs = const_to_token(c, string_intern);
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
            let rhs = const_to_token(c, string_intern);
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
pub(crate) fn const_to_token(constant: &ConstType, string_intern: bool) -> TokenStream {
    match constant {
        ConstType::Int(n) => {
            let lit = proc_macro2::Literal::i64_unsuffixed(*n);
            quote! { #lit }
        }
        ConstType::Float(v) => {
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
    }
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
fn numeric_arithmetic_op_tokens(op: &ArithmeticOperator) -> Result<TokenStream, CodegenError> {
    Ok(match op {
        ArithmeticOperator::Plus => quote! { + },
        ArithmeticOperator::Minus => quote! { - },
        ArithmeticOperator::Multiply => quote! { * },
        ArithmeticOperator::Divide => quote! { / },
        ArithmeticOperator::Modulo => quote! { % },
        _ => {
            return Err(CodegenError::internal(format!(
                "string operator `{op}` reached numeric arithmetic builder"
            )))
        }
    })
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
        // String expr: type checking guarantees every op is Cat; batch
        // into a single `format!` call.
        if expr
            .rest()
            .first()
            .is_some_and(|(op, _)| matches!(op, ArithmeticOperator::Cat))
        {
            let mut factors =
                vec![self.factor_to_display_token(expr.init(), string_intern, resolve_var)?];
            for (_, factor) in expr.rest() {
                factors.push(self.factor_to_display_token(factor, string_intern, resolve_var)?);
            }
            return Ok(build_cat_batch(factors, string_intern));
        }

        // Numeric fold: left-to-right, parenthesising intermediate steps
        // only — the outermost expression stays bare to avoid Rust's
        // `unused_parens` lint when it's used as a function argument.
        let rest = expr.rest();
        let mut result = self.factor_to_token(expr.init(), string_intern, resolve_var)?;
        for (i, (op, factor)) in rest.iter().enumerate() {
            let op_token = numeric_arithmetic_op_tokens(op)?;
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
        let arg_tokens: Vec<TokenStream> = args
            .iter()
            .map(|a| {
                let token = self.build_arithmetic_expr(a, string_intern, resolve_var)?;
                Ok(quote! { (#token).clone() })
            })
            .collect::<Result<_, CodegenError>>()?;
        self.features.mark_udf();
        Ok(quote! { udf::#fn_ident(#(#arg_tokens),*) })
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
            FactorArgument::Const(c) => Ok(const_to_token(c, string_intern)),
            FactorArgument::FnCall { name, args } => {
                self.fncall_to_token(name, args, string_intern, resolve_var)
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
            FactorArgument::Const(c) => Ok(match c {
                // String literals are already displayable – emit them
                // directly without interning first.
                ConstType::Text(s) => quote! { #s },
                _ => const_to_token(c, string_intern),
            }),
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
        }
    }

    fn build_row_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        fields: &[Ident],
        string_intern: bool,
        remaining: Option<&RefCell<HashMap<usize, usize>>>,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((_, idx)) => {
                let ident = fields.get(*idx).ok_or_else(|| {
                    CodegenError::internal(format!(
                        "row index {idx} out of bounds (row arity {})",
                        fields.len()
                    ))
                })?;
                // Clone only when future uses remain; the last use moves.
                let needs_clone = remaining.is_some_and(|rem| {
                    rem.borrow_mut()
                        .get_mut(idx)
                        .map(|count| {
                            *count -= 1;
                            *count > 0
                        })
                        .unwrap_or(false)
                });
                Ok(if needs_clone {
                    quote! { #ident.clone() }
                } else {
                    quote! { #ident }
                })
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
        remaining: Option<&RefCell<HashMap<(bool, usize), usize>>>,
    ) -> Result<TokenStream, CodegenError> {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((is_key, idx))
            | TransformationArgument::Jn((_, is_key, idx)) => {
                let i = Index::from(*idx);
                let needs_clone = remaining.is_some_and(|rem| {
                    let mut map = rem.borrow_mut();
                    map.get_mut(&(*is_key, *idx))
                        .map(|count| {
                            *count -= 1;
                            *count > 0
                        })
                        .unwrap_or(false)
                });
                Ok(match (is_key, needs_clone) {
                    (true, true) => quote! { k.#i.clone() },
                    (true, false) => quote! { k.#i },
                    (false, true) => quote! { v.#i.clone() },
                    (false, false) => quote! { v.#i },
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
