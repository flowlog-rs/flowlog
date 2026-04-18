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

use crate::codegen::ty::data::tuple_tokens;
use crate::codegen::CodeGen;

// ============================================================================
// Row pattern + field identifiers for RowToX transformations
// ============================================================================

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
    ) -> TokenStream {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|arg| self.build_row_args_arithmetic_expr(arg, fields, string_intern, remaining))
            .collect();
        pack_as_tuple(parts)
    }

    /// KV → KV: tuple-shaped expression drawing from the current `(k, v)`
    /// closure bindings.
    pub(super) fn build_key_val_from_kv_args(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
        remaining: Option<&RefCell<HashMap<(bool, usize), usize>>>,
    ) -> TokenStream {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_kv_args_arithmetic_expr(a, string_intern, remaining))
            .collect();
        pack_as_tuple(parts)
    }

    /// Join → KV: tuple-shaped expression drawing from a `join_core`
    /// closure's `(k, lv, rv)` bindings.
    pub(super) fn build_key_val_from_join_args(
        &mut self,
        args: &[ArithmeticArgument],
        string_intern: bool,
    ) -> TokenStream {
        let parts: Vec<TokenStream> = args
            .iter()
            .map(|a| self.build_join_args_arithmetic_expr(a, string_intern))
            .collect();
        pack_as_tuple(parts)
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

    let mut inspect_expr = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark(trans_arg);
        }
    };

    for expr in key_args.iter().chain(value_args.iter()) {
        inspect_expr(expr);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
    }

    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect_expr(arg);
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

/// `join_core` closure idents `(k, lv, rv)` — unused ones get a leading
/// underscore so the emitted closure is warning-free.
pub(super) fn compute_join_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
) -> (TokenStream, TokenStream, TokenStream) {
    let mut use_join_k = false;
    let mut use_join_lv = false;
    let mut use_join_rv = false;

    let mut mark_usage = |arg: &TransformationArgument| {
        if let TransformationArgument::Jn((is_left, is_key, _)) = arg {
            if *is_key {
                use_join_k = true;
            } else if *is_left {
                use_join_lv = true;
            } else {
                use_join_rv = true;
            }
        }
    };

    let mut inspect_expr = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark_usage(trans_arg);
        }
    };

    for a in key_args.iter().chain(value_args.iter()) {
        inspect_expr(a);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
    }

    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect_expr(arg);
        }
    }

    // Build idents, prefixing with `_` when not used to avoid warnings.
    let k_ident = if use_join_k {
        quote! { k }
    } else {
        quote! { _k }
    };
    let lv_ident = if use_join_lv {
        quote! { lv }
    } else {
        quote! { _lv }
    };
    let rv_ident = if use_join_rv {
        quote! { rv }
    } else {
        quote! { _rv }
    };

    (k_ident, lv_ident, rv_ident)
}

/// KV closure idents `(k, v)` — unused ones get a leading underscore so
/// the emitted `flat_map` / `flat_map_ref` closure is warning-free.
pub(super) fn compute_kv_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    fn_call_preds: &[FnCallPredicateArgument],
    constraints: Option<&Constraints>,
) -> (TokenStream, TokenStream) {
    let mut use_k = false;
    let mut use_v = false;

    let mut mark_usage = |arg: &TransformationArgument| match arg {
        TransformationArgument::KV((is_key, _)) => {
            if *is_key {
                use_k = true;
            } else {
                use_v = true;
            }
        }
        TransformationArgument::Jn((_, is_key, _)) => {
            if *is_key {
                use_k = true;
            } else {
                use_v = true;
            }
        }
    };

    let mut inspect_expr = |expr: &ArithmeticArgument| {
        for trans_arg in expr.transformation_arguments() {
            mark_usage(trans_arg);
        }
    };

    for arg in key_args.iter().chain(value_args.iter()) {
        inspect_expr(arg);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
    }

    for fc in fn_call_preds {
        for arg in fc.args() {
            inspect_expr(arg);
        }
    }

    if let Some(cons) = constraints {
        for (arg, _) in cons.constant_eq_constraints().as_ref().iter() {
            mark_usage(arg);
        }
        for (left, right) in cons.variable_eq_constraints().as_ref().iter() {
            mark_usage(left);
            mark_usage(right);
        }
    }

    let k_ident = if use_k {
        quote! { k }
    } else {
        quote! { _k }
    };
    let v_ident = if use_v {
        quote! { v }
    } else {
        quote! { _v }
    };

    (k_ident, v_ident)
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
    /// Build a combined predicate for KV-based closures from comparison expressions.
    pub(super) fn build_kv_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        input_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Option<TokenStream> {
        if comps.is_empty() {
            return None;
        }
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l = self.build_kv_args_arithmetic_expr(c.left(), string_intern, None);
                let r = self.build_kv_args_arithmetic_expr(c.right(), string_intern, None);
                let op = comparison_op_tokens(c.operator());
                if string_intern
                    && c.operator().is_inequality()
                    && self.infer_expr_type(c.left(), input_type, None) == Some(DataType::String)
                    && self.infer_expr_type(c.right(), input_type, None) == Some(DataType::String)
                {
                    self.features.mark_string_resolve();
                    quote! { resolve(#l) #op resolve(#r) }
                } else {
                    quote! { (#l) #op (#r) }
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }

    /// Build a combined predicate for join-core closures (k, lv, rv) from comparison expressions.
    pub(super) fn build_join_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        string_intern: bool,
        left_type: &(Vec<DataType>, Vec<DataType>),
        right_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Option<TokenStream> {
        if comps.is_empty() {
            return None;
        }
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l = self.build_join_args_arithmetic_expr(c.left(), string_intern);
                let r = self.build_join_args_arithmetic_expr(c.right(), string_intern);
                let op = comparison_op_tokens(c.operator());
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
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }

    /// Build a combined predicate for row-based closures from comparison expressions.
    pub(super) fn build_row_compare_predicate(
        &mut self,
        comps: &[ComparisonExprArgument],
        row_fields: &[Ident],
        string_intern: bool,
        input_type: &(Vec<DataType>, Vec<DataType>),
    ) -> Option<TokenStream> {
        if comps.is_empty() {
            return None;
        }
        let parts: Vec<TokenStream> = comps
            .iter()
            .map(|c| {
                let l =
                    self.build_row_args_arithmetic_expr(c.left(), row_fields, string_intern, None);
                let r =
                    self.build_row_args_arithmetic_expr(c.right(), row_fields, string_intern, None);
                let op = comparison_op_tokens(c.operator());
                if string_intern
                    && c.operator().is_inequality()
                    && self.infer_expr_type(c.left(), input_type, None) == Some(DataType::String)
                    && self.infer_expr_type(c.right(), input_type, None) == Some(DataType::String)
                {
                    self.features.mark_string_resolve();
                    quote! { resolve(#l) #op resolve(#r) }
                } else {
                    quote! { #l #op #r }
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }
}

// ==================================================
// FnCall predicate builders
// ==================================================

impl CodeGen {
    /// Build a combined predicate for KV-based closures from fn_call predicates.
    pub(super) fn build_kv_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        string_intern: bool,
    ) -> Option<TokenStream> {
        if fn_calls.is_empty() {
            return None;
        }
        let parts: Vec<TokenStream> = fn_calls
            .iter()
            .map(|fc| {
                let fn_name = format_ident!("{}", fc.name());
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .map(|a| self.build_kv_args_arithmetic_expr(a, string_intern, None))
                    .collect();
                if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }

    /// Build a combined predicate for join-core closures from fn_call predicates.
    pub(super) fn build_join_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        string_intern: bool,
    ) -> Option<TokenStream> {
        if fn_calls.is_empty() {
            return None;
        }
        let parts: Vec<TokenStream> = fn_calls
            .iter()
            .map(|fc| {
                let fn_name = format_ident!("{}", fc.name());
                let args: Vec<TokenStream> = fc
                    .args()
                    .iter()
                    .map(|a| self.build_join_args_arithmetic_expr(a, string_intern))
                    .collect();
                if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }

    /// Build a combined predicate for row-based closures from fn_call predicates.
    pub(super) fn build_row_fn_call_predicate(
        &mut self,
        fn_calls: &[FnCallPredicateArgument],
        row_fields: &[Ident],
        string_intern: bool,
    ) -> Option<TokenStream> {
        if fn_calls.is_empty() {
            return None;
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
                    .collect();
                if fc.is_negated() {
                    quote! { !udf::#fn_name(#( #args ),*) }
                } else {
                    quote! { udf::#fn_name(#( #args ),*) }
                }
            })
            .collect();

        Some(quote! { #( #parts )&&* })
    }
}

// ==================================================
// Constraint predicate builders
// ==================================================

/// Combined `==` predicate for KV const-eq and var-eq constraints.
pub(super) fn build_kv_constraints_predicate(
    constraints: &Constraints,
    string_intern: bool,
) -> Option<TokenStream> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_kv_expr(arg);
            let rhs = const_to_token(c, string_intern);
            quote! { #lhs == #rhs }
        })
        .collect();

    parts.extend(
        constraints
            .variable_eq_constraints()
            .as_ref()
            .iter()
            .map(|(l, r)| {
                let lhs = trans_arg_to_kv_expr(l);
                let rhs = trans_arg_to_kv_expr(r);
                quote! { #lhs == #rhs }
            }),
    );

    if parts.is_empty() {
        None
    } else {
        Some(quote! { #( #parts )&&* })
    }
}

/// Combined `==` predicate for row const-eq and var-eq constraints.
pub(super) fn build_row_constraints_predicate(
    constraints: &Constraints,
    row_fields: &[Ident],
    string_intern: bool,
) -> Option<TokenStream> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_row_expr(arg, row_fields);
            let rhs = const_to_token(c, string_intern);
            quote! { #lhs == #rhs }
        })
        .collect();

    parts.extend(
        constraints
            .variable_eq_constraints()
            .as_ref()
            .iter()
            .map(|(l, r)| {
                let lhs = trans_arg_to_row_expr(l, row_fields);
                let rhs = trans_arg_to_row_expr(r, row_fields);
                quote! { #lhs == #rhs }
            }),
    );

    if parts.is_empty() {
        None
    } else {
        Some(quote! { #( #parts )&&* })
    }
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

fn trans_arg_to_kv_expr(arg: &TransformationArgument) -> TokenStream {
    match arg {
        TransformationArgument::KV((is_key, idx)) => {
            let i = Index::from(*idx);
            if *is_key {
                quote! { k.#i }
            } else {
                quote! { v.#i }
            }
        }
        _ => unreachable!("unexpected non-KV transformation argument for KV constraints"),
    }
}

fn trans_arg_to_row_expr(arg: &TransformationArgument, fields: &[Ident]) -> TokenStream {
    match arg {
        TransformationArgument::KV((_, idx)) => {
            let ident = fields
                .get(*idx)
                .expect("row index out of bounds in row constraints");
            quote! { #ident }
        }
        _ => unreachable!("unexpected non-KV transformation argument for row constraints"),
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
        _ => {
            unreachable!(
                "CodeGen error: string operator {} found in numeric arithmetic expression",
                op
            )
        }
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
    fn build_arithmetic_expr<F: Fn(&TransformationArgument) -> TokenStream>(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> TokenStream {
        // Type system guarantees: if any op is Cat, all ops are Cat (string expr).
        // Batch all factors into a single format! call.
        if expr
            .rest()
            .first()
            .is_some_and(|(op, _)| matches!(op, ArithmeticOperator::Cat))
        {
            let mut factors =
                vec![self.factor_to_display_token(expr.init(), string_intern, resolve_var)];
            for (_, factor) in expr.rest() {
                factors.push(self.factor_to_display_token(factor, string_intern, resolve_var));
            }
            return build_cat_batch(factors, string_intern);
        }

        // Numeric fold: left-to-right with parentheses for precedence.
        // Only intermediate steps are wrapped — the outermost expression is
        // left bare so it can appear as a function argument without triggering
        // Rust's `unused_parens` lint.
        let rest = expr.rest();
        let mut result = self.factor_to_token(expr.init(), string_intern, resolve_var);
        for (i, (op, factor)) in rest.iter().enumerate() {
            let op_token = numeric_arithmetic_op_tokens(op);
            let factor_token = self.factor_to_token(factor, string_intern, resolve_var);
            if i < rest.len() - 1 {
                result = quote! { ( #result #op_token #factor_token ) };
            } else {
                result = quote! { #result #op_token #factor_token };
            }
        }
        result
    }

    /// Generate a UDF call token stream: `udf::fn_name(arg1.clone(), arg2.clone(), ...)`.
    ///
    /// UDFs take ownership of their arguments. We clone each arg to avoid
    /// invalidating variables that may be used elsewhere in the same closure
    /// (e.g., in the output tuple or another predicate). Clone on Copy types is free.
    fn fncall_to_token<F: Fn(&TransformationArgument) -> TokenStream>(
        &mut self,
        name: &str,
        args: &[ArithmeticArgument],
        string_intern: bool,
        resolve_var: &F,
    ) -> TokenStream {
        let fn_ident = format_ident!("{}", name);
        let arg_tokens: Vec<TokenStream> = args
            .iter()
            .map(|a| {
                let token = self.build_arithmetic_expr(a, string_intern, resolve_var);
                quote! { (#token).clone() }
            })
            .collect();
        self.features.mark_udf();
        quote! { udf::#fn_ident(#(#arg_tokens),*) }
    }

    /// Convert a factor to a token stream for numeric/general expressions.
    fn factor_to_token<F: Fn(&TransformationArgument) -> TokenStream>(
        &mut self,
        factor: &FactorArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> TokenStream {
        match factor {
            FactorArgument::Var(arg) => resolve_var(arg),
            FactorArgument::Const(c) => const_to_token(c, string_intern),
            FactorArgument::FnCall { name, args } => {
                self.fncall_to_token(name, args, string_intern, resolve_var)
            }
        }
    }

    /// Convert a factor to a display-ready token stream for cat (string concatenation).
    fn factor_to_display_token<F: Fn(&TransformationArgument) -> TokenStream>(
        &mut self,
        factor: &FactorArgument,
        string_intern: bool,
        resolve_var: &F,
    ) -> TokenStream {
        match factor {
            FactorArgument::Var(arg) => {
                let var_token = resolve_var(arg);
                if string_intern {
                    self.features.mark_string_resolve();
                    quote! { resolve(#var_token) }
                } else {
                    var_token
                }
            }
            FactorArgument::Const(c) => match c {
                // String literals are already displayable – emit them
                // directly without interning first.
                ConstType::Text(s) => quote! { #s },
                _ => const_to_token(c, string_intern),
            },
            FactorArgument::FnCall { name, args } => {
                // UDF returns Spur when string_intern is on — resolve for display.
                let call = self.fncall_to_token(name, args, string_intern, resolve_var);
                if string_intern {
                    self.features.mark_string_resolve();
                    quote! { resolve(#call) }
                } else {
                    call
                }
            }
        }
    }

    fn build_row_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        fields: &[Ident],
        string_intern: bool,
        remaining: Option<&RefCell<HashMap<usize, usize>>>,
    ) -> TokenStream {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((_, idx)) => {
                let ident = fields
                    .get(*idx)
                    .expect("CodeGen error: row index out of bounds in row builder");
                // When tracking remaining uses, clone only when there are future uses.
                // The last use just moves the original value.
                // When not tracking (None), never clone.
                if let Some(rem) = remaining {
                    let mut map = rem.borrow_mut();
                    if let Some(count) = map.get_mut(idx) {
                        *count -= 1;
                        if *count > 0 {
                            return quote! { #ident.clone() };
                        }
                    }
                }
                quote! { #ident }
            }
            _ => unreachable!("CodeGen error: unexpected argument type in row builder"),
        })
    }

    fn build_kv_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
        remaining: Option<&RefCell<HashMap<(bool, usize), usize>>>,
    ) -> TokenStream {
        self.build_arithmetic_expr(expr, string_intern, &|arg| match arg {
            TransformationArgument::KV((is_key, idx))
            | TransformationArgument::Jn((_, is_key, idx)) => {
                let i = Index::from(*idx);
                // When tracking remaining uses, clone only when there are future uses.
                // The last use just moves the original value.
                // When not tracking (None), never clone.
                let needs_clone = if let Some(rem) = remaining {
                    let key = (*is_key, *idx);
                    let mut map = rem.borrow_mut();
                    if let Some(count) = map.get_mut(&key) {
                        *count -= 1;
                        *count > 0
                    } else {
                        false
                    }
                } else {
                    false
                };
                if *is_key {
                    if needs_clone {
                        quote! { k.#i.clone() }
                    } else {
                        quote! { k.#i }
                    }
                } else if needs_clone {
                    quote! { v.#i.clone() }
                } else {
                    quote! { v.#i }
                }
            }
        })
    }

    fn build_join_args_arithmetic_expr(
        &mut self,
        expr: &ArithmeticArgument,
        string_intern: bool,
    ) -> TokenStream {
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
                quote! { #ident.#i.clone() }
            }
            _ => unreachable!("unexpected argument type in join builder"),
        })
    }
}

fn pack_as_tuple(parts: Vec<TokenStream>) -> TokenStream {
    tuple_tokens(parts)
}
