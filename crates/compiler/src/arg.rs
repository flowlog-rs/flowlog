use proc_macro2::{Ident, Span, TokenStream};
use quote::{format_ident, quote};
use syn::Index;

use planner::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument, TransformationArgument,
};

// ============================================================================
// Row pattern + field identifiers for RowToX transformations
// ============================================================================

/// Build a row pattern and a list of field idents `(pattern, fields)` for a given `arity`.
///
/// - Fields that are referenced anywhere in `key_args`, `value_args`, `compares`,
///   or `constraints` become `x<i>`.
/// - Unused fields become `_x<i>` so Rust doesn't warn about unused bindings.
///
/// This is analogous to KV parameter handling, but for row inputs which expose
/// only *value* slots.
pub(super) fn row_pattern_and_fields(
    arity: usize,
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    constraints: &Constraints,
) -> (TokenStream, Vec<Ident>) {
    if arity == 0 {
        return (quote! { () }, Vec::new());
    }

    let used = compute_row_params_tokens(arity, key_args, value_args, compares, constraints);

    let fields: Vec<Ident> = (0..arity)
        .map(|idx| {
            if used[idx] {
                format_ident!("x{}", idx)
            } else {
                format_ident!("_x{}", idx)
            }
        })
        .collect();

    let pat = match fields.len() {
        0 => quote! { () },
        1 => {
            let only = fields[0].clone();
            quote! { ( #only, ) }
        }
        _ => quote! { ( #(#fields),* ) },
    };

    (pat, fields)
}

// ==================================================
// Tuple builder utilities
// ==================================================

/// Row -> KV (keys/values): build from row fields by index.
///
/// Shape policy (tuple-ified):
/// - 0 parts => ()
/// - 1 part  => (x,)  // note the trailing comma: (x) is just x, not a tuple
/// - n parts => (x, y, ...)
pub(super) fn build_key_val_from_row_args(
    args: &[ArithmeticArgument],
    fields: &[Ident],
) -> TokenStream {
    let parts: Vec<TokenStream> = args
        .iter()
        .map(|arg| build_row_args_arithmetic_expr(arg, fields))
        .collect();
    pack_as_tuple(parts)
}

/// KV -> KV (keys/values): build from current (k, v) by index.
///
/// Shape policy (tuple-ified like row builder):
/// - 0 parts => ()
/// - 1 part  => (x,)
/// - n parts => (x, y, ...)
pub(super) fn build_key_val_from_kv_args(args: &[ArithmeticArgument]) -> TokenStream {
    let parts: Vec<TokenStream> = args.iter().map(build_kv_args_arithmetic_expr).collect();
    pack_as_tuple(parts)
}

/// Join -> KV (keys/values): use k.#, lv.#, rv.# as requested.
///
/// Shape policy (tuple-ified like row builder):
/// - 0 parts => ()
/// - 1 part  => (x,)
/// - n parts => (x, y, ...)
pub(super) fn build_key_val_from_join_args(args: &[ArithmeticArgument]) -> TokenStream {
    let parts: Vec<TokenStream> = args.iter().map(build_join_args_arithmetic_expr).collect();
    pack_as_tuple(parts)
}

// ==================================================
// Closure parameter utilities
// ==================================================
/// Inspect row arithmetic & comparison arguments to determine which closure parameters
/// are referenced by the produced expressions or filters.
/// Returns vectors of boolean value indicate which parameters are used. Any unused
/// parameter becomes an underscore-prefixed ident later to silence warnings.
fn compute_row_params_tokens(
    arity: usize,
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
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
        if let FactorArgument::Var(trans_arg) = expr.init() {
            mark(trans_arg);
        }
        for (_op, factor) in expr.rest() {
            if let FactorArgument::Var(trans_arg) = factor {
                mark(trans_arg);
            }
        }
    };

    for expr in key_args.iter().chain(value_args.iter()) {
        inspect_expr(expr);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
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

/// Inspect join arithmetic & comparison arguments to determine which closure parameters
/// are referenced by the produced expressions or filters.
/// Returns three TokenStreams for the closure parameter identifiers. Any unused
/// parameter becomes an underscore-prefixed ident to silence warnings.
pub(super) fn compute_join_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
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
        if let FactorArgument::Var(trans_arg) = expr.init() {
            mark_usage(trans_arg);
        }
        for (_op, factor) in expr.rest() {
            if let FactorArgument::Var(trans_arg) = factor {
                mark_usage(trans_arg);
            }
        }
    };

    for a in key_args.iter().chain(value_args.iter()) {
        inspect_expr(a);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
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

/// Inspect KV arithmetic, comparison, and constraint arguments to determine which closure
/// parameters (`k`, `v`) are referenced. Unused parameters are renamed with an underscore
/// prefix to silence warnings in generated `flat_map`/`flat_map_ref` closures.
pub(super) fn compute_kv_param_tokens(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
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
        if let FactorArgument::Var(trans_arg) = expr.init() {
            mark_usage(trans_arg);
        }
        for (_op, factor) in expr.rest() {
            if let FactorArgument::Var(trans_arg) = factor {
                mark_usage(trans_arg);
            }
        }
    };

    for arg in key_args.iter().chain(value_args.iter()) {
        inspect_expr(arg);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
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

fn comparison_op_tokens(op: &parser::ComparisonOperator) -> TokenStream {
    match op {
        parser::ComparisonOperator::Equal => quote! { == },
        parser::ComparisonOperator::NotEqual => quote! { != },
        parser::ComparisonOperator::GreaterThan => quote! { > },
        parser::ComparisonOperator::GreaterEqualThan => quote! { >= },
        parser::ComparisonOperator::LessThan => quote! { < },
        parser::ComparisonOperator::LessEqualThan => quote! { <= },
    }
}

/// Build a combined predicate for KV-based closures from comparison expressions.
/// Returns None when there are no comparisons.
pub(super) fn build_kv_compare_predicate(comps: &[ComparisonExprArgument]) -> Option<TokenStream> {
    if comps.is_empty() {
        return None;
    }
    let parts: Vec<TokenStream> = comps
        .iter()
        .map(|c| {
            let l = build_kv_args_arithmetic_expr(c.left());
            let r = build_kv_args_arithmetic_expr(c.right());
            let op = comparison_op_tokens(c.operator());
            quote! { (#l) #op (#r) }
        })
        .collect();

    Some(quote! { #( #parts )&&* })
}

/// Build a combined predicate for join-core closures (k, lv, rv) from comparison expressions.
/// Returns None when there are no comparisons.
pub(super) fn build_join_compare_predicate(
    comps: &[ComparisonExprArgument],
) -> Option<TokenStream> {
    if comps.is_empty() {
        return None;
    }
    let parts: Vec<TokenStream> = comps
        .iter()
        .map(|c| {
            let l = build_join_args_arithmetic_expr(c.left());
            let r = build_join_args_arithmetic_expr(c.right());
            let op = comparison_op_tokens(c.operator());
            quote! { (#l) #op (#r) }
        })
        .collect();

    Some(quote! { #( #parts )&&* })
}

/// Build a combined predicate for row-based closures from comparison expressions.
/// Returns None when there are no comparisons.
pub(super) fn build_row_compare_predicate(
    comps: &[ComparisonExprArgument],
    row_fields: &[Ident],
) -> Option<TokenStream> {
    if comps.is_empty() {
        return None;
    }
    let parts: Vec<TokenStream> = comps
        .iter()
        .map(|c| {
            let l = build_row_args_arithmetic_expr(c.left(), row_fields);
            let r = build_row_args_arithmetic_expr(c.right(), row_fields);
            let op = comparison_op_tokens(c.operator());
            quote! { #l #op #r }
        })
        .collect();

    Some(quote! { #( #parts )&&* })
}

// ==================================================
// Constraint predicate builders
// ==================================================

/// Build predicate for KV constraints (const eq and var eq). Returns None if empty.
pub(super) fn build_kv_constraints_predicate(constraints: &Constraints) -> Option<TokenStream> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_kv_expr(arg);
            let rhs = const_to_token(c);
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

/// Build predicate for row constraints (const eq and var eq). Returns None if empty.
pub(super) fn build_row_constraints_predicate(
    constraints: &Constraints,
    row_fields: &[Ident],
) -> Option<TokenStream> {
    let mut parts: Vec<TokenStream> = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| {
            let lhs = trans_arg_to_row_expr(arg, row_fields);
            let rhs = const_to_token(c);
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

fn const_to_token(constant: &parser::ConstType) -> TokenStream {
    match constant {
        parser::ConstType::Integer(n) => quote! { #n },
        parser::ConstType::Text(s) => quote! { #s.to_string() },
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

/// Combine two optional predicates with logical AND, returning the appropriate composed TokenStream.
/// - None & None => None
/// - Some(a) & None => Some(a)
/// - None & Some(b) => Some(b)
/// - Some(a) & Some(b) => Some( (a) && (b) )
pub(super) fn combine_predicates(
    a: Option<TokenStream>,
    b: Option<TokenStream>,
) -> Option<TokenStream> {
    match (a, b) {
        (None, None) => None,
        (Some(x), None) => Some(x),
        (None, Some(y)) => Some(y),
        (Some(x), Some(y)) => Some(quote! { (#x) && (#y) }),
    }
}

// ==================================================
// Arithmetic expression helpers
// ==================================================
fn arithmetic_op_tokens(op: &parser::ArithmeticOperator) -> TokenStream {
    match op {
        parser::ArithmeticOperator::Plus => quote! { + },
        parser::ArithmeticOperator::Minus => quote! { - },
        parser::ArithmeticOperator::Multiply => quote! { * },
        parser::ArithmeticOperator::Divide => quote! { / },
        parser::ArithmeticOperator::Modulo => quote! { % },
    }
}

// Build an arithmetic expression from a position and field identifiers.
fn build_row_args_arithmetic_expr(expr: &ArithmeticArgument, fields: &[Ident]) -> TokenStream {
    let to_expr = |factor: &FactorArgument| -> TokenStream {
        match factor {
            FactorArgument::Var(trans_arg) => match trans_arg {
                TransformationArgument::KV((_, idx)) => {
                    let ident = fields
                        .get(*idx)
                        .expect("row index out of bounds in row->kv builder");
                    quote! { #ident }
                }
                _ => unreachable!("unexpected argument type in row->kv builder"),
            },
            FactorArgument::Const(constant) => const_to_token(constant),
        }
    };

    expr.rest()
        .iter()
        .fold(to_expr(expr.init()), |ts, (op, factor)| {
            let op_token = arithmetic_op_tokens(op);
            let factor_token = to_expr(factor);
            quote! { ( #ts #op_token #factor_token ) }
        })
}

// Build an arithmetic expression from a position.
fn build_kv_args_arithmetic_expr(expr: &ArithmeticArgument) -> TokenStream {
    let to_expr = |factor: &FactorArgument| -> TokenStream {
        match factor {
            FactorArgument::Var(trans_arg) => match trans_arg {
                TransformationArgument::KV((is_key, idx)) => {
                    let i = Index::from(*idx);
                    if *is_key {
                        quote! { k.#i }
                    } else {
                        quote! { v.#i }
                    }
                }
                TransformationArgument::Jn((_, is_key, idx)) => {
                    let i = Index::from(*idx);
                    if *is_key {
                        quote! { k.#i }
                    } else {
                        quote! { v.#i }
                    }
                }
            },
            FactorArgument::Const(constant) => const_to_token(constant),
        }
    };

    expr.rest()
        .iter()
        .fold(to_expr(expr.init()), |ts, (op, factor)| {
            let op_token = arithmetic_op_tokens(op);
            let factor_token = to_expr(factor);
            quote! { ( #ts #op_token #factor_token ) }
        })
}

// Build an arithmetic expression from a position.
fn build_join_args_arithmetic_expr(expr: &ArithmeticArgument) -> TokenStream {
    // Build the initial factor
    let init_token = match expr.init() {
        FactorArgument::Var(trans_arg) => match trans_arg {
            TransformationArgument::Jn((is_left, is_key, idx)) => {
                if *is_key {
                    proj_tuple_field("k", *idx)
                } else if *is_left {
                    proj_tuple_field("lv", *idx)
                } else {
                    proj_tuple_field("rv", *idx)
                }
            }
            _ => unreachable!("unexpected argument type in join->kv value transformation"),
        },
        FactorArgument::Const(constant) => const_to_token(constant),
    };

    // If no operations, return just the initial factor
    if expr.rest().is_empty() {
        return init_token;
    }

    // Build the full expression with proper left-to-right parentheses
    let mut expr_token = init_token;
    for (op, factor) in expr.rest() {
        let factor_token = match factor {
            FactorArgument::Var(trans_arg) => match trans_arg {
                TransformationArgument::Jn((is_left, is_key, idx)) => {
                    if *is_key {
                        proj_tuple_field("k", *idx)
                    } else if *is_left {
                        proj_tuple_field("lv", *idx)
                    } else {
                        proj_tuple_field("rv", *idx)
                    }
                }
                _ => unreachable!("unexpected argument type in join->kv value transformation"),
            },
            FactorArgument::Const(constant) => const_to_token(constant),
        };

        let op_token = arithmetic_op_tokens(op);

        // Wrap in parentheses for left-to-right evaluation: (prev_result op factor)
        expr_token = quote! { ( #expr_token #op_token #factor_token ) };
    }

    expr_token
}

/// Project a field from a tuple by index.
fn proj_tuple_field(base: &str, idx: usize) -> TokenStream {
    let i = Index::from(idx);
    let ident = Ident::new(base, Span::call_site());
    // `lv` and `rv` are references to tuple values in join_core; clone to get owned field
    // values regardless of whether the field is Copy (u64) or owned (String).
    quote! { #ident.#i.clone() }
}

/// Pack parts as a tuple with correct 1-tuple syntax.
/// - 0 => ()
/// - 1 => (x,)  // trailing comma makes it a tuple
/// - n => (x, y, ...)
fn pack_as_tuple(mut parts: Vec<TokenStream>) -> TokenStream {
    match parts.len() {
        0 => quote! { () },
        1 => {
            let p = parts.remove(0);
            quote! { ( #p, ) }
        }
        _ => quote! { ( #(#parts),* ) },
    }
}
