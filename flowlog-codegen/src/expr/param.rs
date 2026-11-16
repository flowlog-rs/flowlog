//! Closure parameters: the row pattern or key-value and join parameter
//! lists an operator's closure destructures its input with, binding only the
//! columns the closure reads.

use flowlog_planner::planner::ArithmeticArgument;
use flowlog_planner::planner::ComparisonExprArgument;
use flowlog_planner::planner::Constraints;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::tuple_tokens;

/// Returns a row closure's destructuring pattern `(x0, x1, ...)` and the
/// matching field idents. A slot no argument reads is prefixed with `_`, so
/// the unused-variable lint stays quiet.
pub(crate) fn row_params(
    arity: usize,
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    constraints: &Constraints,
) -> (TokenStream, Vec<Ident>) {
    if arity == 0 {
        return (quote! { () }, Vec::new());
    }

    let mut used = vec![false; arity];
    for_each_read(key_args, value_args, compares, Some(constraints), |arg| {
        match arg {
            TransformationArgument::KV((_, idx)) => {
                if let Some(slot) = used.get_mut(*idx) {
                    *slot = true;
                }
            }
            // A row closure reads no join argument.
            TransformationArgument::Jn(_) => {}
        }
    });

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

/// Returns a key-value closure's parameters `(k, v)`, each prefixed with
/// `_` when no argument reads it.
pub(crate) fn kv_params(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    constraints: Option<&Constraints>,
) -> (TokenStream, TokenStream) {
    let (mut use_k, mut use_v) = (false, false);
    for_each_read(key_args, value_args, compares, constraints, |arg| {
        // An antijoin's closure reads `Jn` arguments; see
        // `Codegen::kv_arithmetic`.
        let is_key = match arg {
            TransformationArgument::KV((is_key, _))
            | TransformationArgument::Jn((_, is_key, _)) => *is_key,
        };
        if is_key {
            use_k = true;
        } else {
            use_v = true;
        }
    });

    (param_ident(use_k, "k"), param_ident(use_v, "v"))
}

/// Returns a join closure's parameters `(k, lv, rv)`, each prefixed with `_`
/// when no argument reads it.
pub(crate) fn join_params(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
) -> (TokenStream, TokenStream, TokenStream) {
    let (mut use_k, mut use_lv, mut use_rv) = (false, false, false);
    for_each_read(key_args, value_args, compares, None, |arg| match arg {
        TransformationArgument::Jn((_, true, _)) => use_k = true,
        TransformationArgument::Jn((true, false, _)) => use_lv = true,
        TransformationArgument::Jn((false, false, _)) => use_rv = true,
        // A join closure reads only join arguments.
        TransformationArgument::KV(_) => {}
    });

    (
        param_ident(use_k, "k"),
        param_ident(use_lv, "lv"),
        param_ident(use_rv, "rv"),
    )
}

/// Calls `read` on every argument a closure reads: its key and value
/// arguments, both sides of its comparisons, and its constraints.
fn for_each_read(
    key_args: &[ArithmeticArgument],
    value_args: &[ArithmeticArgument],
    compares: &[ComparisonExprArgument],
    constraints: Option<&Constraints>,
    mut read: impl FnMut(&TransformationArgument),
) {
    let exprs = key_args
        .iter()
        .chain(value_args)
        .chain(compares.iter().flat_map(|cmp| [cmp.left(), cmp.right()]));
    for expr in exprs {
        for arg in expr.transformation_arguments() {
            read(arg);
        }
    }
    if let Some(constraints) = constraints {
        for (arg, _) in constraints.constant_eq_constraints().iter() {
            read(arg);
        }
        for (left, right) in constraints.variable_eq_constraints().iter() {
            read(left);
            read(right);
        }
    }
}

/// Emits `name` when `used`, otherwise `_name`, so an unused closure
/// parameter doesn't trigger the `unused_variables` lint in generated code.
fn param_ident(used: bool, name: &str) -> TokenStream {
    let id = format_ident!("{}{}", if used { "" } else { "_" }, name);
    quote! { #id }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::FactorArgument;

    use super::*;
    use crate::test_harness::strings;

    fn var(arg: TransformationArgument) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(arg),
            rest: Vec::new(),
        }
    }

    /// Every slot an argument or a constraint reads keeps its name; the rest
    /// are prefixed with `_`.
    #[test]
    fn a_row_pattern_names_only_the_slots_it_reads() {
        let constraints = Constraints::new(
            vec![(
                TransformationArgument::KV((false, 2)),
                Constant::new(DataType::Int32, "7"),
            )],
            Vec::new(),
        );
        let (pattern, fields) = row_params(
            4,
            &[var(TransformationArgument::KV((false, 0)))],
            &[],
            &[],
            &constraints,
        );
        assert_eq!(
            pattern.to_string(),
            quote! { (x0, _x1, x2, _x3) }.to_string()
        );
        assert_eq!(
            strings(fields.iter().map(|f| quote! { #f })),
            ["x0", "_x1", "x2", "_x3"]
        );
    }

    #[test]
    fn a_nullary_row_is_the_unit_pattern() {
        let (pattern, fields) =
            row_params(0, &[], &[], &[], &Constraints::new(Vec::new(), Vec::new()));
        assert_eq!(pattern.to_string(), quote! { () }.to_string());
        assert!(fields.is_empty());
    }

    /// A key-value closure names the side its arguments read, a constraint
    /// included.
    #[test]
    fn kv_params_name_the_sides_read() {
        let constraints = Constraints::new(
            vec![(
                TransformationArgument::KV((false, 0)),
                Constant::new(DataType::Int32, "7"),
            )],
            Vec::new(),
        );
        let (k, v) = kv_params(&[], &[], &[], Some(&constraints));
        assert_eq!(strings([k, v]), ["_k", "v"]);
        let (k, v) = kv_params(
            &[var(TransformationArgument::KV((true, 0)))],
            &[],
            &[],
            None,
        );
        assert_eq!(strings([k, v]), ["k", "_v"]);
    }

    /// A join closure names the key and whichever sides its arguments read.
    #[test]
    fn join_params_name_the_sides_read() {
        let (k, lv, rv) = join_params(
            &[var(TransformationArgument::Jn((false, false, 1)))],
            &[],
            &[],
        );
        assert_eq!(strings([k, lv, rv]), ["_k", "_lv", "rv"]);
        let (k, lv, rv) = join_params(
            &[var(TransformationArgument::Jn((false, true, 0)))],
            &[var(TransformationArgument::Jn((true, false, 0)))],
            &[],
        );
        assert_eq!(strings([k, lv, rv]), ["k", "lv", "_rv"]);
    }
}
