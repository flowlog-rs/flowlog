//! Equality constraints: a rule's constant and variable equalities on its
//! inputs as the predicate a row or key-value closure filters on.

use flowlog_planner::planner::Constraints;
use flowlog_planner::planner::TransformationArgument;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::codegen::CodegenError;
use crate::codegen::expr::term::constant::const_to_token;

/// Returns a row closure's constraint predicate, or `None` when there is no
/// constraint. Each variable reads from the row pattern's `fields`.
pub(crate) fn row_constraint_predicate(
    constraints: &Constraints,
    fields: &[Ident],
    string_intern: bool,
) -> Result<Option<TokenStream>, CodegenError> {
    constraint_predicate(constraints, string_intern, |arg| match arg {
        TransformationArgument::KV((_, idx)) => {
            let ident = fields.get(*idx).ok_or_else(|| {
                CodegenError::internal(format!(
                    "row index {idx} out of bounds (row arity {})",
                    fields.len()
                ))
            })?;
            Ok(quote! { #ident })
        }
        TransformationArgument::Jn(_) => Err(CodegenError::internal(format!(
            "join argument {arg:?} in a row constraint"
        ))),
    })
}

/// Returns a key-value closure's constraint predicate, or `None` when there
/// is no constraint. Each variable reads from the `(k, v)` parameters.
pub(crate) fn kv_constraint_predicate(
    constraints: &Constraints,
    string_intern: bool,
) -> Result<Option<TokenStream>, CodegenError> {
    constraint_predicate(constraints, string_intern, |arg| match arg {
        TransformationArgument::KV((is_key, idx)) => {
            let i = Index::from(*idx);
            Ok(if *is_key {
                quote! { k.#i }
            } else {
                quote! { v.#i }
            })
        }
        TransformationArgument::Jn(_) => Err(CodegenError::internal(format!(
            "join argument {arg:?} in a key-value constraint"
        ))),
    })
}

/// Returns every constant equality, then every variable equality, joined
/// with `&&`, or `None` when there are neither. `operand` lowers each
/// variable.
fn constraint_predicate(
    constraints: &Constraints,
    string_intern: bool,
    operand: impl Fn(&TransformationArgument) -> Result<TokenStream, CodegenError>,
) -> Result<Option<TokenStream>, CodegenError> {
    // An operand is the binding itself, not the `.clone()` the arithmetic
    // builders emit: `==` takes both sides by reference, so an owned copy
    // would be wasted work.
    let constant_eqs = constraints
        .constant_eq_constraints()
        .as_ref()
        .iter()
        .map(|(arg, c)| Ok((operand(arg)?, const_to_token(c, string_intern)?)));
    let variable_eqs = constraints
        .variable_eq_constraints()
        .as_ref()
        .iter()
        .map(|(l, r)| Ok((operand(l)?, operand(r)?)));
    let eqs = constant_eqs
        .chain(variable_eqs)
        .map(|eq| eq.map(|(lhs, rhs)| quote! { #lhs == #rhs }))
        .collect::<Result<Vec<_>, CodegenError>>()?;
    Ok((!eqs.is_empty()).then(|| quote! { #( #eqs )&&* }))
}

#[cfg(test)]
mod tests {
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::TransformationArgument::KV;
    use quote::format_ident;

    use super::*;

    #[test]
    fn no_constraint_is_no_predicate() {
        let constraints = Constraints::new(Vec::new(), Vec::new());
        assert!(
            kv_constraint_predicate(&constraints, false)
                .expect("nothing to lower")
                .is_none()
        );
        assert!(
            row_constraint_predicate(&constraints, &[], false)
                .expect("nothing to lower")
                .is_none()
        );
    }

    /// Constant equalities come before variable equalities, each in
    /// declaration order.
    #[test]
    fn a_kv_predicate_joins_constant_then_variable_equalities() {
        let constraints = Constraints::new(
            vec![(KV((false, 1)), Constant::new(DataType::Int32, "7"))],
            vec![(KV((true, 0)), KV((false, 0)))],
        );
        let pred = kv_constraint_predicate(&constraints, false)
            .expect("kv arguments")
            .expect("two equalities");
        assert_eq!(
            pred.to_string(),
            quote! { v.1 == 7 && k.0 == v.0 }.to_string()
        );
    }

    #[test]
    fn a_row_predicate_reads_the_pattern_fields() {
        let constraints = Constraints::new(
            vec![(KV((false, 2)), Constant::new(DataType::Int32, "7"))],
            vec![(KV((false, 0)), KV((false, 2)))],
        );
        let fields = [
            format_ident!("x0"),
            format_ident!("_x1"),
            format_ident!("x2"),
        ];
        let pred = row_constraint_predicate(&constraints, &fields, false)
            .expect("row arguments")
            .expect("two equalities");
        assert_eq!(pred.to_string(), quote! { x2 == 7 && x0 == x2 }.to_string());
    }
}
