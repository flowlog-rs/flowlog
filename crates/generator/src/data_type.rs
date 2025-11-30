use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use parser::{DataType, Relation};
use planner::{
    ArithmeticArgument, ComparisonExprArgument, Constraints, FactorArgument, TransformationArgument,
};

// =========================================================================
// DataType Inference Utilities
// =========================================================================

/// Map input fingerprints to collection data types.
pub(super) fn make_type_map(
    input_rels: Vec<&Relation>,
    output_rels: Vec<&Relation>,
) -> HashMap<u64, DataType> {
    input_rels
        .into_iter()
        .chain(output_rels)
        .map(|rel| (rel.fingerprint(), *rel.data_type()))
        .collect()
}

/// Find a fingerprint in the type map and return the associated DataType.
pub(super) fn find_type(fp_to_type: &HashMap<u64, DataType>, fingerprint: u64) -> DataType {
    *fp_to_type.get(&fingerprint).unwrap_or_else(|| {
        panic!(
            "Generator error: input type missing for fingerprint {:016x}",
            fingerprint
        )
    })
}

/// Safely insert or verify a type mapping in the fingerprint-to-type map.
/// If the fingerprint already exists, asserts that the types match.
/// If it doesn't exist, inserts the new mapping.
pub(super) fn insert_or_verify_type(
    fp_to_type: &mut HashMap<u64, DataType>,
    fingerprint: u64,
    expected_type: DataType,
) {
    if let Some(existing_type) = fp_to_type.get(&fingerprint) {
        assert_eq!(
            *existing_type, expected_type,
            "Generator error: type mismatch for fingerprint {:016x}: existing {:?}, expected {:?}",
            fingerprint, existing_type, expected_type
        );
    } else {
        fp_to_type.insert(fingerprint, expected_type);
    }
}

// =========================================================================
// Argument DataType Utilities
// =========================================================================

/// Type tokens based on the configured InputType:
/// - Numeric: () | i32 | (i32, i32, ...)
/// - Text:    () | String | (String, String, ...)
pub(super) fn type_tokens(input_type: DataType, arity: usize) -> TokenStream {
    match input_type {
        DataType::Integer => match arity {
            0 => quote! { () },
            1 => quote! { (i32,) },
            n => {
                let tys = vec![quote! { i32 }; n];
                quote! { ( #(#tys),* ) }
            }
        },
        DataType::String => match arity {
            0 => quote! { () },
            1 => quote! { (String,) },
            n => {
                let tys = vec![quote! { String }; n];
                quote! { ( #(#tys),* ) }
            }
        },
    }
}

/// Build row pattern + identifiers from an arity, renaming unused fields with leading underscores.
/// This mirrors the KV parameter handling but operates on row inputs that only expose value slots.
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

    let mut field_usage = vec![false; arity];

    let mut mark_usage = |arg: &TransformationArgument| {
        if let TransformationArgument::KV((_, idx)) = arg {
            if let Some(slot) = field_usage.get_mut(*idx) {
                *slot = true;
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

    for expr in key_args.iter().chain(value_args.iter()) {
        inspect_expr(expr);
    }

    for cmp in compares {
        inspect_expr(cmp.left());
        inspect_expr(cmp.right());
    }

    for (arg, _) in constraints.constant_eq_constraints().as_ref().iter() {
        mark_usage(arg);
    }
    for (left, right) in constraints.variable_eq_constraints().as_ref().iter() {
        mark_usage(left);
        mark_usage(right);
    }

    let fields: Vec<Ident> = (0..arity)
        .map(|idx| {
            if field_usage[idx] {
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
