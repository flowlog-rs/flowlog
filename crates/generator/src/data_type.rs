use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use parser::{DataType, Relation};

// =========================================================================
// DataType Inference Utilities
// =========================================================================

/// Map input fingerprints to collection data types.
pub(super) fn make_type_map(
    input_rels: Vec<&Relation>,
) -> std::collections::HashMap<u64, DataType> {
    input_rels
        .into_iter()
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

/// Key-Value Type (tuple-ified) used for KV values and join annotations:
/// - Numeric: () | (u64,) | (u64, u64, ...)
/// - Text:    () | (String,) | (String, String, ...)
pub(super) fn key_value_type_tokens(input_type: DataType, arity: usize) -> TokenStream {
    match input_type {
        DataType::Integer => match arity {
            0 => quote! { () },
            1 => quote! { (u64,) },
            n => {
                let tys = std::iter::repeat(quote! { u64 }).take(n);
                quote! { ( #(#tys),* ) }
            }
        },
        DataType::String => match arity {
            0 => quote! { () },
            1 => quote! { (String,) },
            n => {
                let tys = std::iter::repeat(quote! { String }).take(n);
                quote! { ( #(#tys),* ) }
            }
        },
    }
}

/// EDB Row type tokens based on the configured InputType:
/// - Numeric: () | u64 | (u64, u64, ...)
/// - Text:    () | String | (String, String, ...)
pub(super) fn row_type_tokens(input_type: DataType, arity: usize) -> TokenStream {
    match input_type {
        DataType::Integer => match arity {
            0 => quote! { () },
            1 => quote! { u64 },
            n => {
                let tys = std::iter::repeat(quote! { u64 }).take(n);
                quote! { ( #(#tys),* ) }
            }
        },
        DataType::String => match arity {
            0 => quote! { () },
            1 => quote! { String },
            n => {
                let tys = std::iter::repeat(quote! { String }).take(n);
                quote! { ( #(#tys),* ) }
            }
        },
    }
}

/// Build row pattern + identifiers from an arity.
/// - 1 => `x0`
/// - n≥2 => `(x0, x1, ...)`
pub(super) fn row_pattern_and_fields(arity: usize) -> (TokenStream, Vec<Ident>) {
    let fields: Vec<_> = (0..arity).map(|i| format_ident!("x{}", i)).collect();
    let pat = if arity == 1 {
        let x0 = fields[0].clone();
        quote! { #x0 }
    } else {
        quote! { ( #(#fields),* ) }
    };
    (pat, fields)
}
