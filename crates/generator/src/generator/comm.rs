use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use std::collections::HashMap;

use parser::{DataType, Relation};

/// Map input and output fingerprints to collection identifiers.
pub fn make_ident_map(
    input_rels: Vec<Relation>,
    output_rels: Vec<Relation>,
) -> HashMap<u64, Ident> {
    input_rels
        .into_iter()
        .map(|rel| (rel.fingerprint(), format_ident!("c_{}", rel.name())))
        .chain(
            output_rels
                .into_iter()
                .map(|rel| (rel.fingerprint(), format_ident!("o_{}", rel.name()))),
        )
        .collect()
}

/// Return input names in a stable (ascending) order.
pub fn ordered_input_names(input_rels: Vec<Relation>) -> Vec<String> {
    let mut names: Vec<String> = input_rels
        .into_iter()
        .map(|rel| rel.name().to_string())
        .collect();
    names.sort_unstable();
    names
}

/// Lookup the variable name for a given collection fp, or fall back to `t_<fp>`.
pub fn fp_to_var(fp2ident: &HashMap<u64, Ident>, fp: u64) -> Ident {
    fp2ident
        .get(&fp)
        .cloned()
        .unwrap_or_else(|| format_ident!("t_{}", fp))
}

/// Key-Value Type (tuple-ified) used for KV values and join annotations:
/// - Numeric: () | (u64,) | (u64, u64, ...)
/// - Text:    () | (String,) | (String, String, ...)
pub fn key_value_type_tokens(input_type: DataType, arity: usize) -> TokenStream {
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
pub fn row_type_tokens(input_type: DataType, arity: usize) -> TokenStream {
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
pub fn row_pattern_and_fields(arity: usize) -> (TokenStream, Vec<Ident>) {
    let fields: Vec<_> = (0..arity).map(|i| format_ident!("x{}", i)).collect();
    let pat = if arity == 1 {
        let x0 = fields[0].clone();
        quote! { #x0 }
    } else {
        quote! { ( #(#fields),* ) }
    };
    (pat, fields)
}
