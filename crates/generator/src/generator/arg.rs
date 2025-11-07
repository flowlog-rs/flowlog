use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::Index;

use planner::TransformationArgument;

/// Row -> KV (keys/values): build from row fields by index.
///
/// Shape policy (tuple-ified):
/// - 0 parts => ()
/// - 1 part  => (x,)  // note the trailing comma: (x) is just x, not a tuple
/// - n parts => (x, y, ...)
pub fn build_key_val_from_row_args(
    args: &[TransformationArgument],
    fields: &[Ident],
) -> TokenStream {
    let mut parts: Vec<TokenStream> = Vec::new();
    for a in args {
        match a {
            TransformationArgument::KV((_, idx)) => {
                let ident = fields
                    .get(*idx)
                    .expect("row index out of bounds in row->kv builder");
                parts.push(quote! { #ident });
            }
            _ => unreachable!("unexpected argument type in row->kv builder"),
        }
    }
    pack_as_tuple(parts)
}

/// KV -> KV (keys/values): build from current (k, v) by index.
///
/// Shape policy (tuple-ified like row builder):
/// - 0 parts => ()
/// - 1 part  => (x,)
/// - n parts => (x, y, ...)
pub fn build_key_val_from_kv_args(args: &[TransformationArgument]) -> TokenStream {
    use TransformationArgument::*;
    let mut parts: Vec<TokenStream> = Vec::new();
    for a in args {
        match *a {
            KV((is_key, idx)) => {
                let i = Index::from(idx);
                if is_key {
                    parts.push(quote! { k.#i });
                } else {
                    parts.push(quote! { v.#i });
                }
            }
            _ => parts.push(quote! { k }),
        }
    }
    pack_as_tuple(parts)
}

/// Join -> KV (keys/values): use k, lv.#, rv.# as requested.
///
/// Shape policy (tuple-ified like row builder):
/// - 0 parts => ()
/// - 1 part  => (x,)
/// - n parts => (x, y, ...)
pub fn build_key_val_from_join_args(args: &[TransformationArgument]) -> TokenStream {
    let mut parts: Vec<TokenStream> = Vec::new();
    for a in args {
        let ts = match *a {
            TransformationArgument::Jn((is_left, is_key, idx)) => {
                if is_key {
                    // `k` is a reference in join_core; clone to produce an owned key.
                    quote! { k.clone() }
                } else if is_left {
                    proj_tuple_field("lv", idx)
                } else {
                    proj_tuple_field("rv", idx)
                }
            }
            _ => unreachable!("unexpected argument type in join->kv value transformation"),
        };
        parts.push(ts);
    }
    pack_as_tuple(parts)
}

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
