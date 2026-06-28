//! Library-mode engine codegen.
//!
//! Shared tuple-conversion helpers live here; the `batch` and
//! `incremental` submodules consume them for their respective codegen.
//! Per-position conversion only fires for columns whose user-facing
//! type differs from the internal DD tuple type — floats (`f32` →
//! `OrderedFloat<f32>`) and, under interning, strings (`String` →
//! `Spur`). Integer-only relations have identical user / internal
//! tuples, so the identity binding is forwarded instead of emitting a
//! pointless destructure-and-re-tuple.

mod batch;
mod incremental;

pub(crate) use batch::gen_lib_engine;
pub(crate) use incremental::gen_lib_incremental_engine;

use proc_macro2::TokenStream;
use quote::quote;

use crate::build::relation::user::user_to_tuple_expr;
use crate::codegen::tuple_tokens;
use flowlog_parser::{DataType, Relation};

pub(crate) fn needs_conversion(rel: &Relation, string_intern: bool) -> bool {
    // Leaf-aware: a tuple column needs conversion when any of its (possibly
    // nested) leaves is a float or an interned string, since the public
    // `rel::*` tuple alias holds `f32`/`String` while the internal tuple holds
    // `OrderedFloat`/`Spur`.
    rel.data_type().iter().any(|dt| {
        dt.any_leaf(&|l| {
            matches!(l, DataType::Float32 | DataType::Float64)
                || (string_intern && matches!(l, DataType::String))
        })
    })
}

/// `identity` is the binding to forward when no column needs conversion;
/// otherwise emit a tuple literal of `elem(dt, src(i))` for each column.
pub(crate) fn per_position_tuple(
    rel: &Relation,
    string_intern: bool,
    identity: TokenStream,
    mut src: impl FnMut(usize) -> TokenStream,
    mut elem: impl FnMut(&DataType, TokenStream) -> TokenStream,
) -> TokenStream {
    if !needs_conversion(rel, string_intern) {
        return identity;
    }
    tuple_tokens(
        rel.data_type()
            .iter()
            .enumerate()
            .map(|(i, dt)| elem(dt, src(i))),
    )
}

/// User-tuple bound as `item` → internal `Tuple`. Used at insert time by
/// both engine modes.
pub(crate) fn user_to_tuple_convert(rel: &Relation, string_intern: bool) -> TokenStream {
    per_position_tuple(
        rel,
        string_intern,
        quote! { item },
        |i| {
            let idx = proc_macro2::Literal::usize_unsuffixed(i);
            quote! { item.#idx }
        },
        |dt, src| user_to_tuple_expr(dt, string_intern, src),
    )
}
