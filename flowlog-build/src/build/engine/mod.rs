//! Library-mode engine generation.
//!
//! [`batch`] generates a single-run engine; [`incremental`] generates an
//! engine whose dataflow stays alive across commits. Both engines stage
//! host rows through typed methods and delegate input partitioning and
//! conversion to runtime loaders.
//!
//! The engines manage worker execution and collect results. Shared helpers
//! here support converting dataflow output back to user-facing tuples.

mod batch;
mod incremental;

pub(crate) use batch::gen_lib_engine;
use flowlog_parser::DataType;
use flowlog_parser::Relation;
pub(crate) use incremental::gen_lib_incremental_engine;
use proc_macro2::TokenStream;

use crate::codegen::tuple_tokens;

pub(crate) fn needs_conversion(rel: &Relation, string_intern: bool) -> bool {
    // Leaf-aware: a tuple column needs conversion when any of its (possibly
    // nested) leaves is a float or an interned string, since the public
    // `rel::*` tuple alias holds `f32`/`String` while the internal tuple holds
    // `OrderedFloat`/`Spur`.
    rel.data_type().iter().any(|dt| {
        dt.any_scalar(&|l| {
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
