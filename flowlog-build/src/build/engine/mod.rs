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
