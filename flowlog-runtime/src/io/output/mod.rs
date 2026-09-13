//! Collecting and emitting relation results.
//!
//! [`Emitter`] collects worker updates and selects their output order.
//! Internally, `encode` converts tuples into typed rows or text; `writer`
//! owns destination buffering and record layout.

pub(super) mod atomic;
mod emitter;
mod encode;
mod sort;
mod writer;

pub use emitter::Emitter;
