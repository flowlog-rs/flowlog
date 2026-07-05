//! Inline ground facts (`rel(c1, ...).`) as stored on [`Program`].
//!
//! Facts are collected from fact-shaped rules during parsing (see
//! `extract_fact` in `parse.rs`) into a map keyed by canonical relation
//! name. Each entry keeps the pieces of the source site that the canonical
//! key erases — the span and the user's spelling — so later passes can
//! report against exactly what was written.
//!
//! [`Program`]: super::Program

use flowlog_common::Span;

use crate::ConstType;

/// One inline ground fact (`rel(c1, ...).`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineFact {
    /// Head span of the source `rel(c1, ...).` fact.
    pub span: Span,
    /// The relation name as spelled at this fact site.
    pub raw_name: String,
    /// The constant columns.
    pub columns: Vec<ConstType>,
}
