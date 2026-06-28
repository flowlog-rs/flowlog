//! Shared primitives for the FlowLog pipeline crates.

mod config;
mod diag;
mod fmt;
mod hash;
mod source;

pub use config::{Config, ExecutionMode, program_stem};
pub use diag::{
    BUG_URL, BoxError, Diagnostic, InternalError, emit, emit_and_exit, labels, primary_label,
    secondary_label,
};
pub use fmt::{SECTION_BAR, SUBSECTION_BAR, pretty_print};
pub use hash::compute_fp;
pub use source::{FileId, SourceMap, Span};
