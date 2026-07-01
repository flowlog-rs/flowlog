//! Shared primitives for the FlowLog pipeline crates.

mod config;
mod diag;
mod fmt;
mod hash;
mod source;

pub use config::Config;
pub use config::ExecutionMode;
pub use config::program_stem;
pub use diag::BUG_URL;
pub use diag::BoxError;
pub use diag::Diagnostic;
pub use diag::InternalError;
pub use diag::emit;
pub use diag::emit_and_exit;
pub use diag::labels;
pub use diag::primary_label;
pub use diag::secondary_label;
pub use fmt::SECTION_BAR;
pub use fmt::SUBSECTION_BAR;
pub use fmt::pretty_print;
pub use hash::compute_fp;
pub use source::FileId;
pub use source::SourceMap;
pub use source::Span;
