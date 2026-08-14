//! The error vocabulary FlowLog crates share.
//!
//! [`FlowlogError`] is the currency: each stage's error type implements it,
//! so a caller can read a message and tell our bug from the user's mistake
//! without knowing which stage produced it.
//!
//! - [`error`]: the trait, [`InternalError`], and [`BUG_URL`].
//! - [`source`]: spans, file ids, and the map that resolves them.
//! - [`diag`]: rendering a span-carrying error as a source diagnostic.

mod diag;
mod error;
mod source;

pub use diag::BoxError;
pub use diag::Diagnostic;
pub use diag::emit;
pub use diag::emit_and_exit;
pub use diag::labels;
pub use diag::primary_label;
pub use diag::secondary_label;
pub use error::BUG_URL;
pub use error::FlowlogError;
pub use error::InternalError;
pub use source::FileId;
pub use source::SourceMap;
pub use source::Span;
