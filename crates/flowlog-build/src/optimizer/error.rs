//! Errors surfaced by the optimizer.
//!
//! Every optimizer failure today is *internal*: the only fallible work is
//! loading `--profile-hint` feedback — the memory logs and the
//! `feedback_accum.json` accumulator — and those files are written by the
//! compiler and runtime themselves, never authored by the user. A failure
//! to read or parse one is a corrupt compiler artifact, not a user
//! mistake. Cardinality estimation and the DP join search never fail.
//!
//! [`OptimizerError`] is nonetheless a proper enum, not a bare
//! [`InternalError`], so a future fallible operation that *is* the user's
//! fault has a variant to land on — mirroring `ParseError` /
//! `TypeCheckError`, which carry user variants alongside one `Internal`.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use thiserror::Error;

use crate::common::{BUG_URL, Diagnostic, FileId, InternalError};

#[derive(Debug, Error)]
pub enum OptimizerError {
    /// A failure reading `--profile-hint` feedback — a corrupt compiler
    /// artifact. Rendered as a "please file a bug" internal compiler error.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl OptimizerError {
    /// Wrap a profile-load failure (an I/O or JSON error) as an
    /// [`OptimizerError::Internal`], tagged with the `optimizer` stage.
    pub(crate) fn internal(cause: impl std::fmt::Display) -> Self {
        Self::Internal(InternalError::new("optimizer", cause.to_string(), BUG_URL))
    }
}

impl Diagnostic for OptimizerError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            Self::Internal(e) => e.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, Self::Internal(_))
    }
}
