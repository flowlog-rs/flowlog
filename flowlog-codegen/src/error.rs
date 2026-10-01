//! Codegen errors, which are internal errors only.
//!
//! A mistake in the user's program is reported earlier, by flowlog-parser's
//! typecheck pass. By the time a program reaches codegen every fingerprint,
//! variable, and expression has been validated, so the only failures left
//! are invariant violations, such as a missing fingerprint, reported as
//! internal errors that ask the user to file a bug.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use thiserror::Error;

/// An error codegen reports.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CodegenError {
    /// A violated codegen invariant: a bug in FlowLog, not in the program.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CodegenError {
    /// Returns an internal error carrying `detail` and the bug-report link.
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("codegen", detail, BUG_URL))
    }
}

impl Diagnostic for CodegenError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            CodegenError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CodegenError::Internal(_))
    }
}
