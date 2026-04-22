//! Compiler-driver errors.
//!
//! - `Io` — infrastructure failures (project scaffold write, `cargo build`
//!   shell-out, binary install). User fixes their environment.
//! - `Internal` — invariant violations. Rendered as "please file a bug" ICEs.

use std::io;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_build::common::{Diagnostic, InternalError, BUG_URL};
use flowlog_build::common::FileId;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum CompilerError {
    #[error("{0}")]
    Io(#[from] io::Error),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CompilerError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("compiler", detail, BUG_URL))
    }
}

impl Diagnostic for CompilerError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            CompilerError::Io(_) => CsDiagnostic::error().with_message(self.to_string()),
            CompilerError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CompilerError::Internal(_))
    }
}
