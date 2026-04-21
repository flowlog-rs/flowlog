//! Codegen errors — codegen-internal ICEs only.
//!
//! User-facing type/semantic errors are caught earlier by
//! the [`typechecker`] crate. By the time a program reaches codegen every
//! fingerprint, variable, and expression has already been validated; the
//! only failures possible here are invariant violations (missing
//! fingerprint, unreachable match arm) which surface as `please file a
//! bug` ICEs.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use crate::common::diag::{Diagnostic, InternalError, BUG_URL};
use crate::common::source::FileId;
use thiserror::Error;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CodegenError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CodegenError {
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
