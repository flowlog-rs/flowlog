//! Catalog errors: catalog-internal ICEs. The parser rejects every user
//! error in a rule before the catalog sees it.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use thiserror::Error;

#[derive(Debug, Error)]
pub(crate) enum CatalogError {
    /// Catalog-internal invariant violation (typically a planner/catalog
    /// contract break). Rendered as a "please file a bug" ICE.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl CatalogError {
    pub(super) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("catalog", detail, BUG_URL))
    }
}

impl Diagnostic for CatalogError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            CatalogError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CatalogError::Internal(_))
    }
}
