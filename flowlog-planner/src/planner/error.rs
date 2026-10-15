//! Planner errors: ICEs and catalog errors.
//!
//! The parser owns errors within one rule. Catalog errors pass through the
//! planner unchanged, and invariant violations use [`PlanError::internal`].

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use thiserror::Error;

use crate::catalog::CatalogError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub(crate) enum PlanError {
    /// Catalog errors bubble through the planner unchanged.
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// Planner/optimizer invariant violation. Rendered as a "please file a
    /// bug" ICE; optimizer sites share the `"planner"` stage label.
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PlanError {
    pub fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("planner", detail, BUG_URL))
    }
}

impl Diagnostic for PlanError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            PlanError::Catalog(e) => e.to_diagnostic(),
            PlanError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        match self {
            PlanError::Internal(_) => true,
            PlanError::Catalog(e) => e.is_internal(),
        }
    }
}
