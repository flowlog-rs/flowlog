//! Catalog errors — user-facing safety violations and catalog-internal ICEs.

use std::fmt;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use thiserror::Error;

use flowlog_common::{
    BUG_URL, Diagnostic, FileId, InternalError, Span, primary_label, secondary_label,
};

/// Which body predicate carried an unsafe variable. Only affects the
/// rendered wording; both kinds share the same range-restriction rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnsafePredicateKind {
    Negation,
    Comparison,
}

impl fmt::Display for UnsafePredicateKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Negation => write!(f, "negated atom"),
            Self::Comparison => write!(f, "comparison"),
        }
    }
}

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum CatalogError {
    /// A variable in a negated atom, comparison, or function call never
    /// appears in a positive body atom (Datalog¬ range-restriction).
    #[error("unsafe variable `{var}` in {kind} `{predicate}`")]
    UnsafeVariable {
        kind: UnsafePredicateKind,
        /// Display form of the offending predicate.
        predicate: String,
        predicate_span: Span,
        rule_span: Span,
        var: String,
    },

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
            CatalogError::UnsafeVariable {
                predicate_span,
                rule_span,
                var,
                ..
            } => {
                let mut labels = Vec::new();
                if let Some(l) = primary_label(*predicate_span) {
                    labels
                        .push(l.with_message(format!("`{var}` is never bound in a positive atom")));
                }
                if let Some(l) = secondary_label(*rule_span) {
                    labels.push(l.with_message("in this rule"));
                }
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(labels)
                    .with_notes(vec![
                        "every variable in a body predicate must also appear in a \
                         positive atom, so the set of tuples it ranges over is finite"
                            .into(),
                    ])
            }

            CatalogError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        matches!(self, CatalogError::Internal(_))
    }
}
