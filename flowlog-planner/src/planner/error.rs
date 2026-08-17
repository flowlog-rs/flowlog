//! Planner errors: cross-rule planning errors, ICEs, and catalog errors.
//!
//! The parser owns errors within one rule. The planner reports errors that
//! require a completed stratum, while catalog errors pass through unchanged
//! and invariant violations use [`PlanError::internal`].

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_error::BUG_URL;
use flowlog_error::Diagnostic;
use flowlog_error::FileId;
use flowlog_error::FlowlogError;
use flowlog_error::InternalError;
use flowlog_error::Span;
use flowlog_error::primary_label;
use flowlog_error::secondary_label;
use flowlog_parser::AggregationOperator;
use thiserror::Error;

use crate::catalog::CatalogError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub(crate) enum PlanError {
    /// Rules in one stratum derive the same relation with incompatible
    /// aggregation operators or positions.
    #[error(
        "inconsistent aggregation for relation `{rel}` within a stratum: \
         `{found_op}` at position {found_pos} conflicts with previously seen \
         `{existing_op}` at position {existing_pos}"
    )]
    InconsistentAggregation {
        head_span: Span,
        prior_head_span: Span,
        rel: String,
        existing_op: AggregationOperator,
        existing_pos: usize,
        found_op: AggregationOperator,
        found_pos: usize,
    },

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
            PlanError::InconsistentAggregation {
                head_span,
                prior_head_span,
                ..
            } => {
                let mut labels = Vec::new();
                if let Some(label) = primary_label(*head_span) {
                    labels.push(label.with_message("conflicting aggregation declared here"));
                }
                if let Some(label) = secondary_label(*prior_head_span) {
                    labels.push(label.with_message("first aggregation declared here"));
                }
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(labels)
                    .with_notes(vec![
                        "rules producing the same relation within a stratum must agree \
                         on the aggregation operator and its position in the head"
                            .into(),
                    ])
            }
            PlanError::Catalog(e) => e.to_diagnostic(),
            PlanError::Internal(ie) => ie.to_diagnostic(),
        }
    }
}

impl FlowlogError for PlanError {
    fn is_internal(&self) -> bool {
        match self {
            PlanError::InconsistentAggregation { .. } => false,
            PlanError::Internal(_) => true,
            PlanError::Catalog(e) => e.is_internal(),
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_error::Span;

    use super::*;
    use crate::catalog::UnsafePredicateKind;

    #[test]
    fn internal_error_is_flagged_internal() {
        assert!(PlanError::internal("broken invariant").is_internal());
    }

    #[test]
    fn inconsistent_aggregation_is_not_internal() {
        let err = PlanError::InconsistentAggregation {
            head_span: Span::DUMMY,
            prior_head_span: Span::DUMMY,
            rel: "Totals".into(),
            existing_op: AggregationOperator::Sum,
            existing_pos: 0,
            found_op: AggregationOperator::Max,
            found_pos: 0,
        };
        assert!(!err.is_internal());
    }

    /// A bubbled-up user error keeps its user-facing (non-ICE) rendering.
    #[test]
    fn user_catalog_error_is_not_internal() {
        let err = PlanError::Catalog(CatalogError::UnsafeVariable {
            kind: UnsafePredicateKind::Negation,
            predicate: "!Blocked(x)".into(),
            predicate_span: Span::DUMMY,
            rule_span: Span::DUMMY,
            var: "x".into(),
        });
        assert!(!err.is_internal());
    }
}
