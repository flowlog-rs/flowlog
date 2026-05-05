//! Planner errors — user-facing head/aggregation violations, ICEs, and
//! bubbled-up catalog errors.
//!
//! Sites that a user's `.dl` program can trigger (head variable never bound
//! in the body, two rules producing the same relation with different
//! aggregations) surface as dedicated [`PlanError`] variants. Remaining
//! planner/optimizer panics that represented internal invariant checks are
//! wrapped via [`PlanError::internal`] so downstream callers see a "please
//! file a bug" ICE instead of a process abort.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use thiserror::Error;

use crate::catalog::CatalogError;
use crate::common::{
    BUG_URL, Diagnostic, FileId, InternalError, Span, primary_label, secondary_label,
};
use crate::parser::AggregationOperator;

#[non_exhaustive]
#[derive(Debug, Error)]
pub enum PlanError {
    /// A rule head references a variable that is never bound in the body.
    /// Valid syntax but semantically broken — caught during post-processing.
    #[error("unknown head variable `{var}`")]
    UnknownHeadVariable {
        head_span: Span,
        rule_span: Span,
        var: String,
    },

    /// Two rules produce the same IDB relation with different aggregation
    /// operators or column positions. Each rule is individually valid but
    /// together they disagree on the relation's shape.
    #[error(
        "inconsistent aggregation for relation `{rel}` within a stratum: \
         `{found_op:?}` at position {found_pos} conflicts with previously seen \
         `{existing_op:?}` at position {existing_pos}"
    )]
    InconsistentAggregation {
        rule_span: Span,
        prior_span: Span,
        rel: String,
        existing_op: AggregationOperator,
        existing_pos: usize,
        found_op: AggregationOperator,
        found_pos: usize,
    },

    /// A single rule head contains more than one aggregation argument.
    /// FlowLog's evaluator materializes at most one aggregation per head.
    #[error("rule head for `{rel}` contains {count} aggregations; at most one is allowed")]
    MultipleAggregationsInHead {
        head_span: Span,
        rule_span: Span,
        rel: String,
        count: usize,
    },

    /// Catalog errors bubble through the planner unchanged.
    #[error(transparent)]
    Catalog(#[from] CatalogError),

    /// Planner/optimizer invariant violation. Rendered as a "please file a
    /// bug" ICE. Both planner and optimizer sites use the `"planner"` stage
    /// label per the Phase 4 plan (the optimizer folds in).
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PlanError {
    pub(crate) fn internal(detail: impl Into<String>) -> Self {
        Self::Internal(InternalError::new("planner", detail, BUG_URL))
    }
}

impl Diagnostic for PlanError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            PlanError::UnknownHeadVariable {
                head_span,
                rule_span,
                var,
            } => user_diagnostic(
                self.to_string(),
                (
                    *head_span,
                    format!("`{var}` is referenced here but never bound by a positive body atom"),
                ),
                (*rule_span, "in this rule".into()),
                "every variable in the rule head must appear in a positive body \
                 atom so its value is determined during evaluation",
            ),

            PlanError::InconsistentAggregation {
                rule_span,
                prior_span,
                ..
            } => user_diagnostic(
                self.to_string(),
                (*rule_span, "conflicting aggregation declared here".into()),
                (*prior_span, "first aggregation declared here".into()),
                "rules producing the same relation within a stratum must agree \
                 on the aggregation operator and its position in the head",
            ),

            PlanError::MultipleAggregationsInHead {
                head_span,
                rule_span,
                ..
            } => user_diagnostic(
                self.to_string(),
                (*head_span, "multiple aggregations declared here".into()),
                (*rule_span, "in this rule".into()),
                "split the head into multiple rules — each producing a separate \
                 relation — if you need several aggregated columns",
            ),

            PlanError::Catalog(e) => e.to_diagnostic(),
            PlanError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        match self {
            PlanError::Internal(_) => true,
            PlanError::Catalog(e) => e.is_internal(),
            _ => false,
        }
    }
}

/// Build a user-facing diagnostic with a primary + secondary label and a
/// single explanatory note. Each `(span, msg)` pair is dropped silently if
/// the span is dummy, matching the behaviour of [`primary_label`] /
/// [`secondary_label`].
fn user_diagnostic(
    message: String,
    primary: (Span, String),
    secondary: (Span, String),
    note: &'static str,
) -> CsDiagnostic<FileId> {
    let mut labels = Vec::with_capacity(2);
    if let Some(l) = primary_label(primary.0) {
        labels.push(l.with_message(primary.1));
    }
    if let Some(l) = secondary_label(secondary.0) {
        labels.push(l.with_message(secondary.1));
    }
    CsDiagnostic::error()
        .with_message(message)
        .with_labels(labels)
        .with_notes(vec![note.to_string()])
}
