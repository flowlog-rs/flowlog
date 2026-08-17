//! Catalog errors: user-facing safety violations and catalog-internal ICEs.

use std::fmt;

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_error::BUG_URL;
use flowlog_error::Diagnostic;
use flowlog_error::FileId;
use flowlog_error::FlowlogError;
use flowlog_error::InternalError;
use flowlog_error::Span;
use flowlog_error::primary_label;
use flowlog_error::secondary_label;
use thiserror::Error;

/// Which body predicate carried an unsafe variable. Only affects the
/// rendered wording; both kinds share the same range-restriction rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum UnsafePredicateKind {
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

#[derive(Debug, Error)]
pub(crate) enum CatalogError {
    /// A variable in a negated atom, comparison, or function call never
    /// appears in a positive body atom, breaking the range-restriction
    /// rule of Datalog with negation.
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
}

impl FlowlogError for CatalogError {
    fn is_internal(&self) -> bool {
        matches!(self, CatalogError::Internal(_))
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_error::SourceMap;

    use super::*;

    fn unsafe_variable(predicate_span: Span, rule_span: Span) -> CatalogError {
        CatalogError::UnsafeVariable {
            kind: UnsafePredicateKind::Negation,
            predicate: "!Blocked(other)".into(),
            predicate_span,
            rule_span,
            var: "other".into(),
        }
    }

    /// The predicate gets the primary label and the enclosing rule the
    /// secondary one, each with its own message.
    #[test]
    fn unsafe_variable_labels_predicate_and_rule() {
        let mut sm = SourceMap::new();
        let file = sm.add(
            "test.dl".into(),
            "Safe(n) :- Person(i, n), !Blocked(other).".into(),
        );
        let err = unsafe_variable(Span::new(file, 25, 40), Span::new(file, 0, 41));
        let labels = err.to_diagnostic().labels;
        assert_eq!(labels.len(), 2);
        assert!(labels[0].message.contains("never bound in a positive atom"));
        assert_eq!(labels[1].message, "in this rule");
    }

    /// Dummy spans (synthesized nodes) yield no labels instead of bogus
    /// source pointers.
    #[test]
    fn dummy_spans_yield_no_labels() {
        let err = unsafe_variable(Span::DUMMY, Span::DUMMY);
        assert!(err.to_diagnostic().labels.is_empty());
    }
}
