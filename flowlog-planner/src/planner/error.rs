//! Planner errors: user errors that need the strata or a rule's plan, ICEs,
//! and catalog errors.
//!
//! The parser owns errors within one rule as written; the planner reports
//! those that depend on how rules form recursion or how a rule is planned.
//! Catalog errors pass through the planner unchanged, and invariant
//! violations use [`PlanError::internal`].

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use flowlog_common::MAX_ROW_WIDTH;
use flowlog_common::Span;
use flowlog_common::primary_label;
use flowlog_common::secondary_label;
use thiserror::Error;

use crate::catalog::CatalogError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub(crate) enum PlanError {
    /// An aggregate reads `relation`, which its own recursive stratum
    /// derives, at the atom `read_span` covers. An aggregate over a changing
    /// input has no fixpoint to settle on; only a lattice column may merge
    /// values across recursion.
    #[error("aggregate reads `{relation}`, which its own recursion derives")]
    AggregateInRecursion {
        relation: String,
        read_span: Span,
        rule_span: Span,
    },

    /// `choice`, a relation with a choice domain declared at
    /// `choice_span`, and `lattice`, a lattice relation declared at
    /// `lattice_span`, feed back into one recursion over append or mutable
    /// input. There the lattice retracts each value it improves on in a
    /// later round, so the choice's candidates no longer only grow from
    /// round to round, and the round no longer finds the choice a batch run
    /// makes.
    #[error(
        "relations `{choice}`, which declares a choice domain, and `{lattice}`, which declares \
         a lattice column, share a recursion over append or mutable input"
    )]
    ChoiceBesideLatticeInChangingRecursion {
        choice: String,
        choice_span: Span,
        lattice: String,
        lattice_span: Span,
    },

    /// The rule at `rule_span` plans to a collection whose key or value holds
    /// `width` columns, more than a row holds: the rule carries that many
    /// variables from one step of its plan to the next.
    #[error("rule carries {width} columns in one row, more than the {MAX_ROW_WIDTH} a row holds")]
    RowTooWide { width: usize, rule_span: Span },

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
            PlanError::AggregateInRecursion {
                relation,
                read_span,
                rule_span,
            } => {
                let labels = [
                    primary_label(*read_span)
                        .map(|l| l.with_message(format!("`{relation}` changes every round"))),
                    secondary_label(*rule_span).map(|l| l.with_message("in this recursive rule")),
                ];
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(labels.into_iter().flatten().collect())
                    .with_notes(vec![
                        "an aggregate may read only relations computed before the recursion; \
                         to keep the least or greatest value per key across the recursion, \
                         declare a lattice column instead: `.decl R(k: T, v: min<U>)`"
                            .into(),
                    ])
            }
            PlanError::ChoiceBesideLatticeInChangingRecursion {
                choice_span,
                lattice_span,
                ..
            } => {
                let labels = [
                    primary_label(*choice_span).map(|l| l.with_message("declares a choice domain")),
                    secondary_label(*lattice_span)
                        .map(|l| l.with_message("declares a lattice column in the same recursion")),
                ];
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(labels.into_iter().flatten().collect())
                    .with_notes(vec![
                        "a choice may share a recursion with a lattice only over input that \
                         never changes: derive one of them outside the recursion, or from \
                         static input"
                            .into(),
                    ])
            }
            PlanError::RowTooWide { width, rule_span } => {
                let label = primary_label(*rule_span).map(|l| {
                    l.with_message(format!("carries {width} variables between two steps"))
                });
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(label.into_iter().collect())
                    .with_notes(vec![
                        "derive part of the body in a rule of its own, so fewer variables travel \
                         together"
                            .into(),
                    ])
            }
            PlanError::Catalog(e) => e.to_diagnostic(),
            PlanError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        match self {
            PlanError::AggregateInRecursion { .. }
            | PlanError::ChoiceBesideLatticeInChangingRecursion { .. }
            | PlanError::RowTooWide { .. } => false,
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
    use flowlog_common::SourceMap;

    use super::*;

    #[test]
    fn internal_error_is_flagged_internal() {
        assert!(PlanError::internal("broken invariant").is_internal());
    }

    /// An aggregate in a recursive rule is the user's to fix, not an ICE.
    #[test]
    fn aggregate_in_recursion_is_not_internal() {
        let err = PlanError::AggregateInRecursion {
            relation: "R".into(),
            read_span: Span::DUMMY,
            rule_span: Span::DUMMY,
        };
        assert!(!err.is_internal());
    }

    /// The recursive atom the aggregate reads gets the primary label and the
    /// enclosing rule the secondary one.
    #[test]
    fn aggregate_in_recursion_labels_the_read_and_the_rule() {
        let mut sm = SourceMap::new();
        let file = sm.add(
            "test.dl".into(),
            "R(x, c) :- R(x, _), c = count : R(_, _).".into(),
        );
        let err = PlanError::AggregateInRecursion {
            relation: "R".into(),
            read_span: Span::new(file, 32, 39),
            rule_span: Span::new(file, 0, 40),
        };
        let labels = err.to_diagnostic().labels;
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].message, "`R` changes every round");
        assert_eq!(labels[1].message, "in this recursive rule");
    }

    /// A rule too wide to plan is the user's to fix, not an ICE, and its
    /// one label is the rule.
    #[test]
    fn row_too_wide_labels_the_rule() {
        let mut sm = SourceMap::new();
        let file = sm.add("test.dl".into(), "O(x) :- R(x).".into());
        let err = PlanError::RowTooWide {
            width: 65,
            rule_span: Span::new(file, 0, 13),
        };
        assert!(!err.is_internal());
        let labels = err.to_diagnostic().labels;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].message, "carries 65 variables between two steps");
    }

    /// The choice's declaration gets the primary label and the lattice's
    /// the secondary one.
    #[test]
    fn choice_beside_lattice_labels_both_declarations() {
        let mut sm = SourceMap::new();
        let source =
            ".decl C(k: int32, v: int32) choice-domain k\n.decl A(k: int32, v: min<int32>)";
        let file = sm.add("test.dl".into(), source.into());
        let err = PlanError::ChoiceBesideLatticeInChangingRecursion {
            choice: "C".into(),
            choice_span: Span::new(file, 0, 43),
            lattice: "A".into(),
            lattice_span: Span::new(file, 44, 76),
        };
        assert!(!err.is_internal());
        let labels = err.to_diagnostic().labels;
        assert_eq!(labels.len(), 2);
        assert_eq!(labels[0].message, "declares a choice domain");
        assert_eq!(
            labels[1].message,
            "declares a lattice column in the same recursion"
        );
    }
}
