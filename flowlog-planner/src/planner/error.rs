//! Planner errors: user errors that need the strata, ICEs, and catalog
//! errors.
//!
//! The parser owns errors within one rule; the planner reports those that
//! depend on how rules form recursion. Catalog errors pass through the
//! planner unchanged, and invariant violations use [`PlanError::internal`].

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use flowlog_common::BUG_URL;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::InternalError;
use flowlog_common::Span;
use flowlog_common::primary_label;
use flowlog_common::secondary_label;
use flowlog_parser::Lattice;
use thiserror::Error;

use crate::catalog::CatalogError;

#[non_exhaustive]
#[derive(Debug, Error)]
pub(crate) enum PlanError {
    /// A rule of a recursion reads `relation`, which the recursion derives,
    /// in a way a later round could undo; `read` says which. The atom
    /// `read_span` covers reads the relation, and `use_span` covers where
    /// the rule uses what it read. A recursion may only add rows and
    /// improve bounds, so that it has one fixpoint, whatever order its
    /// rows arrive in.
    #[error("{}", .read.message(.relation))]
    NotMonotone {
        read: NonMonotone,
        relation: String,
        read_span: Span,
        use_span: Span,
        rule_span: Span,
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

/// How a rule of a recursion reads a relation the recursion derives in a
/// way a later round could undo. A bound is the value of a lattice column,
/// which improves round by round in the direction its merge names, `min`
/// downwards and `max` upwards; inside the recursion a beaten bound may
/// still be read, so every use must stay valid as the bound improves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NonMonotone {
    /// Negates the relation: a row derived later falsifies the negation.
    Negation,
    /// Aggregates the relation: a row derived later changes the answer.
    Aggregate,
    /// Copies a bound into a plain column, where every beaten bound would
    /// stay a row of its own.
    Copied(Lattice),
    /// Derives a worse bound from a better one.
    Reversed(Lattice),
    /// Tests a bound with a comparison that a better bound may fail.
    Filtered(Lattice),
    /// Matches a bound exactly, which a better bound would no longer do.
    Matched(Lattice),
    /// Passes a bound through an operation other than `+` and `-`, whose
    /// direction the planner does not follow.
    Opaque(Lattice),
}

impl NonMonotone {
    fn message(self, relation: &str) -> String {
        match self {
            Self::Negation => format!("negation of `{relation}` inside its own recursion"),
            Self::Aggregate => {
                format!("aggregate reads `{relation}`, which its own recursion derives")
            }
            Self::Copied(lattice) => {
                format!("the `{lattice}` bound of `{relation}` is copied into a plain column")
            }
            Self::Reversed(lattice) => {
                format!("a better `{lattice}` bound of `{relation}` derives a worse one")
            }
            Self::Filtered(lattice) => {
                format!("a better `{lattice}` bound of `{relation}` may fail this comparison")
            }
            Self::Matched(lattice) => {
                format!("the `{lattice}` bound of `{relation}` is matched exactly")
            }
            Self::Opaque(lattice) => format!(
                "the planner cannot tell which way this moves with the `{lattice}` bound of `{relation}`"
            ),
        }
    }

    fn label(self, relation: &str) -> String {
        match self {
            Self::Negation | Self::Aggregate => format!("`{relation}` changes every round"),
            Self::Copied(_) => "every beaten bound would stay here as a row".into(),
            Self::Reversed(_) => "moves against the order of this column".into(),
            Self::Filtered(_) => "a better bound may make this false".into(),
            Self::Matched(_) => "a better bound would no longer match".into(),
            Self::Opaque(_) => "neither `+` nor `-` alone".into(),
        }
    }

    fn note(self) -> String {
        match self {
            Self::Negation => "a recursion may negate only relations computed before it: \
                 a negation would retract rows the recursion has already derived"
                .into(),
            Self::Aggregate => "an aggregate may read only relations computed before the \
                 recursion; to keep the least or greatest value per key across the \
                 recursion, declare a lattice column instead: `.decl R(k: T, v: min<U>)`"
                .into(),
            Self::Copied(lattice) => format!(
                "to keep the bound, declare this column a lattice too, as in `d: {lattice}<T>`; \
                 to test only that a bound exists, read it as `_`"
            ),
            Self::Reversed(lattice) => match lattice {
                Lattice::Min => "a `min` bound `d` flows into a `min` column as `d + c` or \
                     `d - c`, and into a `max` column as `c - d`"
                    .into(),
                Lattice::Max => "a `max` bound `d` flows into a `max` column as `d + c` or \
                     `d - c`, and into a `min` column as `c - d`"
                    .into(),
            },
            Self::Filtered(lattice) => match lattice {
                Lattice::Min => "a `min` bound `d` may be tested only from above, \
                     as in `d < c` or `d <= c`"
                    .into(),
                Lattice::Max => "a `max` bound `d` may be tested only from below, \
                     as in `d > c` or `d >= c`"
                    .into(),
            },
            Self::Matched(_) => "a bound may flow only into a lattice column or an ordering \
                 test; to test only that a bound exists, read it as `_`"
                .into(),
            Self::Opaque(_) => "a bound may flow through `+` and `-` with values that do not \
                 depend on it"
                .into(),
        }
    }
}

impl Diagnostic for PlanError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        match self {
            PlanError::NotMonotone {
                read,
                relation,
                read_span,
                use_span,
                rule_span,
            } => {
                let mut labels =
                    vec![primary_label(*use_span).map(|l| l.with_message(read.label(relation)))];
                if use_span != read_span {
                    labels.push(
                        secondary_label(*read_span)
                            .map(|l| l.with_message(format!("reads the bound of `{relation}`"))),
                    );
                }
                labels.push(
                    secondary_label(*rule_span).map(|l| l.with_message("in this recursive rule")),
                );
                CsDiagnostic::error()
                    .with_message(self.to_string())
                    .with_labels(labels.into_iter().flatten().collect())
                    .with_notes(vec![read.note()])
            }
            PlanError::Catalog(e) => e.to_diagnostic(),
            PlanError::Internal(ie) => ie.to_diagnostic(),
        }
    }

    fn is_internal(&self) -> bool {
        match self {
            PlanError::NotMonotone { .. } => false,
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
    use codespan_reporting::diagnostic::LabelStyle;
    use flowlog_common::SourceMap;
    use rstest::rstest;

    use super::*;

    #[test]
    fn internal_error_is_flagged_internal() {
        assert!(PlanError::internal("broken invariant").is_internal());
    }

    /// A recursion that is not monotone is the user's to fix, not an ICE.
    #[test]
    fn not_monotone_is_not_internal() {
        let err = PlanError::NotMonotone {
            read: NonMonotone::Aggregate,
            relation: "R".into(),
            read_span: Span::DUMMY,
            use_span: Span::DUMMY,
            rule_span: Span::DUMMY,
        };
        assert!(!err.is_internal());
    }

    /// The use of what a rule read gets the primary label; the read, when
    /// the use is elsewhere, and the enclosing rule get secondary ones.
    // Cases: the read rejected, the rule, its read and use spans, the
    // message, the labels in order.
    #[rstest]
    #[case::used_where_read(
        NonMonotone::Aggregate,
        "R(x, c) :- R(x, _), c = count : R(_, _).",
        (32, 39),
        (32, 39),
        "aggregate reads `R`, which its own recursion derives",
        &["`R` changes every round", "in this recursive rule"]
    )]
    #[case::used_elsewhere(
        NonMonotone::Reversed(Lattice::Min),
        "R(y, 10 - d) :- R(x, d), E(x, y).",
        (16, 23),
        (5, 11),
        "a better `min` bound of `R` derives a worse one",
        &["moves against the order of this column", "reads the bound of `R`", "in this recursive rule"]
    )]
    fn not_monotone_labels_the_use_the_read_and_the_rule(
        #[case] read: NonMonotone,
        #[case] rule: &str,
        #[case] (read_start, read_end): (usize, usize),
        #[case] (use_start, use_end): (usize, usize),
        #[case] message: &str,
        #[case] labels: &[&str],
    ) {
        let mut sm = SourceMap::new();
        let file = sm.add("test.dl".into(), rule.into());
        let span = |start: usize, end: usize| {
            let offset = |at: usize| u32::try_from(at).expect("a short rule");
            Span::new(file, offset(start), offset(end))
        };
        let err = PlanError::NotMonotone {
            read,
            relation: "R".into(),
            read_span: span(read_start, read_end),
            use_span: span(use_start, use_end),
            rule_span: span(0, rule.len()),
        };
        let diagnostic = err.to_diagnostic();
        assert_eq!(diagnostic.message, message);
        assert_eq!(diagnostic.labels[0].style, LabelStyle::Primary);
        assert_eq!(diagnostic.labels[0].range, use_start..use_end);
        let messages: Vec<&str> = diagnostic
            .labels
            .iter()
            .map(|l| l.message.as_str())
            .collect();
        assert_eq!(messages, labels);
    }
}
