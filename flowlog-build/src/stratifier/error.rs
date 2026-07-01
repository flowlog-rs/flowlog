//! Stratification errors.
//!
//! `StratifyError` covers failures reachable from a user-authored `.dl`
//! program: recursion outside a `loop`/`fixpoint` block in extended mode,
//! malformed `.iterative` directives, forward references across a loop
//! barrier, empty recursive strata, and loop conditions that can never
//! fire. Each variant carries at least one [`Span`] so the renderer can
//! point at the offending source.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use codespan_reporting::diagnostic::Label;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::Span;
use flowlog_common::primary_label;
use thiserror::Error;

/// Build one primary label per rule, annotated with `rule {id}`.
fn rule_labels(rules: &[(usize, Span)]) -> Vec<Label<FileId>> {
    rules
        .iter()
        .filter_map(|(rid, span)| {
            primary_label(*span).map(|l| l.with_message(format!("rule {rid}")))
        })
        .collect()
}

/// Errors raised while stratifying a FlowLog program.
#[non_exhaustive]
#[derive(Debug, Error)]
pub enum StratifyError {
    /// Extended Datalog mode: a recursive SCC appeared in plain (non-loop) rules.
    #[error(
        "recursive rules must be inside an explicit `loop`/`fixpoint` block, \
         but recursion was found in plain rules"
    )]
    RecursionOutsideLoop {
        /// Rule IDs and spans of the rules in the offending SCC.
        rules: Vec<(usize, Span)>,
        hint: &'static str,
    },

    /// A `.iterative` directive names a relation with no rule inside the loop body.
    #[error("`iterative` relation `{rel}` has no rule inside the loop body that derives it")]
    IterativeNotInLoopHead {
        rel: String,
        /// Span of the loop/fixpoint block whose `.iterative` directive is wrong.
        decl_span: Span,
    },

    /// A `.iterative` directive names a relation that never appears as a body atom.
    #[error(
        "`iterative` relation `{rel}` is not recursive in this loop \
         (never appears as a body atom)"
    )]
    IterativeNotRecursive {
        rel: String,
        /// Span of the loop/fixpoint block whose `.iterative` directive is wrong.
        decl_span: Span,
    },

    /// A rule body references a relation only derived in a later segment.
    #[error(
        "rule {rule} references relation `{rel}`, which is not yet defined \
         at this point in the program"
    )]
    ForwardReference {
        rule: usize,
        /// Span of the offending body atom.
        span: Span,
        rel: String,
    },

    /// A `loop`/`fixpoint` block's rules have no recursive relation.
    #[error(
        "recursive stratum #{stratum} has no recursive relations \
         (no head relation appears as a body atom)"
    )]
    RecursiveStratumEmpty {
        stratum: usize,
        rules: Vec<(usize, Span)>,
    },

    /// A loop `until` condition names a relation with no deriving rule in
    /// the loop body.
    #[error(
        "loop until condition references relation `{rel}`, which has no \
         rule inside the loop body that derives it"
    )]
    LoopConditionNotDerived {
        rel: String,
        /// Span of the loop block whose condition is invalid.
        span: Span,
    },

    /// A loop `until` condition names a relation that is derived inside the
    /// loop body but does not depend on any recursive relation, so it can
    /// never change across iterations.
    #[error(
        "loop until condition references relation `{rel}`, which does not \
         depend on any recursive relation in this loop"
    )]
    LoopConditionNotRecursive {
        rel: String,
        /// Span of the loop block whose condition is invalid.
        span: Span,
    },
}

impl Diagnostic for StratifyError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            StratifyError::RecursionOutsideLoop { rules, hint } => base
                .with_labels(rule_labels(rules))
                .with_notes(vec![hint.to_string()]),

            StratifyError::IterativeNotInLoopHead { decl_span, .. } => base
                .with_labels(primary_label(*decl_span).into_iter().collect())
                .with_notes(vec![
                    "every relation declared `iterative` must appear as the head \
                     of at least one rule inside the fixpoint/loop block"
                        .into(),
                ]),

            StratifyError::IterativeNotRecursive { decl_span, .. } => base
                .with_labels(primary_label(*decl_span).into_iter().collect())
                .with_notes(vec![
                    "only relations that feed back into their own derivation can \
                     use iterative (replacement) semantics"
                        .into(),
                ]),

            StratifyError::ForwardReference { span, rel, .. } => base
                .with_labels(primary_label(*span).into_iter().collect())
                .with_notes(vec![format!(
                    "`{rel}` appears to be defined in a later segment. \
                     Move the rule after the segment that derives `{rel}`."
                )]),

            StratifyError::RecursiveStratumEmpty { rules, .. } => {
                base.with_labels(rule_labels(rules)).with_notes(vec![
                    "a `loop`/`fixpoint` block must contain at least one rule whose \
                     head relation also appears in a body atom within the same block"
                        .into(),
                ])
            }

            StratifyError::LoopConditionNotDerived { span, .. } => base
                .with_labels(primary_label(*span).into_iter().collect())
                .with_notes(vec![
                    "the until-condition relation must appear as the head of \
                     at least one rule inside the loop block"
                        .into(),
                ]),

            StratifyError::LoopConditionNotRecursive { span, .. } => base
                .with_labels(primary_label(*span).into_iter().collect())
                .with_notes(vec![
                    "an until-condition relation must be derived from the loop's \
                     recursive computation to be a meaningful termination signal"
                        .into(),
                ]),
        }
    }
}
