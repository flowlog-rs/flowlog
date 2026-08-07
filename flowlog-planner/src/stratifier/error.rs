//! User diagnostics produced during stratification.
//!
//! Each [`StratifyError`] describes an invalid program structure and retains
//! the source spans needed to render it.

use codespan_reporting::diagnostic::Diagnostic as CsDiagnostic;
use codespan_reporting::diagnostic::Label;
use flowlog_common::Diagnostic;
use flowlog_common::FileId;
use flowlog_common::Span;
use flowlog_common::primary_label;
use thiserror::Error;

/// Errors raised while stratifying a FlowLog program.
#[derive(Debug, Error)]
pub(crate) enum StratifyError {
    /// A rule body references a relation only derived in a later stratum.
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

    /// A recursive stratum has no relation that feeds back into itself.
    #[error(
        "recursive stratum #{stratum} has no recursive relations \
         (no head relation appears as a body atom)"
    )]
    RecursiveStratumEmpty {
        /// 1-based stratum number, as shown to the user.
        stratum: usize,
        /// 0-based rule IDs and spans of the stratum's rules.
        rules: Vec<(usize, Span)>,
    },
}

impl Diagnostic for StratifyError {
    fn to_diagnostic(&self) -> CsDiagnostic<FileId> {
        let base = CsDiagnostic::error().with_message(self.to_string());
        match self {
            StratifyError::ForwardReference { span, rel, .. } => base
                .with_labels(primary_label(*span).into_iter().collect())
                .with_notes(vec![format!(
                    "`{rel}` appears to be defined in a later stratum. \
                     Move the rule after the rules that derive `{rel}`."
                )]),

            StratifyError::RecursiveStratumEmpty { rules, .. } => {
                base.with_labels(rule_labels(rules)).with_notes(vec![
                    "a recursive stratum must contain at least one rule whose head \
                     relation also appears in a body atom within the same stratum"
                        .into(),
                ])
            }
        }
    }
}

/// Returns one primary label per rule, annotated with its ID.
fn rule_labels(rules: &[(usize, Span)]) -> Vec<Label<FileId>> {
    rules
        .iter()
        .filter_map(|(rid, span)| {
            primary_label(*span).map(|l| l.with_message(format!("rule {rid}")))
        })
        .collect()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::SourceMap;

    use super::*;

    #[test]
    fn dummy_span_yields_no_label() {
        let err = StratifyError::RecursiveStratumEmpty {
            stratum: 1,
            rules: vec![(0, Span::DUMMY)],
        };
        assert!(err.to_diagnostic().labels.is_empty());
    }

    #[test]
    fn label_message_names_the_rule_id() {
        let mut sm = SourceMap::new();
        let file = sm.add("test.dl".into(), "A(x) :- B(x).".into());
        let err = StratifyError::RecursiveStratumEmpty {
            stratum: 1,
            rules: vec![(7, Span::new(file, 0, 4))],
        };
        let labels = err.to_diagnostic().labels;
        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].message, "rule 7");
    }
}
