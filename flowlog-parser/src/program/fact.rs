//! Inline ground facts (`rel(c1, ...).`).
//!
//! An [`InlineFact`] is one ground tuple written directly in source. Its
//! columns seed the relation's initial contents, and it carries the source
//! span and the user's spelling so diagnostics can refer back to the fact as
//! written.

use flowlog_error::Span;

use crate::Constant;
use crate::ast::FlowLogRule;
use crate::error::ParseError;

/// One inline ground fact (`rel(c1, ...).`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineFact {
    /// Head span of the source `rel(c1, ...).` fact.
    pub span: Span,
    /// The relation name as spelled at this fact site.
    pub raw_name: String,
    /// The constant columns.
    pub columns: Vec<Constant>,
}

impl InlineFact {
    /// The `(canonical name, InlineFact)` a fact-shaped rule denotes. `Err`
    /// if the head is not all-constant (see
    /// [`FlowLogRule::extract_constants_from_head`]).
    pub(crate) fn from_rule(rule: &FlowLogRule) -> Result<(String, Self), ParseError> {
        let columns = rule.extract_constants_from_head()?;
        let head = rule.head();
        Ok((
            head.name().to_string(),
            Self {
                span: head.span(),
                raw_name: head.raw_name().to_string(),
                columns,
            },
        ))
    }
}
