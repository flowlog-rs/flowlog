//! [`Segment`] — the ordered unit of a FlowLog program.
//!
//! A program is a sequence of segments processed in source order.
//! The stratifier cannot move rules across a [`Segment::Loop`] boundary.

use crate::logic::{FlowLogRule, LoopBlock};

/// An ordered element of a FlowLog program.
///
/// The stratifier processes segments in source order and cannot move rules
/// across a `Loop` boundary.
///
/// ```text
/// .decl ...
/// rule_a(X) :- edb(X).          // ─┐ Segment::Plain
/// rule_b(X) :- rule_a(X).       //  │
///                                // ─┘
/// loop fixpoint {                // ─┐ Segment::Loop
///     reach(X,Z) :- edge(X,Y),  //  │
///                   reach(Y,Z). //  │
/// }                              // ─┘
/// out(X) :- rule_b(X).          // ─── Segment::Plain
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// A plain segment of rules evaluated to fixpoint (normal stratification).
    Plain(Vec<FlowLogRule>),
    /// A bounded/conditional loop block (hard evaluation barrier).
    Loop(LoopBlock),
}

impl Segment {
    /// Rules in this segment. Returns an empty slice for `Loop`
    /// (use [`LoopBlock::rules`] to access rules inside the loop).
    pub fn as_rules(&self) -> &[FlowLogRule] {
        match self {
            Self::Plain(rules) => rules,
            Self::Loop(_) => &[],
        }
    }

    pub fn as_rules_mut(&mut self) -> Option<&mut Vec<FlowLogRule>> {
        match self {
            Self::Plain(rules) => Some(rules),
            Self::Loop(_) => None,
        }
    }

    pub fn as_loop(&self) -> Option<&LoopBlock> {
        match self {
            Self::Loop(block) => Some(block),
            Self::Plain(_) => None,
        }
    }

    pub fn as_loop_mut(&mut self) -> Option<&mut LoopBlock> {
        match self {
            Self::Loop(block) => Some(block),
            Self::Plain(_) => None,
        }
    }
}
