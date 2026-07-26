//! [`Segment`]: the ordered unit of a FlowLog program.
//!
//! A program is an ordered sequence of segments. A [`Segment::Loop`] or
//! [`Segment::Fixpoint`] carries a [`LoopBlock`] with its [`LoopCondition`]
//! (built from [`StopGroup`], [`StopRelation`], and [`LoopConnective`]).

mod condition;
mod loop_block;

pub use condition::LoopCondition;
pub use condition::LoopConnective;
pub use condition::StopGroup;
pub use condition::StopRelation;
pub use loop_block::IterativeDirective;
pub use loop_block::LoopBlock;

use crate::ast::FlowLogRule;

/// An ordered element of a FlowLog program.
///
/// Segments are processed in source order; a `Loop` or `Fixpoint` is a hard
/// evaluation barrier that no rule may be reordered across.
///
/// ```text
/// .decl ...
/// rule_a(X) :- edb(X).                     // +-- Segment::Plain
/// rule_b(X) :- rule_a(X).                  // |
///                                          // +--
/// fixpoint {                               // +-- Segment::Fixpoint
///     reach(X,Z) :- edge(X,Y), reach(Y,Z). // |
/// }                                        // +--
/// out(X) :- rule_b(X).                     // --- Segment::Plain
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Segment {
    /// A plain segment of rules evaluated to fixpoint (normal stratification).
    Plain(Vec<FlowLogRule>),
    /// A bounded/conditional loop block (hard evaluation barrier).
    Loop(LoopBlock),
    /// A fixpoint block (hard evaluation barrier, no condition).
    Fixpoint(LoopBlock),
}

impl Segment {
    /// Rules in this segment. Returns an empty slice for `Loop`/`Fixpoint`
    /// (use [`LoopBlock::rules`] to access rules inside the block).
    #[must_use]
    pub fn as_rules(&self) -> &[FlowLogRule] {
        match self {
            Self::Plain(rules) => rules,
            Self::Loop(_) | Self::Fixpoint(_) => &[],
        }
    }

    /// The [`LoopBlock`] if this is a `Loop` or `Fixpoint` segment; `None` otherwise.
    #[must_use]
    pub fn as_loop(&self) -> Option<&LoopBlock> {
        match self {
            Self::Loop(block) | Self::Fixpoint(block) => Some(block),
            Self::Plain(_) => None,
        }
    }

    pub(crate) fn as_rules_mut(&mut self) -> &mut [FlowLogRule] {
        match self {
            Self::Plain(rules) => rules,
            Self::Loop(_) | Self::Fixpoint(_) => &mut [],
        }
    }

    pub(crate) fn as_loop_mut(&mut self) -> Option<&mut LoopBlock> {
        match self {
            Self::Loop(block) | Self::Fixpoint(block) => Some(block),
            Self::Plain(_) => None,
        }
    }
}
