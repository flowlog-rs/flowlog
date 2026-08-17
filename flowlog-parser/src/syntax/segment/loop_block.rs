//! Loop and fixpoint blocks for FlowLog Datalog programs.
//!
//! - [`LoopBlock`]: the rules inside a `fixpoint { ... }` or
//!   `loop <cond> { ... }` region, plus its `.iterative` directives. Carried
//!   by a barrier [`Segment`](super::Segment).
//! - [`IterativeDirective`]: a relation marked `.iterative` in a block.
//!
//! The stop conditions of a `loop` block live in [`super::condition`].
//! See `grammar.pest` for the surface syntax.

use std::fmt;

use educe::Educe;
use flowlog_common::compute_fp;
use flowlog_error::Span;

use super::LoopCondition;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::ast::FlowLogRule;
use crate::error::ParseError;
use crate::error::grammar_bug;

// =============================================================================
// IterativeDirective
// =============================================================================

/// A relation marked `.iterative` inside a loop/fixpoint block.
///
/// Iterative relations use replacement semantics (re-derived each
/// iteration, so stale facts are retracted) instead of the default
/// accumulative semantics. The mark is scoped per-block: the same
/// relation can be iterative in one block and accumulative in another.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct IterativeDirective {
    name: String,
    fp: u64,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl IterativeDirective {
    /// Canonical (lowercased) relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Fingerprint of the canonical name (`compute_fp`).
    #[must_use]
    #[inline]
    pub fn fp(&self) -> u64 {
        self.fp
    }

    /// Span of the `.iterative <name>` directive.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }
}

/// Parse an `iterative_directive` node (`.iterative <name>`) into an
/// [`IterativeDirective`].
fn parse_iterative_directive(node: Node) -> Result<IterativeDirective, ParseError> {
    let span = node.span();
    let name = node
        .children()
        .require(Rule::relation_ref)?
        .text()
        .to_ascii_lowercase();
    let fp = compute_fp(&name);
    Ok(IterativeDirective { name, fp, span })
}

// =============================================================================
// LoopBlock
// =============================================================================

/// A `fixpoint`/`loop` block: its `.iterative` directives, an optional stop
/// condition, and the rules evaluated inside.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct LoopBlock {
    /// Relations marked `.iterative`; any relation absent here is accumulative.
    iterative_relations: Vec<IterativeDirective>,
    /// `None` for a pure fixpoint block (terminates when no new tuples derive).
    condition: Option<LoopCondition>,
    rules: Vec<FlowLogRule>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl LoopBlock {
    /// Source location this loop/fixpoint block was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Relations explicitly marked as iterative (replacement semantics).
    #[must_use]
    pub fn iterative_relations(&self) -> &[IterativeDirective] {
        &self.iterative_relations
    }

    /// The loop condition, or `None` for a pure fixpoint block.
    #[must_use]
    pub fn condition(&self) -> Option<&LoopCondition> {
        self.condition.as_ref()
    }

    /// The rules evaluated inside the block.
    #[must_use]
    pub fn rules(&self) -> &[FlowLogRule] {
        &self.rules
    }

    pub(crate) fn rules_mut(&mut self) -> &mut Vec<FlowLogRule> {
        &mut self.rules
    }
}

impl fmt::Display for LoopBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.condition {
            Some(cond) => writeln!(f, "loop {cond} {{")?,
            None => writeln!(f, "fixpoint {{")?,
        }
        for directive in &self.iterative_relations {
            writeln!(f, "    .iterative {}", directive.name())?;
        }
        for rule in &self.rules {
            writeln!(f, "    {rule}")?;
        }
        write!(f, "}}")
    }
}

impl Lexeme for LoopBlock {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();

        // Optional condition clause (fixpoint blocks have no condition).
        let condition: Option<LoopCondition> = children
            .take_if(Rule::loop_condition)
            .map(Node::lower)
            .transpose()?;

        // Block body: interleaved `.iterative` directives and rules. Each
        // rule carries its own trailing `.plan`.
        let mut iterative_relations = Vec::new();
        let mut rules: Vec<FlowLogRule> = Vec::new();
        for item in children {
            match item.rule() {
                Rule::iterative_directive => {
                    iterative_relations.push(parse_iterative_directive(item)?);
                }
                Rule::rule => {
                    // Raw pair bridge: `expand_from_parsed_rule` still takes a
                    // `Pair`/`FileId` (defined in `ast::rule`).
                    let (pair, file) = item.into_parts();
                    rules.extend(FlowLogRule::expand_from_parsed_rule(pair, file)?);
                }
                r => {
                    return Err(grammar_bug(format!(
                        "unexpected rule in loop/fixpoint block: {r:?}"
                    )));
                }
            }
        }

        Ok(Self {
            iterative_relations,
            condition,
            rules,
            span,
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::parse_node;

    fn parse_loop_block(input: &str) -> LoopBlock {
        let start_rule = if input.trim_start().starts_with("fixpoint") {
            Rule::fixpoint_block
        } else {
            Rule::loop_block
        };
        parse_node(start_rule, input)
    }

    #[test]
    fn fixpoint_no_condition() {
        let block = parse_loop_block("fixpoint { }");
        assert!(block.condition().is_none());
        assert!(block.rules().is_empty());
    }

    /// A `loop` block carries its parsed condition; the condition's own
    /// semantics are tested in `condition.rs`.
    #[test]
    fn loop_block_carries_its_condition() {
        let block = parse_loop_block("loop while { @it <= 6 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 6u16)]);
    }

    #[test]
    fn fixpoint_collects_body_rule() {
        let block = parse_loop_block("fixpoint { reach(X, Z) :- edge(X, Y), reach(Y, Z). }");
        assert!(block.condition().is_none());
        assert_eq!(block.rules().len(), 1);
        assert_eq!(block.rules()[0].head().name(), "reach");
    }

    #[test]
    fn display_fixpoint() {
        let block = parse_loop_block("fixpoint { }");
        let s = block.to_string();
        assert!(s.starts_with("fixpoint {"));
    }

    /// Block-level Display stitches the condition and body together.
    #[test]
    fn display_until_or_while() {
        let block = parse_loop_block("loop until { done } or while { @it <= 6 } { }");
        let s = block.to_string();
        assert!(s.contains("done") && s.contains("or") && s.contains("@it <= 6"));
    }

    /// A single `.iterative` directive; the mixed-case name is canonicalized
    /// to lowercase, with its fingerprint taken on the canonical form.
    #[test]
    fn iterative_directive_single() {
        let block = parse_loop_block("fixpoint { .iterative Removed }");
        let itr = block.iterative_relations();
        assert_eq!(itr.len(), 1);
        assert_eq!(itr[0].name(), "removed");
        assert_eq!(itr[0].fp(), flowlog_common::compute_fp("removed"));
        assert!(block.rules().is_empty());
    }

    #[test]
    fn iterative_directive_multiple() {
        let block = parse_loop_block("fixpoint { .iterative active_edge .iterative degree }");
        let itr = block.iterative_relations();
        assert_eq!(itr.len(), 2);
        assert_eq!(itr[0].name(), "active_edge");
        assert_eq!(itr[1].name(), "degree");
    }

    #[test]
    fn iterative_directive_with_rules() {
        let block = parse_loop_block(
            "fixpoint { .iterative reach  reach(X, Z) :- edge(X, Y), reach(Y, Z). }",
        );
        assert_eq!(block.iterative_relations().len(), 1);
        assert_eq!(block.iterative_relations()[0].name(), "reach");
        assert_eq!(block.rules().len(), 1);
    }

    #[test]
    fn iterative_directive_in_loop() {
        let block = parse_loop_block(
            "loop while { @it <= 5 } { .iterative active_edge  active_edge(X,Y) :- edge(X,Y). }",
        );
        assert_eq!(block.iterative_relations().len(), 1);
        assert_eq!(block.iterative_relations()[0].name(), "active_edge");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 5u16)]);
    }

    #[test]
    fn no_iterative_is_empty() {
        let block = parse_loop_block("loop while { @it <= 3 } { }");
        assert!(block.iterative_relations().is_empty());
    }

    #[test]
    fn display_iterative_directive() {
        let block = parse_loop_block("fixpoint { .iterative active_edge .iterative degree }");
        let s = block.to_string();
        assert!(s.contains(".iterative active_edge"));
        assert!(s.contains(".iterative degree"));
    }
}
