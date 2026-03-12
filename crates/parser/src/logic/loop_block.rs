//! Loop block AST types.
//!
//! A loop block wraps a set of rules that are evaluated repeatedly until a
//! stop condition is met.  It acts as a hard evaluation barrier — the
//! stratifier cannot move rules across its boundary.
//!
//! Syntax:
//! ```text
//! loop <condition> { rules... }
//! ```

use super::FlowLogRule;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A boolean connective joining two stop expressions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoopConnective {
    And,
    Or,
}

/// A single stop-condition term inside a [`LoopCondition`].
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoopStopExpr {
    /// Stop when the semi-naive delta is empty (classic Datalog fixpoint).
    Fixpoint,
    /// Stop after exactly N iterations.
    Iteration(u64),
    /// Stop when the named relation becomes empty.
    RelationEmpty(String),
    /// Stop when the named relation becomes non-empty.
    RelationNonEmpty(String),
}

/// The composite stop condition of a loop block.
///
/// Represented as a first expression and a (possibly empty) list of
/// `(connective, expression)` pairs evaluated left-to-right.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopCondition {
    first: LoopStopExpr,
    rest: Vec<(LoopConnective, LoopStopExpr)>,
}

impl LoopCondition {
    #[must_use]
    pub fn new(first: LoopStopExpr, rest: Vec<(LoopConnective, LoopStopExpr)>) -> Self {
        Self { first, rest }
    }

    #[must_use]
    pub fn first(&self) -> &LoopStopExpr {
        &self.first
    }

    #[must_use]
    pub fn rest(&self) -> &[(LoopConnective, LoopStopExpr)] {
        &self.rest
    }
}

/// A loop block: a stop condition plus the rules evaluated inside the loop.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopBlock {
    condition: LoopCondition,
    rules: Vec<FlowLogRule>,
}

impl LoopBlock {
    #[must_use]
    pub fn new(condition: LoopCondition, rules: Vec<FlowLogRule>) -> Self {
        Self { condition, rules }
    }

    #[must_use]
    pub fn condition(&self) -> &LoopCondition {
        &self.condition
    }

    #[must_use]
    pub fn rules(&self) -> &[FlowLogRule] {
        &self.rules
    }

    pub fn rules_mut(&mut self) -> &mut Vec<FlowLogRule> {
        &mut self.rules
    }
}

// =============================================================================
// Display
// =============================================================================

impl fmt::Display for LoopStopExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fixpoint => write!(f, "fixpoint"),
            Self::Iteration(n) => write!(f, "{n}"),
            Self::RelationEmpty(name) => write!(f, "!{name}"),
            Self::RelationNonEmpty(name) => write!(f, "{name}"),
        }
    }
}

impl fmt::Display for LoopConnective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "and"),
            Self::Or => write!(f, "or"),
        }
    }
}

impl fmt::Display for LoopCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.first)?;
        for (conn, expr) in &self.rest {
            write!(f, " {conn} {expr}")?;
        }
        Ok(())
    }
}

impl fmt::Display for LoopBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "loop {} {{", self.condition)?;
        for rule in &self.rules {
            writeln!(f, "    {rule}")?;
        }
        write!(f, "}}")
    }
}

// =============================================================================
// Lexeme implementations
// =============================================================================

impl Lexeme for LoopStopExpr {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        // `pair` is `loop_stop_expr` — its single inner child is one of the
        // four concrete stop-expression rules.
        let inner = pair.into_inner().next().expect("loop_stop_expr: missing child");
        match inner.as_rule() {
            Rule::loop_fixpoint => Self::Fixpoint,
            Rule::loop_iter_count => {
                // inner child: integer
                let int_pair = inner
                    .into_inner()
                    .next()
                    .expect("loop_iter_count: missing integer");
                let n: u64 = int_pair
                    .as_str()
                    .parse()
                    .expect("loop_iter_count: iteration count must be a non-negative integer");
                Self::Iteration(n)
            }
            Rule::loop_relation_empty => {
                // inner child: relation_name
                let name = inner
                    .into_inner()
                    .next()
                    .expect("loop_relation_empty: missing relation_name")
                    .as_str()
                    .to_string();
                Self::RelationEmpty(name)
            }
            Rule::loop_relation_non_empty => {
                // inner child: relation_name
                let name = inner
                    .into_inner()
                    .next()
                    .expect("loop_relation_non_empty: missing relation_name")
                    .as_str()
                    .to_string();
                Self::RelationNonEmpty(name)
            }
            r => panic!("loop_stop_expr: unexpected rule {r:?}"),
        }
    }
}

impl Lexeme for LoopCondition {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let first_stop = inner.next().expect("loop_condition: missing first stop expr");
        let first = LoopStopExpr::from_parsed_rule(first_stop);

        let mut rest = Vec::new();
        // Remaining children alternate: loop_connective, loop_stop_expr, ...
        while let Some(conn_pair) = inner.next() {
            let connective = match conn_pair.into_inner().next().expect("loop_connective: missing child").as_rule() {
                Rule::loop_and => LoopConnective::And,
                Rule::loop_or => LoopConnective::Or,
                r => panic!("loop_connective: unexpected rule {r:?}"),
            };
            let expr_pair = inner.next().expect("loop_condition: missing stop expr after connective");
            let expr = LoopStopExpr::from_parsed_rule(expr_pair);
            rest.push((connective, expr));
        }

        Self::new(first, rest)
    }
}

impl Lexeme for LoopBlock {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner();

        let condition_pair = inner.next().expect("loop_block: missing loop_condition");
        let condition = LoopCondition::from_parsed_rule(condition_pair);

        let rules = inner.map(FlowLogRule::from_parsed_rule).collect();

        Self::new(condition, rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Rule};
    use pest::Parser;

    fn parse_loop_block(input: &str) -> LoopBlock {
        let mut pairs = FlowLogParser::parse(Rule::loop_block, input)
            .unwrap_or_else(|e| panic!("parse error: {e}"));
        LoopBlock::from_parsed_rule(pairs.next().unwrap())
    }

    #[test]
    fn fixpoint_condition() {
        let block = parse_loop_block("loop fixpoint { }");
        assert_eq!(*block.condition().first(), LoopStopExpr::Fixpoint);
        assert!(block.condition().rest().is_empty());
        assert!(block.rules().is_empty());
    }

    #[test]
    fn bare_iteration_count() {
        let block = parse_loop_block("loop 42 { }");
        assert_eq!(*block.condition().first(), LoopStopExpr::Iteration(42));
    }

    #[test]
    fn iteration_keyword_form() {
        let block = parse_loop_block("loop iteration == 10 { }");
        assert_eq!(*block.condition().first(), LoopStopExpr::Iteration(10));
    }

    #[test]
    fn relation_non_empty() {
        let block = parse_loop_block("loop done { }");
        assert_eq!(
            *block.condition().first(),
            LoopStopExpr::RelationNonEmpty("done".to_string())
        );
    }

    #[test]
    fn relation_empty() {
        let block = parse_loop_block("loop !queue { }");
        assert_eq!(
            *block.condition().first(),
            LoopStopExpr::RelationEmpty("queue".to_string())
        );
    }

    #[test]
    fn and_condition() {
        let block = parse_loop_block("loop fixpoint and 100 { }");
        assert_eq!(*block.condition().first(), LoopStopExpr::Fixpoint);
        let rest = block.condition().rest();
        assert_eq!(rest.len(), 1);
        assert_eq!(rest[0].0, LoopConnective::And);
        assert_eq!(rest[0].1, LoopStopExpr::Iteration(100));
    }

    #[test]
    fn or_condition() {
        let block = parse_loop_block("loop fixpoint or done { }");
        let rest = block.condition().rest();
        assert_eq!(rest[0].0, LoopConnective::Or);
        assert_eq!(rest[0].1, LoopStopExpr::RelationNonEmpty("done".to_string()));
    }

    #[test]
    fn with_rule() {
        let block = parse_loop_block("loop fixpoint { reach(X, Z) :- edge(X, Y), reach(Y, Z). }");
        assert_eq!(block.rules().len(), 1);
        assert_eq!(block.rules()[0].head().name(), "reach");
    }

    #[test]
    fn display_roundtrip() {
        let block = parse_loop_block("loop fixpoint and 100 { }");
        let s = block.to_string();
        assert!(s.starts_with("loop fixpoint and 100 {"));
    }
}
