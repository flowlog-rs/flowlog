//! AST types for loop blocks.
//!
//! A loop block groups a set of rules that are evaluated iteratively.  It acts
//! as a hard evaluation barrier: the stratifier cannot move rules across its
//! boundary.  The loop always terminates implicitly when the fixpoint is
//! reached (no new tuples are derived); the optional condition lets you impose
//! an *earlier* stopping criterion.
//!
//! ## Grammar
//!
//! ```text
//! loop_block = { "loop" ~ loop_condition? ~ "{" ~ rule* ~ "}" }
//!
//! loop_condition = { loop_continue ~ (loop_connective ~ loop_stop)?
//!                  | loop_stop     ~ (loop_connective ~ loop_continue)? }
//!
//! loop_continue = { "continue" ~ "{" ~ loop_iter_expr ~ "}" }
//! loop_stop     = { "stop"     ~ "{" ~ loop_stop_group ~ "}" }
//!
//! loop_iter_expr  = { "iter" ~ loop_iter_compare_op ~ integer
//!                     ~ (loop_connective ~ "iter" ~ loop_iter_compare_op ~ integer)* }
//! loop_stop_group = { loop_bool_relation ~ (loop_connective ~ loop_bool_relation)* }
//!
//! loop_iter_compare_op = { "<=" | ">=" | "==" | "<" | ">" }
//! loop_bool_relation   = { relation_name }
//! loop_connective      = { "and" | "or" }
//! ```
//!
//! ## Condition semantics
//!
//! - **`continue { iter_expr }`** — keep looping while the current iteration
//!   count satisfies the expression.  The expression is one or more
//!   `iter op N` sub-conditions joined by `and` (intersection) or `or`
//!   (union), resolved at parse time into a list of `(lo, hi)` windows.
//!
//! - **`stop { rel_expr }`** — halt as soon as the given nullary (boolean)
//!   relation(s) become true.  Multiple relations may be joined by `and`/`or`.
//!
//! - **Combining both clauses** — the two clauses may appear in either order,
//!   joined by a connective whose meaning applies *across* the clauses:
//!   - `continue C and stop S` — stop when **either** fires (minimum).
//!   - `continue C or  stop S` — stop when **both** fire (maximum).
//!
//! ## Examples
//!
//! ```text
//! loop { ... }
//! loop continue { iter <= 10 } { ... }
//! loop stop { Done } { ... }
//! loop stop { Done1 or Done2 } { ... }
//! loop continue { iter <= 100 } and stop { Done } { ... }
//! loop stop { Done } or continue { iter >= 5 and iter <= 10 } { ... }
//! ```

use super::FlowLogRule;
use crate::{Lexeme, Rule};
use common::compute_fp;
use pest::iterators::Pair;
use std::fmt;

/// A boolean connective joining two clauses or sub-conditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoopConnective {
    And,
    Or,
}

/// A resolved iteration window list.
///
/// Each `(lo, hi)` pair is an inclusive range of allowed iterations.
/// The loop continues while `time.inner` falls inside any window.
///
/// Examples:
/// - `iter <= 6`                → `[(0, 6)]`
/// - `iter >= 5 and iter <= 10` → `[(5, 10)]`
/// - `iter < 5 or iter > 10`    → `[(0, 4), (11, u16::MAX)]`
pub type IterWindows = Vec<(u16, u16)>;

/// A single nullary (boolean) relation referenced in a `stop` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StopRelation {
    pub name: String,
    pub fp: u64,
}

/// A group of one or more stop relations joined by `and`/`or`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StopGroup {
    first: StopRelation,
    rest: Vec<(LoopConnective, StopRelation)>,
}

impl StopGroup {
    #[must_use]
    pub fn new(first: StopRelation, rest: Vec<(LoopConnective, StopRelation)>) -> Self {
        Self { first, rest }
    }

    #[must_use]
    pub fn first(&self) -> &StopRelation {
        &self.first
    }

    #[must_use]
    pub fn rest(&self) -> &[(LoopConnective, StopRelation)] {
        &self.rest
    }

    /// Iterator over all stop relations (first + rest).
    pub fn relations(&self) -> impl Iterator<Item = &StopRelation> {
        std::iter::once(&self.first).chain(self.rest.iter().map(|(_, r)| r))
    }
}

/// The composite condition of a loop block.
///
/// At most one `continue` clause (iter windows) and at most one `stop` clause
/// (relation group), in either source order, joined by an optional connective.
///
/// `and` = min (stop when EITHER fires).
/// `or`  = max (stop when BOTH fire).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopCondition {
    continue_part: Option<IterWindows>,
    connective: Option<LoopConnective>,
    stop_part: Option<StopGroup>,
}

impl LoopCondition {
    #[must_use]
    pub fn new(
        continue_part: Option<IterWindows>,
        connective: Option<LoopConnective>,
        stop_part: Option<StopGroup>,
    ) -> Self {
        Self {
            continue_part,
            connective,
            stop_part,
        }
    }

    /// The parsed iter windows from the `continue { ... }` clause, if present.
    #[must_use]
    pub fn continue_part(&self) -> Option<&[(u16, u16)]> {
        self.continue_part.as_deref()
    }

    /// The connective joining the `continue` and `stop` clauses, if both are present.
    #[must_use]
    pub fn connective(&self) -> Option<&LoopConnective> {
        self.connective.as_ref()
    }

    /// The `stop { ... }` relation group, if present.
    #[must_use]
    pub fn stop_part(&self) -> Option<&StopGroup> {
        self.stop_part.as_ref()
    }
}

/// A loop block: an optional condition plus the rules evaluated inside.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopBlock {
    /// `None` means a pure fixpoint loop (DD terminates when delta is empty).
    condition: Option<LoopCondition>,
    rules: Vec<FlowLogRule>,
}

impl LoopBlock {
    #[must_use]
    pub fn new(condition: Option<LoopCondition>, rules: Vec<FlowLogRule>) -> Self {
        Self { condition, rules }
    }

    /// The stop condition, or `None` for a pure fixpoint loop.
    #[must_use]
    pub fn condition(&self) -> Option<&LoopCondition> {
        self.condition.as_ref()
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

impl fmt::Display for LoopConnective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "and"),
            Self::Or => write!(f, "or"),
        }
    }
}

impl fmt::Display for StopRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl fmt::Display for StopGroup {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.first)?;
        for (conn, rel) in &self.rest {
            write!(f, " {conn} {rel}")?;
        }
        Ok(())
    }
}

impl fmt::Display for LoopCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (&self.continue_part, &self.connective, &self.stop_part) {
            (Some(windows), conn, stop) => {
                if !windows.is_empty() {
                    write!(f, "continue {{ {} }}", display_windows(windows))?;
                }
                if let Some(sg) = stop {
                    let c = conn.as_ref().map(|c| format!(" {c} ")).unwrap_or_default();
                    write!(f, "{c}stop {{ {sg} }}")?;
                }
            }
            (None, _, Some(sg)) => {
                write!(f, "stop {{ {sg} }}")?;
            }
            (None, _, None) => {}
        }
        Ok(())
    }
}

fn display_windows(windows: &[(u16, u16)]) -> String {
    let parts: Vec<String> = windows
        .iter()
        .map(|(lo, hi)| {
            if *lo == 0 && *hi == u16::MAX {
                "iter >= 0".to_string()
            } else if *lo == 0 {
                format!("iter <= {hi}")
            } else if *hi == u16::MAX {
                format!("iter >= {lo}")
            } else if lo == hi {
                format!("iter == {lo}")
            } else {
                format!("iter >= {lo} and iter <= {hi}")
            }
        })
        .collect();
    if parts.is_empty() {
        String::new()
    } else {
        parts.join(" or ")
    }
}

impl fmt::Display for LoopBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "loop")?;
        if let Some(cond) = &self.condition {
            writeln!(f, " {} {{", cond)?;
        } else {
            writeln!(f, " {{")?;
        }
        for rule in &self.rules {
            writeln!(f, "    {rule}")?;
        }
        write!(f, "}}")
    }
}

// =============================================================================
// Iteration-range helpers (used during parsing)
// =============================================================================

/// Compute the allowed iteration range for a single `iter op n` constraint.
fn range_for_op(op: &str, n: u16) -> Vec<(u16, u16)> {
    match op {
        "==" => vec![(n, n)],
        "<" => {
            if n == 0 {
                vec![]
            } else {
                vec![(0, n - 1)]
            }
        }
        "<=" => vec![(0, n)],
        ">" => {
            if n == u16::MAX {
                vec![]
            } else {
                vec![(n + 1, u16::MAX)]
            }
        }
        ">=" => vec![(n, u16::MAX)],
        _ => panic!("Parser error: loop_iter_expr unknown comparison operator '{op}'"),
    }
}

/// Intersect two range sets (AND semantics).
fn intersect_ranges(a: &[(u16, u16)], b: &[(u16, u16)]) -> Vec<(u16, u16)> {
    let mut result = Vec::new();
    for &(a_lo, a_hi) in a {
        for &(b_lo, b_hi) in b {
            let lo = a_lo.max(b_lo);
            let hi = a_hi.min(b_hi);
            if lo <= hi {
                result.push((lo, hi));
            }
        }
    }
    result
}

/// Union two range sets (OR semantics).
fn union_ranges(a: &[(u16, u16)], b: &[(u16, u16)]) -> Vec<(u16, u16)> {
    let mut result = a.to_vec();
    result.extend_from_slice(b);
    result
}

// =============================================================================
// Parsing helpers
// =============================================================================

/// Parse a `loop_connective` pair into a [`LoopConnective`].
fn parse_connective(pair: Pair<Rule>) -> LoopConnective {
    let inner = pair
        .into_inner()
        .next()
        .expect("Parser error: loop_connective missing child");
    match inner.as_rule() {
        Rule::loop_and => LoopConnective::And,
        Rule::loop_or => LoopConnective::Or,
        r => panic!("Parser error: loop_connective unexpected rule {r:?}"),
    }
}

/// Parse a `loop_iter_expr` pair into an [`IterWindows`] list.
fn parse_iter_expr(pair: Pair<Rule>) -> IterWindows {
    let mut children = pair.into_inner();

    let first_op = children
        .next()
        .expect("Parser error: loop_iter_expr missing first compare op")
        .as_str()
        .to_string();
    let first_n: u16 = children
        .next()
        .expect("Parser error: loop_iter_expr missing first integer")
        .as_str()
        .trim_start_matches('+')
        .parse()
        .expect("Parser error: loop_iter_expr iteration bound must fit in u16");
    let mut ranges = range_for_op(&first_op, first_n);

    // Subsequent: loop_connective, compare_op, integer (repeated).
    while let Some(conn_pair) = children.next() {
        let connective = parse_connective(conn_pair);
        let op = children
            .next()
            .expect("Parser error: loop_iter_expr missing compare op in repeat")
            .as_str()
            .to_string();
        let n: u16 = children
            .next()
            .expect("Parser error: loop_iter_expr missing integer in repeat")
            .as_str()
            .trim_start_matches('+')
            .parse()
            .expect("Parser error: loop_iter_expr iteration bound must fit in u16");
        let new_range = range_for_op(&op, n);
        ranges = match connective {
            LoopConnective::And => intersect_ranges(&ranges, &new_range),
            LoopConnective::Or => union_ranges(&ranges, &new_range),
        };
    }

    ranges
}

/// Parse a `loop_stop_group` pair into a [`StopGroup`].
fn parse_stop_group(pair: Pair<Rule>) -> StopGroup {
    let mut children = pair.into_inner();

    let first_rel_pair = children
        .next()
        .expect("Parser error: loop_stop_group missing first bool relation");
    let first = parse_bool_relation(first_rel_pair);

    let mut rest = Vec::new();
    while let Some(conn_pair) = children.next() {
        let connective = parse_connective(conn_pair);
        let rel_pair = children
            .next()
            .expect("Parser error: loop_stop_group missing bool relation after connective");
        rest.push((connective, parse_bool_relation(rel_pair)));
    }

    StopGroup::new(first, rest)
}

/// Parse a `loop_bool_relation` pair into a [`StopRelation`].
fn parse_bool_relation(pair: Pair<Rule>) -> StopRelation {
    let raw = pair
        .into_inner()
        .next()
        .expect("Parser error: loop_bool_relation missing relation_name")
        .as_str();
    let fp = compute_fp(raw);
    let name = raw.to_ascii_lowercase();
    StopRelation { name, fp }
}

// =============================================================================
// Lexeme implementations
// =============================================================================

impl Lexeme for LoopCondition {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        // `pair` is `loop_condition`.
        // Children: first child is either `loop_continue` or `loop_stop`,
        // followed optionally by `loop_connective` and the other clause.
        let mut children = pair.into_inner().peekable();

        let first = children
            .next()
            .expect("Parser error: loop_condition missing first clause");

        match first.as_rule() {
            Rule::loop_continue => {
                // continue clause first
                let iter_expr = first
                    .into_inner()
                    .next()
                    .expect("Parser error: loop_continue missing loop_iter_expr");
                let windows = parse_iter_expr(iter_expr);

                // Optional: connective + stop clause
                if let Some(conn_pair) = children.next() {
                    let connective = parse_connective(conn_pair);
                    let stop_pair = children
                        .next()
                        .expect("Parser error: loop_condition missing loop_stop after connective");
                    let stop_group_pair = stop_pair
                        .into_inner()
                        .next()
                        .expect("Parser error: loop_stop missing loop_stop_group");
                    let stop_group = parse_stop_group(stop_group_pair);
                    Self::new(Some(windows), Some(connective), Some(stop_group))
                } else {
                    Self::new(Some(windows), None, None)
                }
            }
            Rule::loop_stop => {
                // stop clause first
                let stop_group_pair = first
                    .into_inner()
                    .next()
                    .expect("Parser error: loop_stop missing loop_stop_group");
                let stop_group = parse_stop_group(stop_group_pair);

                // Optional: connective + continue clause
                if let Some(conn_pair) = children.next() {
                    let connective = parse_connective(conn_pair);
                    let cont_pair = children.next().expect(
                        "Parser error: loop_condition missing loop_continue after connective",
                    );
                    let iter_expr = cont_pair
                        .into_inner()
                        .next()
                        .expect("Parser error: loop_continue missing loop_iter_expr");
                    let windows = parse_iter_expr(iter_expr);
                    Self::new(Some(windows), Some(connective), Some(stop_group))
                } else {
                    Self::new(None, None, Some(stop_group))
                }
            }
            r => panic!("Parser error: loop_condition unexpected first child rule {r:?}"),
        }
    }
}

impl Lexeme for LoopBlock {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner().peekable();

        // Optional condition clause.
        let condition = if inner.peek().map(|p| p.as_rule()) == Some(Rule::loop_condition) {
            let cond_pair = inner.next().unwrap();
            Some(LoopCondition::from_parsed_rule(cond_pair))
        } else {
            None
        };

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
    fn no_condition_pure_fixpoint() {
        let block = parse_loop_block("loop { }");
        assert!(block.condition().is_none());
        assert!(block.rules().is_empty());
    }

    #[test]
    fn continue_iter_less_equal() {
        let block = parse_loop_block("loop continue { iter <= 6 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(0u16, 6u16)]);
        assert!(cond.stop_part().is_none());
    }

    #[test]
    fn continue_iter_greater_equal() {
        let block = parse_loop_block("loop continue { iter >= 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(10u16, u16::MAX)]);
    }

    #[test]
    fn continue_iter_and_intersection() {
        let block = parse_loop_block("loop continue { iter >= 5 and iter <= 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(5u16, 10u16)]);
    }

    #[test]
    fn continue_iter_or_union() {
        let block = parse_loop_block("loop continue { iter < 5 or iter > 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(
            cond.continue_part().unwrap(),
            &[(0u16, 4u16), (11u16, u16::MAX)]
        );
        assert!(cond.stop_part().is_none());
    }

    #[test]
    fn stop_single_relation() {
        let block = parse_loop_block("loop stop { done } { }");
        let cond = block.condition().unwrap();
        assert!(cond.continue_part().is_none());
        let sg = cond.stop_part().unwrap();
        assert_eq!(sg.first().name, "done");
        assert_eq!(sg.first().fp, compute_fp("done"));
        assert!(sg.rest().is_empty());
    }

    #[test]
    fn stop_two_relations_or() {
        let block = parse_loop_block("loop stop { done1 or done2 } { }");
        let cond = block.condition().unwrap();
        let sg = cond.stop_part().unwrap();
        assert_eq!(sg.first().name, "done1");
        assert_eq!(sg.rest().len(), 1);
        assert_eq!(sg.rest()[0].0, LoopConnective::Or);
        assert_eq!(sg.rest()[0].1.name, "done2");
    }

    #[test]
    fn continue_and_stop() {
        let block = parse_loop_block("loop continue { iter <= 6 } and stop { done } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(0u16, 6u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::And));
        let sg = cond.stop_part().unwrap();
        assert_eq!(sg.first().name, "done");
    }

    #[test]
    fn stop_and_continue() {
        let block = parse_loop_block("loop stop { done } and continue { iter <= 3 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(0u16, 3u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::And));
        assert_eq!(cond.stop_part().unwrap().first().name, "done");
    }

    #[test]
    fn stop_or_continue() {
        let block = parse_loop_block("loop stop { done } or continue { iter <= 1 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(0u16, 1u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::Or));
        assert_eq!(cond.stop_part().unwrap().first().name, "done");
    }

    #[test]
    fn continue_or_stop() {
        let block = parse_loop_block("loop continue { iter <= 0 } or stop { done } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.continue_part().unwrap(), &[(0u16, 0u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::Or));
        assert_eq!(cond.stop_part().unwrap().first().name, "done");
    }

    #[test]
    fn with_rule() {
        let block = parse_loop_block("loop { reach(X, Z) :- edge(X, Y), reach(Y, Z). }");
        assert!(block.condition().is_none());
        assert_eq!(block.rules().len(), 1);
        assert_eq!(block.rules()[0].head().name(), "reach");
    }

    #[test]
    fn display_no_condition() {
        let block = parse_loop_block("loop { }");
        let s = block.to_string();
        assert!(s.starts_with("loop {"));
    }

    #[test]
    fn display_continue_only() {
        let block = parse_loop_block("loop continue { iter <= 6 } { }");
        let s = block.to_string();
        assert!(s.contains("continue") && s.contains("iter <= 6"));
    }

    #[test]
    fn display_stop_or_continue() {
        let block = parse_loop_block("loop stop { done } or continue { iter <= 6 } { }");
        let s = block.to_string();
        assert!(s.contains("done") && s.contains("or") && s.contains("iter <= 6"));
    }

    #[test]
    fn relations_iterator() {
        let block = parse_loop_block("loop stop { done1 or done2 } { }");
        let cond = block.condition().unwrap();
        let sg = cond.stop_part().unwrap();
        let names: Vec<&str> = sg.relations().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["done1", "done2"]);
    }
}
