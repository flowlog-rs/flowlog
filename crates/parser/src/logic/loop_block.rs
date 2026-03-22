//! AST types for loop blocks and fixpoint blocks.
//!
//! A fixpoint block groups rules evaluated iteratively until no new tuples are
//! derived.  A loop block adds bounded/conditional iteration on top of that.
//! Both act as hard evaluation barriers: the stratifier cannot move rules
//! across their boundary.
//!
//! ## Grammar
//!
//! ```text
//! fixpoint_block = { "fixpoint" ~ "{" ~ iterative_directive* ~ rule* ~ "}" }
//!
//! loop_block = { "loop" ~ loop_condition ~ "{" ~ iterative_directive* ~ rule* ~ "}" }
//!
//! iterative_directive = { ".iterative" ~ relation_name }
//!
//! loop_condition = { loop_while ~ (loop_or? ~ loop_until)?
//!                  | loop_until ~ (loop_or? ~ loop_while)? }
//!
//! loop_while = { "while" ~ "{" ~ loop_iter_expr ~ "}" }
//! loop_until = { "until" ~ "{" ~ loop_stop_group ~ "}" }
//!
//! loop_iter_expr  = { "@it" ~ loop_iter_compare_op ~ integer
//!                     ~ (loop_connective ~ "@it" ~ loop_iter_compare_op ~ integer)* }
//! loop_stop_group = { loop_bool_relation ~ (loop_connective ~ loop_bool_relation)* }
//!
//! loop_iter_compare_op = { "<=" | ">=" | "==" | "<" | ">" }
//! loop_bool_relation   = { relation_name }
//! loop_connective      = { "and" | "or" }
//! ```
//!
//! ## Iterative semantics
//!
//! Relations marked with `.iterative <name>` inside the block use *replacement*
//! semantics: each iteration they are re-derived from scratch
//! (`Variable::new_from`), so stale facts are retracted when they are no
//! longer derivable.  Unmarked relations use *accumulative* semantics: facts
//! once derived are never retracted (`Variable::new`).
//!
//! The `.iterative` directive is scoped per-block: the same relation can be
//! iterative in one block and accumulative in another.
//!
//! ## Condition semantics
//!
//! - **`while { iter_expr }`** — keep looping while the current iteration
//!   count satisfies the expression.  The expression is one or more
//!   `@it op N` sub-conditions joined by `and` (intersection) or `or`
//!   (union), resolved at parse time into a list of `(lo, hi)` windows.
//!
//! - **`until { rel_expr }`** — halt as soon as the given nullary (boolean)
//!   relation(s) become true.  Multiple relations may be joined by `and`/`or`.
//!
//! - **Combining both clauses** — the two clauses may appear in either order:
//!   - `while W until U`       — stop when **either** fires (minimum).
//!   - `while W or until U`    — stop when **both** fire (maximum).
//!
//! ## Examples
//!
//! ```text
//! fixpoint { ... }
//! fixpoint { .iterative X  ... }
//! loop while { @it <= 10 } { ... }
//! loop until { Done } { .iterative X  ... }
//! loop while { @it <= 100 } until { Done } { ... }
//! loop until { Done } or while { @it >= 5 and @it <= 10 } { ... }
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
/// - `@it <= 6`                → `[(0, 6)]`
/// - `@it >= 5 and @it <= 10` → `[(5, 10)]`
/// - `@it < 5 or @it > 10`    → `[(0, 4), (11, u16::MAX)]`
pub type IterWindows = Vec<(u16, u16)>;

/// A single nullary (boolean) relation referenced in an `until` clause.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StopRelation {
    pub name: String,
    pub fp: u64,
}

/// A group of one or more `until` relations joined by `and`/`or`.
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

    /// Iterator over all `until` relations (first + rest).
    pub fn relations(&self) -> impl Iterator<Item = &StopRelation> {
        std::iter::once(&self.first).chain(self.rest.iter().map(|(_, r)| r))
    }
}

/// The composite condition of a loop block.
///
/// At most one `while` clause (iter windows) and at most one `until` clause
/// (relation group), in either source order, joined by an optional `or`.
///
/// No connective (default) = min (stop when EITHER fires).
/// Explicit `or`           = max (stop when BOTH fire).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopCondition {
    while_part: Option<IterWindows>,
    connective: Option<LoopConnective>,
    until_part: Option<StopGroup>,
}

impl LoopCondition {
    #[must_use]
    pub fn new(
        while_part: Option<IterWindows>,
        connective: Option<LoopConnective>,
        until_part: Option<StopGroup>,
    ) -> Self {
        Self {
            while_part,
            connective,
            until_part,
        }
    }

    /// The parsed iter windows from the `while { ... }` clause, if present.
    #[must_use]
    pub fn while_part(&self) -> Option<&[(u16, u16)]> {
        self.while_part.as_deref()
    }

    /// The connective joining the `while` and `until` clauses, if both are present.
    /// `None` (default when both present) means And / min semantics.
    #[must_use]
    pub fn connective(&self) -> Option<&LoopConnective> {
        self.connective.as_ref()
    }

    /// The `until { ... }` relation group, if present.
    #[must_use]
    pub fn until_part(&self) -> Option<&StopGroup> {
        self.until_part.as_ref()
    }
}

/// A loop/fixpoint block: an optional iterative relation list, an optional
/// condition, and the rules evaluated inside.
///
/// The `iterative` list is scoped per-block: the same relation can be
/// iterative in one block and accumulative in another.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopBlock {
    /// Relations using replacement (iterative) semantics — `Variable::new_from`.
    /// Relations absent from this list default to accumulative (`Variable::new`) semantics.
    iterative_relations: Vec<(String, u64)>,
    /// `None` means a pure fixpoint loop (DD terminates when delta is empty).
    condition: Option<LoopCondition>,
    rules: Vec<FlowLogRule>,
}

impl LoopBlock {
    #[must_use]
    pub fn new(
        iterative_relations: Vec<(String, u64)>,
        condition: Option<LoopCondition>,
        rules: Vec<FlowLogRule>,
    ) -> Self {
        Self {
            iterative_relations,
            condition,
            rules,
        }
    }

    /// Relations explicitly marked as iterative (replacement semantics).
    #[must_use]
    pub fn iterative_relations(&self) -> &[(String, u64)] {
        &self.iterative_relations
    }

    /// The loop condition, or `None` for a pure fixpoint block.
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
        match (&self.while_part, &self.connective, &self.until_part) {
            (Some(windows), conn, until) => {
                if !windows.is_empty() {
                    write!(f, "while {{ {} }}", display_windows(windows))?;
                }
                if let Some(sg) = until {
                    if conn.as_ref() == Some(&LoopConnective::Or) {
                        write!(f, " or until {{ {sg} }}")?;
                    } else {
                        write!(f, " until {{ {sg} }}")?;
                    }
                }
            }
            (None, conn, Some(sg)) => {
                write!(f, "until {{ {sg} }}")?;
                // If there were a while part after, it would have been (Some, _, _)
                let _ = conn;
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
                "@it >= 0".to_string()
            } else if *lo == 0 {
                format!("@it <= {hi}")
            } else if *hi == u16::MAX {
                format!("@it >= {lo}")
            } else if lo == hi {
                format!("@it == {lo}")
            } else {
                format!("@it >= {lo} and @it <= {hi}")
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
        if self.condition.is_some() {
            write!(f, "loop")?;
        } else {
            write!(f, "fixpoint")?;
        }
        if let Some(cond) = &self.condition {
            writeln!(f, " {} {{", cond)?;
        } else {
            writeln!(f, " {{")?;
        }
        for (name, _) in &self.iterative_relations {
            writeln!(f, "    .iterative {name}")?;
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

/// Compute the allowed iteration range for a single `@it op n` constraint.
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
    let name = pair
        .into_inner()
        .next()
        .expect("Parser error: loop_bool_relation missing relation_name")
        .as_str()
        .to_ascii_lowercase();
    let fp = compute_fp(&name);
    StopRelation { name, fp }
}

// =============================================================================
// Lexeme implementations
// =============================================================================

impl Lexeme for LoopCondition {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        // `pair` is `loop_condition`.
        // Children: first child is either `loop_while` or `loop_until`,
        // followed optionally by `loop_or` (connective) and the other clause.
        // If no `loop_or` is present between two clauses, default is And (min).
        let mut children = pair.into_inner().peekable();

        let first = children
            .next()
            .expect("Parser error: loop_condition missing first clause");

        match first.as_rule() {
            Rule::loop_while => {
                // while clause first
                let iter_expr = first
                    .into_inner()
                    .next()
                    .expect("Parser error: loop_while missing loop_iter_expr");
                let windows = parse_iter_expr(iter_expr);

                // Optional: [or] + until clause
                let mut connective = None;
                let mut until_group = None;
                for next in children.by_ref() {
                    match next.as_rule() {
                        Rule::loop_or => connective = Some(LoopConnective::Or),
                        Rule::loop_until => {
                            let stop_group_pair = next
                                .into_inner()
                                .next()
                                .expect("Parser error: loop_until missing loop_stop_group");
                            until_group = Some(parse_stop_group(stop_group_pair));
                        }
                        r => panic!("Parser error: loop_condition unexpected rule after loop_while: {r:?}"),
                    }
                }
                // Default to And when both clauses present but no explicit "or"
                if until_group.is_some() && connective.is_none() {
                    connective = Some(LoopConnective::And);
                }
                Self::new(Some(windows), connective, until_group)
            }
            Rule::loop_until => {
                // until clause first
                let stop_group_pair = first
                    .into_inner()
                    .next()
                    .expect("Parser error: loop_until missing loop_stop_group");
                let stop_group = parse_stop_group(stop_group_pair);

                // Optional: [or] + while clause
                let mut connective = None;
                let mut while_windows = None;
                for next in children {
                    match next.as_rule() {
                        Rule::loop_or => connective = Some(LoopConnective::Or),
                        Rule::loop_while => {
                            let iter_expr = next
                                .into_inner()
                                .next()
                                .expect("Parser error: loop_while missing loop_iter_expr");
                            while_windows = Some(parse_iter_expr(iter_expr));
                        }
                        r => panic!("Parser error: loop_condition unexpected rule after loop_until: {r:?}"),
                    }
                }
                // Default to And when both clauses present but no explicit "or"
                if while_windows.is_some() && connective.is_none() {
                    connective = Some(LoopConnective::And);
                }
                Self::new(while_windows, connective, Some(stop_group))
            }
            r => panic!("Parser error: loop_condition unexpected first child rule {r:?}"),
        }
    }
}

impl Lexeme for LoopBlock {
    fn from_parsed_rule(pair: Pair<Rule>) -> Self {
        let mut inner = pair.into_inner().peekable();

        // Optional condition clause (fixpoint blocks have no condition).
        let condition = if inner.peek().map(|p| p.as_rule()) == Some(Rule::loop_condition) {
            Some(LoopCondition::from_parsed_rule(inner.next().unwrap()))
        } else {
            None
        };

        // Block body: interleaved `.iterative` directives and rules.
        let mut iterative_relations = Vec::new();
        let mut rules = Vec::new();
        for item in inner {
            match item.as_rule() {
                Rule::iterative_directive => {
                    let name = item
                        .into_inner()
                        .next()
                        .expect("Parser error: iterative_directive missing relation_name")
                        .as_str()
                        .to_ascii_lowercase();
                    let fp = compute_fp(&name);
                    iterative_relations.push((name, fp));
                }
                Rule::rule => {
                    rules.push(FlowLogRule::from_parsed_rule(item));
                }
                r => panic!("Parser error: unexpected rule in loop/fixpoint block: {r:?}"),
            }
        }

        Self::new(iterative_relations, condition, rules)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Rule};
    use pest::Parser;

    fn parse_loop_block(input: &str) -> LoopBlock {
        // Try as loop_block first, then fixpoint_block
        if let Ok(mut pairs) = FlowLogParser::parse(Rule::loop_block, input) {
            LoopBlock::from_parsed_rule(pairs.next().unwrap())
        } else {
            let mut pairs = FlowLogParser::parse(Rule::fixpoint_block, input)
                .unwrap_or_else(|e| panic!("parse error: {e}"));
            LoopBlock::from_parsed_rule(pairs.next().unwrap())
        }
    }

    #[test]
    fn fixpoint_no_condition() {
        let block = parse_loop_block("fixpoint { }");
        assert!(block.condition().is_none());
        assert!(block.rules().is_empty());
    }

    #[test]
    fn while_iter_less_equal() {
        let block = parse_loop_block("loop while { @it <= 6 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 6u16)]);
        assert!(cond.until_part().is_none());
    }

    #[test]
    fn while_iter_greater_equal() {
        let block = parse_loop_block("loop while { @it >= 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(10u16, u16::MAX)]);
    }

    #[test]
    fn while_iter_and_intersection() {
        let block = parse_loop_block("loop while { @it >= 5 and @it <= 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(5u16, 10u16)]);
    }

    #[test]
    fn while_iter_or_union() {
        let block = parse_loop_block("loop while { @it < 5 or @it > 10 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(
            cond.while_part().unwrap(),
            &[(0u16, 4u16), (11u16, u16::MAX)]
        );
        assert!(cond.until_part().is_none());
    }

    #[test]
    fn until_single_relation() {
        let block = parse_loop_block("loop until { done } { }");
        let cond = block.condition().unwrap();
        assert!(cond.while_part().is_none());
        let sg = cond.until_part().unwrap();
        assert_eq!(sg.first().name, "done");
        assert_eq!(sg.first().fp, compute_fp("done"));
        assert!(sg.rest().is_empty());
    }

    #[test]
    fn until_two_relations_or() {
        let block = parse_loop_block("loop until { done1 or done2 } { }");
        let cond = block.condition().unwrap();
        let sg = cond.until_part().unwrap();
        assert_eq!(sg.first().name, "done1");
        assert_eq!(sg.rest().len(), 1);
        assert_eq!(sg.rest()[0].0, LoopConnective::Or);
        assert_eq!(sg.rest()[0].1.name, "done2");
    }

    #[test]
    fn while_until_default_and() {
        // No connective between while and until → default And (min semantics)
        let block = parse_loop_block("loop while { @it <= 6 } until { done } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 6u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::And));
        let sg = cond.until_part().unwrap();
        assert_eq!(sg.first().name, "done");
    }

    #[test]
    fn until_while_default_and() {
        // until first, then while — no connective → default And
        let block = parse_loop_block("loop until { done } while { @it <= 3 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 3u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::And));
        assert_eq!(cond.until_part().unwrap().first().name, "done");
    }

    #[test]
    fn until_or_while() {
        // Explicit "or" → Or / max semantics
        let block = parse_loop_block("loop until { done } or while { @it <= 1 } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 1u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::Or));
        assert_eq!(cond.until_part().unwrap().first().name, "done");
    }

    #[test]
    fn while_or_until() {
        let block = parse_loop_block("loop while { @it <= 0 } or until { done } { }");
        let cond = block.condition().unwrap();
        assert_eq!(cond.while_part().unwrap(), &[(0u16, 0u16)]);
        assert_eq!(cond.connective(), Some(&LoopConnective::Or));
        assert_eq!(cond.until_part().unwrap().first().name, "done");
    }

    #[test]
    fn with_rule() {
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

    #[test]
    fn display_while_only() {
        let block = parse_loop_block("loop while { @it <= 6 } { }");
        let s = block.to_string();
        assert!(s.contains("while") && s.contains("@it <= 6"));
    }

    #[test]
    fn display_until_or_while() {
        let block = parse_loop_block("loop until { done } or while { @it <= 6 } { }");
        let s = block.to_string();
        assert!(s.contains("done") && s.contains("or") && s.contains("@it <= 6"));
    }

    #[test]
    fn relations_iterator() {
        let block = parse_loop_block("loop until { done1 or done2 } { }");
        let cond = block.condition().unwrap();
        let sg = cond.until_part().unwrap();
        let names: Vec<&str> = sg.relations().map(|r| r.name.as_str()).collect();
        assert_eq!(names, vec!["done1", "done2"]);
    }

    #[test]
    fn iterative_directive_single() {
        let block = parse_loop_block("fixpoint { .iterative removed }");
        let itr = block.iterative_relations();
        assert_eq!(itr.len(), 1);
        assert_eq!(itr[0].0, "removed");
        assert_eq!(itr[0].1, compute_fp("removed"));
        assert!(block.rules().is_empty());
    }

    #[test]
    fn iterative_directive_multiple() {
        let block = parse_loop_block(
            "fixpoint { .iterative active_edge .iterative degree }",
        );
        let itr = block.iterative_relations();
        assert_eq!(itr.len(), 2);
        assert_eq!(itr[0].0, "active_edge");
        assert_eq!(itr[1].0, "degree");
    }

    #[test]
    fn iterative_directive_with_rules() {
        let block = parse_loop_block(
            "fixpoint { .iterative reach  reach(X, Z) :- edge(X, Y), reach(Y, Z). }",
        );
        assert_eq!(block.iterative_relations().len(), 1);
        assert_eq!(block.iterative_relations()[0].0, "reach");
        assert_eq!(block.rules().len(), 1);
    }

    #[test]
    fn iterative_directive_in_loop() {
        let block = parse_loop_block(
            "loop while { @it <= 5 } { .iterative active_edge  active_edge(X,Y) :- edge(X,Y). }",
        );
        assert_eq!(block.iterative_relations().len(), 1);
        assert_eq!(block.iterative_relations()[0].0, "active_edge");
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
        let block = parse_loop_block(
            "fixpoint { .iterative active_edge .iterative degree }",
        );
        let s = block.to_string();
        assert!(s.contains(".iterative active_edge"));
        assert!(s.contains(".iterative degree"));
    }
}
