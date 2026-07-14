//! Stop conditions for `loop` blocks.
//!
//! - [`LoopCondition`]: a loop's stop condition (a `while` clause, an
//!   `until` clause, or both).
//! - [`StopGroup`] / [`StopRelation`]: the nullary relations of an `until`
//!   clause.
//! - [`LoopConnective`]: `and` or `or`.
//!
//! See `grammar.pest` for the surface syntax.

use std::fmt;
use std::iter;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::syntax::node::Children;

// =============================================================================
// LoopConnective
// =============================================================================

/// A boolean connective joining two clauses or sub-conditions.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LoopConnective {
    And,
    Or,
}

impl fmt::Display for LoopConnective {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::And => write!(f, "and"),
            Self::Or => write!(f, "or"),
        }
    }
}

/// Parse a `loop_connective` node into a [`LoopConnective`].
fn parse_connective(node: Node) -> Result<LoopConnective, ParseError> {
    let inner = node.children().next_any("child")?;
    Ok(match inner.rule() {
        Rule::loop_and => LoopConnective::And,
        Rule::loop_or => LoopConnective::Or,
        r => {
            return Err(grammar_bug(format!(
                "loop_connective unexpected rule {r:?}"
            )));
        }
    })
}

// =============================================================================
// StopRelation
// =============================================================================

/// A single nullary (boolean) relation referenced in an `until` clause.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct StopRelation {
    name: String,
    fp: u64,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl StopRelation {
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

    /// Span of the relation name in the `until { ... }` clause.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for StopRelation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name)
    }
}

/// Parse a `loop_bool_relation` node into a [`StopRelation`].
fn parse_bool_relation(node: Node) -> Result<StopRelation, ParseError> {
    let span = node.span();
    let name = node
        .children()
        .require(Rule::relation_ref)?
        .text()
        .to_ascii_lowercase();
    let fp = compute_fp(&name);
    Ok(StopRelation { name, fp, span })
}

// =============================================================================
// StopGroup
// =============================================================================

/// A group of one or more `until` relations joined by `and`/`or`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StopGroup {
    first: StopRelation,
    rest: Vec<(LoopConnective, StopRelation)>,
}

impl StopGroup {
    #[must_use]
    pub(crate) fn new(first: StopRelation, rest: Vec<(LoopConnective, StopRelation)>) -> Self {
        Self { first, rest }
    }

    /// The first `until` relation.
    #[must_use]
    pub fn first(&self) -> &StopRelation {
        &self.first
    }

    /// The remaining `(connective, relation)` pairs, in source order.
    #[must_use]
    pub fn rest(&self) -> &[(LoopConnective, StopRelation)] {
        &self.rest
    }

    /// Iterator over all `until` relations (first + rest).
    pub fn relations(&self) -> impl Iterator<Item = &StopRelation> {
        iter::once(&self.first).chain(self.rest.iter().map(|(_, r)| r))
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

/// Parse a `loop_stop_group` node into a [`StopGroup`].
fn parse_stop_group(node: Node) -> Result<StopGroup, ParseError> {
    let mut children = node.children();

    let first = parse_bool_relation(children.next_any("first bool relation")?)?;

    let mut rest = Vec::new();
    while let Some(conn) = children.next() {
        let connective = parse_connective(conn)?;
        let rel = children.next_any("bool relation after connective")?;
        rest.push((connective, parse_bool_relation(rel)?));
    }

    Ok(StopGroup::new(first, rest))
}

// =============================================================================
// LoopCondition
// =============================================================================

/// A resolved iteration window list.
///
/// Each `(lo, hi)` pair is an inclusive range of allowed iterations; the
/// loop continues while the current iteration falls inside any window. For
/// example, `@it <= 6` resolves to `[(0, 6)]` and `@it < 5 or @it > 10` to
/// `[(0, 4), (11, u16::MAX)]`.
pub(crate) type IterWindows = Vec<(u16, u16)>;

/// The composite condition of a loop block.
///
/// At most one `while` clause (iter windows) and at most one `until` clause
/// (relation group), in either source order, joined by an optional `or`.
///
/// With no connective (the default) the loop stops when EITHER clause fires
/// (min); an explicit `or` stops only when BOTH fire (max).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LoopCondition {
    while_part: Option<IterWindows>,
    connective: Option<LoopConnective>,
    until_part: Option<StopGroup>,
}

impl LoopCondition {
    #[must_use]
    pub(crate) fn new(
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

    /// The connective joining the `while` and `until` clauses, or `None`
    /// when the condition has a single clause. When both are present the
    /// default (no explicit `or`) is `And`; see [`LoopCondition`] for the
    /// min/max semantics.
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
            (None, _, Some(sg)) => {
                // Pure `until` form; any connective only matters when both
                // clauses are present, so it is irrelevant here.
                write!(f, "until {{ {sg} }}")?;
            }
            (None, _, None) => {}
        }
        Ok(())
    }
}

fn display_windows(windows: &[(u16, u16)]) -> String {
    windows
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
        .collect::<Vec<_>>()
        .join(" or ")
}

// --- while-clause iteration-window parsing ---

/// Compute the allowed iteration range for a single `@it op n` constraint.
fn range_for_op(op: &str, n: u16) -> Result<Vec<(u16, u16)>, ParseError> {
    Ok(match op {
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
        other => {
            return Err(grammar_bug(format!(
                "loop_iter_expr unknown comparison operator '{other}'"
            )));
        }
    })
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

/// Pull the next `(compare_op, integer)` pair out of `children` and resolve it
/// to its allowed iteration range.
///
/// `position` is woven into grammar-bug messages to disambiguate the leading
/// term from continuation terms.
fn next_iter_term(children: &mut Children, position: &str) -> Result<Vec<(u16, u16)>, ParseError> {
    let op = children
        .next_any(&format!("{position} compare op"))?
        .text()
        .to_string();
    let n: u16 = children
        .next_any(&format!("{position} integer"))?
        .text()
        .trim_start_matches('+')
        .parse()
        .map_err(|e| {
            grammar_bug(format!(
                "loop_iter_expr iteration bound must fit in u16: {e}"
            ))
        })?;
    range_for_op(&op, n)
}

/// Parse a `loop_iter_expr` node into an [`IterWindows`] list.
fn parse_iter_expr(node: Node) -> Result<IterWindows, ParseError> {
    let mut children = node.children();
    let mut ranges = next_iter_term(&mut children, "first")?;

    // Subsequent: loop_connective, compare_op, integer (repeated).
    while let Some(conn) = children.next() {
        let connective = parse_connective(conn)?;
        let new_range = next_iter_term(&mut children, "repeat")?;
        ranges = match connective {
            LoopConnective::And => intersect_ranges(&ranges, &new_range),
            LoopConnective::Or => union_ranges(&ranges, &new_range),
        };
    }

    Ok(ranges)
}

impl Lexeme for LoopCondition {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let mut children = node.children();

        let first = children.next_any("first clause")?;

        Ok(match first.rule() {
            Rule::loop_while => {
                let windows = parse_iter_expr(first.children().next_any("loop_iter_expr")?)?;

                let mut connective = None;
                let mut until_group = None;
                for next in children {
                    match next.rule() {
                        Rule::loop_or => connective = Some(LoopConnective::Or),
                        Rule::loop_until => {
                            let stop_group = next.children().next_any("loop_stop_group")?;
                            until_group = Some(parse_stop_group(stop_group)?);
                        }
                        r => {
                            return Err(grammar_bug(format!(
                                "loop_condition unexpected rule after loop_while: {r:?}"
                            )));
                        }
                    }
                }
                // No explicit connective means And (min semantics).
                if until_group.is_some() && connective.is_none() {
                    connective = Some(LoopConnective::And);
                }
                Self::new(Some(windows), connective, until_group)
            }
            Rule::loop_until => {
                let stop_group = parse_stop_group(first.children().next_any("loop_stop_group")?)?;

                let mut connective = None;
                let mut while_windows = None;
                for next in children {
                    match next.rule() {
                        Rule::loop_or => connective = Some(LoopConnective::Or),
                        Rule::loop_while => {
                            let iter_expr = next.children().next_any("loop_iter_expr")?;
                            while_windows = Some(parse_iter_expr(iter_expr)?);
                        }
                        r => {
                            return Err(grammar_bug(format!(
                                "loop_condition unexpected rule after loop_until: {r:?}"
                            )));
                        }
                    }
                }
                if while_windows.is_some() && connective.is_none() {
                    connective = Some(LoopConnective::And);
                }
                Self::new(while_windows, connective, Some(stop_group))
            }
            r => {
                return Err(grammar_bug(format!(
                    "loop_condition unexpected first child rule {r:?}"
                )));
            }
        })
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_util::parse_node;

    /// A `while { ... }` clause resolves to inclusive iteration windows;
    /// `and` intersects the sub-conditions, `or` unions them.
    #[rstest]
    #[case("@it <= 6", &[(0, 6)])]
    #[case("@it >= 10", &[(10, u16::MAX)])]
    #[case("@it >= 5 and @it <= 10", &[(5, 10)])]
    #[case("@it < 5 or @it > 10", &[(0, 4), (11, u16::MAX)])]
    fn while_clause_resolves_to_windows(#[case] expr: &str, #[case] windows: &[(u16, u16)]) {
        let cond: LoopCondition = parse_node(Rule::loop_condition, &format!("while {{ {expr} }}"));
        assert_eq!(cond.while_part().unwrap(), windows);
        assert!(cond.until_part().is_none());
    }

    /// A single-relation `until` clause; the mixed-case name is canonicalized
    /// to lowercase, with its fingerprint taken on the canonical form.
    #[test]
    fn until_single_relation() {
        let cond: LoopCondition = parse_node(Rule::loop_condition, "until { Done }");
        assert!(cond.while_part().is_none());
        let sg = cond.until_part().unwrap();
        assert_eq!(sg.first().name(), "done");
        assert_eq!(sg.first().fp(), compute_fp("done"));
        assert!(sg.rest().is_empty());
    }

    #[test]
    fn until_two_relations_or() {
        let cond: LoopCondition = parse_node(Rule::loop_condition, "until { done1 or done2 }");
        let sg = cond.until_part().unwrap();
        assert_eq!(sg.first().name(), "done1");
        assert_eq!(sg.rest().len(), 1);
        assert_eq!(sg.rest()[0].0, LoopConnective::Or);
        assert_eq!(sg.rest()[0].1.name(), "done2");
    }

    /// `while` and `until` combine in either order. With no connective the
    /// default is `And` (stop when either fires); an explicit `or` is `Or`
    /// (stop when both fire).
    #[rstest]
    #[case("while { @it <= 6 } until { done }", &[(0, 6)], LoopConnective::And)]
    #[case("until { done } while { @it <= 3 }", &[(0, 3)], LoopConnective::And)]
    #[case("until { done } or while { @it <= 1 }", &[(0, 1)], LoopConnective::Or)]
    #[case("while { @it <= 0 } or until { done }", &[(0, 0)], LoopConnective::Or)]
    fn while_until_connective_resolution(
        #[case] src: &str,
        #[case] windows: &[(u16, u16)],
        #[case] connective: LoopConnective,
    ) {
        let cond: LoopCondition = parse_node(Rule::loop_condition, src);
        assert_eq!(cond.while_part().unwrap(), windows);
        assert_eq!(cond.connective(), Some(&connective));
        assert_eq!(cond.until_part().unwrap().first().name(), "done");
    }

    /// Each window shape renders back to its surface `@it` expression: an
    /// open lower bound as `<=`, an open upper bound as `>=`, a point as
    /// `==`, a full range as `>= 0`, and a bounded range with `and`.
    #[rstest]
    #[case("@it <= 6")]
    #[case("@it >= 5")]
    #[case("@it == 3")]
    #[case("@it >= 0")]
    #[case("@it >= 5 and @it <= 10")]
    fn while_clause_display_round_trips(#[case] expr: &str) {
        let src = format!("while {{ {expr} }}");
        let cond: LoopCondition = parse_node(Rule::loop_condition, &src);
        assert_eq!(cond.to_string(), src, "{expr} did not round-trip");
    }

    /// The pure-`until` form (no `while`) renders its stop group back to
    /// surface syntax, including a multi-relation group joined by `or`.
    #[rstest]
    #[case("until { done }")]
    #[case("until { done1 or done2 }")]
    fn until_clause_display_round_trips(#[case] src: &str) {
        let cond: LoopCondition = parse_node(Rule::loop_condition, src);
        assert_eq!(cond.to_string(), src, "{src} did not round-trip");
    }

    #[rstest]
    #[case(LoopConnective::And, "and")]
    #[case(LoopConnective::Or, "or")]
    fn connective_displays_its_keyword(#[case] conn: LoopConnective, #[case] expected: &str) {
        assert_eq!(conn.to_string(), expected);
    }

    #[test]
    fn relations_iterator() {
        let cond: LoopCondition = parse_node(Rule::loop_condition, "until { done1 or done2 }");
        let sg = cond.until_part().unwrap();
        let names: Vec<&str> = sg.relations().map(StopRelation::name).collect();
        assert_eq!(names, vec!["done1", "done2"]);
    }
}
