//! Rules for FlowLog Datalog programs.
//!
//! - [`FlowLogRule`]: `head :- p1, p2, ..., pn.`, a single derived head
//!   and the body predicates that must all hold.

use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;

use super::Constant;
use super::Factor;
use super::Head;
use super::HeadArg;
use super::Predicate;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// A complete FlowLog rule: `head :- p1, ..., pn.`
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct FlowLogRule {
    head: Head,
    rhs: Vec<Predicate>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
    /// Tombstone for the future cost-based optimizer: when true, the user
    /// supplied a `.plan` hint and the positive-atom order in `rhs` has
    /// already been permuted to match it; the optimizer must not reorder.
    plan_pinned: bool,
}

impl fmt::Display for FlowLogRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl FlowLogRule {
    /// Creates a synthesized rule with no source location (`Span::DUMMY`)
    /// and no `.plan` pin.
    // TODO: pub(crate) once flowlog-build's catalog stops hand-constructing
    // rules (rewrites should go through a sanctioned API).
    #[must_use]
    pub fn new(head: Head, rhs: Vec<Predicate>) -> Self {
        Self {
            head,
            rhs,
            span: Span::DUMMY,
            plan_pinned: false,
        }
    }

    /// Number of positive atoms in the body.
    #[must_use]
    #[inline]
    pub fn positive_atom_count(&self) -> usize {
        self.rhs
            .iter()
            .filter(|p| matches!(p, Predicate::PositiveAtom(_)))
            .count()
    }

    /// Whether the rule's positive-atom order is pinned by a user `.plan`
    /// (a future cost-based optimizer must not reorder it).
    #[must_use]
    #[inline]
    pub fn plan_pinned(&self) -> bool {
        self.plan_pinned
    }

    /// Reorders the positive atoms in `rhs`, leaving negations /
    /// comparisons / fn-calls in their original global slots. Marks the
    /// rule as `plan_pinned`.
    ///
    /// `order` is a gather: the atom landing in positive position `i` is
    /// the one originally at `order[i]`. It must be a permutation of
    /// `0..positive_atom_count()`; the caller validates, and debug builds
    /// re-check the length only.
    pub(crate) fn apply_plan_order(&mut self, order: &[usize]) {
        let pos_indices: Vec<usize> = self
            .rhs
            .iter()
            .enumerate()
            .filter_map(|(i, p)| matches!(p, Predicate::PositiveAtom(_)).then_some(i))
            .collect();

        debug_assert_eq!(order.len(), pos_indices.len());

        // Cycle-decomposition in place: for each cycle of `order`, rotate
        // the positive atoms via `Vec::swap`. Zero clones, one bitmap
        // allocation. Correctness sketch: applying the swaps along each
        // cycle resolves all but the last element automatically, since by
        // then the cycle's other slots already hold their final values.
        let n = order.len();
        let mut visited = vec![false; n];
        for start in 0..n {
            if visited[start] {
                continue;
            }
            let mut current = start;
            while !visited[current] {
                visited[current] = true;
                let next = order[current];
                if next != current && !visited[next] {
                    self.rhs.swap(pos_indices[current], pos_indices[next]);
                }
                current = next;
            }
        }

        self.plan_pinned = true;
    }

    /// Source location this rule was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Rule head.
    #[must_use]
    #[inline]
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Rule body (right-hand side predicates).
    #[must_use]
    #[inline]
    pub fn rhs(&self) -> &[Predicate] {
        &self.rhs
    }

    #[inline]
    pub(crate) fn head_mut(&mut self) -> &mut Head {
        &mut self.head
    }

    #[inline]
    pub(crate) fn rhs_mut(&mut self) -> &mut [Predicate] {
        &mut self.rhs
    }

    /// Replaces the rule body wholesale; unlike [`Self::rhs_mut`], this
    /// can change the number of predicates.
    #[inline]
    pub(crate) fn set_rhs(&mut self, rhs: Vec<Predicate>) {
        self.rhs = rhs;
    }

    /// Extract constants from a fact's head.
    ///
    /// Returns [`ParseError::GroundRuleNotConst`] if any head argument is
    /// not a simple constant, e.g. an unbound variable (`k(E).`), an
    /// aggregation, or a non-constant arithmetic expression.
    pub fn extract_constants_from_head(&self) -> Result<Vec<Constant>, ParseError> {
        let args = self.head.head_arguments();
        let mut out = Vec::with_capacity(args.len());
        let not_const = || ParseError::GroundRuleNotConst {
            span: self.head.span(),
        };
        for arg in args {
            let HeadArg::Arith(arith) = arg else {
                return Err(not_const());
            };
            let Factor::Const(c) = arith.init() else {
                return Err(not_const());
            };
            if !arith.is_const() {
                return Err(not_const());
            }
            out.push(c.clone());
        }
        Ok(out)
    }

    /// Parses a `rule` node into the rules it denotes.
    ///
    /// One source clause can denote several rules: multi-head (`a, b :- ...`)
    /// and multi-body (`... :- p ; q`, including nested `(p ; q)`) each fan
    /// out by cross-product, so H heads and B body alternatives yield H*B
    /// rules. A trailing `.plan` then pins the join order of all of them.
    ///
    /// Grammar: `rule_heads ":-" rule_bodies "." plan_directive?`.
    pub(crate) fn expand_from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Vec<Self>, ParseError> {
        let node = Node::new(parsed_rule, file);
        let span = node.span();
        let mut children = node.children();

        let heads: Vec<Head> = children
            .require(Rule::rule_heads)?
            .children()
            .map(Node::lower)
            .collect::<Result<_, _>>()?;
        let bodies = expand_bodies(children.require(Rule::rule_bodies)?)?;

        // Fan out to one rule per (head, body-alternative) pair: multi-head
        // and multi-body both multiply here.
        let mut rules = Vec::with_capacity(heads.len() * bodies.len());
        for head in &heads {
            for body in &bodies {
                rules.push(Self {
                    head: head.clone(),
                    rhs: body.clone(),
                    span,
                    plan_pinned: false,
                });
            }
        }

        // A trailing `.plan` pins the join order of every one of them.
        if let Some(plan) = children.take_if(Rule::plan_directive) {
            pin_plan(&mut rules, plan)?;
        }
        Ok(rules)
    }
}

// =============================================================================
// Body expansion: `,` conjunction, `;` disjunction
// =============================================================================

/// Expands a `rule_bodies` node into one predicate list per body
/// alternative. `;` between bodies fans out; `,` within a body conjoins.
fn expand_bodies(node: Node) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut alternatives = Vec::new();
    for predicates_node in node.children() {
        alternatives.extend(expand_conjunction(predicates_node)?);
    }
    Ok(alternatives)
}

/// Expands one `,`-separated `predicates` node into its body alternatives.
/// A plain predicate appends to every alternative; a nested `(p ; q)`
/// disjunction group multiplies the alternatives by cross-product.
fn expand_conjunction(node: Node) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut alternatives: Vec<Vec<Predicate>> = vec![Vec::new()];
    for pred_node in node.children() {
        let inner = pred_node.children().next_any("predicate value")?;
        if inner.rule() == Rule::disjunction_group {
            let nested = expand_bodies(inner.children().next_any("rule_bodies")?)?;
            let mut crossed = Vec::with_capacity(alternatives.len() * nested.len());
            for prefix in &alternatives {
                for alt in &nested {
                    let mut combined = prefix.clone();
                    combined.extend(alt.iter().cloned());
                    crossed.push(combined);
                }
            }
            alternatives = crossed;
        } else {
            let predicate = Predicate::from_inner(inner)?;
            for alternative in &mut alternatives {
                alternative.push(predicate.clone());
            }
        }
    }
    Ok(alternatives)
}

// =============================================================================
// `.plan` join-order hints
// =============================================================================

/// Pins the join order of every rule a clause expanded to (they share the
/// clause, so they share the hint) from its trailing `.plan` node.
fn pin_plan(rules: &mut [FlowLogRule], plan: Node) -> Result<(), ParseError> {
    let (span, indices) = parse_plan_indices(plan)?;
    for rule in rules {
        apply_indices_to_rule(rule, span, &indices)?;
    }
    Ok(())
}

/// Parses a `plan_directive` node into its span and raw 1-based indices.
fn parse_plan_indices(node: Node) -> Result<(Span, Vec<usize>), ParseError> {
    let span = node.span();
    let mut raw_indices = Vec::new();
    for child in node.children() {
        match child.rule() {
            // The optional version index disambiguates the clauses of a
            // multi-head/multi-body rule; we expand those clauses at parse
            // time, so it has no clause to bind to: parse and discard.
            Rule::plan_version => continue,
            Rule::plan_index => {
                let parsed: usize = child
                    .text()
                    .parse()
                    .map_err(|_| grammar_bug("plan_index is not a valid integer"))?;
                raw_indices.push(parsed);
            }
            other => {
                return Err(grammar_bug(format!(
                    "plan_directive unexpected child rule {other:?}"
                )));
            }
        }
    }
    Ok((span, raw_indices))
}

/// Validates `.plan` indices and applies them to `rule`. Indices are
/// 1-based positions of the body's positive atoms only, and must be a
/// permutation of `1..=k` where `k` is the positive-atom count.
fn apply_indices_to_rule(
    rule: &mut FlowLogRule,
    span: Span,
    raw_indices: &[usize],
) -> Result<(), ParseError> {
    let k = rule.positive_atom_count();
    if raw_indices.len() != k {
        return Err(ParseError::PlanArityMismatch {
            span,
            expected: k,
            found: raw_indices.len(),
        });
    }
    let mut seen = vec![false; k];
    let mut order: Vec<usize> = Vec::with_capacity(k);
    for &idx in raw_indices {
        if idx == 0 || idx > k {
            return Err(ParseError::PlanIndexOutOfRange {
                span,
                index: idx,
                max: k,
            });
        }
        let zero = idx - 1;
        if seen[zero] {
            return Err(ParseError::PlanDuplicateIndex { span, index: idx });
        }
        seen[zero] = true;
        order.push(zero);
    }
    rule.apply_plan_order(&order);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Constant;
    use super::*;
    use crate::AggregationOperator;
    use crate::assert_err;
    use crate::ast::Aggregation;
    use crate::ast::Arithmetic;
    use crate::ast::Factor;
    use crate::test_util::parse_pair;
    use crate::test_util::parse_rule;
    use crate::types::DataType;

    // `.plan` operates on a built `FlowLogRule`, so `parse_rule` produces the
    // input by parsing one; the producer under test is then called directly.

    /// A `.plan` permutation reorders the positive atoms (1-based, gather
    /// order) and slides them *under* negations, which keep their slots. The
    /// rule is pinned so a later optimizer won't reorder it.
    #[test]
    fn apply_indices_permutes_positive_atoms_under_negations() {
        let mut rule = parse_rule("h(X) :- a(X), !d(X), b(X), c(X).");
        assert!(!rule.plan_pinned(), "a parsed rule starts unpinned");
        apply_indices_to_rule(&mut rule, Span::DUMMY, &[3, 1, 2]).expect("valid plan");
        let labelled: Vec<String> = rule
            .rhs()
            .iter()
            .map(|p| match p {
                Predicate::PositiveAtom(a) => a.name().to_string(),
                Predicate::NegativeAtom(a) => format!("!{}", a.name()),
                other => other.to_string(),
            })
            .collect();
        assert_eq!(labelled, ["c", "!d", "a", "b"]);
        assert!(rule.plan_pinned(), "a successful .plan pins the rule");
    }

    /// The index count must equal the positive-atom count.
    #[test]
    fn apply_indices_rejects_arity_mismatch() {
        let mut rule = parse_rule("h(X) :- a(X), b(X).");
        assert_err!(
            apply_indices_to_rule(&mut rule, Span::DUMMY, &[1, 2, 3]),
            ParseError::PlanArityMismatch {
                expected: 2,
                found: 3,
                ..
            }
        );
    }

    /// Index 0 or one past the positive-atom count is out of range.
    #[test]
    fn apply_indices_rejects_out_of_range_index() {
        let mut rule = parse_rule("h(X) :- a(X), b(X).");
        assert_err!(
            apply_indices_to_rule(&mut rule, Span::DUMMY, &[1, 3]),
            ParseError::PlanIndexOutOfRange {
                index: 3,
                max: 2,
                ..
            }
        );
    }

    /// Each index must appear exactly once.
    #[test]
    fn apply_indices_rejects_duplicate_index() {
        let mut rule = parse_rule("h(X) :- a(X), b(X).");
        assert_err!(
            apply_indices_to_rule(&mut rule, Span::DUMMY, &[1, 1]),
            ParseError::PlanDuplicateIndex { index: 1, .. }
        );
    }

    /// A trailing `.plan` parses as part of its rule and permutes it: the
    /// grammar binds the two, so `expand_from_parsed_rule` applies the hint.
    #[test]
    fn trailing_plan_permutes_the_rule() {
        let rule = parse_rule("h(X) :- a(X), b(X), c(X).\n.plan (3, 1, 2)");
        let names: Vec<&str> = rule
            .rhs()
            .iter()
            .filter_map(|p| match p {
                Predicate::PositiveAtom(a) => Some(a.name()),
                _ => None,
            })
            .collect();
        assert_eq!(names, ["c", "a", "b"]);
        assert!(rule.plan_pinned());
    }

    fn head_const(v: Constant) -> HeadArg {
        HeadArg::Arith(Arithmetic::new(Factor::Const(v), vec![]))
    }

    #[test]
    fn extract_constants() {
        let head = Head::new(
            "facts".into(),
            vec![
                head_const(Constant::new(DataType::IntLit, "42")),
                head_const(Constant::new(DataType::String, "hello")),
            ],
        );
        let r = FlowLogRule::new(head, vec![]);
        let c = r.extract_constants_from_head().expect("all-const head");
        assert_eq!(
            c,
            vec![
                Constant::new(DataType::IntLit, "42"),
                Constant::new(DataType::String, "hello"),
            ]
        );
    }

    /// A head argument that is not a bare constant (a variable, an
    /// aggregation, or an arithmetic expression with operators) must yield
    /// `GroundRuleNotConst`, not a panic.
    fn assert_head_arg_rejected(invalid: HeadArg) {
        let head = Head::new(
            "invalid".into(),
            vec![head_const(Constant::new(DataType::IntLit, "1")), invalid],
        );
        let err = FlowLogRule::new(head, vec![])
            .extract_constants_from_head()
            .expect_err("non-constant head arg must be rejected");
        assert!(
            matches!(err, ParseError::GroundRuleNotConst { .. }),
            "expected GroundRuleNotConst, got {err:?}"
        );
    }

    #[test]
    fn extract_constants_rejects_var() {
        assert_head_arg_rejected(HeadArg::Var("X".into()));
    }

    #[test]
    fn extract_constants_rejects_aggregation() {
        let agg = Aggregation::new(
            AggregationOperator::Sum,
            Arithmetic::new(Factor::Var("X".into()), vec![]),
        );
        assert_head_arg_rejected(HeadArg::Aggregation(agg));
    }

    /// Multi-head rules and body disjunctions expand by cross-product:
    /// one rule per (head, body) pair.
    #[test]
    fn multi_head_and_disjunction_expand_by_cross_product() {
        let rules = FlowLogRule::expand_from_parsed_rule(
            parse_pair(Rule::rule, "h1(X), h2(X) :- ( a(X) ; b(X) )."),
            FileId::new(0),
        )
        .expect("expansion succeeds");
        let shapes: Vec<String> = rules.iter().map(|r| r.to_string()).collect();
        assert_eq!(
            shapes,
            [
                "h1(X) :- a(X).",
                "h1(X) :- b(X).",
                "h2(X) :- a(X).",
                "h2(X) :- b(X).",
            ]
        );
    }

    /// A disjunction arm may itself be a conjunction: `(a, b ; c, d)` expands
    /// to one rule per arm, each keeping its full comma-separated body.
    #[test]
    fn disjunction_arm_can_be_a_conjunction() {
        let rules = FlowLogRule::expand_from_parsed_rule(
            parse_pair(Rule::rule, "r(X) :- ( a(X), b(X) ; c(X), d(X) )."),
            FileId::new(0),
        )
        .expect("expansion succeeds");
        assert_eq!(rules.len(), 2);
        let bodies: Vec<Vec<&str>> = rules
            .iter()
            .map(|r| r.rhs().iter().map(|p| p.name()).collect())
            .collect();
        assert!(bodies.contains(&vec!["a", "b"]));
        assert!(bodies.contains(&vec!["c", "d"]));
    }

    /// Two body disjunctions cross-multiply: `(a ; b), (c ; d)` expands to the
    /// four combinations.
    #[test]
    fn nested_disjunctions_cross_product() {
        let rules = FlowLogRule::expand_from_parsed_rule(
            parse_pair(Rule::rule, "r(X) :- ( a(X) ; b(X) ), ( c(X) ; d(X) )."),
            FileId::new(0),
        )
        .expect("expansion succeeds");
        assert_eq!(rules.len(), 4);
        let bodies: Vec<(&str, &str)> = rules
            .iter()
            .map(|r| (r.rhs()[0].name(), r.rhs()[1].name()))
            .collect();
        assert!(bodies.contains(&("a", "c")));
        assert!(bodies.contains(&("a", "d")));
        assert!(bodies.contains(&("b", "c")));
        assert!(bodies.contains(&("b", "d")));
    }

    /// Souffle's `.plan N:(...)` form is an alias for the native `.plan (...)`:
    /// the leading version index is stripped and the permutation applied.
    #[test]
    fn plan_souffle_form_applies_permutation() {
        let rule = parse_rule("h(X) :- a(X), b(X), c(X).\n.plan 1:(3, 1, 2)");
        let names: Vec<&str> = rule
            .rhs()
            .iter()
            .filter_map(|p| match p {
                Predicate::PositiveAtom(a) => Some(a.name()),
                _ => None,
            })
            .collect();
        assert_eq!(names, ["c", "a", "b"]);
    }

    /// Display round-trips the source, joining predicates with `, ` and
    /// closing with `.`.
    #[test]
    fn display_round_trips_source() {
        assert_eq!(
            parse_rule("h(X) :- a(X), !b(X), X < y.").to_string(),
            "h(X) :- a(X), !b(X), X < y."
        );
    }

    // --- `.plan` index application ---
    //
    // `.plan` operates on a built `FlowLogRule`, so `parse_rule` produces the
    // input by parsing one; the producer under test is then called directly.
}
