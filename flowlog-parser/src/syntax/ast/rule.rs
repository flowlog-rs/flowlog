//! Rules for FlowLog Datalog programs.
//!
//! - [`FlowLogRule`]: `head :- p1, p2, ..., pn.`, a single derived head
//!   and the body predicates that must all hold.

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;

use super::AtomArg;
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

    /// Variable names the positive body atoms bind; constants and
    /// placeholders bind nothing. This is the grounded set: a variable
    /// used anywhere else in the rule must appear in it to have a value.
    #[must_use]
    pub fn positive_body_vars(&self) -> HashSet<&str> {
        self.rhs
            .iter()
            .filter_map(|p| match p {
                Predicate::PositiveAtom(atom) => Some(atom),
                Predicate::NegativeAtom(_) | Predicate::Compare(_) => None,
            })
            .flat_map(|atom| atom.arguments().iter())
            .filter_map(|arg| match arg {
                AtomArg::Var(v) => Some(v.as_str()),
                AtomArg::Const(_) | AtomArg::Placeholder => None,
            })
            .collect()
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

/// Expands the `;` alternatives in `rule_bodies` or `paren_bodies`, preserving
/// their source order. Each alternative is a conjunction of predicates.
fn expand_bodies(node: Node) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut alternatives = Vec::new();
    for conjunction in node.children() {
        alternatives.extend(expand_conjunction(conjunction)?);
    }
    Ok(alternatives)
}

/// Expands a `predicates` or `paren_items` conjunction. Nested disjunctions
/// multiply its alternatives; ordinary predicates append to each alternative.
fn expand_conjunction(node: Node) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut alternatives: Vec<Vec<Predicate>> = vec![Vec::new()];
    // Comma-only groups are associative. Keep their remaining siblings here
    // so deeply nested conjunctions need neither recursive calls nor repeated
    // copies of an already-expanded body. Only real disjunctions recurse.
    let mut pending = vec![node.children()];
    while let Some(items) = pending.last_mut() {
        let Some(item) = items.next() else {
            pending.pop();
            continue;
        };
        let Some(group) = condition_group(item.clone())? else {
            let predicate: Predicate = item.lower()?;
            for alternative in &mut alternatives {
                alternative.push(predicate.clone());
            }
            continue;
        };

        let mut bodies = group.clone().children();
        let first = bodies.next_any("condition group body")?;
        if bodies.next().is_none() {
            pending.push(first.children());
            continue;
        }

        let nested = expand_bodies(group)?;
        // An empty prefix adds no predicates. Taking the nested alternatives
        // avoids cloning every result through surrounding disjunctions.
        if let [prefix] = alternatives.as_slice()
            && prefix.is_empty()
        {
            alternatives = nested;
            continue;
        }
        let mut crossed = Vec::with_capacity(alternatives.len() * nested.len());
        for prefix in &alternatives {
            for suffix in &nested {
                let mut body = prefix.clone();
                body.extend(suffix.iter().cloned());
                crossed.push(body);
            }
        }
        alternatives = crossed;
    }
    Ok(alternatives)
}

/// Returns the body of a bare condition group, or `None` for a value or
/// individual predicate. A trailing comma is valid for tuples, not groups.
fn condition_group(mut node: Node) -> Result<Option<Node>, ParseError> {
    loop {
        match node.rule() {
            Rule::paren_factor => {
                let mut children = node.children();
                let body = children.require(Rule::paren_bodies)?;
                if let Some(comma) = children.next() {
                    return Err(ParseError::Syntax {
                        span: comma.span(),
                        message: "a condition group cannot end with a comma".into(),
                    });
                }
                return Ok(Some(body));
            }
            Rule::predicate
            | Rule::paren_item
            | Rule::disjunction_group
            | Rule::arithmetic_expr
            | Rule::factor => {
                let mut children = node.children();
                let first = children.next_any("body item")?;
                // Operators belong to the whole predicate. Stripping them
                // would mistake `(x) > 0` or `(Edge(x)) + 1` for a body group.
                if children.next().is_some() {
                    return Ok(None);
                }
                node = first;
            }
            _ => return Ok(None),
        }
    }
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
    use rstest::rstest;

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
            Span::DUMMY,
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
    fn disjunction_cross_product_preserves_source_order() {
        let rules = FlowLogRule::expand_from_parsed_rule(
            parse_pair(Rule::rule, "r(X) :- ( a(X) ; b(X) ), ( c(X) ; d(X) )."),
            FileId::new(0),
        )
        .expect("expansion succeeds");
        let bodies: Vec<(&str, &str)> = rules
            .iter()
            .map(|r| (r.rhs()[0].name(), r.rhs()[1].name()))
            .collect();
        assert_eq!(bodies, [("a", "c"), ("a", "d"), ("b", "c"), ("b", "d")]);
    }

    #[test]
    fn nested_conjunctions_keep_each_predicate_once_in_source_order() {
        let source = format!(
            "{}middle(x){}",
            "before(x), (".repeat(200),
            "), after(x)".repeat(200),
        );
        let bodies = expand_bodies(Node::new(
            parse_pair(Rule::rule_bodies, &source),
            FileId::new(0),
        ))
        .unwrap();
        assert_eq!(bodies.len(), 1);
        let body = &bodies[0];
        assert_eq!(body.len(), 401);
        assert!(
            body[..200]
                .iter()
                .all(|predicate| predicate.name() == "before")
        );
        assert_eq!(body[200].name(), "middle");
        assert!(
            body[201..]
                .iter()
                .all(|predicate| predicate.name() == "after")
        );
    }

    #[test]
    fn nested_disjunctions_preserve_all_alternatives() {
        let source = format!("{}last(x){}", "first(x); (".repeat(200), ")".repeat(200));
        let bodies = expand_bodies(Node::new(
            parse_pair(Rule::rule_bodies, &source),
            FileId::new(0),
        ))
        .unwrap();
        assert_eq!(bodies.len(), 201);
        assert!(bodies.iter().all(|body| body.len() == 1));
        assert!(bodies[..200].iter().all(|body| body[0].name() == "first"));
        assert_eq!(bodies[200][0].name(), "last");
    }

    #[test]
    fn redundant_body_groups_preserve_predicates_and_source_spans() {
        let body = "Edge(x), !Other(x), x > 1";
        let source = format!("{}{body}{}", "(".repeat(200), ")".repeat(200));
        let bodies = expand_bodies(Node::new(
            parse_pair(Rule::rule_bodies, &source),
            FileId::new(0),
        ))
        .unwrap();
        assert_eq!(bodies.len(), 1);
        let predicates = &bodies[0];
        assert_eq!(
            predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            ["Edge(x)", "!Other(x)", "x > 1"]
        );
        let spans: Vec<_> = predicates
            .iter()
            .map(|predicate| {
                let span = match predicate {
                    Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => atom.span(),
                    Predicate::Compare(expr) => expr.span(),
                };
                &source[span.range()]
            })
            .collect();
        assert_eq!(spans, ["Edge(x)", "Other(x)", "x > 1"]);
    }

    #[test]
    fn surrounding_predicates_are_preserved_in_each_group_alternative() {
        let bodies = expand_bodies(Node::new(
            parse_pair(
                Rule::rule_bodies,
                "First(x), (Edge(x), (x + 1) > 2; Other(x)), !Last(x)",
            ),
            FileId::new(0),
        ))
        .unwrap();
        let shapes: Vec<Vec<_>> = bodies
            .iter()
            .map(|body| body.iter().map(ToString::to_string).collect())
            .collect();
        assert_eq!(
            shapes,
            [
                vec!["First(x)", "Edge(x)", "(x + 1) > 2", "!Last(x)"],
                vec!["First(x)", "Other(x)", "!Last(x)"],
            ]
        );
    }

    #[rstest]
    #[case("(Edge(x),)")]
    #[case("((Edge(x),))")]
    fn trailing_comma_is_not_a_condition_group(#[case] source: &str) {
        assert_err!(
            expand_bodies(Node::new(
                parse_pair(Rule::rule_bodies, source),
                FileId::new(0),
            )),
            ParseError::Syntax { span, message }
                if &source[span.range()] == ","
                    && message == "a condition group cannot end with a comma"
        );
    }

    #[rstest]
    #[case("((Edge(x)) + 1)")]
    #[case("(1 + (Edge(x)))")]
    fn arithmetic_around_a_group_is_not_discarded(#[case] source: &str) {
        assert_err!(
            expand_bodies(Node::new(
                parse_pair(Rule::rule_bodies, source),
                FileId::new(0),
            )),
            ParseError::Syntax { span, .. }
                if source[span.range()] == source[1..source.len() - 1]
        );
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
