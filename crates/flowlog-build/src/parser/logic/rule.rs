//! Rule structures for FlowLog Datalog programs.
//!
//! A rule is `head :- p1, p2, ..., pn.`
//! - Head: a single derived relation
//! - Body: predicates that must all be satisfied

use super::{Factor, Head, HeadArg, Predicate};
use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::ConstType;
use crate::parser::{Lexeme, Rule, span_of};
use pest::iterators::Pair;
use std::fmt;

/// A complete FlowLog rule.
///
/// Declared `pub` (not `pub(crate)`) because it leaks through
/// `Catalog::from_rule` / `Program::rules` signatures that external
/// callers (flowlog-compiler, integration tests) consume. The rule's
/// inherent methods stay `pub(crate)` — external code only passes rules
/// by reference without inspecting fields.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct FlowLogRule {
    head: Head,
    rhs: Vec<Predicate>,
    span: Ignored<Span>,
    /// Tombstone for the future cost-based optimizer: when true, the user
    /// supplied a `.plan` hint and the positive-atom order in `rhs` has
    /// already been permuted to match it — the optimizer must not reorder.
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

impl fmt::Debug for FlowLogRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|p| format!("{p:?}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl FlowLogRule {
    /// Construct a rule.
    #[must_use]
    pub(crate) fn new(head: Head, rhs: Vec<Predicate>) -> Self {
        Self {
            head,
            rhs,
            span: Ignored(Span::DUMMY),
            plan_pinned: false,
        }
    }

    #[must_use]
    #[inline]
    pub(crate) fn positive_atom_count(&self) -> usize {
        self.rhs
            .iter()
            .filter(|p| matches!(p, Predicate::PositiveAtom(_)))
            .count()
    }

    /// Whether the rule's positive-atom order is pinned by a user `.plan`.
    /// Future cost-based optimizer reads this to decide whether to
    /// reorder; today no caller.
    #[must_use]
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn plan_pinned(&self) -> bool {
        self.plan_pinned
    }

    /// Reorder the positive atoms in `rhs` according to a 0-based
    /// permutation, leaving negations / comparisons / fn-calls in their
    /// original global slots. Marks the rule as `plan_pinned`.
    ///
    /// `perm` must be a permutation of `0..positive_atom_count()`. The
    /// caller validates; debug builds re-check the length only.
    pub(crate) fn permute_positive_atoms(&mut self, perm: &[usize]) {
        let pos_indices: Vec<usize> = self
            .rhs
            .iter()
            .enumerate()
            .filter_map(|(i, p)| matches!(p, Predicate::PositiveAtom(_)).then_some(i))
            .collect();

        debug_assert_eq!(perm.len(), pos_indices.len());

        // Cycle-decomposition in place: for each cycle of `perm`, rotate
        // the positive atoms via `Vec::swap`. Zero clones, one bitmap
        // allocation. Correctness sketch: applying the swaps along each
        // cycle resolves all but the last element automatically, since by
        // then the cycle's other slots already hold their final values.
        let n = perm.len();
        let mut visited = vec![false; n];
        for start in 0..n {
            if visited[start] {
                continue;
            }
            let mut current = start;
            while !visited[current] {
                visited[current] = true;
                let next = perm[current];
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
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }

    /// Rule head.
    #[must_use]
    #[inline]
    pub(crate) fn head(&self) -> &Head {
        &self.head
    }

    /// Rule body (right-hand side predicates).
    #[must_use]
    #[inline]
    pub(crate) fn rhs(&self) -> &[Predicate] {
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

    /// Extract constants from a fact's head.
    ///
    /// Panics if any head argument is not a simple constant.
    #[must_use]
    pub(crate) fn extract_constants_from_head(&self) -> Vec<ConstType> {
        let args = self.head.head_arguments();
        let mut out = Vec::with_capacity(args.len());
        for arg in args {
            let HeadArg::Arith(arith) = arg else {
                panic!("Fact head must contain only constants: {self}");
            };
            let Factor::Const(c) = arith.init() else {
                panic!("Fact head must contain only constants: {self}");
            };
            assert!(
                arith.is_const(),
                "Fact head must contain only constants: {self}"
            );
            out.push(c.clone());
        }
        out
    }

    /// Expand a multi-head / multi-body rule into one rule per
    /// (head, body) pair. Parenthesised `(A ; B)` disjunctions inside
    /// a conjunction distribute by cross-product.
    pub(crate) fn expand_from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Vec<Self>, ParseError> {
        let rule_span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let rule_heads_node = inner
            .next()
            .ok_or_else(|| grammar_bug("rule missing heads"))?;
        let mut heads: Vec<Head> = Vec::new();
        for h in rule_heads_node.into_inner() {
            heads.push(Head::from_parsed_rule(h, file)?);
        }

        let rule_bodies_node = inner
            .next()
            .ok_or_else(|| grammar_bug("rule missing bodies"))?;
        let bodies = expand_rule_bodies(rule_bodies_node, file)?;

        let mut out = Vec::with_capacity(heads.len() * bodies.len());
        for head in &heads {
            for body in &bodies {
                out.push(Self {
                    head: head.clone(),
                    rhs: body.clone(),
                    span: Ignored(rule_span),
                    plan_pinned: false,
                });
            }
        }
        Ok(out)
    }
}

fn parse_plan_indices(pair: Pair<Rule>, file: FileId) -> Result<(Span, Vec<usize>), ParseError> {
    let span = span_of(&pair, file);
    let mut raw_indices = Vec::new();
    for child in pair.into_inner() {
        if !matches!(child.as_rule(), Rule::plan_index) {
            return Err(grammar_bug(format!(
                "plan_directive unexpected child rule {:?}",
                child.as_rule()
            )));
        }
        let parsed: usize = child
            .as_str()
            .parse()
            .map_err(|_| grammar_bug("plan_index is not a valid integer"))?;
        raw_indices.push(parsed);
    }
    Ok((span, raw_indices))
}

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
    let mut perm: Vec<usize> = Vec::with_capacity(k);
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
        perm.push(zero);
    }
    rule.permute_positive_atoms(&perm);
    Ok(())
}

/// State-machine consumer for top-level programs and loop/fixpoint blocks:
/// `plan_target_start` was set by the caller when the most recent rule
/// clause emitted its first rule, and `.take()` here both consumes it and
/// makes a subsequent `.plan` an orphan.
pub(crate) fn consume_plan_directive(
    pair: Pair<Rule>,
    file: FileId,
    rules: &mut [FlowLogRule],
    plan_target_start: &mut Option<usize>,
) -> Result<(), ParseError> {
    let (span, raw_indices) = parse_plan_indices(pair, file)?;
    let start = plan_target_start
        .take()
        .ok_or(ParseError::PlanOrphan { span })?;
    for rule in &mut rules[start..] {
        apply_indices_to_rule(rule, span, &raw_indices)?;
    }
    Ok(())
}

/// Single-rule consumer for `.comp` bodies: comp body items run through
/// the inliner and reject multi-head/multi-body, so there's exactly one
/// rule to pin. Caller owns the "last RawItem::Rule" anchor.
pub(crate) fn apply_plan_directive_to_rule(
    pair: Pair<Rule>,
    file: FileId,
    rule: &mut FlowLogRule,
) -> Result<(), ParseError> {
    let (span, raw_indices) = parse_plan_indices(pair, file)?;
    apply_indices_to_rule(rule, span, &raw_indices)
}

fn expand_rule_bodies(node: Pair<Rule>, file: FileId) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut alternatives = Vec::new();
    for predicates_node in node.into_inner() {
        alternatives.extend(expand_predicates(predicates_node, file)?);
    }
    Ok(alternatives)
}

/// Expand one comma-separated `predicates` node, distributing any
/// nested `(A ; B)` groups by cross-product.
fn expand_predicates(node: Pair<Rule>, file: FileId) -> Result<Vec<Vec<Predicate>>, ParseError> {
    let mut acc: Vec<Vec<Predicate>> = vec![Vec::new()];
    for pred_node in node.into_inner() {
        let inner = pred_node
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("predicate missing inner"))?;
        if matches!(inner.as_rule(), Rule::disjunction_group) {
            let bodies_node = inner
                .into_inner()
                .next()
                .ok_or_else(|| grammar_bug("disjunction_group missing inner rule_bodies"))?;
            let nested = expand_rule_bodies(bodies_node, file)?;
            let mut next = Vec::with_capacity(acc.len() * nested.len());
            for prefix in &acc {
                for alt in &nested {
                    let mut combined = prefix.clone();
                    combined.extend(alt.iter().cloned());
                    next.push(combined);
                }
            }
            acc = next;
        } else {
            let p = Predicate::from_inner(inner, file)?;
            for conjunction in &mut acc {
                conjunction.push(p.clone());
            }
        }
    }
    Ok(acc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::AggregationOperator;
    use crate::parser::logic::{Aggregation, Arithmetic, Factor};
    use crate::parser::primitive::ConstType;

    fn head_const(v: ConstType) -> HeadArg {
        HeadArg::Arith(Arithmetic::new(Factor::Const(v), vec![]))
    }
    fn head_named(name: &str, args: Vec<HeadArg>) -> Head {
        Head::new(name.into(), args)
    }

    #[test]
    fn extract_constants() {
        let head = head_named(
            "facts",
            vec![
                head_const(ConstType::Int(42)),
                head_const(ConstType::Text("hello".into())),
            ],
        );
        let r = FlowLogRule::new(head, vec![]);
        let c = r.extract_constants_from_head();
        assert_eq!(c, vec![ConstType::Int(42), ConstType::Text("hello".into())]);
    }

    #[test]
    #[should_panic(expected = "Fact head must contain only constants")]
    fn extract_constants_panics_on_var() {
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Int(1)), HeadArg::Var("X".into())],
        );
        let _ = FlowLogRule::new(head, vec![]).extract_constants_from_head();
    }

    #[test]
    #[should_panic(expected = "Fact head must contain only constants")]
    fn extract_constants_panics_on_aggregation() {
        let agg = Aggregation::new(
            AggregationOperator::Sum,
            Arithmetic::new(Factor::Var("X".into()), vec![]),
        );
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Int(1)), HeadArg::Aggregation(agg)],
        );
        let _ = FlowLogRule::new(head, vec![]).extract_constants_from_head();
    }
}
