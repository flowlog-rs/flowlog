//! Aggregation expressions for FlowLog Datalog programs.
//!
//! - [`AggregationOperator`]: `min | max | count | sum | mean`
//! - [`AggregateExpr`]: `op target : { body }`, a value computed from the
//!   matches of a nested body (e.g., `sum w : { Emp(_, d, w) }`), and the
//!   variable a rule body binds to it once lowering hoists it

use std::collections::HashSet;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use super::Atom;
use super::Predicate;
use super::Variable;
use super::predicate;
use crate::DataType;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

// =============================================================================
// AggregationOperator
// =============================================================================

/// Supported aggregation operators.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AggregationOperator {
    Min,
    Max,
    Count,
    Sum,
    Mean,
}

impl AggregationOperator {
    /// Returns `true` if the operator gives the same answer over a bag as
    /// over its set: `min` and `max`, which a repeated row cannot change.
    /// `count`, `sum` and `mean` add every row up.
    #[must_use]
    pub fn is_idempotent(self) -> bool {
        match self {
            Self::Min | Self::Max => true,
            Self::Count | Self::Sum | Self::Mean => false,
        }
    }

    /// Returns `true` if the operator is defined over no matches at all:
    /// `count` and `sum` answer `0` for such a group. `min`, `max` and
    /// `mean` have no value over nothing, so such a group has no answer.
    #[must_use]
    pub fn defined_on_empty(self) -> bool {
        match self {
            Self::Count | Self::Sum => true,
            Self::Min | Self::Max | Self::Mean => false,
        }
    }
}

impl fmt::Display for AggregationOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Min => write!(f, "min"),
            Self::Max => write!(f, "max"),
            Self::Count => write!(f, "count"),
            Self::Sum => write!(f, "sum"),
            Self::Mean => write!(f, "mean"),
        }
    }
}

impl Lexeme for AggregationOperator {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        Ok(match node.rule() {
            Rule::min => Self::Min,
            Rule::max => Self::Max,
            Rule::count => Self::Count,
            Rule::sum => Self::Sum,
            Rule::mean => Self::Mean,
            other => {
                return Err(grammar_bug(format!(
                    "unexpected aggregation operator rule: {other:?}"
                )));
            }
        })
    }
}

// =============================================================================
// AggregateExpr
// =============================================================================

/// `op target : { body }`: a value computed from the matches of `body`, the
/// distinct bindings of the variables it names. `count` counts them; the
/// other operators evaluate `target` once per match.
///
/// The grammar guarantees `count` has no target and every other operator has
/// one, and that `body` holds only positive atoms and comparisons.
///
/// Rule lowering hoists each aggregate out of its expression into a body
/// binding, `var = op target : { body }`, with a fresh `var` that no source
/// can spell and that takes the aggregate's place. An aggregate nested in
/// another's target or comparisons is bound in that one's body, so after
/// lowering every target and comparison is free of aggregates. No atom binds
/// `var`, so its type lives here too, pinned by type checking.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct AggregateExpr {
    operator: AggregationOperator,
    target: Option<Arithmetic>,
    body: Vec<Predicate>,
    var: Option<Variable>,
    var_type: Option<DataType>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl AggregateExpr {
    /// Source location this aggregate was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    #[must_use]
    #[inline]
    pub fn operator(&self) -> AggregationOperator {
        self.operator
    }

    /// The expression evaluated per match; `None` exactly for `count`.
    #[must_use]
    #[inline]
    pub fn target(&self) -> Option<&Arithmetic> {
        self.target.as_ref()
    }

    /// The nested body whose matches are aggregated.
    #[must_use]
    #[inline]
    pub fn body(&self) -> &[Predicate] {
        &self.body
    }

    /// Every atom the body reads, positive or negated, those inside nested
    /// aggregates included, in source order.
    #[must_use]
    pub fn body_atoms(&self) -> Vec<&Atom> {
        predicate::body_atoms(&self.body)
    }

    /// Every variable the aggregate mentions: its target's and its body's,
    /// nested aggregates included, once rule lowering has hoisted those
    /// into the body. Not the variable it binds.
    #[must_use]
    pub fn vars(&self) -> HashSet<&Variable> {
        let mut vars: HashSet<&Variable> = self
            .target
            .iter()
            .flat_map(|target| target.vars())
            .collect();
        for predicate in &self.body {
            match predicate {
                Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                    vars.extend(atom.vars());
                }
                Predicate::Compare(comparison) => vars.extend(comparison.vars_set()),
                Predicate::Aggregate(nested) => vars.extend(nested.vars()),
            }
        }
        vars
    }

    /// Variables the body binds: those of its positive atoms and each
    /// nested aggregate's variable. A variable the aggregate holds that
    /// occurs nowhere outside it must be one of them to have a value.
    #[must_use]
    pub fn bound_vars(&self) -> HashSet<&Variable> {
        predicate::bound_vars(&self.body)
    }

    /// The variable this aggregate binds: `Some` once lowering has hoisted
    /// it into a rule body binding, `None` while it sits in an expression.
    #[must_use]
    #[inline]
    pub fn var(&self) -> Option<&Variable> {
        self.var.as_ref()
    }

    /// The type of [`Self::var`]: `Some` once type checking has pinned it.
    #[must_use]
    #[inline]
    pub fn var_type(&self) -> Option<&DataType> {
        self.var_type.as_ref()
    }

    pub(crate) fn set_var(&mut self, var: Variable) {
        self.var = Some(var);
    }

    pub(crate) fn set_var_type(&mut self, ty: DataType) {
        self.var_type = Some(ty);
    }

    #[inline]
    pub(crate) fn target_mut(&mut self) -> Option<&mut Arithmetic> {
        self.target.as_mut()
    }

    #[inline]
    pub(crate) fn body_mut(&mut self) -> &mut Vec<Predicate> {
        &mut self.body
    }
}

impl fmt::Display for AggregateExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(var) = &self.var {
            write!(f, "{var} = ")?;
        }
        write!(f, "{}", self.operator)?;
        if let Some(target) = &self.target {
            write!(f, " {target}")?;
        }
        write!(f, " : {{ ")?;
        for (i, predicate) in self.body.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{predicate}")?;
        }
        write!(f, " }}")
    }
}

impl Lexeme for AggregateExpr {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let operator = children.lower_next("aggregate operator")?;
        let target = children
            .take_if(Rule::arithmetic_expr)
            .map(Node::lower)
            .transpose()?;
        // A lone atom without braces is the body's only child; braced items
        // each lower as a predicate.
        let mut body_items = children.require(Rule::aggregate_body)?.children();
        let body = match body_items.take_if(Rule::atom) {
            Some(atom) => vec![Predicate::PositiveAtom(atom.lower()?)],
            None => body_items.map(Node::lower).collect::<Result<_, _>>()?,
        };
        Ok(Self {
            operator,
            target,
            body,
            var: None,
            var_type: None,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use pest::Parser as _;
    use pest::error::ErrorVariant;
    use rstest::rstest;

    use super::*;
    use crate::FlowLogParser;
    use crate::assert_err;
    use crate::test_harness::parse_node;
    use crate::test_harness::parse_pair;
    use crate::test_harness::parse_rule;

    // Cases: operator, idempotent.
    #[rstest]
    #[case(AggregationOperator::Min, true)]
    #[case(AggregationOperator::Max, true)]
    #[case(AggregationOperator::Count, false)]
    #[case(AggregationOperator::Sum, false)]
    #[case(AggregationOperator::Mean, false)]
    fn only_the_extremes_are_idempotent(#[case] op: AggregationOperator, #[case] idempotent: bool) {
        assert_eq!(op.is_idempotent(), idempotent);
    }

    // Cases: operator, defined on empty.
    #[rstest]
    #[case(AggregationOperator::Count, true)]
    #[case(AggregationOperator::Sum, true)]
    #[case(AggregationOperator::Min, false)]
    #[case(AggregationOperator::Max, false)]
    #[case(AggregationOperator::Mean, false)]
    fn only_count_and_sum_are_defined_on_empty(
        #[case] op: AggregationOperator,
        #[case] answers: bool,
    ) {
        assert_eq!(op.defined_on_empty(), answers);
    }

    /// Each keyword lowers to its operator, which displays as that keyword.
    // Cases: start rule, keyword, operator.
    #[rstest]
    #[case(Rule::min, "min", AggregationOperator::Min)]
    #[case(Rule::max, "max", AggregationOperator::Max)]
    #[case(Rule::count, "count", AggregationOperator::Count)]
    #[case(Rule::sum, "sum", AggregationOperator::Sum)]
    #[case(Rule::mean, "mean", AggregationOperator::Mean)]
    fn operator_keywords_roundtrip_through_display(
        #[case] start_rule: Rule,
        #[case] keyword: &str,
        #[case] expected: AggregationOperator,
    ) {
        let op: AggregationOperator = parse_node(start_rule, keyword);
        assert_eq!(op, expected);
        assert_eq!(op.to_string(), keyword);
    }

    #[test]
    fn aggregate_keeps_operator_target_and_body() {
        let agg: AggregateExpr =
            parse_node(Rule::aggregate_expr, "sum w : { Emp(_, d, w), w > 0 }");
        assert_eq!(agg.operator(), AggregationOperator::Sum);
        assert_eq!(agg.target().map(ToString::to_string).as_deref(), Some("w"));
        let body: Vec<String> = agg.body().iter().map(ToString::to_string).collect();
        assert_eq!(body, ["Emp(_, d, w)", "w > 0"]);
    }

    /// An aggregate still in its expression binds no variable.
    #[test]
    fn a_lowered_aggregate_binds_no_variable() {
        let agg: AggregateExpr = parse_node(Rule::aggregate_expr, "count : R(x)");
        assert_eq!(agg.var(), None);
    }

    /// The variables an aggregate mentions span its target and its body,
    /// a nested aggregate's own and those it holds included, and leave out
    /// the variable the aggregate itself binds.
    #[test]
    fn vars_cover_the_target_and_the_body_but_not_the_bound_variable() {
        let rule =
            parse_rule("R(s) :- s = sum w : { Emp(_, d, w), count : { Team(d, q) } > 1, w > z }.");
        let Predicate::Aggregate(agg) = &rule.rhs()[0] else {
            panic!("expected an aggregate binding, got {}", rule.rhs()[0]);
        };
        assert_eq!(agg.var(), Some(&Variable::Aggregate(0)));
        let mut vars: Vec<String> = agg.vars().into_iter().map(ToString::to_string).collect();
        vars.sort_unstable();
        assert_eq!(vars, ["@agg_1", "d", "q", "w", "z"]);
    }

    /// Once it binds a variable, an aggregate displays as that binding.
    #[test]
    fn a_bound_aggregate_displays_its_variable() {
        let mut agg: AggregateExpr = parse_node(Rule::aggregate_expr, "count : R(x)");
        agg.set_var(Variable::Aggregate(0));
        assert_eq!(agg.to_string(), "@agg_0 = count : { R(x) }");
    }

    #[test]
    fn a_lone_atom_may_drop_the_braces() {
        let agg: AggregateExpr = parse_node(Rule::aggregate_expr, "count : Edge(x, _)");
        assert_eq!(agg.operator(), AggregationOperator::Count);
        assert!(agg.target().is_none());
        assert!(matches!(agg.body(), [Predicate::PositiveAtom(_)]));
    }

    #[test]
    fn aggregate_span_covers_the_whole_expression() {
        let agg: AggregateExpr = parse_node(Rule::aggregate_expr, "min d : R(d)");
        assert_eq!((agg.span().start(), agg.span().end()), (0, 12));
    }

    // Cases: source, rendered.
    #[rstest]
    #[case("count : { E(x, _) }", "count : { E(x, _) }")]
    #[case("count : E(x, _)", "count : { E(x, _) }")]
    #[case("sum y * 2 : { A(x, y), y != 3 }", "sum y * 2 : { A(x, y), y != 3 }")]
    #[case("mean v : { A(v) }", "mean v : { A(v) }")]
    #[case("sum (y + 1) * 2 : A(y)", "sum (y + 1) * 2 : { A(y) }")]
    fn aggregate_display_reparses_to_the_same_aggregate(
        #[case] source: &str,
        #[case] rendered: &str,
    ) {
        let agg: AggregateExpr = parse_node(Rule::aggregate_expr, source);
        assert_eq!(agg.to_string(), rendered);
        assert_eq!(
            parse_node::<AggregateExpr>(Rule::aggregate_expr, rendered),
            agg
        );
    }

    /// The grammar gives `count` no target and every other operator one, and
    /// admits only positive atoms and comparisons in the body.
    #[rstest]
    #[case("count x : { A(x) }")]
    #[case("sum : { A(x) }")]
    #[case("min : A(x)")]
    #[case("mean : A(x)")]
    #[case("COUNT : A(x)")]
    #[case("average v : A(v)")]
    #[case("count : { A(x), !B(x) }")]
    fn malformed_aggregates_are_syntax_errors(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::aggregate_expr, source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    /// The body shares `paren_item`'s expression prefix, so the grammar
    /// admits a bare value or a condition group there; lowering refuses both.
    #[rstest]
    #[case("count : { x + 1 }")]
    #[case("count : { (A(x); B(x)) }")]
    fn values_and_condition_groups_are_not_aggregate_body_items(#[case] source: &str) {
        let node = Node::new(parse_pair(Rule::aggregate_expr, source), FileId::new(0));
        assert_err!(node.lower::<AggregateExpr>(), ParseError::Syntax { .. });
    }

    /// Each body item parses its `match(` prefix once, whether it becomes a
    /// call, an atom, or a string constraint.
    #[rstest]
    #[case::complete(true)]
    #[case::unclosed(false)]
    fn nested_aggregate_bodies_read_each_item_once(#[case] closed: bool) {
        let depth = 64;
        let open = "count : { match(".repeat(depth);
        let source = if closed {
            format!("{open}x{}", ", y) }".repeat(depth))
        } else {
            open
        };
        let result = FlowLogParser::parse(Rule::aggregate_expr, &source);
        if closed {
            assert_eq!(result.unwrap().as_str(), source);
        } else {
            assert!(matches!(
                result.unwrap_err().variant,
                ErrorVariant::ParsingError { .. }
            ));
        }
    }

    /// The operators are keywords, so a call cannot take their name and a
    /// target may open with a parenthesis.
    #[test]
    fn a_parenthesized_target_is_an_aggregate_not_a_call() {
        let agg: AggregateExpr = parse_node(Rule::aggregate_expr, "sum (x + y) : A(x, y)");
        assert_eq!(
            agg.target().map(ToString::to_string).as_deref(),
            Some("(x + y)")
        );
    }

    /// Parenthesized targets nest without backtracking: with the operators
    /// reserved, each level has one reading.
    #[test]
    fn deeply_nested_parenthesized_targets_are_fully_consumed() {
        let mut source = "x".to_string();
        // Deep enough that reparsing each level would never finish, shallow
        // enough for pest's stack limit.
        for _ in 0..64 {
            source = format!("sum ({source}) : A(x)");
        }
        let pairs = FlowLogParser::parse(Rule::aggregate_expr, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }
}
