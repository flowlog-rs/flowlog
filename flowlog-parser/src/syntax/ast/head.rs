//! Rule heads for FlowLog Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith | Aggregation`
//! - [`Head`]: `rel(arg1, ..., argN)`

use std::fmt;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

use super::Aggregation;
use super::Arithmetic;
use super::Factor;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// Pass-through variable.
    Var(String),
    /// Arithmetic expression (includes UDF calls).
    Arith(Arithmetic),
    /// Aggregation (e.g., `count(X)`).
    Aggregation(Aggregation),
}

impl HeadArg {
    /// Variables referenced by this argument (order preserved, duplicates kept).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Arith(a) => a.vars(),
            Self::Aggregation(agg) => agg.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Arith(a) => write!(f, "{a}"),
            Self::Aggregation(agg) => write!(f, "{agg}"),
        }
    }
}

impl Lexeme for HeadArg {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("head argument value")?;
        Ok(match inner.rule() {
            Rule::aggregate_expr => Self::Aggregation(inner.lower()?),
            Rule::arithmetic_expr => {
                let arith: Arithmetic = inner.lower()?;
                if arith.rest().is_empty()
                    && let Factor::Var(name) = arith.init()
                {
                    Self::Var(name.clone())
                } else {
                    Self::Arith(arith)
                }
            }
            other => {
                return Err(grammar_bug(format!(
                    "unexpected rule for HeadArg: {other:?}"
                )));
            }
        })
    }
}

/// `rel(arg1, ..., argN)`
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Head {
    name: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    raw_name: String,
    head_fingerprint: u64,
    head_arguments: Vec<HeadArg>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Head {
    #[cfg(test)]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        let raw_name = name.clone();
        let name = name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span: Span::DUMMY,
        }
    }

    /// Rename in-place. Lowercases and refreshes the cached fingerprint.
    /// Leaves `raw_name` untouched.
    pub fn set_name(&mut self, name: String) {
        let lname = name.to_lowercase();
        self.head_fingerprint = compute_fp(&lname);
        self.name = lname;
    }

    /// Source location this head was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Canonical relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface spelling of the relation name as the user wrote it.
    #[must_use]
    #[inline]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Head fingerprint.
    #[must_use]
    #[inline]
    pub fn head_fingerprint(&self) -> u64 {
        self.head_fingerprint
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    #[inline]
    pub fn head_arguments_mut(&mut self) -> &mut [HeadArg] {
        &mut self.head_arguments
    }

    /// Arity (number of arguments).
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, arg) in self.head_arguments.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl Lexeme for Head {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        let name = children.require(Rule::relation_ref)?;
        let raw_name = name.text().to_string();
        let name = raw_name.to_lowercase();
        let head_fingerprint = compute_fp(&name);
        let head_arguments = children.map(Node::lower).collect::<Result<_, _>>()?;
        Ok(Self {
            name,
            raw_name,
            head_fingerprint,
            head_arguments,
            span,
        })
    }
}

#[cfg(test)]
mod tests {
    use pest::Parser as _;
    use pest::error::ErrorVariant;
    use rstest::rstest;

    use super::*;
    use crate::AggregationOperator;
    use crate::FlowLogParser;
    use crate::test_util::parse_node;

    #[test]
    fn cast_keyword_cannot_name_a_head_relation() {
        let err = FlowLogParser::parse(Rule::head, "as(x, T)").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("sum(x) + 1")]
    #[case("count(x) * y")]
    fn aggregate_must_occupy_the_entire_head_argument(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::head, &format!("H({source})")).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[test]
    fn head_arguments_do_not_admit_negation() {
        let err = FlowLogParser::parse(Rule::head, "H(!f(x))").unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case::aggregate("f(", "")]
    #[case::value_fallback("f(", ", y")]
    #[case::aggregate_named_calls("sum(", "")]
    fn nested_head_operands_are_fully_consumed(#[case] open: &str, #[case] suffix: &str) {
        let operand = format!("{}x{}", open.repeat(256), ")".repeat(256));
        let source = format!("H(sum({operand}{suffix}))");
        let pairs = FlowLogParser::parse(Rule::head, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }

    #[rstest]
    #[case("f(")]
    #[case("sum(")]
    fn unclosed_nested_head_operands_are_rejected(#[case] open: &str) {
        let source = format!("H(sum({}x", open.repeat(256));
        let err = FlowLogParser::parse(Rule::head, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("count(x)", "aggregate")]
    #[case("COUNT(x)", "aggregate")]
    #[case("average(x)", "aggregate")]
    #[case("AVG(x)", "aggregate")]
    #[case("sum(x + 1)", "aggregate")]
    #[case("min(x)", "aggregate")]
    #[case("max(x)", "aggregate")]
    #[case("x", "variable")]
    #[case("(x)", "variable")]
    #[case("(sum(x))", "value")]
    #[case("sum(x, y)", "value")]
    #[case("sum()", "value")]
    #[case("summary(x)", "value")]
    #[case("countdown(x)", "value")]
    #[case("Sum(x)", "value")]
    #[case("avg(x)", "value")]
    #[case("f(sum(x))", "value")]
    #[case("as(x, T)", "value")]
    fn calls_are_aggregates_only_as_direct_head_arguments(
        #[case] source: &str,
        #[case] expected: &str,
    ) {
        let arg: HeadArg = parse_node(Rule::head_arg, source);
        let kind = match arg {
            HeadArg::Var(_) => "variable",
            HeadArg::Arith(_) => "value",
            HeadArg::Aggregation(_) => "aggregate",
        };
        assert_eq!(kind, expected);
    }

    /// `HeadArg::vars` must return the *real* referenced variables for each
    /// variant: a constant/empty/"xyzzy" stand-in return would break every
    /// downstream binding pass. Covers `Var` (itself), `Arith`, `Aggregation`.
    #[test]
    fn head_arg_vars_returns_real_variables() {
        let x = "X".to_string();
        assert_eq!(HeadArg::Var("X".into()).vars(), vec![&x]);

        let y = "Y".to_string();
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).vars(), vec![&y]);

        let z = "Z".to_string();
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("Z"), Span::DUMMY);
        assert_eq!(HeadArg::Aggregation(agg).vars(), vec![&z]);
    }

    /// `HeadArg`'s `Display` renders each variant exactly; an empty (default)
    /// rendering would corrupt every `.dl` round-trip and diagnostic.
    #[test]
    fn head_arg_display_renders_each_variant() {
        assert_eq!(HeadArg::Var("X".into()).to_string(), "X");
        assert_eq!(HeadArg::Arith(Arithmetic::var("Y")).to_string(), "Y");
        let agg = Aggregation::new(AggregationOperator::Sum, Arithmetic::var("Z"), Span::DUMMY);
        assert_eq!(HeadArg::Aggregation(agg).to_string(), "sum(Z)");
    }

    /// `head_fingerprint` returns the cached `compute_fp(name)` (lowercased),
    /// not a constant: distinct names yield distinct fingerprints, and the
    /// value matches `compute_fp` exactly.
    #[test]
    fn head_fingerprint_is_name_hash_not_constant() {
        let foo = Head::new("Foo".into(), vec![]);
        let bar = Head::new("Bar".into(), vec![]);
        assert_eq!(foo.head_fingerprint(), compute_fp("foo"));
        assert_ne!(foo.head_fingerprint(), bar.head_fingerprint());
    }

    /// `arity` is the argument count, not a constant 0 or 1. Table spans 0..=3
    /// so both the `-> 0` and `-> 1` mutants die.
    #[test]
    fn head_arity_counts_arguments() {
        for n in 0..=3usize {
            let args: Vec<HeadArg> = (0..n).map(|i| HeadArg::Var(format!("v{i}"))).collect();
            let head = Head::new("r".into(), args);
            assert_eq!(head.arity(), n, "arity for {n} args");
        }
    }

    /// `Head`'s `Display` writes `name(a, b, c)`: commas separate arguments
    /// (the `i > 0` guard) with none before the first. Three args pin the
    /// boundary: `==`/`<`/`>=` each produce a different string, and an empty
    /// (default) rendering is caught too.
    #[test]
    fn head_display_comma_separates_arguments() {
        let head = Head::new(
            "r".into(),
            vec![
                HeadArg::Var("a".into()),
                HeadArg::Var("b".into()),
                HeadArg::Var("c".into()),
            ],
        );
        assert_eq!(head.to_string(), "r(a, b, c)");
    }
}
