//! Predicate types for FlowLog Datalog programs.
//!
//! - Positive atoms: `edge(X, Y)`
//! - Negative atoms: `!edge(X, Y)`
//! - Comparisons: `X > 5`, `Age >= 18`
//!
//! Predicates form the antecedent of rules: `head(...) :- p1, !p2, X > Y.`

use std::fmt;

use super::Atom;
use super::ComparisonExpr;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// A predicate in a rule body.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// Positive atom, e.g. `edge(X, Y)`.
    PositiveAtom(Atom),
    /// Negative atom (negation as failure), e.g. `!edge(X, Y)`.
    NegativeAtom(Atom),
    /// Comparison expression. Covers arithmetic comparisons (`X > 5`), UDF
    /// filters (`f(X) = True`, since UDFs are value-only), and the string
    /// constraints `match`/`contains` (whose operator carries the `!`).
    Compare(ComparisonExpr),
}

#[cfg(test)]
impl Predicate {
    /// Relation name for atom / negative-atom predicates. Tests only;
    /// production code pattern-matches the variant.
    pub fn name(&self) -> &str {
        match self {
            Self::PositiveAtom(atom) | Self::NegativeAtom(atom) => atom.name(),
            Self::Compare(_) => unreachable!("no name on Compare"),
        }
    }
}

impl fmt::Display for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom}"),
            Self::Compare(expr) => write!(f, "{expr}"),
        }
    }
}

impl fmt::Debug for Predicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtom(atom) => write!(f, "{atom:?}"),
            Self::NegativeAtom(atom) => write!(f, "!{atom:?}"),
            Self::Compare(expr) => write!(f, "{expr}"),
        }
    }
}

impl Lexeme for Predicate {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.clone().children();
        let inner = children.next_any("predicate")?;
        Ok(match inner.rule() {
            Rule::compare_expr | Rule::string_constraint | Rule::negative_string_constraint => {
                Self::Compare(inner.lower()?)
            }
            Rule::atom => Self::PositiveAtom(inner.lower()?),
            Rule::negative_atom => {
                let mut children = inner.children();
                children.require(Rule::not_op)?;
                Self::NegativeAtom(children.lower_next("negated atom")?)
            }
            Rule::disjunction_group => {
                return Err(ParseError::Syntax {
                    span,
                    message: "expected a single predicate".into(),
                });
            }
            Rule::arithmetic_expr => {
                // Only `paren_item` delays classification. Its recursive
                // expression prefix was parsed once; the comparison parser
                // owns the operands and operator when a suffix is present.
                if children.next().is_some() {
                    return Ok(Self::Compare(node.lower()?));
                }
                let mut operands = inner.children();
                let factor = operands.require(Rule::factor)?;
                if operands.next().is_some() {
                    return Err(ParseError::Syntax {
                        span,
                        message: "expected a relation or comparison".into(),
                    });
                }
                let primary = factor.children().next_any("predicate value")?;
                match primary.rule() {
                    Rule::call_expr => {
                        if let Some(comparison) =
                            ComparisonExpr::from_parenthesized_call(primary.clone())?
                        {
                            Self::Compare(comparison)
                        } else {
                            Self::PositiveAtom(primary.lower()?)
                        }
                    }
                    _ => {
                        return Err(ParseError::Syntax {
                            span,
                            message: "expected a relation or comparison".into(),
                        });
                    }
                }
            }
            Rule::placeholder => {
                return Err(ParseError::Syntax {
                    span,
                    message: "expected a predicate, found a placeholder".into(),
                });
            }
            other => return Err(grammar_bug(format!("invalid predicate rule: {other:?}"))),
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
    use crate::test_util::parse_node;
    use crate::test_util::parse_pair;

    #[rstest]
    #[case("as(x, 5)")]
    #[case("as()")]
    #[case("!as(x, T)")]
    #[case("!(as(x, T))")]
    fn invalid_casts_cannot_fall_back_to_relations_in_groups(#[case] inner: &str) {
        let source = format!("({inner})");
        let err = FlowLogParser::parse(Rule::disjunction_group, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("R(x)")]
    #[case("R(x), S(x); T(x)")]
    #[case("!R(x)")]
    #[case("!contains(cat(x, y), z)")]
    #[case("match(_, x)")]
    #[case("as(x, number) = y")]
    fn nested_condition_groups_are_fully_consumed(#[case] inner: &str) {
        let source = format!("{}{inner}{}", "(".repeat(256), ")".repeat(256));
        let pairs = FlowLogParser::parse(Rule::predicate, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }

    #[rstest]
    #[case("!x > 0")]
    #[case("!f(x) > 0")]
    #[case("!True")]
    #[case("!(R(x), S(x))")]
    #[case("!(R(x); S(x))")]
    #[case("!((R(x)))")]
    #[case("!(match(cat(x, y), z))")]
    fn negation_does_not_extend_to_values_or_condition_groups(#[case] source: &str) {
        // Closing parentheses prevent a successful prefix such as `!f(x)`
        // from hiding the invalid comparison suffix.
        let source = format!("({source})");
        let err = FlowLogParser::parse(Rule::disjunction_group, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case("x + 1")]
    #[case("_")]
    fn bare_values_are_not_predicate_syntax(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::predicate, source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[test]
    fn unclosed_condition_groups_ignore_comment_delimiters() {
        let source = format!("(R(), S(), # ignored (\n{}", "(".repeat(256));
        let err = FlowLogParser::parse(Rule::predicate, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[test]
    fn comment_and_string_delimiters_do_not_change_condition_groups() {
        let source = r##"(Edge(x), # unmatched ( in comment
            (x + 1) > 2; match(r#"(a,b);#"#, x))"##;
        let pairs = FlowLogParser::parse(Rule::disjunction_group, source).unwrap();
        assert_eq!(pairs.as_str(), source);
        let strings: Vec<_> = pairs
            .flatten()
            .filter(|pair| pair.as_rule() == Rule::string)
            .map(|pair| pair.as_str())
            .collect();
        assert_eq!(strings, [r##"r#"(a,b);#"#"##]);
    }

    #[rstest]
    #[case("Edge(x, 1, _)", "atom", "Edge(x, 1, _)")]
    #[case("c.Edge(?x)", "atom", "c.Edge(?x)")]
    #[case("match(_, x)", "atom", "match(_, x)")]
    #[case("contains(x)", "atom", "contains(x)")]
    #[case("match()", "atom", "match()")]
    #[case("match(x, y, z)", "atom", "match(x, y, z)")]
    #[case("Match(x, y)", "atom", "Match(x, y)")]
    #[case("matches(x, y)", "atom", "matches(x, y)")]
    #[case("match.Edge(x)", "atom", "match.Edge(x)")]
    #[case("!Edge(x)", "negative", "!Edge(x)")]
    #[case("!(Edge(x))", "negative", "!Edge(x)")]
    #[case("!(match(x, y))", "negative", "!match(x, y)")]
    #[case("!match(_, x)", "negative", "!match(_, x)")]
    #[case("!contains(x)", "negative", "!contains(x)")]
    #[case("x < y", "compare", "x < y")]
    #[case("f(x) + 1 > 5", "compare", "f(x) + 1 > 5")]
    #[case("as(x, T) = y", "compare", "as(x, T) = y")]
    #[case("match(x, y) = True", "compare", "match(x, y) = True")]
    #[case("contains(x, y) + 1 > 0", "compare", "contains(x, y) + 1 > 0")]
    #[case("-1 < 0", "compare", "-1 < 0")]
    #[case("match(x, y)", "compare", "match(x, y)")]
    #[case("!match(x, y)", "compare", "!match(x, y)")]
    #[case("!contains(cat(x, y), z)", "compare", "!contains(cat(x, y), z)")]
    fn predicate_forms_preserve_their_meaning(
        #[values(Rule::predicate, Rule::paren_item)] start_rule: Rule,
        #[case] src: &str,
        #[case] kind: &str,
        #[case] rendered: &str,
    ) {
        let predicate: Predicate = parse_node(start_rule, src);
        let actual = match &predicate {
            Predicate::PositiveAtom(_) => "atom",
            Predicate::NegativeAtom(_) => "negative",
            Predicate::Compare(_) => "compare",
        };
        assert_eq!(actual, kind);
        assert_eq!(predicate.to_string(), rendered);
    }

    #[rstest]
    #[case("x + 1")]
    #[case("_")]
    #[case("x")]
    #[case("True")]
    #[case("as(x, T)")]
    fn parenthesized_values_cannot_be_used_as_predicates(#[case] src: &str) {
        let node = Node::new(parse_pair(Rule::paren_item, src), FileId::new(0));
        assert_err!(node.lower::<Predicate>(), ParseError::Syntax { .. });
    }

    #[rstest]
    #[case("(Edge(x))")]
    #[case("(Edge(x), Other(x))")]
    #[case("(Edge(x); Other(x))")]
    fn condition_groups_are_not_single_predicates(
        #[values(Rule::predicate, Rule::paren_item)] start_rule: Rule,
        #[case] source: &str,
    ) {
        let node = Node::new(parse_pair(start_rule, source), FileId::new(0));
        assert_err!(node.lower::<Predicate>(), ParseError::Syntax { .. });
    }

    #[test]
    fn bare_atom_parses_as_a_positive_predicate() {
        let p = parse_node::<Predicate>(Rule::predicate, "edge(x, y)");
        assert!(matches!(p, Predicate::PositiveAtom(_)));
        assert_eq!(p.name(), "edge");
    }

    #[test]
    fn leading_bang_parses_as_a_negated_predicate() {
        let p = parse_node::<Predicate>(Rule::predicate, "!edge(x)");
        assert!(matches!(p, Predicate::NegativeAtom(_)));
        assert_eq!(p.name(), "edge");
    }

    #[test]
    fn comparison_parses_as_a_compare_predicate() {
        assert!(matches!(
            parse_node::<Predicate>(Rule::predicate, "x < y"),
            Predicate::Compare(_)
        ));
    }
}
