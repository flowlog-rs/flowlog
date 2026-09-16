//! `as(factor, T)` cast expressions and their dedicated grammar lowering.

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Factor;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;

/// `as(factor, target_type)`. `inner` is a single [`Factor`] (not a
/// full [`Arithmetic`](super::Arithmetic)) so the typechecker can lower
/// `Cast(inner)` to `inner` after subtype validation; downstream never
/// sees a cast.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct Cast {
    inner: Box<Factor>,
    target_type: String,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl Cast {
    /// The operand being cast.
    #[must_use]
    #[inline]
    pub fn inner(&self) -> &Factor {
        &self.inner
    }

    #[inline]
    pub(crate) fn inner_mut(&mut self) -> &mut Factor {
        &mut self.inner
    }

    /// User-written target type name; resolved by the typechecker.
    #[must_use]
    #[inline]
    pub fn target_type(&self) -> &str {
        &self.target_type
    }

    /// Source location this cast was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }
}

impl fmt::Display for Cast {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "as({}, {})", self.inner, self.target_type)
    }
}

impl Lexeme for Cast {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let span = node.span();
        let mut children = node.children();
        // The grammar fixes both argument kinds, so type names never need
        // to be recovered from value expressions here.
        children.require(Rule::as_kw)?;
        let inner = children.lower_next::<Factor>("cast operand")?;
        let target_type = children.require(Rule::type_ref)?.text().trim().to_string();
        Ok(Self {
            inner: Box::new(inner),
            target_type,
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
    use crate::FlowLogParser;
    use crate::test_util::parse_node;

    #[test]
    fn nested_casts_are_fully_consumed() {
        let source = format!("{}x{}", "as(".repeat(256), ", T)".repeat(256));
        let pairs = FlowLogParser::parse(Rule::as_cast, &source).unwrap();
        assert_eq!(pairs.as_str(), source);
    }

    #[rstest]
    #[case::unclosed("x", "")]
    #[case::missing_operand("x +", ", T)")]
    #[case::invalid_target("x", ", 5)")]
    fn malformed_nested_casts_do_not_retry_as_calls(#[case] inner: &str, #[case] close: &str) {
        let source = format!("{}{inner}{}", "as(".repeat(256), close.repeat(256));
        // Enter through `factor` to include the competing call alternative.
        let err = FlowLogParser::parse(Rule::factor, &source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }

    #[rstest]
    #[case::complete(true)]
    #[case::unclosed(false)]
    fn cast_target_syntax_handles_nested_tuple_types(#[case] closed: bool) {
        let close = if closed {
            ")".repeat(256)
        } else {
            String::new()
        };
        let source = format!("as(x, {}number{close})", "(field:".repeat(256));
        let result = FlowLogParser::parse(Rule::as_cast, &source);
        if closed {
            assert_eq!(result.unwrap().as_str(), source);
        } else {
            assert!(matches!(
                result.unwrap_err().variant,
                ErrorVariant::ParsingError { .. }
            ));
        }
    }

    #[rstest]
    #[case("as(x, uint32)", "uint32")]
    #[case("as # ignored after the keyword\n(x, uint32)", "uint32")]
    #[case("as(x, uint32 # ignored after the name\n)", "uint32")]
    #[case("as(x, cfg.Context)", "cfg.Context")]
    #[case("as(x, (a: number, b: (c: symbol)))", "(a: number, b: (c: symbol))")]
    fn cast_keeps_the_target_type(#[case] source: &str, #[case] target: &str) {
        let cast: Cast = parse_node(Rule::as_cast, source);
        assert_eq!(cast.target_type(), target);
        assert!(matches!(cast.inner(), Factor::Var(v) if v == "x"));
        assert_eq!(cast.to_string(), format!("as(x, {target})"));
        assert_eq!(&source[cast.span().range()], source);
        assert_eq!(cast, parse_node(Rule::as_cast, &cast.to_string()));
    }

    #[rstest]
    #[case("as()")]
    #[case("as(x)")]
    #[case("as(x, y, z)")]
    #[case("as(x + 1, T)")]
    #[case("as(x, ?T)")]
    #[case("as(x, (T))")]
    #[case("as(x, 42)")]
    #[case("as(x, f(y))")]
    #[case("as(_, T)")]
    #[case("as(x, as)")]
    #[case("as(x, True)")]
    fn cast_rejects_invalid_argument_forms(#[case] source: &str) {
        let err = FlowLogParser::parse(Rule::as_cast, source).unwrap_err();
        assert!(matches!(err.variant, ErrorVariant::ParsingError { .. }));
    }
}
