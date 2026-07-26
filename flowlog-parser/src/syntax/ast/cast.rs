//! `as(expr, T)` cast expressions.

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
    #[must_use]
    fn new(inner: Factor, target_type: String, span: Span) -> Self {
        Self {
            inner: Box::new(inner),
            target_type,
            span,
        }
    }

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
        // Grammar: `as( factor , type_ref )`, always in that order.
        let span = node.span();
        let mut children = node.children();
        let inner = children.lower_next::<Factor>("cast operand")?;
        let target = children.require(Rule::type_ref)?.text().trim().to_string();
        Ok(Self::new(inner, target, span))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::parse_node;

    /// `as(x, T)` carries the user-written target type name and
    /// round-trips through Display.
    #[test]
    fn cast_parses_and_round_trips() {
        let cast: Cast = parse_node(Rule::as_cast, "as(x, uint32)");
        assert_eq!(cast.target_type(), "uint32");
        assert!(matches!(cast.inner(), Factor::Var(v) if v == "x"));
        assert_eq!(cast.to_string(), "as(x, uint32)");
    }
}
