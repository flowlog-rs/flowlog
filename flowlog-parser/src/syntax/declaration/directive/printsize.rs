//! `.printsize R`: report a relation's row count.

use educe::Educe;
use flowlog_error::Span;

use crate::Lexeme;
use crate::Node;
use crate::error::ParseError;

/// `.printsize R`: print the size of an EDB relation.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct PrintSizeDirective {
    relation_name: String,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl PrintSizeDirective {
    /// Creates a directive from already-parsed parts. See
    /// [`InputDirective::new`](super::InputDirective::new).
    pub(crate) fn new(relation_name: String, span: Span) -> Self {
        Self {
            relation_name,
            span,
        }
    }

    /// Canonical (lowercased) target relation name.
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span
    }
}

impl Lexeme for PrintSizeDirective {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let name_node = node.children().next_any("relation name")?;
        Ok(Self {
            span: name_node.span(),
            relation_name: name_node.text().to_lowercase(),
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;

    use super::*;
    use crate::Rule;
    use crate::test_util::parse_pair;

    /// `.printsize` lowercases the target relation name.
    #[test]
    fn printsize_lowercases_name() {
        let d = PrintSizeDirective::from_parsed_rule(Node::new(
            parse_pair(Rule::printsize_directive, ".printsize Edge"),
            FileId::new(0),
        ))
        .expect("printsize_directive parses");
        assert_eq!(d.relation_name(), "edge");
    }
}
