//! `.input`, `.output`, and `.printsize` directive types.

use std::collections::HashMap;

use educe::Educe;
use flowlog_error::Span;

use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;

// =============================================================================
// InputDirective
// =============================================================================

/// `.input R(...)`: an EDB source plus parameters (IO type, file path, ...).
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct InputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl InputDirective {
    /// Creates a directive from already-parsed parts, for a directive
    /// synthesized rather than parsed from a pest node (e.g. a comp-internal
    /// directive deferred to the enclosing scope).
    pub(crate) fn new(
        relation_name: String,
        parameters: HashMap<String, String>,
        span: Span,
    ) -> Self {
        Self {
            relation_name,
            parameters,
            span,
        }
    }

    /// Canonical (lowercased) target relation name.
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Parsed I/O parameters, keyed by name (`IO`, `filename`, ...).
    #[must_use]
    pub(crate) fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span
    }
}

impl Lexeme for InputDirective {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let (relation_name, parameters, span) = parse_io_directive(node)?;
        Ok(Self {
            relation_name,
            parameters,
            span,
        })
    }
}

// =============================================================================
// OutputDirective
// =============================================================================

/// `.output R(...)`: which relation to write, with optional parameters.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct OutputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl OutputDirective {
    /// Creates a directive from already-parsed parts. See [`InputDirective::new`].
    pub(crate) fn new(
        relation_name: String,
        parameters: HashMap<String, String>,
        span: Span,
    ) -> Self {
        Self {
            relation_name,
            parameters,
            span,
        }
    }

    /// Canonical (lowercased) target relation name.
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Parsed output parameters, keyed by name.
    #[must_use]
    pub(crate) fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span
    }
}

impl Lexeme for OutputDirective {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let (relation_name, parameters, span) = parse_io_directive(node)?;
        Ok(Self {
            relation_name,
            parameters,
            span,
        })
    }
}

// =============================================================================
// PrintSizeDirective
// =============================================================================

/// `.printsize R`: print the size of an EDB relation.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct PrintSizeDirective {
    relation_name: String,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl PrintSizeDirective {
    /// Creates a directive from already-parsed parts. See [`InputDirective::new`].
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

// =============================================================================
// I/O directive parsing
// =============================================================================

/// Parse the relation-name token (lowercased) plus an optional `io_params`
/// child: the common form of an `.input` / `.output` directive.
fn parse_io_directive(node: Node) -> Result<(String, HashMap<String, String>, Span), ParseError> {
    let mut children = node.children();
    let name_node = children.next_any("relation name")?;
    let span = name_node.span();
    let relation_name = name_node.text().to_lowercase();
    let parameters = match children.take_if(Rule::io_params) {
        Some(params) => parse_io_params(params)?,
        None => HashMap::new(),
    };
    Ok((relation_name, parameters, span))
}

/// Parse an `io_params` node (`key = "value", ...`) into a name-to-value
/// map. Values are string literals, decoded via [`crate::decode_string`].
pub(crate) fn parse_io_params(node: Node) -> Result<HashMap<String, String>, ParseError> {
    debug_assert_eq!(node.rule(), Rule::io_params);
    let mut parameters = HashMap::new();
    for io_param in node.children() {
        let mut kv = io_param.children();
        let key = kv.next_any("parameter name")?.text().to_string();
        let value_node = kv.next_any("parameter value")?;
        let value = crate::decode_string(value_node.text(), value_node.span())?;
        parameters.insert(key, value);
    }
    Ok(parameters)
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;

    use super::*;
    use crate::test_util::parse_pair;

    fn input(src: &str) -> InputDirective {
        InputDirective::from_parsed_rule(Node::new(
            parse_pair(Rule::input_directive, src),
            FileId::new(0),
        ))
        .expect("input_directive parses")
    }

    /// `.input` lowercases the target relation name and parses its
    /// parameters into a name-to-value map with the string values decoded
    /// (quotes stripped).
    #[test]
    fn input_lowercases_name_and_decodes_params() {
        let d = input(r#".input Edge(IO="file", filename="edge.csv")"#);
        assert_eq!(d.relation_name(), "edge");
        assert_eq!(d.parameters()["IO"], "file");
        assert_eq!(d.parameters()["filename"], "edge.csv");
    }

    /// A bare `.input Edge` with no `(...)` yields empty parameters.
    #[test]
    fn input_without_params_is_empty() {
        assert!(input(".input Edge").parameters().is_empty());
    }

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
