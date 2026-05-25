//! Input/Output directive types for FlowLog Datalog programs.

use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::{Lexeme, Rule, span_of};
use pest::iterators::Pair;
use std::collections::HashMap;

/// Represents an input directive (EDB source + parameters like file path)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    /// Span of the directive's target relation-name token.
    span: Ignored<Span>,
}

impl InputDirective {
    /// Get the relation name
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Get all input parameters (IO type, filename, etc.)
    #[must_use]
    pub(crate) fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }
}

/// Parse the leading relation-name token plus any `io_params` children of an
/// I/O directive. Shared by [`InputDirective`] and [`OutputDirective`], which
/// differ only in the "missing relation name" diagnostic.
fn parse_io_directive(
    parsed_rule: Pair<Rule>,
    file: FileId,
    missing_name_msg: &'static str,
) -> Result<(String, HashMap<String, String>, Span), ParseError> {
    let mut inner = parsed_rule.into_inner();
    let name_pair = inner.next().ok_or_else(|| grammar_bug(missing_name_msg))?;
    let span = span_of(&name_pair, file);
    let relation_name = name_pair.as_str().to_lowercase();
    let parameters = parse_io_params(inner)?;
    Ok((relation_name, parameters, span))
}

/// Parse `io_params` children from a directive's inner pairs into a key→value map.
fn parse_io_params<'i>(
    inner: impl Iterator<Item = pest::iterators::Pair<'i, Rule>>,
) -> Result<HashMap<String, String>, ParseError> {
    let mut parameters = HashMap::new();
    for node in inner {
        if node.as_rule() == Rule::io_params {
            parameters = parse_io_params_node(node)?;
        }
    }
    Ok(parameters)
}

/// Parse a single `io_params` pest node into a key→value map.
/// Shared with the raw-comp parser in
/// [`crate::parser::declaration::comp`].
pub(super) fn parse_io_params_node(
    node: Pair<Rule>,
) -> Result<HashMap<String, String>, ParseError> {
    debug_assert_eq!(node.as_rule(), Rule::io_params);
    let mut parameters = HashMap::new();
    for io_param in node.into_inner() {
        let mut kv = io_param.into_inner();
        let key = kv
            .next()
            .ok_or_else(|| grammar_bug("io parameter missing name"))?
            .as_str()
            .to_string();
        let value = kv
            .next()
            .ok_or_else(|| grammar_bug("io parameter missing value"))?
            .as_str()
            .trim_matches('"')
            .to_string();
        parameters.insert(key, value);
    }
    Ok(parameters)
}

impl Lexeme for InputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let (relation_name, parameters, span) =
            parse_io_directive(parsed_rule, file, "input directive missing relation name")?;
        Ok(Self {
            relation_name,
            parameters,
            span: Ignored(span),
        })
    }
}

/// Represents an output directive (which relation to write, with optional parameters)
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OutputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    span: Ignored<Span>,
}

impl OutputDirective {
    /// Get the relation name
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Get all output parameters
    #[must_use]
    pub(crate) fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }
}

impl Lexeme for OutputDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let (relation_name, parameters, span) =
            parse_io_directive(parsed_rule, file, "output directive missing relation name")?;
        Ok(Self {
            relation_name,
            parameters,
            span: Ignored(span),
        })
    }
}

/// Directive for printing the size of an EDB relation
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrintSizeDirective {
    relation_name: String,
    span: Ignored<Span>,
}

impl PrintSizeDirective {
    /// Get the relation name
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span.0
    }
}

impl Lexeme for PrintSizeDirective {
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let mut inner = parsed_rule.into_inner();

        // First child is the relation name
        let name_pair = inner
            .next()
            .ok_or_else(|| grammar_bug("printsize directive missing relation name"))?;
        let span = span_of(&name_pair, file);
        let relation_name = name_pair.as_str().to_lowercase();

        Ok(Self {
            relation_name,
            span: Ignored(span),
        })
    }
}
