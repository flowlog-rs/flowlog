//! Parameters `.input` and `.output` share: the decoded `key = "value"` map
//! both directives carry, and the `delimiter` both of them resolve the same
//! way. Every other parameter's meaning is the directive's own business, so
//! neither borrows the other's resolution.

use std::collections::HashMap;

use flowlog_common::Span;

use crate::Node;
use crate::Rule;
use crate::decode_string;
use crate::error::ParseError;

/// TAB, matching Souffle, when a directive names no `delimiter`.
const DEFAULT_DELIMITER: u8 = b'\t';

/// Parse the common form of an `.input` / `.output` directive: the
/// relation-name token as written, that token's span, and an optional
/// `io_params` child. Canonicalizing the name is the caller's, because a
/// component body can only lowercase after resolving it against its scope.
pub(in crate::syntax::declaration) fn parse_io_directive(
    node: Node,
) -> Result<(String, HashMap<String, String>, Span), ParseError> {
    let mut children = node.children();
    let name_node = children.next_any("relation name")?;
    let span = name_node.span();
    let raw_name = name_node.text().to_string();
    let parameters = match children.take_if(Rule::io_params) {
        Some(params) => parse_io_params(params)?,
        None => HashMap::new(),
    };
    Ok((raw_name, parameters, span))
}

/// Parse an `io_params` node (`key = "value", ...`) into a name-to-value
/// map. Values are string literals, decoded via [`crate::decode_string`].
/// No key is validated here: an unknown name reaches the directive intact,
/// and a name repeated in the list keeps only its last value.
fn parse_io_params(node: Node) -> Result<HashMap<String, String>, ParseError> {
    debug_assert_eq!(node.rule(), Rule::io_params);
    let mut parameters = HashMap::new();
    for io_param in node.children() {
        let mut kv = io_param.children();
        let key = kv.next_any("parameter name")?.text().to_string();
        let value_node = kv.next_any("parameter value")?;
        let value = decode_string(value_node.text(), value_node.span())?;
        parameters.insert(key, value);
    }
    Ok(parameters)
}

/// The `delimiter` parameter as the one byte a reader splits cells on.
///
/// A value that is not exactly one byte names no byte to split on, and a
/// line terminator names one no cell can be split on: the text reader
/// consumes `\n` and `\r` to end a line before any cell is taken. One byte
/// of a UTF-8 string is necessarily ASCII, which is what lets the runtime
/// scan for the delimiter bytewise without landing inside a character.
pub(super) fn parse_delimiter(
    params: &HashMap<String, String>,
    span: Span,
) -> Result<u8, ParseError> {
    let Some(value) = params.get("delimiter") else {
        return Ok(DEFAULT_DELIMITER);
    };
    match value.as_bytes() {
        &[b] if !matches!(b, b'\n' | b'\r') => Ok(b),
        _ => Err(ParseError::InvalidDelimiter {
            span,
            value: value.clone(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::assert_err;

    /// Parameters holding one `delimiter` entry, already decoded as the
    /// grammar hands it over.
    fn delim_params(value: &str) -> HashMap<String, String> {
        HashMap::from([("delimiter".to_string(), value.to_string())])
    }

    #[test]
    fn delimiter_defaults_to_tab_when_unset() {
        assert_eq!(
            parse_delimiter(&HashMap::new(), Span::DUMMY).unwrap(),
            b'\t'
        );
    }

    #[rstest]
    #[case(",", b',')]
    #[case("|", b'|')]
    #[case("\t", b'\t')]
    #[case(" ", b' ')]
    fn delimiter_takes_one_ascii_character(#[case] value: &str, #[case] expected: u8) {
        assert_eq!(
            parse_delimiter(&delim_params(value), Span::DUMMY).unwrap(),
            expected
        );
    }

    /// Anything that is not one byte names no byte to split cells on: two
    /// characters, none at all, or a non-ASCII character (which is several
    /// bytes, so a bytewise scan for it could land mid-character). The two
    /// line terminators are one byte but still refused, because the reader
    /// consumes them to end the line before any cell is taken.
    #[rstest]
    #[case("::")]
    #[case("")]
    #[case("\u{e9}")]
    #[case("\n")]
    #[case("\r")]
    fn delimiter_rejects_what_no_reader_could_split_on(#[case] value: &str) {
        assert_err!(
            parse_delimiter(&delim_params(value), Span::DUMMY),
            ParseError::InvalidDelimiter { .. }
        );
    }
}
