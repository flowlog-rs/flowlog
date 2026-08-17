//! What `.input` and `.output` share: reading a `key = "value"` list, and
//! the `delimiter` both of them split cells on. What every other parameter
//! *means* is the directive's own business, so neither borrows the other's
//! resolution.

use std::collections::HashMap;

use flowlog_error::Span;

use crate::Node;
use crate::Rule;
use crate::decode_string;
use crate::error::ParseError;

/// Parse an `io_params` node (`key = "value", ...`) into a name-to-value
/// map. Values are string literals, decoded via [`crate::decode_string`].
pub(crate) fn parse_io_params(node: Node) -> Result<HashMap<String, String>, ParseError> {
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

/// The `delimiter` parameter as the one byte a reader splits on, TAB by
/// default, matching Souffle.
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
        return Ok(b'\t');
    };
    match value.as_bytes() {
        &[b] if !matches!(b, b'\n' | b'\r') => Ok(b),
        _ => Err(ParseError::InvalidDelimiter {
            span,
            value: value.clone(),
        }),
    }
}
