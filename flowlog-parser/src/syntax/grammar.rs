//! Pest parser binding and the token readers over its output.
//!
//! [`FlowLogParser`] and [`Rule`] are generated from `grammar.pest`. The
//! readers each pull one value off a raw token: a span, or a decoded
//! string literal.

use flowlog_error::FileId;
use flowlog_error::Span;
use litrs::StringLit;
use pest::iterators::Pair;
use pest_derive::Parser;

use crate::error::ParseError;

/// Pest parser over individual grammar rules; most consumers want
/// [`parse`](crate::parse). The generated [`Rule`] enum mirrors `grammar.pest`
/// and is not a stability surface.
#[derive(Parser)]
#[grammar = "grammar.pest"]
pub(crate) struct FlowLogParser;

/// Build a [`Span`] from a Pest `Span`, anchored to the given [`FileId`].
pub(crate) fn span_of(pair: &Pair<Rule>, file: FileId) -> Span {
    let s = pair.as_span();
    Span::new(file, s.start() as u32, s.end() as u32)
}

/// Decodes a `string` token (quotes and any `r`/`#` framing included)
/// into its value. FlowLog strings follow Rust string-literal syntax:
/// quoted strings with Rust's escape alphabet (including `\u{...}`),
/// and raw strings (`r"..."`, `r#"..."#`) with no escape processing;
/// `litrs` does the decoding. `Err` when the token is not a valid Rust
/// string literal, e.g. an unknown escape: regex patterns belong in raw
/// strings (`r"a\.b"`), not quoted ones.
pub(crate) fn decode_string(lexeme: &str, span: Span) -> Result<String, ParseError> {
    match StringLit::parse(lexeme) {
        Ok(lit) => Ok(lit.into_value().into_owned()),
        Err(e) => Err(ParseError::InvalidStringLiteral {
            span,
            reason: e.to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use pest::Parser as _;
    use rstest::rstest;

    use super::*;
    use crate::assert_err;

    /// Every accepted form: Rust's escape alphabet decodes, raw strings
    /// pass through verbatim.
    #[rstest]
    #[case(r#""plain""#, "plain")]
    #[case(r#""""#, "")]
    #[case(r#""a\tb""#, "a\tb")]
    #[case(r#""a\nb""#, "a\nb")]
    #[case(r#""a\rb""#, "a\rb")]
    #[case(r#""a\0b""#, "a\0b")]
    #[case(r#""a\\b""#, "a\\b")]
    #[case(r#""a\"b""#, "a\"b")]
    #[case(r#""a\u{e9}b""#, "a\u{e9}b")]
    #[case(r#""a\x41b""#, "aAb")]
    #[case(r#"r"a\.b""#, "a\\.b")]
    #[case(r##"r#"a"b"#"##, "a\"b")]
    fn decode_string_accepts_rust_literals(#[case] lexeme: &str, #[case] expected: &str) {
        assert_eq!(
            decode_string(lexeme, Span::DUMMY).unwrap(),
            expected,
            "{lexeme}"
        );
    }

    /// Every refusal class: unknown escapes (which Souffle 2.5 refuses
    /// too, so a raw string is the portable spelling), malformed unicode
    /// escapes, and a trailing lone backslash.
    #[rstest]
    #[case(r#""a\.b""#)]
    #[case(r#""\d+""#)]
    #[case(r#""a\qb""#)]
    #[case(r#""\u{zz}""#)]
    #[case(r#""\u{110000}""#)]
    #[case(r#""a\""#)]
    fn decode_string_rejects_invalid_escapes(#[case] lexeme: &str) {
        assert_err!(
            decode_string(lexeme, Span::DUMMY),
            ParseError::InvalidStringLiteral { .. }
        );
    }

    /// Infix `cat` was removed: `x cat y` must not parse. A re-introduction
    /// would silently change the grammar; this pins its absence.
    #[test]
    fn infix_cat_no_longer_parses() {
        assert!(
            FlowLogParser::parse(Rule::main_grammar, "C(x cat y) :- A(x), B(y).\n").is_err(),
            "infix `cat` should be a grammar error"
        );
    }

    /// `.override` is a comp-only directive: at the top level the grammar
    /// rejects it (it appears only inside a `.comp` body).
    #[test]
    fn override_outside_comp_is_rejected() {
        assert!(
            FlowLogParser::parse(Rule::main_grammar, ".decl Foo(x: number)\n.override Foo\n")
                .is_err(),
            "top-level `.override` should be a grammar error"
        );
    }
}
