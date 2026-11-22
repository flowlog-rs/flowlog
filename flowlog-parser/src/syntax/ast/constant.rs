//! Literal constants.
//!
//! - [`Constant`]: one literal as written in the source, carrying its
//!   spelling and its type. Values are parsed from the spelling on
//!   demand.

use std::fmt;
use std::str::FromStr;

use flowlog_common::Span;

use crate::Lexeme;
use crate::Node;
use crate::Rule;
use crate::decode_string;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::types::DataType;

/// A literal constant: its source spelling and its type.
///
/// Constants live in two stages:
///
/// - Pre-typecheck (parser-emitted): a numeric literal's type is its
///   polymorphic family ([`DataType::IntLit`] / [`DataType::FloatLit`]);
///   the concrete width is unknown until the typechecker pins it.
/// - Post-typecheck: `pin` has replaced the family with the concrete
///   width, validating that the spelling fits it. `String` and `Bool`
///   constants are born concrete and pass through unchanged.
///
/// A `String` constant stores its decoded (unquoted, unescaped)
/// content and an integer its decimal spelling (a `0x` literal is
/// converted); every other type stores the literal as written.
/// Downstream of the typechecker, every constant is concrete and a
/// number carries the canonical spelling of its value, so two constants
/// of one value are equal.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Constant {
    text: String,
    ty: DataType,
}

impl Constant {
    /// Creates a constant from a type and a spelling. The caller
    /// guarantees the spelling is a valid rendering of a `ty` value;
    /// nothing re-validates it.
    #[must_use]
    pub fn new(ty: DataType, text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            ty,
        }
    }

    /// The stored spelling.
    #[must_use]
    #[inline]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The constant's type; a polymorphic literal family until the
    /// typechecker pins it.
    #[must_use]
    #[inline]
    pub fn ty(&self) -> &DataType {
        &self.ty
    }

    /// Resolved column type, or `None` while the constant is still a
    /// polymorphic literal the typechecker must pin first.
    #[must_use]
    pub fn data_type(&self) -> Option<DataType> {
        if self.ty.is_literal() {
            None
        } else {
            Some(self.ty.clone())
        }
    }

    /// Returns `true` if the constant's type is still a polymorphic
    /// literal family.
    #[must_use]
    pub fn is_polymorphic(&self) -> bool {
        self.ty.is_literal()
    }

    /// Pins a polymorphic literal to the concrete `target` width,
    /// validating that the spelling fits it: `300` refuses `int8` with
    /// [`ParseError::LiteralOutOfRange`] at `span`. The spelling becomes
    /// the canonical one for the value, so `01` and `1` pin to equal
    /// constants. No-op on already-concrete constants (debug-asserts the
    /// type matches).
    ///
    /// Floats never range-error: any float spelling parses (overflowing
    /// to infinity), matching the generated code's semantics. A family
    /// mismatch (pinning an `IntLit` to `String`) is an internal error:
    /// the typechecker must accept the literal's family against `target`
    /// before calling.
    pub(crate) fn pin(&mut self, target: DataType, span: Span) -> Result<(), ParseError> {
        match self.ty {
            DataType::IntLit | DataType::FloatLit => {
                if !self.ty.fits(&target) {
                    return Err(grammar_bug(format!(
                        "pin({target}) on `{}`: family mismatch",
                        self.text
                    )));
                }
                let Some(canonical) = canonical_spelling(&self.text, &target) else {
                    return Err(ParseError::LiteralOutOfRange {
                        span,
                        literal: self.text.clone(),
                        target,
                    });
                };
                self.text = canonical;
                self.ty = target;
            }
            _ => {
                debug_assert_eq!(
                    self.ty, target,
                    "Constant::pin() on already-concrete literal with mismatched target",
                );
            }
        }
        Ok(())
    }
}

/// Returns the canonical spelling of `text` parsed as a `target` value
/// (`01` and `+1` become `1`, `1e0` becomes `1`), or `None` when it does
/// not parse: integer widths range-check, floats always parse (overflow
/// becomes infinity), and non-numeric targets never host a numeric
/// spelling.
fn canonical_spelling(text: &str, target: &DataType) -> Option<String> {
    fn parsed<T: FromStr + ToString>(text: &str) -> Option<String> {
        text.parse::<T>().ok().map(|value| value.to_string())
    }
    match target {
        DataType::Int8 => parsed::<i8>(text),
        DataType::Int16 => parsed::<i16>(text),
        DataType::Int32 => parsed::<i32>(text),
        DataType::Int64 => parsed::<i64>(text),
        DataType::UInt8 => parsed::<u8>(text),
        DataType::UInt16 => parsed::<u16>(text),
        DataType::UInt32 => parsed::<u32>(text),
        DataType::UInt64 => parsed::<u64>(text),
        DataType::Float32 => parsed::<f32>(text),
        DataType::Float64 => parsed::<f64>(text),
        DataType::IntLit
        | DataType::FloatLit
        | DataType::String
        | DataType::Bool
        | DataType::FixedTuple(_) => None,
    }
}

impl fmt::Display for Constant {
    /// Prints the constant in Datalog syntax: the spelling as written,
    /// with strings re-quoted (escapes are not re-encoded).
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.ty {
            DataType::String => write!(f, "\"{}\"", self.text),
            _ => write!(f, "{}", self.text),
        }
    }
}

/// Rewrites a `0x` integer spelling (optionally signed) to decimal;
/// a decimal spelling passes through unchanged. Every consumer of the
/// spelling parses it with Rust's decimal `FromStr` (`pin`, folding,
/// codegen), so hexadecimal is normalized once, here. `Err` with
/// [`ParseError::Syntax`] when the magnitude exceeds 64 bits, the widest
/// integer width: `pin` then range-checks the sign against the target
/// exactly as it does a decimal spelling.
fn decimal_spelling(text: &str, span: Span) -> Result<String, ParseError> {
    let (sign, digits) = match text.as_bytes() {
        [b'-', ..] => ("-", &text[1..]),
        [b'+', ..] => ("", &text[1..]),
        _ => ("", text),
    };
    let Some(hex) = digits.strip_prefix("0x") else {
        return Ok(text.to_string());
    };
    let value = u64::from_str_radix(hex, 16).map_err(|_| ParseError::Syntax {
        span,
        message: format!("hexadecimal literal `{text}` does not fit any integer type"),
    })?;
    Ok(format!("{sign}{value}"))
}

impl Lexeme for Constant {
    /// Lowers a `constant` node into its pre-typecheck form: numbers keep
    /// their spelling under a polymorphic family type, hexadecimal
    /// integers becoming decimal; strings decode their escapes.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("constant value")?;
        Ok(match inner.rule() {
            Rule::float => Self::new(DataType::FloatLit, inner.text()),
            Rule::integer => Self::new(
                DataType::IntLit,
                decimal_spelling(inner.text(), inner.span())?,
            ),
            Rule::string => {
                let text = decode_string(inner.text(), inner.span())?;
                Self::new(DataType::String, text)
            }
            Rule::boolean => match inner.text() {
                s @ ("True" | "False") => Self::new(DataType::Bool, s),
                other => {
                    return Err(grammar_bug(format!("invalid boolean constant: {other}")));
                }
            },
            other => {
                return Err(grammar_bug(format!(
                    "unexpected constant rule variant: {other:?}"
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::test_harness::parse_node;
    use crate::test_harness::parse_pair;

    /// The `Some`/`None` split on `data_type` is how downstream consumers
    /// distinguish "concrete, known width" from "polymorphic placeholder".
    #[rstest]
    #[case(Constant::new(DataType::Int32, "42"), Some(DataType::Int32))]
    #[case(Constant::new(DataType::String, "x"), Some(DataType::String))]
    #[case(Constant::new(DataType::IntLit, "42"), None)]
    #[case(Constant::new(DataType::FloatLit, "1.5"), None)]
    fn data_type_none_iff_polymorphic(#[case] c: Constant, #[case] expected: Option<DataType>) {
        assert_eq!(c.data_type(), expected);
        assert_eq!(c.is_polymorphic(), c.data_type().is_none());
    }

    /// `pin` is the sole path from polymorphic literal to concrete width:
    /// it retypes the constant and keeps a canonical spelling as is.
    #[rstest]
    #[case(DataType::Int8, "7")]
    #[case(DataType::Int16, "7")]
    #[case(DataType::Int32, "7")]
    #[case(DataType::Int64, "7")]
    #[case(DataType::UInt8, "7")]
    #[case(DataType::UInt16, "7")]
    #[case(DataType::UInt32, "7")]
    #[case(DataType::UInt64, "7")]
    fn pin_int_to_each_width(#[case] target: DataType, #[case] text: &str) {
        let mut c = Constant::new(DataType::IntLit, text);
        c.pin(target.clone(), Span::DUMMY).unwrap();
        assert_eq!(c.ty(), &target);
        assert_eq!(c.text(), text);
    }

    #[rstest]
    #[case(DataType::Float32)]
    #[case(DataType::Float64)]
    fn pin_float_to_each_width(#[case] target: DataType) {
        let mut c = Constant::new(DataType::FloatLit, "0.1");
        c.pin(target.clone(), Span::DUMMY).unwrap();
        assert_eq!(c.ty(), &target);
        assert_eq!(c.text(), "0.1");
    }

    /// A spelling that does not fit the pinned width is a check-time
    /// error, not a silent wrap: `300` refuses `int8`.
    #[rstest]
    #[case("300", DataType::Int8)]
    #[case("-1", DataType::UInt8)]
    #[case("8589934592", DataType::Int32)] // 2^33
    #[case("99999999999999999999", DataType::Int64)] // > i64::MAX
    fn pin_out_of_range_is_rejected(#[case] text: &str, #[case] target: DataType) {
        let mut c = Constant::new(DataType::IntLit, text);
        assert_err!(
            c.pin(target, Span::DUMMY),
            ParseError::LiteralOutOfRange { .. }
        );
    }

    /// Two spellings of one value pin to equal constants.
    // Cases: family, spelling, target, canonical spelling.
    #[rstest]
    #[case::leading_zero(DataType::IntLit, "01", DataType::Int32, "1")]
    #[case::plus_sign(DataType::IntLit, "+7", DataType::UInt8, "7")]
    #[case::exponent(DataType::FloatLit, "1e0", DataType::Float64, "1")]
    #[case::trailing_zero(DataType::FloatLit, "1.50", DataType::Float32, "1.5")]
    fn pin_rewrites_the_spelling_to_its_canonical_form(
        #[case] family: DataType,
        #[case] text: &str,
        #[case] target: DataType,
        #[case] canonical: &str,
    ) {
        let mut c = Constant::new(family, text);
        c.pin(target.clone(), Span::DUMMY).unwrap();
        assert_eq!(c, Constant::new(target, canonical));
    }

    /// Floats never range-error: an overflowing spelling parses to
    /// infinity, matching what the generated code computes.
    #[test]
    fn pin_float_overflow_is_accepted() {
        let mut c = Constant::new(DataType::FloatLit, "1e999");
        c.pin(DataType::Float32, Span::DUMMY).unwrap();
        assert_eq!(c.ty(), &DataType::Float32);
    }

    /// A family mismatch is an internal error: the typechecker is
    /// required to match literal families before calling `pin`.
    #[rstest]
    #[case(Constant::new(DataType::IntLit, "1"), DataType::String)]
    #[case(Constant::new(DataType::FloatLit, "1.5"), DataType::Int32)]
    fn pin_family_mismatch_is_an_internal_error(#[case] mut c: Constant, #[case] target: DataType) {
        assert_err!(c.pin(target, Span::DUMMY), ParseError::Internal(_));
    }

    /// `pin` on an already-concrete literal is a no-op when the target
    /// matches; downstream passes may re-run `pin` defensively.
    #[rstest]
    #[case(Constant::new(DataType::Int32, "5"), DataType::Int32)]
    #[case(Constant::new(DataType::String, "hi"), DataType::String)]
    fn pin_already_concrete_is_noop(#[case] mut c: Constant, #[case] target: DataType) {
        let before = c.clone();
        c.pin(target, Span::DUMMY).unwrap();
        assert_eq!(c, before);
    }

    #[rstest]
    #[case(Constant::new(DataType::IntLit, "3"), "3")]
    #[case(Constant::new(DataType::String, "hi"), "\"hi\"")]
    #[case(Constant::new(DataType::Bool, "True"), "True")]
    #[case(Constant::new(DataType::Bool, "False"), "False")]
    // Escapes are not re-encoded: a quote in the decoded content prints raw.
    #[case(Constant::new(DataType::String, "a\"b"), "\"a\"b\"")]
    fn display_uses_datalog_syntax(#[case] c: Constant, #[case] expected: &str) {
        assert_eq!(c.to_string(), expected);
    }

    /// Lowering a `constant` node decodes string escapes. The decode
    /// alphabet is unit-tested on `unescape`; these cases pin the escapes
    /// that interact with the token boundary, which no lower layer can
    /// observe.
    #[rstest]
    #[case(r#""a\"b""#, "a\"b")] // escaped quote must not end the token
    #[case(r#""a\\b""#, "a\\b")] // escaped backslash mid-token
    #[case(r#""x\\""#, "x\\")] // escaped backslash just before the closing quote
    fn string_literal_decodes_escapes(#[case] src: &str, #[case] expected: &str) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(DataType::String, expected), "src={src}");
    }

    /// Numeric literals lower to their polymorphic family with the
    /// spelling preserved verbatim.
    #[rstest]
    #[case("42", DataType::IntLit)]
    #[case("-17", DataType::IntLit)]
    #[case("1.5", DataType::FloatLit)]
    fn numeric_literal_keeps_spelling_under_family_type(#[case] src: &str, #[case] ty: DataType) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(ty, src));
    }

    /// A hexadecimal integer lowers to its decimal spelling, so every
    /// later parse of the text sees one notation. The sign survives and
    /// a 64-bit magnitude converts exactly.
    #[rstest]
    #[case("0xFF", "255")]
    #[case("0xff", "255")]
    #[case("0x0", "0")]
    #[case("-0x10", "-16")]
    #[case("+0x10", "16")]
    #[case("0xFFFFFFFFFFFFFFFF", "18446744073709551615")]
    #[case("-0x8000000000000000", "-9223372036854775808")]
    fn hex_integer_literal_lowers_to_decimal(#[case] src: &str, #[case] decimal: &str) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(DataType::IntLit, decimal), "src={src}");
    }

    /// A magnitude beyond 64 bits fits no integer width, so lowering
    /// rejects it as syntax instead of leaving `pin` to fail later.
    #[rstest]
    #[case("0x10000000000000000")]
    #[case("-0x10000000000000000")]
    fn hex_integer_literal_beyond_64_bits_is_rejected(#[case] src: &str) {
        let node = Node::new(parse_pair(Rule::constant, src), FileId::new(0));
        assert_err!(Constant::from_parsed_rule(node), ParseError::Syntax { .. });
    }

    /// A 64-bit hex magnitude with a sign lowers fine and is then
    /// range-checked by `pin` like any decimal spelling: the widest
    /// signed width refuses it.
    #[test]
    fn negative_hex_beyond_int64_is_caught_by_pin() {
        let mut c: Constant = parse_node(Rule::constant, "-0xFFFFFFFFFFFFFFFF");
        assert_err!(
            c.pin(DataType::Int64, Span::DUMMY),
            ParseError::LiteralOutOfRange { .. }
        );
    }

    /// Raw strings reach the AST undecoded; the natural home for regex
    /// patterns.
    #[rstest]
    #[case(r#"r"a\.b""#, "a\\.b")]
    #[case(r##"r#"a"b"#"##, "a\"b")]
    fn raw_string_literal_skips_decoding(#[case] src: &str, #[case] expected: &str) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(DataType::String, expected), "src={src}");
    }

    /// Booleans are born concrete: no family stage, no pin needed.
    #[rstest]
    #[case("True")]
    #[case("False")]
    fn boolean_literal_lowers_concrete(#[case] src: &str) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(DataType::Bool, src));
        assert!(!c.is_polymorphic());
    }
}
