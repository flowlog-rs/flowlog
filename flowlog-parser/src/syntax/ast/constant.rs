//! Literal constants.
//!
//! - [`Constant`]: one literal as written in the source, carrying its
//!   spelling and its type. Values are parsed from the spelling on
//!   demand.

use std::fmt;

use flowlog_error::Span;

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
/// content; every other type stores the literal as written. Downstream
/// of the typechecker, every constant is concrete and its spelling
/// parses as its type.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
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
    /// [`ParseError::LiteralOutOfRange`] at `span`. No-op on
    /// already-concrete constants (debug-asserts the type matches).
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
                if !spelling_fits(&self.text, &target) {
                    return Err(ParseError::LiteralOutOfRange {
                        span,
                        literal: self.text.clone(),
                        target,
                    });
                }
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

/// Returns `true` if `text` parses as a `target` value: integer widths
/// range-check, floats always parse (overflow becomes infinity), and
/// non-numeric targets never host a numeric spelling.
fn spelling_fits(text: &str, target: &DataType) -> bool {
    match target {
        DataType::Int8 => text.parse::<i8>().is_ok(),
        DataType::Int16 => text.parse::<i16>().is_ok(),
        DataType::Int32 => text.parse::<i32>().is_ok(),
        DataType::Int64 => text.parse::<i64>().is_ok(),
        DataType::UInt8 => text.parse::<u8>().is_ok(),
        DataType::UInt16 => text.parse::<u16>().is_ok(),
        DataType::UInt32 => text.parse::<u32>().is_ok(),
        DataType::UInt64 => text.parse::<u64>().is_ok(),
        DataType::Float32 => text.parse::<f32>().is_ok(),
        DataType::Float64 => text.parse::<f64>().is_ok(),
        DataType::IntLit
        | DataType::FloatLit
        | DataType::String
        | DataType::Bool
        | DataType::FixedTuple(_) => false,
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

impl Lexeme for Constant {
    /// Lowers a `constant` node into its pre-typecheck form: numbers keep
    /// their spelling under a polymorphic family type; strings decode
    /// their escapes.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let inner = node.children().next_any("constant value")?;
        Ok(match inner.rule() {
            Rule::float => Self::new(DataType::FloatLit, inner.text()),
            Rule::integer => Self::new(DataType::IntLit, inner.text()),
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
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::test_util::parse_node;

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
    /// it retypes the constant and leaves the spelling untouched.
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
    #[case("1.5", DataType::FloatLit)]
    fn numeric_literal_keeps_spelling_under_family_type(#[case] src: &str, #[case] ty: DataType) {
        let c: Constant = parse_node(Rule::constant, src);
        assert_eq!(c, Constant::new(ty, src));
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
