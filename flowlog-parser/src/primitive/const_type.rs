//! Constant value types for FlowLog Datalog programs.

use super::DataType;
use crate::error::{ParseError, grammar_bug};
use crate::{Lexeme, Rule};
use flowlog_common::FileId;
use ordered_float::OrderedFloat;
use pest::iterators::Pair;
use std::fmt;

/// A literal constant in a FlowLog Datalog program.
///
/// Constants live in two stages:
///
/// - **Pre-typecheck** (parser-emitted): [`ConstType::Int`] and
///   [`ConstType::Float`] are *polymorphic placeholders*. Their concrete
///   width isn't known until the typechecker unifies them against column
///   types from the `.decl` (or a sibling variable's type).
/// - **Post-typecheck**: constant type inference pins every polymorphic
///   variant to its concrete counterpart — [`ConstType::Int8`] through
///   [`ConstType::Int64`], [`ConstType::UInt8`] through [`ConstType::UInt64`],
///   [`ConstType::Float32`] / [`ConstType::Float64`] — via [`ConstType::pin`].
///
/// [`ConstType::Text`] and [`ConstType::Bool`] are already concrete; they
/// pass through unchanged.
///
/// **Invariant for downstream consumers.** Catalog, planner, and codegen
/// may assume all literals are concrete and call [`ConstType::data_type`]
/// unconditionally.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ConstType {
    /// Polymorphic integer literal (parser-emitted, stored as i64).
    /// Replaced by one of the width-specific variants during typechecking.
    Int(i64),

    /// Polymorphic floating-point literal (parser-emitted, stored as
    /// `OrderedFloat<f64>`). Replaced by `Float32` / `Float64` during
    /// typechecking.
    Float(OrderedFloat<f64>),

    /// 8-bit signed integer constant.
    Int8(i8),
    /// 16-bit signed integer constant.
    Int16(i16),
    /// 32-bit signed integer constant.
    Int32(i32),
    /// 64-bit signed integer constant.
    Int64(i64),
    /// 8-bit unsigned integer constant.
    UInt8(u8),
    /// 16-bit unsigned integer constant.
    UInt16(u16),
    /// 32-bit unsigned integer constant.
    UInt32(u32),
    /// 64-bit unsigned integer constant.
    UInt64(u64),
    /// 32-bit floating-point constant.
    Float32(OrderedFloat<f32>),
    /// 64-bit floating-point constant.
    Float64(OrderedFloat<f64>),

    /// UTF-8 string constant.
    Text(String),

    /// Boolean constant.
    Bool(bool),
}

impl ConstType {
    /// Resolved column type, or `None` for the polymorphic `Int` / `Float`
    /// variants — the typechecker must pin those before downstream
    /// consumers can rely on a concrete width.
    pub fn data_type(&self) -> Option<DataType> {
        Some(match self {
            Self::Int8(_) => DataType::Int8,
            Self::Int16(_) => DataType::Int16,
            Self::Int32(_) => DataType::Int32,
            Self::Int64(_) => DataType::Int64,
            Self::UInt8(_) => DataType::UInt8,
            Self::UInt16(_) => DataType::UInt16,
            Self::UInt32(_) => DataType::UInt32,
            Self::UInt64(_) => DataType::UInt64,
            Self::Float32(_) => DataType::Float32,
            Self::Float64(_) => DataType::Float64,
            Self::Text(_) => DataType::String,
            Self::Bool(_) => DataType::Bool,
            Self::Int(_) | Self::Float(_) => return None,
        })
    }

    /// `true` only for the parser-emitted `Int` / `Float` variants. Useful
    /// for debug-assertions that the typechecker's const-inference pass ran.
    pub fn is_polymorphic(&self) -> bool {
        matches!(self, Self::Int(_) | Self::Float(_))
    }

    /// Pin a polymorphic literal to the concrete variant matching `target`.
    /// No-op on already-concrete variants (asserts the type matches).
    ///
    /// Integer casts use `as` (truncating); float `f64` → `f32` uses `as f32`
    /// (lossy). Range-checking is deferred to rustc on the generated code,
    /// matching the typechecker's "we do not range-check integer
    /// literals" contract.
    ///
    /// Panics on family mismatch (e.g. `Int` → `String`) — the typechecker
    /// must accept the literal's family against `target` first.
    pub fn pin(&mut self, target: DataType) {
        match self {
            Self::Int(v) => {
                let v = *v;
                *self = match target {
                    DataType::Int8 => Self::Int8(v as i8),
                    DataType::Int16 => Self::Int16(v as i16),
                    DataType::Int32 => Self::Int32(v as i32),
                    DataType::Int64 => Self::Int64(v),
                    DataType::UInt8 => Self::UInt8(v as u8),
                    DataType::UInt16 => Self::UInt16(v as u16),
                    DataType::UInt32 => Self::UInt32(v as u32),
                    DataType::UInt64 => Self::UInt64(v as u64),
                    _ => panic!("ConstType error: pin({target}) on Int({v}): family mismatch"),
                };
            }
            Self::Float(v) => {
                let f = v.into_inner();
                *self = match target {
                    DataType::Float32 => Self::Float32(OrderedFloat(f as f32)),
                    DataType::Float64 => Self::Float64(OrderedFloat(f)),
                    _ => panic!("ConstType error: pin({target}) on Float({f}): family mismatch"),
                };
            }
            _ => {
                debug_assert_eq!(
                    self.data_type(),
                    Some(target),
                    "ConstType::pin() on already-concrete literal with mismatched target",
                );
            }
        }
    }
}

impl fmt::Display for ConstType {
    /// Prints constants in Datalog syntax: numbers as-is (all widths),
    /// strings with quotes, bools as `True` / `False`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Int(v) => write!(f, "{v}"),
            Self::Float(v) => write!(f, "{v}"),
            Self::Int8(v) => write!(f, "{v}"),
            Self::Int16(v) => write!(f, "{v}"),
            Self::Int32(v) => write!(f, "{v}"),
            Self::Int64(v) => write!(f, "{v}"),
            Self::UInt8(v) => write!(f, "{v}"),
            Self::UInt16(v) => write!(f, "{v}"),
            Self::UInt32(v) => write!(f, "{v}"),
            Self::UInt64(v) => write!(f, "{v}"),
            Self::Float32(v) => write!(f, "{v}"),
            Self::Float64(v) => write!(f, "{v}"),
            Self::Text(s) => write!(f, "\"{s}\""),
            Self::Bool(b) => write!(f, "{}", if *b { "True" } else { "False" }),
        }
    }
}

impl Lexeme for ConstType {
    /// Parses a constant from the grammar.
    ///
    /// Integer literals are parsed directly as `i64`.
    fn from_parsed_rule(parsed_rule: Pair<Rule>, _file: FileId) -> Result<Self, ParseError> {
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("constant rule had no inner value"))?;
        Ok(match inner.as_rule() {
            Rule::float => {
                let s = inner.as_str();
                let v = s
                    .parse::<f64>()
                    .map_err(|e| grammar_bug(format!("failed to parse float literal: {e}")))?;
                Self::Float(OrderedFloat(v))
            }
            Rule::integer => {
                let s = inner.as_str();
                let v = s
                    .parse::<i64>()
                    .map_err(|e| grammar_bug(format!("failed to parse integer literal: {e}")))?;
                Self::Int(v)
            }
            Rule::string => Self::Text(inner.as_str().trim_matches('"').to_string()),
            Rule::boolean => match inner.as_str() {
                "True" => Self::Bool(true),
                "False" => Self::Bool(false),
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
    use super::*;

    /// The variant → `DataType` bridge is a hand-written 1:1 table — a
    /// typo here (e.g. `Int16 → DataType::Int8`) would silently emit the
    /// wrong tuple type in codegen without failing the e2e suite unless
    /// the exact width was covered.
    #[test]
    fn data_type_matches_variant() {
        let cases = [
            (ConstType::Int8(0), DataType::Int8),
            (ConstType::Int16(0), DataType::Int16),
            (ConstType::Int32(0), DataType::Int32),
            (ConstType::Int64(0), DataType::Int64),
            (ConstType::UInt8(0), DataType::UInt8),
            (ConstType::UInt16(0), DataType::UInt16),
            (ConstType::UInt32(0), DataType::UInt32),
            (ConstType::UInt64(0), DataType::UInt64),
            (ConstType::Float32(OrderedFloat(0.0)), DataType::Float32),
            (ConstType::Float64(OrderedFloat(0.0)), DataType::Float64),
            (ConstType::Text("x".into()), DataType::String),
            (ConstType::Bool(true), DataType::Bool),
        ];
        for (c, expected) in cases {
            assert_eq!(c.data_type(), Some(expected));
        }
    }

    /// Downstream consumers distinguish "concrete, known width" from
    /// "polymorphic placeholder" by this `Some`/`None` split; a silent
    /// default width would leak through typechecker bugs.
    #[test]
    fn data_type_is_none_on_polymorphic() {
        assert_eq!(ConstType::Int(42).data_type(), None);
        assert_eq!(ConstType::Float(OrderedFloat(1.5)).data_type(), None);
    }

    /// `pin` is the sole path from polymorphic-literal to concrete width.
    /// Out-of-range values use `as`-casts (truncating), matching the
    /// typechecker's contract that range checking is delegated to rustc.
    #[test]
    fn pin_truncates_out_of_range_integer() {
        let mut c = ConstType::Int(256);
        c.pin(DataType::Int8);
        assert_eq!(c, ConstType::Int8(0));

        let mut c = ConstType::Int(-1);
        c.pin(DataType::UInt8);
        assert_eq!(c, ConstType::UInt8(255));

        let mut c = ConstType::Int(1 << 33);
        c.pin(DataType::Int32);
        assert_eq!(c, ConstType::Int32(0));
    }

    /// Pinning an `Int` to a string-family target must panic — the typechecker
    /// is required to match literal families before calling `pin`. A silent
    /// acceptance here would let family-mismatched code reach codegen.
    #[test]
    #[should_panic(expected = "family mismatch")]
    fn pin_int_to_string_panics() {
        let mut c = ConstType::Int(1);
        c.pin(DataType::String);
    }

    /// `pin` on an already-concrete literal is a no-op when the target
    /// matches; downstream passes may re-run `pin` defensively.
    #[test]
    fn pin_already_concrete_is_noop() {
        let mut c = ConstType::Int32(5);
        c.pin(DataType::Int32);
        assert_eq!(c, ConstType::Int32(5));

        let mut c = ConstType::Text("hi".into());
        c.pin(DataType::String);
        assert_eq!(c, ConstType::Text("hi".into()));
    }
}
