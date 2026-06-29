//! Built-in value functions — intrinsic functors resolved by name from a
//! unified `call_expr` (see [`BuiltinOperator::from_keyword`]) and built via
//! [`BuiltinCall::new`]. Per-op signatures live on [`BuiltinOperator`];
//! lowerings live in codegen.

use std::fmt;

use educe::Educe;

use super::Arithmetic;
use crate::error::ParseError;
use crate::primitive::DataType;
use flowlog_common::Span;

/// Built-in operator kinds; one per reserved keyword in `grammar.pest`.
///
/// Public because it appears in `TypeCheckError::BuiltinArgType`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BuiltinOperator {
    /// `strlen(s) -> int32` — character count.
    Strlen,
    /// `substr(s, start, len) -> string` — character-indexed slice.
    Substr,
    /// `ord(s) -> int32` — Soufflé symbol ordinal; requires `--str-intern`.
    Ord,
    /// `to_string(n) -> string` — renders any numeric/bool scalar.
    ToString,
    /// `to_number(s) -> int32` — 0 on parse failure.
    ToNumber,
    /// `cat(a, b) -> string` — binary string concat function.
    Cat,
}

impl BuiltinOperator {
    /// Surface keyword used in `.dl` source — matches the grammar token.
    pub fn keyword(self) -> &'static str {
        match self {
            BuiltinOperator::Strlen => "strlen",
            BuiltinOperator::Substr => "substr",
            BuiltinOperator::Ord => "ord",
            BuiltinOperator::ToString => "to_string",
            BuiltinOperator::ToNumber => "to_number",
            BuiltinOperator::Cat => "cat",
        }
    }

    /// Every built-in variant, scanned by [`Self::from_keyword`].
    /// (`match`/`contains` are not here — they are boolean string
    /// *constraints*, modeled as
    /// [`ComparisonOperator`](super::ComparisonOperator) variants, not functors.)
    const VALUE_BUILTINS: [BuiltinOperator; 6] = [
        BuiltinOperator::Strlen,
        BuiltinOperator::Substr,
        BuiltinOperator::Ord,
        BuiltinOperator::ToString,
        BuiltinOperator::ToNumber,
        BuiltinOperator::Cat,
    ];

    /// Resolve a built-in by its surface keyword. This is the reserved-name
    /// lookup that distinguishes a built-in call from a UDF call when parsing
    /// a unified `call_expr` (any `name(args)`). A name that is not a built-in
    /// resolves to a UDF, which the typechecker validates against `.extern fn`.
    #[must_use]
    pub fn from_keyword(name: &str) -> Option<Self> {
        Self::VALUE_BUILTINS
            .into_iter()
            .find(|op| op.keyword() == name)
    }

    /// Declared arity (number of arguments). Derived from
    /// [`Self::param_allowed_types`] so both stay in sync.
    pub fn arity(self) -> usize {
        self.param_allowed_types().len()
    }

    /// The set of types each parameter accepts, in argument
    /// order. An arg is valid if its type is in the set. A multi-element set is
    /// a polymorphic parameter (e.g. `to_string` over any numeric/bool scalar).
    pub fn param_allowed_types(self) -> &'static [&'static [DataType]] {
        // Scalars `to_string` renders. `string` is excluded: it would be a
        // no-op and, under `--str-intern`, the operand is an interned key the
        // codegen would have to `resolve` (it reads the raw token). Tuples are
        // excluded (no `Display`).
        const TO_STRING_INPUTS: &[DataType] = &[
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Bool,
        ];
        match self {
            BuiltinOperator::Strlen => &[&[DataType::String]],
            BuiltinOperator::Substr => {
                &[&[DataType::String], &[DataType::Int32], &[DataType::Int32]]
            }
            BuiltinOperator::Ord => &[&[DataType::String]],
            BuiltinOperator::ToString => &[TO_STRING_INPUTS],
            BuiltinOperator::ToNumber => &[&[DataType::String]],
            BuiltinOperator::Cat => &[&[DataType::String], &[DataType::String]],
        }
    }

    /// Return type produced by this built-in.
    pub fn ret_type(self) -> DataType {
        match self {
            BuiltinOperator::Strlen | BuiltinOperator::Ord | BuiltinOperator::ToNumber => {
                DataType::Int32
            }
            BuiltinOperator::Substr | BuiltinOperator::ToString | BuiltinOperator::Cat => {
                DataType::String
            }
        }
    }
}

impl fmt::Display for BuiltinOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.keyword())
    }
}

/// A built-in function call site: operator + argument expressions.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq, Hash)]
pub struct BuiltinCall {
    op: BuiltinOperator,
    args: Vec<Arithmetic>,
    #[educe(PartialEq(ignore), Hash(ignore))]
    span: Span,
}

impl BuiltinCall {
    /// Build a value built-in call from an already-resolved operator and
    /// argument list (the unified `call_expr` parser resolves the name via
    /// [`BuiltinOperator::from_keyword`] first). Enforces per-op arity.
    pub fn new(op: BuiltinOperator, args: Vec<Arithmetic>, span: Span) -> Result<Self, ParseError> {
        if args.len() != op.arity() {
            return Err(ParseError::BuiltinArity {
                span,
                op: op.keyword(),
                expected: op.arity(),
                found: args.len(),
            });
        }
        Ok(Self { op, args, span })
    }

    #[must_use]
    #[inline]
    pub fn op(&self) -> BuiltinOperator {
        self.op
    }

    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    #[inline]
    pub fn args_mut(&mut self) -> &mut [Arithmetic] {
        &mut self.args
    }

    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        self.args.iter().flat_map(|a| a.vars()).collect()
    }
}

impl fmt::Display for BuiltinCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args = self
            .args
            .iter()
            .map(|a| a.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.op, args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Lexeme, Rule};
    use flowlog_common::FileId;
    use pest::Parser;

    /// Parse a unified `call_expr` and resolve it as a built-in (the same
    /// path the factor parser takes). Panics if the name is not a built-in.
    fn try_parse(input: &str) -> Result<BuiltinCall, ParseError> {
        let mut pairs = FlowLogParser::parse(Rule::call_expr, input).unwrap();
        let mut children = pairs.next().unwrap().into_inner();
        let name = children.next().unwrap().as_str().to_string();
        let args = children
            .filter(|p| p.as_rule() == Rule::arithmetic_expr)
            .map(|p| Arithmetic::from_parsed_rule(p, FileId::new(0)))
            .collect::<Result<Vec<_>, _>>()?;
        let op = BuiltinOperator::from_keyword(&name).expect("not a built-in keyword");
        BuiltinCall::new(op, args, Span::DUMMY)
    }

    #[test]
    fn parses_a_3_arg_call() {
        let bc = try_parse("substr(s, 0, 3)").unwrap();
        assert_eq!(bc.op(), BuiltinOperator::Substr);
        assert_eq!(bc.args().len(), 3);
    }

    #[test]
    fn keyword_not_a_prefix() {
        // `strlen_foo` is a distinct identifier — it resolves to a UDF, not
        // the `strlen` built-in. Name resolution (not the grammar) enforces
        // the keyword boundary now that calls share one `call_expr` rule.
        assert!(BuiltinOperator::from_keyword("strlen_foo").is_none());
        assert!(BuiltinOperator::from_keyword("strlen").is_some());
    }

    #[test]
    fn wrong_arity_is_user_facing_error() {
        match try_parse("strlen(a, b)").expect_err("expected arity error") {
            ParseError::BuiltinArity {
                op,
                expected,
                found,
                ..
            } => {
                assert_eq!(op, "strlen");
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            e => panic!("expected BuiltinArity, got {e:?}"),
        }
    }

    /// `cat(a, b)` parses as a binary string-concat built-in. Pinning
    /// the op kind + arity protects against accidental rewrites that
    /// would silently route through `FnCall` (which never resolves
    /// the typechecker's string concat semantics).
    #[test]
    fn cat_parses_as_binary_string_builtin() {
        let bc = try_parse("cat(a, b)").unwrap();
        assert_eq!(bc.op(), BuiltinOperator::Cat);
        assert_eq!(bc.args().len(), 2);
        assert_eq!(
            BuiltinOperator::Cat.param_allowed_types(),
            &[&[DataType::String], &[DataType::String]]
        );
        assert_eq!(BuiltinOperator::Cat.ret_type(), DataType::String);
    }

    /// `cat(...)` with the wrong number of args is rejected up-front
    /// with [`ParseError::BuiltinArity`]. Souffle's `cat` is strictly
    /// binary; chains must nest explicitly.
    #[test]
    fn cat_wrong_arity_rejected() {
        match try_parse("cat(a, b, c)").expect_err("expected arity error") {
            ParseError::BuiltinArity {
                op,
                expected,
                found,
                ..
            } => {
                assert_eq!(op, "cat");
                assert_eq!(expected, 2);
                assert_eq!(found, 3);
            }
            e => panic!("expected BuiltinArity, got {e:?}"),
        }
    }
}
