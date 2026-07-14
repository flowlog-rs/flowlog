//! Built-in value functions for FlowLog Datalog programs.
//!
//! - [`BuiltinOperator`]: the intrinsic operators (`strlen`, `substr`,
//!   `ord`, `to_string`, `to_number`, `cat`).
//! - [`BuiltinCall`]: an operator applied to argument expressions.

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Arithmetic;
use crate::error::ParseError;
use crate::types::DataType;

/// Built-in operator kinds; one per reserved keyword in `grammar.pest`.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub enum BuiltinOperator {
    /// `strlen(s) -> int32`: character count.
    Strlen,
    /// `substr(s, start, len) -> string`: character-indexed slice.
    Substr,
    /// `ord(s) -> int32`: Souffle symbol ordinal; requires `--str-intern`.
    Ord,
    /// `to_string(n) -> string`: renders any numeric or bool scalar.
    ToString,
    /// `to_number(s) -> int32`: 0 on parse failure.
    ToNumber,
    /// `cat(a, b) -> string`: binary string concatenation.
    Cat,
}

impl BuiltinOperator {
    /// Surface keyword used in `.dl` source; matches the grammar token.
    #[must_use]
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

    /// Resolve a built-in by its surface keyword. This is the reserved-name
    /// lookup that distinguishes a built-in call from a UDF call when parsing
    /// a unified `call_expr` (any `name(args)`). A name that is not a built-in
    /// resolves to a UDF, which the typechecker validates against `.extern fn`.
    #[must_use]
    pub(crate) fn from_keyword(name: &str) -> Option<Self> {
        const VALUE_BUILTINS: [BuiltinOperator; 6] = [
            BuiltinOperator::Strlen,
            BuiltinOperator::Substr,
            BuiltinOperator::Ord,
            BuiltinOperator::ToString,
            BuiltinOperator::ToNumber,
            BuiltinOperator::Cat,
        ];
        VALUE_BUILTINS.into_iter().find(|op| op.keyword() == name)
    }

    /// Declared arity (number of arguments). Derived from
    /// [`Self::param_allowed_types`] so both stay in sync.
    #[must_use]
    pub fn arity(self) -> usize {
        self.param_allowed_types().len()
    }

    /// The set of types each parameter accepts, in argument order. An
    /// argument is valid if its type is in the set; a multi-element set is
    /// a polymorphic parameter (e.g. `to_string` over any numeric or bool
    /// scalar).
    #[must_use]
    pub fn param_allowed_types(self) -> &'static [&'static [DataType]] {
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
    #[must_use]
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
    pub(crate) fn new(
        op: BuiltinOperator,
        args: Vec<Arithmetic>,
        span: Span,
    ) -> Result<Self, ParseError> {
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

    /// The operator being called.
    #[must_use]
    #[inline]
    pub fn op(&self) -> BuiltinOperator {
        self.op
    }

    /// Argument expressions, in source order.
    #[must_use]
    #[inline]
    pub fn args(&self) -> &[Arithmetic] {
        &self.args
    }

    #[inline]
    pub(crate) fn args_mut(&mut self) -> &mut [Arithmetic] {
        &mut self.args
    }

    /// Source location this call was parsed from.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Variables appearing in the argument expressions.
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
    use rstest::rstest;

    use super::*;

    /// `n` placeholder argument expressions (`a0`, `a1`, ...), for arity
    /// checks and Display.
    fn args(n: usize) -> Vec<Arithmetic> {
        (0..n).map(|i| Arithmetic::var(&format!("a{i}"))).collect()
    }

    /// Every keyword resolves back to its own operator: `from_keyword` and
    /// `keyword` are inverses over the built-in set.
    #[rstest]
    #[case(BuiltinOperator::Strlen)]
    #[case(BuiltinOperator::Substr)]
    #[case(BuiltinOperator::Ord)]
    #[case(BuiltinOperator::ToString)]
    #[case(BuiltinOperator::ToNumber)]
    #[case(BuiltinOperator::Cat)]
    fn keyword_round_trips_through_from_keyword(#[case] op: BuiltinOperator) {
        assert_eq!(BuiltinOperator::from_keyword(op.keyword()), Some(op));
    }

    /// A name that merely contains a keyword is not a built-in: calls share
    /// one `call_expr` rule, so name resolution (not the grammar) draws the
    /// boundary and an unmatched name falls through to a UDF.
    #[test]
    fn from_keyword_rejects_non_builtins() {
        assert!(BuiltinOperator::from_keyword("strlen_foo").is_none());
        assert!(BuiltinOperator::from_keyword("f").is_none());
    }

    /// Per-op signature: keyword, arity, and return type, bundled in one
    /// row per op so the three tables cannot drift apart.
    #[rstest]
    #[case(BuiltinOperator::Strlen, "strlen", 1, DataType::Int32)]
    #[case(BuiltinOperator::Substr, "substr", 3, DataType::String)]
    #[case(BuiltinOperator::Ord, "ord", 1, DataType::Int32)]
    #[case(BuiltinOperator::ToString, "to_string", 1, DataType::String)]
    #[case(BuiltinOperator::ToNumber, "to_number", 1, DataType::Int32)]
    #[case(BuiltinOperator::Cat, "cat", 2, DataType::String)]
    fn operator_signature(
        #[case] op: BuiltinOperator,
        #[case] keyword: &str,
        #[case] arity: usize,
        #[case] ret: DataType,
    ) {
        assert_eq!(op.keyword(), keyword);
        assert_eq!(op.arity(), arity);
        assert_eq!(op.ret_type(), ret);
    }

    /// `to_string` takes one polymorphic parameter over numeric and bool
    /// scalars; `string` (a no-op) and tuples (no `Display`) are excluded.
    #[test]
    fn to_string_accepts_numeric_and_bool_scalars() {
        let params = BuiltinOperator::ToString.param_allowed_types();
        assert_eq!(params.len(), 1);
        assert!(params[0].contains(&DataType::Int32));
        assert!(params[0].contains(&DataType::Bool));
        assert!(!params[0].contains(&DataType::String));
    }

    /// `new` enforces per-op arity, rejecting a mismatch with a user-facing
    /// [`ParseError::BuiltinArity`] that names the op and the counts.
    #[test]
    fn new_enforces_arity() {
        assert!(BuiltinCall::new(BuiltinOperator::Substr, args(3), Span::DUMMY).is_ok());

        match BuiltinCall::new(BuiltinOperator::Strlen, args(2), Span::DUMMY) {
            Err(ParseError::BuiltinArity {
                op,
                expected,
                found,
                ..
            }) => {
                assert_eq!(op, "strlen");
                assert_eq!(expected, 1);
                assert_eq!(found, 2);
            }
            other => panic!("expected BuiltinArity, got {other:?}"),
        }
    }

    /// The read accessors expose the call's parts: the operator, the
    /// argument expressions, and the variables collected across them.
    #[test]
    fn accessors_expose_operator_args_and_vars() {
        let call = BuiltinCall::new(BuiltinOperator::Cat, args(2), Span::DUMMY).unwrap();
        assert_eq!(call.op(), BuiltinOperator::Cat);
        assert_eq!(call.args().len(), 2);
        assert_eq!(call.vars(), [&"a0".to_string(), &"a1".to_string()]);
    }

    #[test]
    fn display_renders_call_syntax() {
        let call = BuiltinCall::new(BuiltinOperator::Cat, args(2), Span::DUMMY).unwrap();
        assert_eq!(call.to_string(), "cat(a0, a1)");
    }
}
