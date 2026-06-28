//! Built-in function calls — Soufflé-style intrinsic functors. Each
//! builtin is a reserved keyword in `grammar.pest` and parses into a
//! distinct [`BuiltinCall`] node, never a [`FnCall`](super::FnCall).
//! Per-op signatures live on [`BuiltinOperator`]; lowerings live in
//! codegen.

use std::fmt;

use educe::Educe;
use pest::iterators::Pair;

use super::Arithmetic;
use crate::error::{ParseError, grammar_bug};
use crate::primitive::DataType;
use crate::{Lexeme, Rule, span_of};
use flowlog_common::{FileId, Span};

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
    /// `contains(needle, hay) -> bool`.
    Contains,
    /// `match(pattern, s) -> bool` — full (anchored) regex match, Soufflé semantics.
    Match,
    /// `to_string(n: int32) -> string`.
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
            BuiltinOperator::Contains => "contains",
            BuiltinOperator::Match => "match",
            BuiltinOperator::ToString => "to_string",
            BuiltinOperator::ToNumber => "to_number",
            BuiltinOperator::Cat => "cat",
        }
    }

    /// Declared arity (number of arguments). Derived from
    /// [`Self::param_types`] so both stay in sync.
    pub fn arity(self) -> usize {
        self.param_types().len()
    }

    /// Per-parameter declared types, in argument order. Consulted by
    /// the typechecker to validate each call site.
    pub fn param_types(self) -> &'static [DataType] {
        use DataType::{Int32, String as Str};
        match self {
            BuiltinOperator::Strlen => &[Str],
            BuiltinOperator::Substr => &[Str, Int32, Int32],
            BuiltinOperator::Ord => &[Str],
            BuiltinOperator::Contains => &[Str, Str],
            BuiltinOperator::Match => &[Str, Str],
            BuiltinOperator::ToString => &[Int32],
            BuiltinOperator::ToNumber => &[Str],
            BuiltinOperator::Cat => &[Str, Str],
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
            BuiltinOperator::Contains | BuiltinOperator::Match => DataType::Bool,
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

impl Lexeme for BuiltinCall {
    /// Parse a `builtin_fn_call` and enforce per-op arity (the grammar
    /// accepts any positive arity uniformly).
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        if parsed_rule.as_rule() != Rule::builtin_fn_call {
            return Err(grammar_bug(format!(
                "expected builtin_fn_call, got {:?}",
                parsed_rule.as_rule()
            )));
        }
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let op_node = inner
            .next()
            .ok_or_else(|| grammar_bug("builtin_fn_call missing operator"))?;
        let op = op_from_node(&op_node)?;

        let args: Vec<Arithmetic> = inner
            .filter(|p| p.as_rule() == Rule::arithmetic_expr)
            .map(|p| Arithmetic::from_parsed_rule(p, file))
            .collect::<Result<_, _>>()?;

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
}

/// Map a `builtin_op` grammar node to its [`BuiltinOperator`] variant.
fn op_from_node(node: &Pair<Rule>) -> Result<BuiltinOperator, ParseError> {
    if node.as_rule() != Rule::builtin_op {
        return Err(grammar_bug(format!(
            "expected builtin_op, got {:?}",
            node.as_rule()
        )));
    }
    let kw = node
        .clone()
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("builtin_op missing keyword"))?;
    Ok(match kw.as_rule() {
        Rule::strlen_op => BuiltinOperator::Strlen,
        Rule::substr_op => BuiltinOperator::Substr,
        Rule::ord_op => BuiltinOperator::Ord,
        Rule::contains_op => BuiltinOperator::Contains,
        Rule::match_op => BuiltinOperator::Match,
        Rule::to_string_op => BuiltinOperator::ToString,
        Rule::to_number_op => BuiltinOperator::ToNumber,
        Rule::cat_op => BuiltinOperator::Cat,
        other => return Err(grammar_bug(format!("unknown built-in operator: {other:?}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{FlowLogParser, Lexeme, Rule};
    use flowlog_common::FileId;
    use pest::Parser;

    fn try_parse(input: &str) -> Result<BuiltinCall, ParseError> {
        let mut pairs = FlowLogParser::parse(Rule::builtin_fn_call, input).unwrap();
        BuiltinCall::from_parsed_rule(pairs.next().unwrap(), FileId::new(0))
    }

    #[test]
    fn parses_a_3_arg_call() {
        let bc = try_parse("substr(s, 0, 3)").unwrap();
        assert_eq!(bc.op(), BuiltinOperator::Substr);
        assert_eq!(bc.args().len(), 3);
    }

    #[test]
    fn keyword_not_a_prefix() {
        assert!(FlowLogParser::parse(Rule::builtin_fn_call, "strlen_foo(x)").is_err());
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
            BuiltinOperator::Cat.param_types(),
            &[DataType::String, DataType::String]
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
