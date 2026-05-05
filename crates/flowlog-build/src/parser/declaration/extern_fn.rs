//! External function declarations for FlowLog programs.
//!
//! Syntax: `.extern fn name(p1: type1, p2: type2) -> ret_type`

use std::fmt;

use pest::iterators::Pair;

use crate::common::{FileId, Ignored, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::DataType;
use crate::parser::{span_of, Lexeme, Rule};

use super::Attribute;

/// Common data for an external (user-defined) function declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ExternFn {
    /// Function name (must be a valid Rust identifier).
    name: String,
    /// Typed parameters.
    params: Vec<Attribute>,
    /// Return type.
    ret_type: DataType,
    /// Span of the `.extern fn` declaration.
    span: Ignored<Span>,
}

impl ExternFn {
    /// Function name.
    #[must_use]
    #[inline]
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Typed parameter list.
    #[must_use]
    #[inline]
    pub(crate) fn params(&self) -> &[Attribute] {
        &self.params
    }

    /// Return type.
    #[must_use]
    #[inline]
    pub(crate) fn ret_type(&self) -> DataType {
        self.ret_type
    }

    /// Number of parameters (arity). Tests only; production code uses `params().len()`.
    #[cfg(test)]
    #[must_use]
    #[inline]
    pub(crate) fn arity(&self) -> usize {
        self.params.len()
    }
}

impl fmt::Display for ExternFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let params = self
            .params
            .iter()
            .map(|a| format!("{}: {}", a.name(), a.data_type()))
            .collect::<Vec<_>>()
            .join(", ");
        write!(
            f,
            ".extern fn {}({}) -> {}",
            self.name, params, self.ret_type
        )
    }
}

impl Lexeme for ExternFn {
    /// Parse an `extern_fn` grammar node into an [`ExternFn`].
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        if parsed_rule.as_rule() != Rule::extern_fn {
            return Err(grammar_bug(format!(
                "expected extern_fn, got {:?}",
                parsed_rule.as_rule()
            )));
        }

        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("extern fn missing name"))?
            .as_str()
            .to_string();

        let mut params = Vec::new();
        let mut ret_type = None;

        for node in inner {
            match node.as_rule() {
                Rule::extern_fn_params => {
                    for param_node in node.into_inner() {
                        let mut pi = param_node.into_inner();
                        let pname = pi
                            .next()
                            .ok_or_else(|| grammar_bug("extern fn param missing name"))?
                            .as_str()
                            .to_string();
                        let ptype = pi
                            .next()
                            .ok_or_else(|| grammar_bug("extern fn param missing type"))?
                            .as_str()
                            .parse::<DataType>()
                            .map_err(|e| {
                                grammar_bug(format!("invalid type in extern fn param: {e}"))
                            })?;
                        params.push(Attribute::new(pname, ptype));
                    }
                }
                Rule::data_type => {
                    ret_type = Some(node.as_str().parse::<DataType>().map_err(|e| {
                        grammar_bug(format!("invalid return type in extern fn: {e}"))
                    })?);
                }
                _ => {}
            }
        }

        let ret_type = ret_type.ok_or_else(|| grammar_bug("extern fn missing return type"))?;
        Ok(Self {
            name,
            params,
            ret_type,
            span: Ignored(span),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::FileId;
    use crate::parser::FlowLogParser;
    use pest::Parser;

    #[test]
    fn parse_scalar_no_params() {
        let input = ".extern fn get_time() -> int64";
        let mut pairs = FlowLogParser::parse(Rule::extern_fn, input).unwrap();
        let ext = ExternFn::from_parsed_rule(pairs.next().unwrap(), FileId(0)).unwrap();
        assert_eq!(ext.name(), "get_time");
        assert!(ext.params().is_empty());
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn parse_scalar_with_params() {
        let input = ".extern fn my_hash(x: int64, y: int32) -> int64";
        let mut pairs = FlowLogParser::parse(Rule::extern_fn, input).unwrap();
        let ext = ExternFn::from_parsed_rule(pairs.next().unwrap(), FileId(0)).unwrap();
        assert_eq!(ext.name(), "my_hash");
        assert_eq!(ext.arity(), 2);
        assert_eq!(ext.params()[0].name(), "x");
        assert_eq!(*ext.params()[0].data_type(), DataType::Int64);
        assert_eq!(ext.params()[1].name(), "y");
        assert_eq!(*ext.params()[1].data_type(), DataType::Int32);
        assert_eq!(ext.ret_type(), DataType::Int64);
    }
}
