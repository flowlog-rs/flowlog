//! External function declarations for FlowLog programs.
//!
//! Syntax: `.extern fn name(p1: type1, p2: type2) -> ret_type`

use std::fmt;

use pest::iterators::Pair;

use crate::primitive::DataType;
use crate::{Lexeme, Rule};

use super::Attribute;

/// Common data for an external (user-defined) function declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternFn {
    /// Function name (must be a valid Rust identifier).
    name: String,
    /// Typed parameters.
    params: Vec<Attribute>,
    /// Return type.
    ret_type: DataType,
}

impl ExternFn {
    /// Create a new extern function declaration.
    #[must_use]
    pub fn new(name: String, params: Vec<Attribute>, ret_type: DataType) -> Self {
        Self {
            name,
            params,
            ret_type,
        }
    }

    /// Function name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Typed parameter list.
    #[must_use]
    #[inline]
    pub fn params(&self) -> &[Attribute] {
        &self.params
    }

    /// Return type.
    #[must_use]
    #[inline]
    pub fn ret_type(&self) -> DataType {
        self.ret_type
    }

    /// Number of parameters (arity).
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
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
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        assert_eq!(
            parsed_rule.as_rule(),
            Rule::extern_fn,
            "Parser error: expected extern_fn, got {:?}",
            parsed_rule.as_rule()
        );

        let mut inner = parsed_rule.into_inner();

        let name = inner
            .next()
            .expect("Parser error: extern fn missing name")
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
                            .expect("Parser error: extern fn param missing name")
                            .as_str()
                            .to_string();
                        let ptype = pi
                            .next()
                            .expect("Parser error: extern fn param missing type")
                            .as_str()
                            .parse::<DataType>()
                            .expect("Parser error: invalid type in extern fn param");
                        params.push(Attribute::new(pname, ptype));
                    }
                }
                Rule::data_type => {
                    ret_type = Some(
                        node.as_str()
                            .parse::<DataType>()
                            .expect("Parser error: invalid return type in extern fn"),
                    );
                }
                _ => {}
            }
        }

        let ret_type = ret_type.expect("Parser error: extern fn missing return type");
        ExternFn::new(name, params, ret_type)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::FlowLogParser;
    use pest::Parser;

    #[test]
    fn parse_scalar_no_params() {
        let input = ".extern fn get_time() -> int64";
        let mut pairs = FlowLogParser::parse(Rule::extern_fn, input).unwrap();
        let ext = ExternFn::from_parsed_rule(pairs.next().unwrap());
        assert_eq!(ext.name(), "get_time");
        assert!(ext.params().is_empty());
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn parse_scalar_with_params() {
        let input = ".extern fn my_hash(x: int64, y: int32) -> int64";
        let mut pairs = FlowLogParser::parse(Rule::extern_fn, input).unwrap();
        let ext = ExternFn::from_parsed_rule(pairs.next().unwrap());
        assert_eq!(ext.name(), "my_hash");
        assert_eq!(ext.arity(), 2);
        assert_eq!(ext.params()[0].name(), "x");
        assert_eq!(*ext.params()[0].data_type(), DataType::Int64);
        assert_eq!(ext.params()[1].name(), "y");
        assert_eq!(*ext.params()[1].data_type(), DataType::Int32);
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn display_roundtrip_scalar() {
        let ext = ExternFn::new(
            "foo".into(),
            vec![
                Attribute::new("a".into(), DataType::Int32),
                Attribute::new("b".into(), DataType::String),
            ],
            DataType::Int64,
        );
        assert_eq!(
            ext.to_string(),
            ".extern fn foo(a: int32, b: string) -> int64"
        );
    }
}
