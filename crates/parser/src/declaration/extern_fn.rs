//! External function declarations for FlowLog programs.
//!
//! Supports two kinds of UDFs via the [`Udf`] enum:
//! - **Scalar**: `.extern fn name(p1: type1, p2: type2) -> ret_type`
//! - **Aggregate**: `.extern agg name(p1: type1) -> ret_type`

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
        write!(f, "{}({}) -> {}", self.name, params, self.ret_type)
    }
}

/// A user-defined function declaration — either scalar or aggregate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Udf {
    /// Per-tuple scalar function: `.extern fn ...`
    Scalar(ExternFn),
    /// Aggregate function (group-by semantics): `.extern agg ...`
    Aggregate(ExternFn),
}

impl fmt::Display for Udf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(ext) => write!(f, ".extern fn {ext}"),
            Self::Aggregate(ext) => write!(f, ".extern agg {ext}"),
        }
    }
}

impl Lexeme for Udf {
    /// Parse an `extern_fn` or `extern_agg` grammar node into a [`Udf`].
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let is_agg = match parsed_rule.as_rule() {
            Rule::extern_fn => false,
            Rule::extern_agg => true,
            other => panic!(
                "Parser error: expected extern_fn or extern_agg, got {:?}",
                other
            ),
        };

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
        let ext = ExternFn::new(name, params, ret_type);

        if is_agg {
            Self::Aggregate(ext)
        } else {
            Self::Scalar(ext)
        }
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
        let Udf::Scalar(ext) = Udf::from_parsed_rule(pairs.next().unwrap()) else {
            panic!("expected Scalar");
        };
        assert_eq!(ext.name(), "get_time");
        assert!(ext.params().is_empty());
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn parse_scalar_with_params() {
        let input = ".extern fn my_hash(x: int64, y: int32) -> int64";
        let mut pairs = FlowLogParser::parse(Rule::extern_fn, input).unwrap();
        let Udf::Scalar(ext) = Udf::from_parsed_rule(pairs.next().unwrap()) else {
            panic!("expected Scalar");
        };
        assert_eq!(ext.name(), "my_hash");
        assert_eq!(ext.arity(), 2);
        assert_eq!(ext.params()[0].name(), "x");
        assert_eq!(*ext.params()[0].data_type(), DataType::Int64);
        assert_eq!(ext.params()[1].name(), "y");
        assert_eq!(*ext.params()[1].data_type(), DataType::Int32);
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn parse_aggregate_with_params() {
        let input = ".extern agg my_median(v: int32) -> int32";
        let mut pairs = FlowLogParser::parse(Rule::extern_agg, input).unwrap();
        let Udf::Aggregate(ext) = Udf::from_parsed_rule(pairs.next().unwrap()) else {
            panic!("expected Aggregate");
        };
        assert_eq!(ext.name(), "my_median");
        assert_eq!(ext.arity(), 1);
        assert_eq!(ext.params()[0].name(), "v");
        assert_eq!(*ext.params()[0].data_type(), DataType::Int32);
        assert_eq!(ext.ret_type(), DataType::Int32);
    }

    #[test]
    fn display_roundtrip_scalar() {
        let udf = Udf::Scalar(ExternFn::new(
            "foo".into(),
            vec![
                Attribute::new("a".into(), DataType::Int32),
                Attribute::new("b".into(), DataType::String),
            ],
            DataType::Int64,
        ));
        assert_eq!(
            udf.to_string(),
            ".extern fn foo(a: int32, b: string) -> int64"
        );
    }

    #[test]
    fn display_roundtrip_aggregate() {
        let udf = Udf::Aggregate(ExternFn::new(
            "my_median".into(),
            vec![Attribute::new("v".into(), DataType::Int32)],
            DataType::Int32,
        ));
        assert_eq!(udf.to_string(), ".extern agg my_median(v: int32) -> int32");
    }
}
