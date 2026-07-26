//! External function declarations for FlowLog programs.
//!
//! Syntax: `.extern fn name(p1: type1, p2: type2) -> ret_type`

use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::Attribute;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::types::DataType;
use crate::types::TypeRegistry;

/// An external (user-defined) scalar function declared with `.extern fn`.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub struct ExternFn {
    name: String,
    params: Vec<Attribute>,
    ret_type: DataType,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl ExternFn {
    /// Function name (a valid Rust identifier).
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
        self.ret_type.clone()
    }

    /// Span of the `.extern fn` declaration.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Number of parameters (arity). Tests only; production code uses `params().len()`.
    #[cfg(test)]
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.params.len()
    }

    /// Parse an `extern_fn` node. `registry` only supplies the primitive
    /// [`TypeId`](crate::types::TypeId) for each parameter: extern-fn params
    /// are always primitives (the grammar uses `data_type`, not `type_ref`).
    pub(crate) fn from_parsed_rule(
        node: Node,
        registry: &TypeRegistry,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::extern_fn);
        let span = node.span();
        let mut children = node.children();

        let name = children.next_any("name")?.text().to_string();

        let mut params = Vec::new();
        let mut ret_type = None;
        for child in children {
            match child.rule() {
                Rule::extern_fn_params => {
                    for param in child.children() {
                        params.push(parse_param(param, registry)?);
                    }
                }
                Rule::data_type => {
                    ret_type = Some(child.text().parse::<DataType>().map_err(|e| {
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
            span,
        })
    }
}

impl fmt::Display for ExternFn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".extern fn {}(", self.name)?;
        for (i, attr) in self.params.iter().enumerate() {
            if i > 0 {
                f.write_str(", ")?;
            }
            attr.fmt(f)?;
        }
        write!(f, ") -> {}", self.ret_type)
    }
}

/// Parse a single `extern_fn_param` node (`name: type`) into an [`Attribute`].
fn parse_param(node: Node, registry: &TypeRegistry) -> Result<Attribute, ParseError> {
    let mut parts = node.children();
    let name = parts.next_any("name")?.text().to_string();
    let data_type = parts
        .next_any("type")?
        .text()
        .parse::<DataType>()
        .map_err(|e| grammar_bug(format!("invalid type in extern fn param: {e}")))?;
    let declared_id = registry.primitive_id(data_type.clone()).ok_or_else(|| {
        grammar_bug(format!(
            "extern fn param type `{data_type}` is not a seeded primitive"
        ))
    })?;
    Ok(Attribute::with_type(name, data_type, declared_id))
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;

    use super::*;
    use crate::test_util::parse_pair;

    fn ext(src: &str) -> ExternFn {
        let registry = TypeRegistry::new();
        ExternFn::from_parsed_rule(
            Node::new(parse_pair(Rule::extern_fn, src), FileId::new(0)),
            &registry,
        )
        .expect("extern_fn parses")
    }

    #[test]
    fn parse_scalar_no_params() {
        let ext = ext(".extern fn get_time() -> int64");
        assert_eq!(ext.name(), "get_time");
        assert!(ext.params().is_empty());
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    #[test]
    fn parse_scalar_with_params() {
        let ext = ext(".extern fn my_hash(x: int64, y: int32) -> int64");
        assert_eq!(ext.name(), "my_hash");
        assert_eq!(ext.arity(), 2);
        assert_eq!(ext.params()[0].name(), "x");
        assert_eq!(*ext.params()[0].data_type(), DataType::Int64);
        assert_eq!(ext.params()[1].name(), "y");
        assert_eq!(*ext.params()[1].data_type(), DataType::Int32);
        assert_eq!(ext.ret_type(), DataType::Int64);
    }

    /// Display round-trips the surface syntax, with and without parameters.
    #[test]
    fn display_round_trips_surface_syntax() {
        for src in [
            ".extern fn get_time() -> int64",
            ".extern fn my_hash(x: int64, y: int32) -> int64",
        ] {
            assert_eq!(ext(src).to_string(), src);
        }
    }
}
