//! Raw parsing of `.type` declarations.
//!
//! [`split_type_alias`] lowers a `type_alias_decl` node to its
//! `(name, op, parent)` parts, with [`RawTypeOp`] distinguishing the
//! alias (`=`), subtype (`<:`), and tuple forms. Referenced type names
//! stay as source strings for [`crate::types::TypeRegistry`] to resolve.

use flowlog_error::Span;

use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;

/// `.type` operator: `=` (alias), `<:` (subtype), or a tuple declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RawTypeOp {
    Alias,
    Subtype,
    /// A tuple `.type`, as `(field_name, field_type_source)` pairs.
    Tuple(Vec<(String, String)>),
}

/// Split a `type_alias_decl` node into `(name, op, parent, span)`.
pub(crate) fn split_type_alias(
    node: Node,
) -> Result<(String, RawTypeOp, String, Span), ParseError> {
    debug_assert_eq!(node.rule(), Rule::type_alias_decl);
    let span = node.span();
    let mut children = node.children();

    let name = children.next_any("name")?.text().to_string();
    let op_inner = children
        .next_any("operator")?
        .children()
        .next_any("inner op")?;
    let rhs = children.next_any("RHS type")?;
    // `children()` consumes `rhs` below, so read its surface text (the
    // alias/subtype parent name) before walking into it.
    let parent = rhs.text().trim().to_string();

    // A tuple RHS (`( f0: T0, ... )`) is its own kind of `.type`. It must be
    // defined with `=`: tuples cannot be subtyped.
    if let Some(tuple_type) = rhs.children().take_if(Rule::tuple_type) {
        if op_inner.rule() != Rule::alias_op {
            return Err(ParseError::TupleSubtypeDecl { span, name });
        }
        let fields = parse_tuple_fields(tuple_type)?;
        return Ok((name, RawTypeOp::Tuple(fields), String::new(), span));
    }

    let op = match op_inner.rule() {
        Rule::subtype_op => RawTypeOp::Subtype,
        Rule::alias_op => RawTypeOp::Alias,
        other => return Err(grammar_bug(format!("unexpected type op: {other:?}"))),
    };
    Ok((name, op, parent, span))
}

/// Parse a `tuple_type` node into the field pairs of a [`RawTypeOp::Tuple`].
/// Field types are kept as source strings and resolved (and recursion-checked)
/// by [`crate::types::TypeRegistry::register_tuple`].
fn parse_tuple_fields(tuple_type: Node) -> Result<Vec<(String, String)>, ParseError> {
    debug_assert_eq!(tuple_type.rule(), Rule::tuple_type);
    let mut fields = Vec::new();
    for field in tuple_type.children() {
        if field.rule() != Rule::tuple_field {
            return Err(grammar_bug(format!(
                "unexpected child of tuple_type: {:?}",
                field.rule()
            )));
        }
        let mut parts = field.children();
        let fname = parts.next_any("name")?.text().to_string();
        let ftype = parts.next_any("type")?.text().trim().to_string();
        fields.push((fname, ftype));
    }
    Ok(fields)
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;

    use super::*;
    use crate::assert_err;
    use crate::test_util::parse_pair;

    fn node(src: &str) -> Node<'_> {
        Node::new(parse_pair(Rule::type_alias_decl, src), FileId::new(0))
    }

    /// The scalar (non-tuple) forms carry the name and the parent type name,
    /// with the operator mapped to its [`RawTypeOp`]: `=` to `Alias`, `<:` to
    /// `Subtype`.
    #[test]
    fn split_type_alias_reads_scalar_alias_and_subtype() {
        let (name, op, parent, _) = split_type_alias(node(".type Money = number")).unwrap();
        assert_eq!(
            (name, op, parent),
            ("Money".to_string(), RawTypeOp::Alias, "number".to_string())
        );

        let (name, op, parent, _) = split_type_alias(node(".type UserId <: number")).unwrap();
        assert_eq!(
            (name, op, parent),
            (
                "UserId".to_string(),
                RawTypeOp::Subtype,
                "number".to_string()
            )
        );
    }

    /// The `=` (alias) form on a tuple RHS is accepted and carries its fields.
    /// A tuple has no parent type, so `parent` comes back empty.
    #[test]
    fn split_type_alias_accepts_tuple_with_alias_op() {
        let (name, op, parent, _span) =
            split_type_alias(node(".type Pair = (a: symbol, b: number)")).unwrap();
        assert_eq!(name, "Pair");
        assert_eq!(parent, "");
        assert_eq!(
            op,
            RawTypeOp::Tuple(vec![
                ("a".to_string(), "symbol".to_string()),
                ("b".to_string(), "number".to_string()),
            ])
        );
    }

    /// `split_type_alias` rejects `<:` (subtype) on a tuple RHS: a tuple must
    /// be defined with `=`. Tested at the producing function, not through a
    /// whole-program parse.
    #[test]
    fn split_type_alias_rejects_subtype_op_on_tuple() {
        assert_err!(
            split_type_alias(node(".type Pair <: (a: symbol, b: symbol)")),
            ParseError::TupleSubtypeDecl { .. }
        );
    }
}
