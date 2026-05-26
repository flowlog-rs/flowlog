//! Raw AST for `.comp` / `.init`. Names stay as strings (no
//! [`TypeRegistry`] lookups) because comp bodies can reference
//! unbound type parameters; the inliner resolves them. Inlined and
//! discarded before typechecking — see [`crate::parser::inliner`].

use std::collections::HashMap;

use pest::iterators::Pair;

use crate::common::{FileId, Span};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::logic::{FlowLogRule, Head};
use crate::parser::{Lexeme, Rule, span_of, type_ref_name};

/// `.type` operator: `=` (alias) or `<:` (subtype).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RawTypeOp {
    Alias,
    Subtype,
}

/// `.comp Name<T1, T2, ...> [: Super<args>] { body... }`.
#[derive(Debug, Clone)]
pub(crate) struct CompDecl {
    pub(crate) name: String,
    pub(crate) type_params: Vec<String>,
    pub(crate) supertype: Option<SuperRef>,
    pub(crate) body: Vec<RawItem>,
    pub(crate) span: Span,
}

/// `: Base<arg1, arg2>` on a `.comp` header.
#[derive(Debug, Clone)]
pub(crate) struct SuperRef {
    pub(crate) name: String,
    /// Type-argument source strings (e.g. `["symbol", "NodeId"]`). May
    /// be primitive names, `.type` aliases, or component type-params
    /// from the enclosing `.comp`. Resolved per-instantiation.
    pub(crate) args: Vec<String>,
    pub(crate) span: Span,
}

/// `.init instance = Comp<args...>`.
#[derive(Debug, Clone)]
pub(crate) struct InitDecl {
    pub(crate) instance: String,
    pub(crate) comp: String,
    pub(crate) args: Vec<String>,
    pub(crate) span: Span,
}

/// One body item inside a `.comp { ... }` block.
#[derive(Debug, Clone)]
pub(crate) enum RawItem {
    Decl(RawRelation),
    TypeAlias {
        name: String,
        op: RawTypeOp,
        parent: String,
        span: Span,
    },
    /// A rule. Reused verbatim from the standard parser — atom/head
    /// names get rewritten at inline time, body predicates contain no
    /// type-name references that need substitution.
    Rule(FlowLogRule),
    /// A ground fact (`rel(c1, c2).`). Same rationale as `Rule`.
    Fact(FlowLogRule),
    Input {
        name: String,
        params: HashMap<String, String>,
        span: Span,
    },
    Output {
        name: String,
        params: HashMap<String, String>,
        span: Span,
    },
    Printsize {
        name: String,
        span: Span,
    },
    /// Nested `.init` inside a component body.
    Init(InitDecl),
    /// Nested `.comp` inside a component body. Rare; hoisted to the
    /// global component map with a mangled name at inline time.
    Comp(CompDecl),
    /// `.override Name` — replace the parent's rules/facts for `Name`
    /// with this component's own derivations. Resolved (and stripped)
    /// during `resolve_inheritance`; never reaches `inline_one`.
    Override {
        name: String,
        span: Span,
    },
}

/// `.decl name(attr: TypeName, ...)` with **un-resolved** type names.
#[derive(Debug, Clone)]
pub(crate) struct RawRelation {
    pub(crate) name: String,
    /// `(attribute_name, type_source_string)` pairs in declaration order.
    pub(crate) attrs: Vec<(String, String)>,
    /// `overridable` keyword on the `.decl`. Only meaningful inside a
    /// `.comp` body; the inliner uses it to validate `.override`
    /// targets in subcomponents.
    pub(crate) overridable: bool,
    pub(crate) span: Span,
}

// =============================================================================
// Parsing
// =============================================================================

impl CompDecl {
    /// Parse a `.comp` pest node.
    pub(crate) fn from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(parsed_rule.as_rule(), Rule::comp_decl);
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("comp_decl missing name"))?
            .as_str()
            .to_string();

        let mut type_params = Vec::new();
        let mut supertype: Option<SuperRef> = None;
        let mut body = Vec::new();

        for node in inner {
            match node.as_rule() {
                Rule::comp_type_params => {
                    type_params.extend(node.into_inner().map(|p| p.as_str().to_string()));
                }
                Rule::comp_supertype => {
                    supertype = Some(SuperRef::from_parsed_rule(node, file)?);
                }
                Rule::comp_body_item => {
                    body.push(RawItem::from_parsed_rule(node, file)?);
                }
                other => {
                    return Err(grammar_bug(format!(
                        "unexpected child of comp_decl: {other:?}"
                    )));
                }
            }
        }

        Ok(Self {
            name,
            type_params,
            supertype,
            body,
            span,
        })
    }
}

impl SuperRef {
    fn from_parsed_rule(node: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        debug_assert_eq!(node.as_rule(), Rule::comp_supertype);
        let span = span_of(&node, file);
        let mut inner = node.into_inner();
        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("comp_supertype missing name"))?
            .as_str()
            .to_string();
        let args = parse_comp_type_args(inner.next())?;
        Ok(Self { name, args, span })
    }
}

impl InitDecl {
    /// Parse an `.init` pest node.
    pub(crate) fn from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(parsed_rule.as_rule(), Rule::init_decl);
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();
        let instance = inner
            .next()
            .ok_or_else(|| grammar_bug("init_decl missing instance name"))?
            .as_str()
            .to_string();
        let comp = inner
            .next()
            .ok_or_else(|| grammar_bug("init_decl missing component name"))?
            .as_str()
            .to_string();
        let args = parse_comp_type_args(inner.next())?;
        Ok(Self {
            instance,
            comp,
            args,
            span,
        })
    }
}

/// Parse the optional `comp_type_args` node (`<a, b, c>`) into a list
/// of type-reference source strings. Returns an empty vec if `None`.
fn parse_comp_type_args(node: Option<Pair<Rule>>) -> Result<Vec<String>, ParseError> {
    let Some(node) = node else {
        return Ok(Vec::new());
    };
    debug_assert_eq!(node.as_rule(), Rule::comp_type_args);
    Ok(node.into_inner().map(|p| type_ref_name(&p)).collect())
}

impl RawItem {
    /// Parse a `comp_body_item` pest node.
    pub(crate) fn from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(parsed_rule.as_rule(), Rule::comp_body_item);
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("comp_body_item missing inner"))?;
        match inner.as_rule() {
            Rule::declaration => Ok(RawItem::Decl(RawRelation::from_parsed_rule(inner, file)?)),
            Rule::type_alias_decl => {
                let (name, op, parent, span) = split_type_alias(inner, file)?;
                Ok(RawItem::TypeAlias {
                    name,
                    op,
                    parent,
                    span,
                })
            }
            Rule::rule => parse_raw_rule(inner, file),
            Rule::fact => parse_raw_fact(inner, file),
            Rule::input_directive => {
                let (name, params, span) = parse_io_parts(inner, file)?;
                Ok(RawItem::Input { name, params, span })
            }
            Rule::output_directive => {
                let (name, params, span) = parse_io_parts(inner, file)?;
                Ok(RawItem::Output { name, params, span })
            }
            Rule::printsize_directive => parse_raw_printsize(inner, file),
            Rule::comp_decl => Ok(RawItem::Comp(CompDecl::from_parsed_rule(inner, file)?)),
            Rule::init_decl => Ok(RawItem::Init(InitDecl::from_parsed_rule(inner, file)?)),
            Rule::override_directive => parse_raw_override(inner, file),
            other => Err(grammar_bug(format!(
                "unexpected rule inside comp_body_item: {other:?}"
            ))),
        }
    }
}

/// Split a `type_alias_decl` pest node into `(name, op, parent, span)`.
/// Shared between the top-level type-registry pre-pass in
/// [`crate::parser::program`] and the raw-comp parser above.
pub(crate) fn split_type_alias(
    node: Pair<Rule>,
    file: FileId,
) -> Result<(String, RawTypeOp, String, Span), ParseError> {
    debug_assert_eq!(node.as_rule(), Rule::type_alias_decl);
    let span = span_of(&node, file);
    let mut inner = node.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| grammar_bug("type_alias_decl missing name"))?
        .as_str()
        .to_string();
    let op_inner = inner
        .next()
        .ok_or_else(|| grammar_bug("type_alias_decl missing operator"))?
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("type_decl_op missing inner op"))?;
    let op = match op_inner.as_rule() {
        Rule::subtype_op => RawTypeOp::Subtype,
        Rule::alias_op => RawTypeOp::Alias,
        other => return Err(grammar_bug(format!("unexpected type op: {other:?}"))),
    };
    let parent = type_ref_name(
        &inner
            .next()
            .ok_or_else(|| grammar_bug("type_alias_decl missing parent"))?,
    );
    Ok((name, op, parent, span))
}

/// Reuse the standard rule parser, rejecting multi-head / multi-body
/// rules inside comp bodies (one `RawItem::Rule` per `comp_body_item`).
fn parse_raw_rule(node: Pair<Rule>, file: FileId) -> Result<RawItem, ParseError> {
    let mut rules = FlowLogRule::expand_from_parsed_rule(node, file)?;
    match rules.len() {
        1 => Ok(RawItem::Rule(rules.remove(0))),
        n => Err(grammar_bug(format!(
            "multi-head/multi-body rules are not supported inside `.comp` bodies (expanded to {n} rules)"
        ))),
    }
}

fn parse_raw_fact(node: Pair<Rule>, file: FileId) -> Result<RawItem, ParseError> {
    let head_node = node
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("fact missing head"))?;
    let head = Head::from_parsed_rule(head_node, file)?;
    Ok(RawItem::Fact(FlowLogRule::new(head, vec![])))
}

/// Parse the shared `(name, params?, span)` shape of `.input` /
/// `.output` directives. The caller wraps into the appropriate
/// `RawItem` variant.
fn parse_io_parts(
    node: Pair<Rule>,
    file: FileId,
) -> Result<(String, HashMap<String, String>, Span), ParseError> {
    let span = span_of(&node, file);
    let mut inner = node.into_inner();
    let name = inner
        .next()
        .ok_or_else(|| grammar_bug("io directive missing relation name"))?
        .as_str()
        .to_string();
    let params = match inner.next() {
        Some(params_node) => super::directive::parse_io_params_node(params_node)?,
        None => HashMap::new(),
    };
    Ok((name, params, span))
}

fn parse_raw_printsize(node: Pair<Rule>, file: FileId) -> Result<RawItem, ParseError> {
    let span = span_of(&node, file);
    let name = node
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("printsize missing relation name"))?
        .as_str()
        .to_string();
    Ok(RawItem::Printsize { name, span })
}

fn parse_raw_override(node: Pair<Rule>, file: FileId) -> Result<RawItem, ParseError> {
    let span = span_of(&node, file);
    let name = node
        .into_inner()
        .next()
        .ok_or_else(|| grammar_bug("override directive missing relation name"))?
        .as_str()
        .to_string();
    Ok(RawItem::Override { name, span })
}

impl RawRelation {
    /// Parse a `.decl` pest node into raw form (attribute types stay
    /// as source strings — no [`TypeRegistry`] lookup).
    pub(crate) fn from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(parsed_rule.as_rule(), Rule::declaration);
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();
        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("relation missing name"))?
            .as_str()
            .to_string();

        let mut attrs = Vec::new();
        let mut overridable = false;
        for node in inner {
            match node.as_rule() {
                Rule::attributes_decl => {
                    for attr in node.into_inner() {
                        let mut parts = attr.into_inner();
                        let aname = parts
                            .next()
                            .ok_or_else(|| grammar_bug("attribute missing name"))?
                            .as_str()
                            .to_string();
                        let type_name = type_ref_name(
                            &parts
                                .next()
                                .ok_or_else(|| grammar_bug("attribute missing type_ref"))?,
                        );
                        attrs.push((aname, type_name));
                    }
                }
                Rule::overridable_kw => {
                    overridable = true;
                }
                _ => {}
            }
        }

        Ok(Self {
            name,
            attrs,
            overridable,
            span,
        })
    }
}
