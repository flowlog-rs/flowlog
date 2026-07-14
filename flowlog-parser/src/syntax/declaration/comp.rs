//! Raw AST for `.comp` / `.init`. Names stay as strings (no
//! [`TypeRegistry`] lookups) because comp bodies can reference
//! unbound type parameters; the inliner resolves them. Inlined and
//! discarded before typechecking; see [`crate::pipeline::inline`].

use std::collections::HashMap;

use flowlog_common::Span;

use crate::Node;
use crate::Rule;
use crate::ast::FlowLogRule;
use crate::ast::Head;
use crate::declaration::directive::parse_io_params;
use crate::declaration::type_decl::RawTypeOp;
use crate::declaration::type_decl::split_type_alias;
use crate::error::ParseError;
use crate::error::grammar_bug;

// =============================================================================
// CompDecl
// =============================================================================

/// `.comp Name<T1, T2, ...> [: Super<args>] { body... }`.
#[derive(Debug, Clone)]
pub(crate) struct CompDecl {
    pub(crate) name: String,
    pub(crate) type_params: Vec<String>,
    pub(crate) supertype: Option<SuperRef>,
    pub(crate) body: Vec<RawItem>,
    pub(crate) span: Span,
}

impl CompDecl {
    /// Parse a `.comp` pest node.
    pub(crate) fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::comp_decl);
        let span = node.span();
        let mut children = node.children();

        let name = children.next_any("name")?.text().to_string();

        let mut type_params = Vec::new();
        let mut supertype: Option<SuperRef> = None;
        let mut body: Vec<RawItem> = Vec::new();

        for child in children {
            match child.rule() {
                Rule::comp_type_params => {
                    type_params.extend(child.children().map(|p| p.text().to_string()));
                }
                Rule::comp_supertype => {
                    supertype = Some(SuperRef::from_parsed_rule(child)?);
                }
                // A body rule carries its own trailing `.plan`, applied when
                // `RawItem::from_parsed_rule` expands the rule.
                Rule::comp_body_item => {
                    let inner = child.children().next_any("body item")?;
                    body.extend(RawItem::from_parsed_rule(inner)?);
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

// =============================================================================
// SuperRef
// =============================================================================

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

impl SuperRef {
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::comp_supertype);
        let span = node.span();
        let mut children = node.children();
        let name = children.next_any("name")?.text().to_string();
        let args = parse_comp_type_args(children.next())?;
        Ok(Self { name, args, span })
    }
}

// =============================================================================
// InitDecl
// =============================================================================

/// `.init instance = Comp<args...>`.
#[derive(Debug, Clone)]
pub(crate) struct InitDecl {
    pub(crate) instance: String,
    pub(crate) comp: String,
    pub(crate) args: Vec<String>,
    pub(crate) span: Span,
}

impl InitDecl {
    /// Parse an `.init` pest node.
    pub(crate) fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::init_decl);
        let span = node.span();
        let mut children = node.children();
        let instance = children.next_any("instance name")?.text().to_string();
        let comp = children.next_any("component name")?.text().to_string();
        let args = parse_comp_type_args(children.next())?;
        Ok(Self {
            instance,
            comp,
            args,
            span,
        })
    }
}

/// Parse the optional `comp_type_args` node (`<a, b, c>`) into a list of
/// type-reference source strings, empty when `None`. Shared by the
/// `<...>` lists on a `SuperRef` header and an `InitDecl`.
fn parse_comp_type_args(node: Option<Node>) -> Result<Vec<String>, ParseError> {
    let Some(node) = node else {
        return Ok(Vec::new());
    };
    debug_assert_eq!(node.rule(), Rule::comp_type_args);
    Ok(node
        .children()
        .map(|child| child.text().trim().to_string())
        .collect())
}

// =============================================================================
// RawRelation
// =============================================================================

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

impl RawRelation {
    /// Parse a `.decl` pest node into raw form (attribute types stay
    /// as source strings, no [`TypeRegistry`] lookup).
    pub(crate) fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::declaration);
        let span = node.span();
        let mut children = node.children();
        let name = children.next_any("name")?.text().to_string();

        let mut attrs = Vec::new();
        let mut overridable = false;
        for child in children {
            match child.rule() {
                Rule::attributes_decl => {
                    for attr in child.children() {
                        let mut parts = attr.children();
                        let aname = parts.next_any("name")?.text().to_string();
                        let type_name = parts.next_any("type_ref")?.text().trim().to_string();
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

// =============================================================================
// RawItem
// =============================================================================

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
    /// A rule. Reused verbatim from the standard parser: atom/head
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
    /// `.override Name`: replace the parent's rules/facts for `Name`
    /// with this component's own derivations. Resolved (and stripped)
    /// during `resolve_inheritance`; never reaches `inline_one`.
    Override {
        name: String,
        span: Span,
    },
}

impl RawItem {
    /// Parse a `comp_body_item`'s inner item node into one or more raw
    /// items.
    ///
    /// Returns a `Vec` because a single rule clause can expand to several
    /// rules: multi-head (`A, B :- C.`) and multi-body (`A :- B ; C.`)
    /// distribute the same way they do at top level. Every other body
    /// item yields exactly one element.
    pub(crate) fn from_parsed_rule(node: Node) -> Result<Vec<Self>, ParseError> {
        let item = match node.rule() {
            Rule::declaration => RawItem::Decl(RawRelation::from_parsed_rule(node)?),
            Rule::type_alias_decl => {
                let (name, op, parent, span) = split_type_alias(node)?;
                RawItem::TypeAlias {
                    name,
                    op,
                    parent,
                    span,
                }
            }
            Rule::rule => return parse_raw_rule(node),
            Rule::fact => parse_raw_fact(node)?,
            Rule::input_directive => {
                let (name, params, span) = parse_io_parts(node)?;
                RawItem::Input { name, params, span }
            }
            Rule::output_directive => {
                let (name, params, span) = parse_io_parts(node)?;
                RawItem::Output { name, params, span }
            }
            Rule::printsize_directive => parse_raw_printsize(node)?,
            Rule::comp_decl => RawItem::Comp(CompDecl::from_parsed_rule(node)?),
            Rule::init_decl => RawItem::Init(InitDecl::from_parsed_rule(node)?),
            Rule::override_directive => parse_raw_override(node)?,
            other => {
                return Err(grammar_bug(format!(
                    "unexpected rule inside comp_body_item: {other:?}"
                )));
            }
        };
        Ok(vec![item])
    }
}

/// Reuse the standard rule parser, expanding multi-head / multi-body
/// rules into one `RawItem::Rule` per (head, body) pair. The inliner
/// then prefixes and rewrites each expanded rule independently, exactly
/// as it would a hand-written single-clause rule.
fn parse_raw_rule(node: Node) -> Result<Vec<RawItem>, ParseError> {
    let (pair, file) = node.into_parts();
    Ok(FlowLogRule::expand_from_parsed_rule(pair, file)?
        .into_iter()
        .map(RawItem::Rule)
        .collect())
}

fn parse_raw_fact(node: Node) -> Result<RawItem, ParseError> {
    let head = node.children().lower_next::<Head>("head")?;
    Ok(RawItem::Fact(FlowLogRule::new(head, vec![])))
}

/// Parse the shared `(name, params?, span)` shape of `.input` /
/// `.output` directives. The caller wraps into the appropriate
/// `RawItem` variant.
fn parse_io_parts(node: Node) -> Result<(String, HashMap<String, String>, Span), ParseError> {
    let span = node.span();
    let mut children = node.children();
    let name = children.next_any("relation name")?.text().to_string();
    let params = match children.next() {
        Some(params_node) => parse_io_params(params_node)?,
        None => HashMap::new(),
    };
    Ok((name, params, span))
}

fn parse_raw_printsize(node: Node) -> Result<RawItem, ParseError> {
    let span = node.span();
    let name = node
        .children()
        .next_any("relation name")?
        .text()
        .to_string();
    Ok(RawItem::Printsize { name, span })
}

fn parse_raw_override(node: Node) -> Result<RawItem, ParseError> {
    let span = node.span();
    let name = node
        .children()
        .next_any("relation name")?
        .text()
        .to_string();
    Ok(RawItem::Override { name, span })
}
