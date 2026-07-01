//! Raw AST for `.comp` / `.init`. Names stay as strings (no
//! [`TypeRegistry`] lookups) because comp bodies can reference
//! unbound type parameters; the inliner resolves them. Inlined and
//! discarded before typechecking — see [`crate::inliner`].

use std::collections::HashMap;

use flowlog_common::FileId;
use flowlog_common::Span;
use pest::iterators::Pair;

use crate::Lexeme;
use crate::Rule;
use crate::declaration::directive::parse_io_params_node;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::logic::FlowLogRule;
use crate::logic::Head;
use crate::logic::apply_indices_to_rule;
use crate::logic::parse_plan_indices;
use crate::span_of;
use crate::type_ref_name;

/// `.type` operator: `=` (alias), `<:` (subtype), or a tuple declaration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RawTypeOp {
    Alias,
    Subtype,
    Tuple(Vec<(String, String)>),
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
    pub fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
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
        let mut body: Vec<RawItem> = Vec::new();
        // Index into `body` of the first rule emitted by the most recent
        // rule clause. A `.plan` pins every rule from here to the end of
        // `body` — a single source clause can expand to several
        // `RawItem::Rule`s (multi-head / multi-body), all of which share
        // the hint. Applied at parse time so the inliner sees an
        // already-permuted RHS and stays oblivious to `.plan`.
        let mut plan_target_start: Option<usize> = None;

        for node in inner {
            match node.as_rule() {
                Rule::comp_type_params => {
                    type_params.extend(node.into_inner().map(|p| p.as_str().to_string()));
                }
                Rule::comp_supertype => {
                    supertype = Some(SuperRef::from_parsed_rule(node, file)?);
                }
                Rule::comp_body_item => {
                    let inner_kind = node.clone().into_inner().peek().map(|p| p.as_rule());
                    if matches!(inner_kind, Some(Rule::plan_directive)) {
                        let plan_pair = node
                            .into_inner()
                            .next()
                            .ok_or_else(|| grammar_bug("comp_body_item missing inner"))?;
                        let start =
                            plan_target_start
                                .take()
                                .ok_or_else(|| ParseError::PlanOrphan {
                                    span: span_of(&plan_pair, file),
                                })?;
                        let (span, raw_indices) = parse_plan_indices(plan_pair, file)?;
                        for item in &mut body[start..] {
                            let RawItem::Rule(rule) = item else {
                                return Err(grammar_bug(
                                    "plan_target_start should only ever point at RawItem::Rule items",
                                ));
                            };
                            apply_indices_to_rule(rule, span, &raw_indices)?;
                        }
                    } else {
                        let items = RawItem::from_parsed_rule(node, file)?;
                        plan_target_start =
                            matches!(items.first(), Some(RawItem::Rule(_))).then_some(body.len());
                        body.extend(items);
                    }
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
    pub fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
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
    /// Parse a `comp_body_item` pest node into one or more raw items.
    ///
    /// Returns a `Vec` because a single rule clause can expand to several
    /// rules — multi-head (`A, B :- C.`) and multi-body (`A :- B ; C.`)
    /// distribute the same way they do at top level. Every other body
    /// item yields exactly one element.
    pub fn from_parsed_rule(
        parsed_rule: Pair<Rule>,
        file: FileId,
    ) -> Result<Vec<Self>, ParseError> {
        debug_assert_eq!(parsed_rule.as_rule(), Rule::comp_body_item);
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("comp_body_item missing inner"))?;
        let item = match inner.as_rule() {
            Rule::declaration => RawItem::Decl(RawRelation::from_parsed_rule(inner, file)?),
            Rule::type_alias_decl => {
                let (name, op, parent, span) = split_type_alias(inner, file)?;
                RawItem::TypeAlias {
                    name,
                    op,
                    parent,
                    span,
                }
            }
            Rule::rule => return parse_raw_rule(inner, file),
            Rule::fact => parse_raw_fact(inner, file)?,
            Rule::input_directive => {
                let (name, params, span) = parse_io_parts(inner, file)?;
                RawItem::Input { name, params, span }
            }
            Rule::output_directive => {
                let (name, params, span) = parse_io_parts(inner, file)?;
                RawItem::Output { name, params, span }
            }
            Rule::printsize_directive => parse_raw_printsize(inner, file)?,
            Rule::comp_decl => RawItem::Comp(CompDecl::from_parsed_rule(inner, file)?),
            Rule::init_decl => RawItem::Init(InitDecl::from_parsed_rule(inner, file)?),
            Rule::override_directive => parse_raw_override(inner, file)?,
            other => {
                return Err(grammar_bug(format!(
                    "unexpected rule inside comp_body_item: {other:?}"
                )));
            }
        };
        Ok(vec![item])
    }
}

/// Split a `type_alias_decl` pest node into `(name, op, parent, span)`.
/// Shared between the top-level type-registry pre-pass in
/// [`crate::program`] and the raw-comp parser above.
pub fn split_type_alias(
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
    let rhs = inner
        .next()
        .ok_or_else(|| grammar_bug("type_alias_decl missing RHS type"))?;

    // A tuple RHS (`( f0: T0, … )`) is its own kind of `.type`. It must be
    // defined with `=`: tuples cannot be subtyped.
    if let Some(tuple_type) = rhs
        .clone()
        .into_inner()
        .find(|p| p.as_rule() == Rule::tuple_type)
    {
        if op_inner.as_rule() != Rule::alias_op {
            return Err(ParseError::TupleSubtypeDecl { span, name });
        }
        let fields = parse_tuple_fields(tuple_type)?;
        return Ok((name, RawTypeOp::Tuple(fields), String::new(), span));
    }

    let op = match op_inner.as_rule() {
        Rule::subtype_op => RawTypeOp::Subtype,
        Rule::alias_op => RawTypeOp::Alias,
        other => return Err(grammar_bug(format!("unexpected type op: {other:?}"))),
    };
    let parent = type_ref_name(&rhs);
    Ok((name, op, parent, span))
}

/// Parse a `tuple_type` pest node into `(field_name, field_type_source)` pairs.
/// Field types are kept as source strings and resolved (and recursion-checked)
/// by [`crate::primitive::TypeRegistry::register_tuple`].
pub fn parse_tuple_fields(tuple_type: Pair<Rule>) -> Result<Vec<(String, String)>, ParseError> {
    debug_assert_eq!(tuple_type.as_rule(), Rule::tuple_type);
    let mut fields = Vec::new();
    for field in tuple_type.into_inner() {
        if field.as_rule() != Rule::tuple_field {
            return Err(grammar_bug(format!(
                "unexpected child of tuple_type: {:?}",
                field.as_rule()
            )));
        }
        let mut parts = field.into_inner();
        let fname = parts
            .next()
            .ok_or_else(|| grammar_bug("tuple_field missing name"))?
            .as_str()
            .to_string();
        let ftype = type_ref_name(
            &parts
                .next()
                .ok_or_else(|| grammar_bug("tuple_field missing type"))?,
        );
        fields.push((fname, ftype));
    }
    Ok(fields)
}

/// Reuse the standard rule parser, expanding multi-head / multi-body
/// rules into one `RawItem::Rule` per (head, body) pair. The inliner
/// then prefixes and rewrites each expanded rule independently, exactly
/// as it would a hand-written single-clause rule.
fn parse_raw_rule(node: Pair<Rule>, file: FileId) -> Result<Vec<RawItem>, ParseError> {
    Ok(FlowLogRule::expand_from_parsed_rule(node, file)?
        .into_iter()
        .map(RawItem::Rule)
        .collect())
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
        Some(params_node) => parse_io_params_node(params_node)?,
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
    pub fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
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
