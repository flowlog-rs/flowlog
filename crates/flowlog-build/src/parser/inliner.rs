//! Component inliner — eliminates `.comp` / `.init` before typechecking.
//!
//! ```text
//! .comp Container<T> { .decl Holds(x: T)  Holds(x) :- Source(x). }
//! .init c = Container<symbol>
//! ```
//!
//! becomes
//!
//! ```text
//! .decl c.holds(x: symbol)
//! c.holds(x) :- Source(x).
//! ```
//!
//! Per-instance types register into the program's existing
//! [`TypeRegistry`] under a prefixed name; the typechecker runs
//! unmodified against the lowered program.

use std::collections::{HashMap, HashSet};

use crate::common::Span;
use crate::parser::declaration::{
    Attribute, CompDecl, InitDecl, RawItem, RawTypeOp, Relation, SuperRef,
};
use crate::parser::error::ParseError;
use crate::parser::logic::{FlowLogRule, Predicate};
use crate::parser::primitive::TypeRegistry;

/// Output of inlining one `.init`.
#[derive(Default)]
pub(crate) struct InlinerOutput {
    pub(crate) relations: Vec<Relation>,
    pub(crate) rules: Vec<FlowLogRule>,
    pub(crate) facts: Vec<FlowLogRule>,
}

// =============================================================================
// Core recursion
// =============================================================================

/// Per-comp-body resolution context. Threaded through the type and
/// relation resolvers so callers stay one-line.
struct Scope<'a> {
    env: &'a HashMap<String, String>,
    prefix: &'a str,
    local_decls: &'a HashSet<String>,
    nested_inits: &'a HashSet<String>,
}

pub(crate) fn inline_one(
    parent_prefix: &str,
    init: InitDecl,
    comps: &mut HashMap<String, CompDecl>,
    output: &mut InlinerOutput,
    registry: &mut TypeRegistry,
) -> Result<(), ParseError> {
    let prefix = qualify(parent_prefix, &init.instance);

    let comp = comps
        .get(&init.comp)
        .cloned()
        .ok_or_else(|| ParseError::UnknownComponent {
            span: init.span,
            name: init.comp.clone(),
        })?;

    if comp.type_params.len() != init.args.len() {
        return Err(ParseError::ComponentArityMismatch {
            span: init.span,
            name: init.comp,
            expected: comp.type_params.len(),
            found: init.args.len(),
        });
    }

    let env: HashMap<String, String> = comp.type_params.iter().cloned().zip(init.args).collect();

    let mut inheritance_stack = HashSet::new();
    let body = resolve_inheritance(&comp, &env, comps, &mut inheritance_stack)?;

    // Index local decls / nested-init names and hoist nested `.comp`
    // decls so subsequent nested `.init`s can resolve them.
    let mut local_decls = HashSet::new();
    let mut nested_inits = HashSet::new();
    for item in &body {
        match item {
            RawItem::Decl(r) => {
                local_decls.insert(r.name.to_lowercase());
            }
            RawItem::Init(j) => {
                nested_inits.insert(j.instance.to_lowercase());
            }
            RawItem::Comp(nested) => {
                let mangled = qualify(&prefix, &nested.name);
                comps.insert(
                    mangled.clone(),
                    CompDecl {
                        name: mangled,
                        ..nested.clone()
                    },
                );
            }
            _ => {}
        }
    }

    let scope = Scope {
        env: &env,
        prefix: &prefix,
        local_decls: &local_decls,
        nested_inits: &nested_inits,
    };

    for item in body {
        match item {
            RawItem::Decl(raw) => {
                let prefixed = qualify(&prefix, &raw.name);
                let attrs = resolve_attributes(&raw.attrs, raw.span, &scope, registry)?;
                output
                    .relations
                    .push(Relation::from_components(&prefixed, attrs, raw.span));
            }
            RawItem::TypeAlias {
                name,
                op,
                parent,
                span,
            } => {
                let prefixed = qualify(&prefix, &name);
                let resolved = resolve_type_str(&parent, &scope);
                match op {
                    RawTypeOp::Alias => registry.register_alias(&prefixed, &resolved, span)?,
                    RawTypeOp::Subtype => registry.register_subtype(&prefixed, &resolved, span)?,
                };
            }
            RawItem::Rule(mut rule) => {
                rewrite_rule(&mut rule, &scope)?;
                output.rules.push(rule);
            }
            RawItem::Fact(mut fact) => {
                rewrite_rule(&mut fact, &scope)?;
                output.facts.push(fact);
            }
            RawItem::Input { name, params, span } => {
                resolve_directive_target(&name, span, &scope, &mut output.relations)?
                    .set_input_params(params);
            }
            RawItem::Output { name, params, span } => {
                let rel = resolve_directive_target(&name, span, &scope, &mut output.relations)?;
                rel.set_output(true);
                if !params.is_empty() {
                    rel.set_output_params(params)?;
                }
            }
            RawItem::Printsize { name, span } => {
                resolve_directive_target(&name, span, &scope, &mut output.relations)?
                    .set_printsize(true);
            }
            RawItem::Init(nested) => {
                inline_one(&prefix, resolve_init(nested, &env), comps, output, registry)?;
            }
            RawItem::Comp(_) => {} // already hoisted above
        }
    }

    Ok(())
}

/// Resolve a `.decl`'s attribute list against the current scope: each
/// attribute's type string is substituted through `env`/prefix/locals
/// and looked up in the registry to obtain a `TypeId` + primitive.
fn resolve_attributes(
    attrs: &[(String, String)],
    span: Span,
    scope: &Scope<'_>,
    registry: &TypeRegistry,
) -> Result<Vec<Attribute>, ParseError> {
    attrs
        .iter()
        .map(|(aname, tname)| {
            let resolved = resolve_type_str(tname, scope);
            let tid =
                registry
                    .lookup(&resolved)
                    .ok_or_else(|| ParseError::UnknownAttributeType {
                        span,
                        name: resolved.clone(),
                    })?;
            Ok(Attribute::with_type(
                aname.clone(),
                registry.root_primitive(tid),
                tid,
            ))
        })
        .collect()
}

/// Substitute `env` into a nested `.init`'s comp name and type args so
/// that outer type-params propagate through to the recursive call.
fn resolve_init(init: InitDecl, env: &HashMap<String, String>) -> InitDecl {
    InitDecl {
        instance: init.instance,
        comp: subst(env, &init.comp),
        args: init.args.iter().map(|a| subst(env, a)).collect(),
        span: init.span,
    }
}

// =============================================================================
// Inheritance
// =============================================================================

fn resolve_inheritance(
    comp: &CompDecl,
    env: &HashMap<String, String>,
    comps: &HashMap<String, CompDecl>,
    stack: &mut HashSet<String>,
) -> Result<Vec<RawItem>, ParseError> {
    if !stack.insert(comp.name.clone()) {
        return Err(ParseError::CircularInheritance {
            span: comp.span,
            name: comp.name.clone(),
        });
    }

    let mut result = Vec::new();
    if let Some(super_ref) = &comp.supertype {
        let SuperRef {
            name: super_name,
            args: super_args,
            span: super_span,
        } = super_ref;
        let super_comp = comps
            .get(super_name)
            .ok_or_else(|| ParseError::UnknownComponent {
                span: *super_span,
                name: super_name.clone(),
            })?;
        if super_comp.type_params.len() != super_args.len() {
            return Err(ParseError::ComponentArityMismatch {
                span: *super_span,
                name: super_name.clone(),
                expected: super_comp.type_params.len(),
                found: super_args.len(),
            });
        }
        let resolved_args: Vec<String> = super_args.iter().map(|a| subst(env, a)).collect();
        let super_env: HashMap<String, String> = super_comp
            .type_params
            .iter()
            .cloned()
            .zip(resolved_args)
            .collect();
        for item in resolve_inheritance(super_comp, &super_env, comps, stack)? {
            result.push(substitute_in_raw_item(item, &super_env));
        }
    }
    result.extend(comp.body.iter().cloned());

    stack.remove(&comp.name);
    Ok(result)
}

/// Substitute supertype type-parameter names in raw body items at the
/// splice site. Rules, facts, and directives carry no type-name
/// references at substitution sites — nested comps re-enter the
/// inliner later and get substituted then.
fn substitute_in_raw_item(item: RawItem, env: &HashMap<String, String>) -> RawItem {
    match item {
        RawItem::Decl(mut r) => {
            for (_, t) in r.attrs.iter_mut() {
                *t = subst(env, t);
            }
            RawItem::Decl(r)
        }
        RawItem::TypeAlias {
            name,
            op,
            parent,
            span,
        } => RawItem::TypeAlias {
            name,
            op,
            parent: subst(env, &parent),
            span,
        },
        RawItem::Init(init) => RawItem::Init(resolve_init(init, env)),
        other => other,
    }
}

// =============================================================================
// Name & type resolution helpers
// =============================================================================

fn qualify(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

/// Look up `s` in `env`, falling back to a fresh `s.to_string()`.
fn subst(env: &HashMap<String, String>, s: &str) -> String {
    env.get(s).cloned().unwrap_or_else(|| s.to_string())
}

/// Resolve a type-reference string (an attribute type, an alias parent,
/// or a `.type` parent). Cases:
///
/// 1. exact match against a type-param → bound value
/// 2. dotted, head matches a nested-init → `prefix.head.rest`
/// 3. dotted, head matches a type-param → `bound.rest`
/// 4. single segment local-alias declared inside this comp → `prefix.name`
/// 5. otherwise → unchanged, resolved later via the global registry
fn resolve_type_str(s: &str, scope: &Scope<'_>) -> String {
    if let Some(bound) = scope.env.get(s) {
        return bound.clone();
    }
    if let Some((head, rest)) = s.split_once('.') {
        if scope.nested_inits.contains(&head.to_lowercase()) {
            return format!("{}.{}.{}", scope.prefix, head, rest);
        }
        if let Some(bound) = scope.env.get(head) {
            return format!("{bound}.{rest}");
        }
        return s.to_string();
    }
    if scope.local_decls.contains(&s.to_lowercase()) {
        return qualify(scope.prefix, s);
    }
    s.to_string()
}

/// Resolve a relation reference (head, body atom, or directive target).
///
/// Strict on dotted refs: the head segment must be a nested-init in
/// scope, otherwise reject with [`ParseError::UnresolvedQualifiedRef`].
fn resolve_relation_ref(name: &str, span: Span, scope: &Scope<'_>) -> Result<String, ParseError> {
    if let Some((head, rest)) = name.split_once('.') {
        if scope.nested_inits.contains(&head.to_lowercase()) {
            return Ok(format!("{}.{}.{}", scope.prefix, head, rest));
        }
        return Err(ParseError::UnresolvedQualifiedRef {
            span,
            path: name.to_string(),
        });
    }
    if scope.local_decls.contains(&name.to_lowercase()) {
        return Ok(qualify(scope.prefix, name));
    }
    Ok(name.to_string())
}

fn rewrite_rule(rule: &mut FlowLogRule, scope: &Scope<'_>) -> Result<(), ParseError> {
    let head = rule.head_mut();
    let rewritten = resolve_relation_ref(head.name(), head.span(), scope)?;
    if rewritten != head.name() {
        head.set_name(rewritten);
    }
    for pred in rule.rhs_mut() {
        if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
            let rewritten = resolve_relation_ref(atom.name(), atom.span(), scope)?;
            if rewritten != atom.name() {
                atom.set_name(rewritten);
            }
        }
    }
    Ok(())
}

/// Resolve an `.input` / `.output` / `.printsize` directive's target
/// relation. The target must already exist in `rels` (declared earlier
/// in this comp body, or by a nested `.init` we just expanded).
fn resolve_directive_target<'a>(
    name: &str,
    span: Span,
    scope: &Scope<'_>,
    rels: &'a mut [Relation],
) -> Result<&'a mut Relation, ParseError> {
    let resolved = resolve_relation_ref(name, span, scope)?;
    let target_lc = resolved.to_lowercase();
    rels.iter_mut()
        .find(|r| r.name() == target_lc)
        .ok_or(ParseError::UndeclaredInRule {
            span,
            name: resolved,
        })
}
