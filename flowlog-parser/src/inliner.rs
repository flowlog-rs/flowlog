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

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Span;

use crate::declaration::Attribute;
use crate::declaration::CompDecl;
use crate::declaration::InitDecl;
use crate::declaration::InputDirective;
use crate::declaration::OutputDirective;
use crate::declaration::PrintSizeDirective;
use crate::declaration::RawItem;
use crate::declaration::RawRelation;
use crate::declaration::RawTypeOp;
use crate::declaration::Relation;
use crate::declaration::SuperRef;
use crate::error::ParseError;
use crate::logic::FlowLogRule;
use crate::logic::Predicate;
use crate::primitive::TypeRegistry;

/// Output of inlining one `.init`.
#[derive(Default)]
pub(crate) struct InlinerOutput {
    pub(crate) relations: Vec<Relation>,
    pub(crate) rules: Vec<FlowLogRule>,
    pub(crate) facts: Vec<FlowLogRule>,
    /// Comp-internal directives whose target was not a relation of this
    /// instance — deferred for the driver to apply against the full
    /// (global + inlined) relation set via `apply_directives`.
    pub(crate) input_directives: Vec<InputDirective>,
    pub(crate) output_directives: Vec<OutputDirective>,
    pub(crate) printsize_directives: Vec<PrintSizeDirective>,
}

// =============================================================================
// Core recursion
// =============================================================================

/// Per-comp-body resolution context. Threaded through the type and
/// relation resolvers so callers stay one-line.
struct Scope<'a> {
    env: &'a HashMap<String, String>,
    prefix: &'a str,
    /// Simple name of the instance being inlined (the last segment of
    /// `prefix`). A dotted type whose head equals this is a
    /// self-reference to the instance's own member types.
    instance: &'a str,
    local_decls: &'a HashSet<String>,
    /// Names of `.type` aliases declared in this comp body. A bare
    /// (unqualified) type name matching one resolves to the prefixed
    /// alias, which `collect_instance` registered under the instance.
    local_types: &'a HashSet<String>,
    nested_inits: &'a HashSet<String>,
    /// Instances visible in the *enclosing* scope where this instance was
    /// instantiated (its sibling / global `.init`s), mapped from name to
    /// the instance's absolute prefix. A dotted relation/type head that is
    /// not a nested `.init` may resolve to one of these (Soufflé
    /// sibling-scope visibility, e.g. `basic.SubtypeOf`).
    enclosing_instances: &'a HashMap<String, String>,
    /// Relations declared in the *enclosing* component instances (and up the
    /// instantiation chain), mapped from lowercase simple name to the
    /// absolute qualified relation name. A bare relation reference that is
    /// not local to this component resolves against these — Soufflé's
    /// lexical scoping lets a nested instance's rules name a relation
    /// declared in the component it was instantiated within (e.g. a
    /// `configuration` sub-instance referencing the enclosing analysis's
    /// `isImmutableHContext`).
    enclosing_decls: &'a HashMap<String, String>,
}

pub(crate) fn inline_one(
    parent_prefix: &str,
    enclosing: &HashMap<String, String>,
    enclosing_decls: &HashMap<String, String>,
    init: InitDecl,
    comps: &mut HashMap<String, CompDecl>,
    output: &mut InlinerOutput,
    registry: &mut TypeRegistry,
) -> Result<(), ParseError> {
    let prefix = qualify(parent_prefix, &init.instance);
    let instance = init.instance;

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
    let mut local_types = HashSet::new();
    let mut nested_inits = HashSet::new();
    for item in &body {
        match item {
            RawItem::Decl(r) => {
                local_decls.insert(r.name.to_lowercase());
            }
            RawItem::TypeAlias { name, .. } => {
                local_types.insert(name.to_lowercase());
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
        instance: &instance,
        local_decls: &local_decls,
        local_types: &local_types,
        nested_inits: &nested_inits,
        enclosing_instances: enclosing,
        enclosing_decls,
    };

    // Instances visible to a nested `.init` in this body: everything
    // visible to *this* instance (its enclosing scope) plus this body's
    // own nested `.init`s, each keyed by name to its absolute prefix.
    let mut child_enclosing = enclosing.clone();
    for name in &nested_inits {
        child_enclosing.insert(name.clone(), qualify(&prefix, name));
    }

    // Relations visible to a nested `.init`: those visible to this instance
    // plus this body's own `.decl`s, qualified under this instance's prefix.
    // A nested instance's bare relation references resolve against these.
    let mut child_enclosing_decls = enclosing_decls.clone();
    for name in &local_decls {
        child_enclosing_decls.insert(name.clone(), qualify(&prefix, name));
    }

    // Resolution proceeds in two walks of the body, NOT in textual order.
    // INVARIANT: `collect_instance` fully populates this instance's symbol
    // table — its registered member `.type`s plus the `local_decls` /
    // `nested_inits` already indexed above — before `resolve_instance`
    // resolves a single `.decl` attribute, rule, or directive. This makes
    // attribute and relation resolution independent of where a `.decl` sits
    // relative to the `.init`/`.type` it depends on. Keep the two walks
    // separate: merging them would reintroduce that order-dependence.
    //
    // NOTE: this does NOT make a `.type` alias's *parent* order-independent.
    // `collect_instance` registers aliases in body order with eager parent
    // resolution, so a member alias must be declared after the type it
    // references (`.type B = A` requires `A` earlier) — the same
    // define-before-use rule top-level `.type`s follow (see
    // `build_type_registry` in program.rs). Cycles surface as
    // `UnknownTypeParent`, not a hang.
    collect_instance(
        &body,
        &scope,
        &child_enclosing,
        &child_enclosing_decls,
        comps,
        output,
        registry,
    )?;
    resolve_instance(body, &scope, output, registry)?;

    Ok(())
}

/// First walk — build this instance's symbol table. Recursively inline
/// nested `.init`s (registering their member `.type`s under the instance
/// prefix) and register local `.type` aliases. Aliases follow the inits
/// because an alias may reference a nested instance's member type. After
/// this returns, every member type referenced in the body is registered.
fn collect_instance(
    body: &[RawItem],
    scope: &Scope<'_>,
    child_enclosing: &HashMap<String, String>,
    child_enclosing_decls: &HashMap<String, String>,
    comps: &mut HashMap<String, CompDecl>,
    output: &mut InlinerOutput,
    registry: &mut TypeRegistry,
) -> Result<(), ParseError> {
    for item in body {
        if let RawItem::Init(nested) = item {
            inline_one(
                scope.prefix,
                child_enclosing,
                child_enclosing_decls,
                resolve_init(nested.clone(), scope.env),
                comps,
                output,
                registry,
            )?;
        }
    }
    for item in body {
        if let RawItem::TypeAlias {
            name,
            op,
            parent,
            span,
        } = item
        {
            let prefixed = qualify(scope.prefix, name);
            match op {
                RawTypeOp::Alias => {
                    let resolved = resolve_qualified(parent, *span, scope, false)?;
                    registry.register_alias(&prefixed, &resolved, *span)?;
                }
                RawTypeOp::Subtype => {
                    let resolved = resolve_qualified(parent, *span, scope, false)?;
                    registry.register_subtype(&prefixed, &resolved, *span)?;
                }
                RawTypeOp::Tuple(fields) => {
                    // Resolve each field type against the instance scope, then register.
                    let resolved_fields = fields
                        .iter()
                        .map(|(fname, ftype)| {
                            Ok::<_, ParseError>((
                                fname.clone(),
                                resolve_qualified(ftype, *span, scope, false)?,
                            ))
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    registry.register_tuple(&prefixed, &resolved_fields, *span)?;
                }
            };
        }
    }
    Ok(())
}

/// Second walk — resolve against the now-complete symbol table. Declares
/// relations (attribute types all resolve regardless of source order),
/// then rewrites rules / facts and applies `.input`/`.output`/`.printsize`
/// directives over the relations just declared.
fn resolve_instance(
    body: Vec<RawItem>,
    scope: &Scope<'_>,
    output: &mut InlinerOutput,
    registry: &TypeRegistry,
) -> Result<(), ParseError> {
    for item in &body {
        if let RawItem::Decl(raw) = item {
            let prefixed = qualify(scope.prefix, &raw.name);
            let attrs = resolve_attributes(&raw.attrs, raw.span, scope, registry)?;
            output
                .relations
                .push(Relation::from_components(&prefixed, attrs, raw.span));
        }
    }

    for item in body {
        match item {
            RawItem::Rule(mut rule) => {
                rewrite_rule(&mut rule, scope)?;
                output.rules.push(rule);
            }
            RawItem::Fact(mut fact) => {
                rewrite_rule(&mut fact, scope)?;
                output.facts.push(fact);
            }
            // A directive targets a relation of this instance, or — like a
            // rule-body reference — one in the enclosing/global scope. When
            // the target is not local, defer it so the driver applies it
            // against the full relation set (`apply_directives`).
            RawItem::Input { name, params, span } => {
                let lc = resolve_qualified(&name, span, scope, true)?.to_lowercase();
                match output.relations.iter_mut().find(|r| r.name() == lc) {
                    Some(rel) => rel.set_input_params(params),
                    None => output
                        .input_directives
                        .push(InputDirective::new(lc, params, span)),
                }
            }
            RawItem::Output { name, params, span } => {
                let lc = resolve_qualified(&name, span, scope, true)?.to_lowercase();
                match output.relations.iter_mut().find(|r| r.name() == lc) {
                    Some(rel) => {
                        rel.set_output(true);
                        if !params.is_empty() {
                            rel.set_output_params(params)?;
                        }
                    }
                    None => output
                        .output_directives
                        .push(OutputDirective::new(lc, params, span)),
                }
            }
            RawItem::Printsize { name, span } => {
                let lc = resolve_qualified(&name, span, scope, true)?.to_lowercase();
                match output.relations.iter_mut().find(|r| r.name() == lc) {
                    Some(rel) => rel.set_printsize(true),
                    None => output
                        .printsize_directives
                        .push(PrintSizeDirective::new(lc, span)),
                }
            }
            // Decl / TypeAlias / Init handled in `collect_instance`; Comp
            // hoisted before the walks; Override stripped in inheritance.
            _ => {}
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
            let resolved = resolve_qualified(tname, span, scope, false)?;
            let tid =
                registry
                    .lookup(&resolved)
                    .ok_or_else(|| ParseError::UnknownAttributeType {
                        span,
                        name: resolved.clone(),
                    })?;
            // Recursive tuples were rejected at registration, so this erases to
            // a finite fixed tuple.
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

    let mut inherited = Vec::new();
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
            inherited.push(substitute_in_raw_item(item, &super_env));
        }
    }

    let overrides = collect_overrides(&comp.body);
    validate_overrides(&overrides, &inherited, &comp.body)?;

    let mut result = Vec::with_capacity(inherited.len() + comp.body.len());
    for item in inherited {
        if is_overridden_rule_or_fact(&item, &overrides) {
            continue;
        }
        result.push(item);
    }
    // Own body. Strip `.override` directives — they have served their
    // purpose above and must not reach `inline_one` or any further
    // ancestor splice (e.g. if this comp is itself inherited later).
    for item in &comp.body {
        if matches!(item, RawItem::Override { .. }) {
            continue;
        }
        result.push(item.clone());
    }

    stack.remove(&comp.name);
    Ok(result)
}

/// Map of override-target-name → `(declaration span, raw spelling)`,
/// keyed by the canonical (lowercased) name. Two `.override Foo`
/// directives in the same comp collapse to one entry
/// (Soufflé-compatible). The raw spelling is kept for diagnostics.
fn collect_overrides(body: &[RawItem]) -> HashMap<String, (Span, String)> {
    let mut out: HashMap<String, (Span, String)> = HashMap::new();
    for item in body {
        if let RawItem::Override { name, span } = item {
            out.entry(name.to_lowercase())
                .or_insert_with(|| (*span, name.clone()));
        }
    }
    out
}

fn validate_overrides(
    overrides: &HashMap<String, (Span, String)>,
    inherited: &[RawItem],
    own_body: &[RawItem],
) -> Result<(), ParseError> {
    let inherited_decls = decl_map(inherited);
    let own_decls = decl_map(own_body);

    for (name_lc, (span, raw_name)) in overrides {
        // A local `.decl` would shadow the inherited one and make the
        // override target ambiguous — reject.
        if let Some(prior) = own_decls.get(name_lc.as_str()) {
            return Err(ParseError::OverrideRedeclaresRelation {
                span: *span,
                prior: prior.span,
                name: raw_name.clone(),
            });
        }

        let Some(decl) = inherited_decls.get(name_lc.as_str()) else {
            return Err(ParseError::OverrideUnknownRelation {
                span: *span,
                name: raw_name.clone(),
            });
        };
        if !decl.overridable {
            return Err(ParseError::OverrideOfNonOverridable {
                span: *span,
                prior: decl.span,
                name: raw_name.clone(),
            });
        }
    }
    Ok(())
}

/// Index a body's `.decl` items by their canonical (lowercased) name,
/// so `validate_overrides` can perform O(1) lookups instead of K×N
/// linear scans with per-comparison `to_lowercase()` allocations.
fn decl_map(items: &[RawItem]) -> HashMap<String, &RawRelation> {
    items
        .iter()
        .filter_map(|item| match item {
            RawItem::Decl(r) => Some((r.name.to_lowercase(), r)),
            _ => None,
        })
        .collect()
}

/// Whether an inherited `RawItem` is a rule or fact whose head matches
/// one of this comp's `.override` targets — if so, it gets dropped from
/// the spliced body and replaced by the comp's own derivations.
fn is_overridden_rule_or_fact(item: &RawItem, overrides: &HashMap<String, (Span, String)>) -> bool {
    if overrides.is_empty() {
        return false;
    }
    let head_name = match item {
        RawItem::Rule(r) | RawItem::Fact(r) => r.head().name(),
        _ => return false,
    };
    overrides.contains_key(head_name)
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

/// Resolve a qualified name against the current scope. One resolver
/// serves both namespaces; `strict` selects which:
///
/// - **types** (`strict = false`): attribute types, alias / `.type`
///   parents. Lenient — an unrecognised name passes through to be
///   resolved later by the global [`TypeRegistry`].
/// - **relations** (`strict = true`): rule heads, body atoms, directive
///   targets. A dotted ref whose head is not a nested-init is rejected
///   with [`ParseError::UnresolvedQualifiedRef`].
///
/// Resolution cases, in precedence order:
/// 1. *(types only)* exact match against a type-param → bound value
/// 2. dotted, head matches a nested-init → `prefix.head.rest`
/// 3. dotted, head matches a sibling/enclosing-scope instance →
///    `that-instance-prefix.rest` (e.g. `basic.SubtypeOf` inside a
///    component instantiated alongside the global `.init basic`)
/// 4. *(types only)* dotted, head matches this instance's own name →
///    `prefix.rest` (self-reference: the member type is supplied by this
///    instance's own/inherited `.type`s, e.g. `configuration.Context`
///    written inside the component instantiated as `configuration`)
/// 5. *(types only)* dotted, head matches a type-param → `bound.rest`
/// 6. dotted, none of the above → pass through (types) / error (relations)
/// 7. single segment matching a local `.decl`, or *(types only)* a local
///    `.type` alias declared in this comp → `prefix.name`
/// 8. *(relations only)* single segment matching a relation declared in an
///    enclosing instance → that instance's qualified name
/// 9. otherwise → unchanged, resolved later via the global registry
///
/// Cases 1/4/5 and the alias half of 7 are gated on `!strict` so the
/// relation path resolves a dotted head only via a nested-init (2) or a
/// sibling/enclosing instance (3). The nested-init check (2) precedes the
/// enclosing check (3) so an inner instance shadows an outer one of the
/// same name.
fn resolve_qualified(
    s: &str,
    span: Span,
    scope: &Scope<'_>,
    strict: bool,
) -> Result<String, ParseError> {
    if !strict && let Some(bound) = scope.env.get(s) {
        return Ok(bound.clone());
    }
    if let Some((head, rest)) = s.split_once('.') {
        let head_lc = head.to_lowercase();
        if scope.nested_inits.contains(&head_lc) {
            return Ok(format!("{}.{}.{}", scope.prefix, head, rest));
        }
        if let Some(inst_prefix) = scope.enclosing_instances.get(&head_lc) {
            return Ok(format!("{inst_prefix}.{rest}"));
        }
        if !strict {
            if head.eq_ignore_ascii_case(scope.instance) {
                return Ok(format!("{}.{}", scope.prefix, rest));
            }
            if let Some(bound) = scope.env.get(head) {
                return Ok(format!("{bound}.{rest}"));
            }
            return Ok(s.to_string());
        }
        return Err(ParseError::UnresolvedQualifiedRef {
            span,
            path: s.to_string(),
        });
    }
    let key = s.to_lowercase();
    if scope.local_decls.contains(&key) || (!strict && scope.local_types.contains(&key)) {
        return Ok(qualify(scope.prefix, s));
    }
    // A bare relation reference that isn't local resolves to a relation
    // declared in an enclosing instance, if one exists (Soufflé lexical
    // scoping). Types are excluded — they fall through to the registry.
    if strict && let Some(qualified) = scope.enclosing_decls.get(&key) {
        return Ok(qualified.clone());
    }
    Ok(s.to_string())
}

fn rewrite_rule(rule: &mut FlowLogRule, scope: &Scope<'_>) -> Result<(), ParseError> {
    // Resolve on `raw_name()`, not `name()`: `resolve_qualified` reports its
    // input verbatim in `UnresolvedQualifiedRef`, so feeding it the surface
    // spelling makes that diagnostic echo what the user wrote. The result is
    // canonicalized here — as the directive callers also do — so the
    // `!= name()` check compares like with like.
    let head = rule.head_mut();
    let rewritten = resolve_qualified(head.raw_name(), head.span(), scope, true)?.to_lowercase();
    if rewritten != head.name() {
        head.set_name(rewritten);
    }
    for pred in rule.rhs_mut() {
        if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
            let rewritten =
                resolve_qualified(atom.raw_name(), atom.span(), scope, true)?.to_lowercase();
            if rewritten != atom.name() {
                atom.set_name(rewritten);
            }
        }
    }
    Ok(())
}
