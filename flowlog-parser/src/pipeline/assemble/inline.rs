//! Component inliner: expands `.comp` / `.init` into concrete relations and rules.
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

use flowlog_error::Span;

use crate::ast::FlowLogRule;
use crate::ast::Predicate;
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
use crate::segment::Segment;
use crate::types::TypeRegistry;

/// Output of inlining one `.init`.
#[derive(Default)]
pub(crate) struct InlinerOutput {
    pub(crate) relations: Vec<Relation>,
    pub(crate) rules: Vec<FlowLogRule>,
    pub(crate) facts: Vec<FlowLogRule>,
    /// Comp-internal directives whose target is not a relation of this
    /// instance: deferred, to be applied against the full (global + inlined)
    /// relation set once all instances are inlined.
    pub(crate) input_directives: Vec<InputDirective>,
    pub(crate) output_directives: Vec<OutputDirective>,
    pub(crate) printsize_directives: Vec<PrintSizeDirective>,
}

// =============================================================================
// Core recursion
// =============================================================================

/// Per-comp-body resolution context: the scope parameters for type and
/// relation resolution, bundled so they pass as a single argument.
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
    /// not a nested `.init` may resolve to one of these (Souffle
    /// sibling-scope visibility, e.g. `basic.SubtypeOf`).
    enclosing_instances: &'a HashMap<String, String>,
    /// Relations declared in the *enclosing* component instances (and up the
    /// instantiation chain), mapped from lowercase simple name to the
    /// absolute qualified relation name. A bare relation reference that is
    /// not local to this component resolves against these: Souffle's
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
    // table: its registered member `.type`s plus the `local_decls` /
    // `nested_inits` already indexed above: before `resolve_instance`
    // resolves a single `.decl` attribute, rule, or directive. This makes
    // attribute and relation resolution independent of where a `.decl` sits
    // relative to the `.init`/`.type` it depends on. Keep the two walks
    // separate: merging them would reintroduce that order-dependence.
    //
    // NOTE: this does NOT make a `.type` alias's *parent* order-independent.
    // `collect_instance` registers aliases in body order with eager parent
    // resolution, so a member alias must be declared after the type it
    // references (`.type B = A` requires `A` earlier): the same
    // define-before-use rule top-level `.type`s follow (see
    // `TypeRegistry::from_type_declarations`). Cycles surface as
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

/// First walk: build this instance's symbol table. Recursively inline
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

/// Second walk: resolve against the now-complete symbol table. Declares
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
            // A directive targets a relation of this instance, or: like a
            // rule-body reference: one in the enclosing/global scope. When
            // the target is not local, defer it so the driver applies it
            // against the full relation set (`apply_directives`).
            RawItem::Input { name, params, span } => {
                let lc = resolve_qualified(&name, span, scope, true)?.to_lowercase();
                match output.relations.iter_mut().find(|r| r.name() == lc) {
                    Some(rel) => rel.set_input(&params, span)?,
                    None => output
                        .input_directives
                        .push(InputDirective::new(lc, params, span)),
                }
            }
            RawItem::Output { name, params, span } => {
                let lc = resolve_qualified(&name, span, scope, true)?.to_lowercase();
                match output.relations.iter_mut().find(|r| r.name() == lc) {
                    Some(rel) => rel.set_output(&params, span)?,
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
        comp: apply_type_env(env, &init.comp),
        args: init.args.iter().map(|a| apply_type_env(env, a)).collect(),
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
        let resolved_args: Vec<String> =
            super_args.iter().map(|a| apply_type_env(env, a)).collect();
        let super_env: HashMap<String, String> = super_comp
            .type_params
            .iter()
            .cloned()
            .zip(resolved_args)
            .collect();
        for item in resolve_inheritance(super_comp, &super_env, comps, stack)? {
            inherited.push(apply_type_env_to_item(item, &super_env));
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
    // Own body. Strip `.override` directives: they have served their
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

/// Map of override-target-name -> `(declaration span, raw spelling)`,
/// keyed by the canonical (lowercased) name. Two `.override Foo`
/// directives in the same comp collapse to one entry
/// (Souffle-compatible). The raw spelling is kept for diagnostics.
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
        // override target ambiguous: reject.
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

/// Index a body's `.decl` items by their canonical (lowercased) name, for
/// O(1) override lookups instead of repeated linear scans with per-comparison
/// `to_lowercase()` allocations.
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
/// one of this comp's `.override` targets: if so, it gets dropped from
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

/// Apply the type-parameter env to a raw body item at the supertype splice
/// site. Rules, facts, and directives carry no type-name references here;
/// nested comps re-enter the inliner later and bind then.
fn apply_type_env_to_item(item: RawItem, env: &HashMap<String, String>) -> RawItem {
    match item {
        RawItem::Decl(mut r) => {
            for (_, t) in r.attrs.iter_mut() {
                *t = apply_type_env(env, t);
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
            parent: apply_type_env(env, &parent),
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

/// Resolve `s` against the type-parameter env, leaving any name that is not a
/// bound parameter unchanged.
fn apply_type_env(env: &HashMap<String, String>, s: &str) -> String {
    env.get(s).cloned().unwrap_or_else(|| s.to_string())
}

/// Resolve a qualified name against the current scope. One resolver
/// serves both namespaces; `strict` selects which:
///
/// - **types** (`strict = false`): attribute types, alias / `.type`
///   parents. Lenient: an unrecognised name passes through to be
///   resolved later by the global [`TypeRegistry`].
/// - **relations** (`strict = true`): rule heads, body atoms, directive
///   targets. A dotted ref whose head is not a nested-init is rejected
///   with [`ParseError::UnresolvedQualifiedRef`].
///
/// Resolution cases, in precedence order:
/// 1. *(types only)* exact match against a type-param -> bound value
/// 2. dotted, head matches a nested-init -> `prefix.head.rest`
/// 3. dotted, head matches a sibling/enclosing-scope instance ->
///    `that-instance-prefix.rest` (e.g. `basic.SubtypeOf` inside a
///    component instantiated alongside the global `.init basic`)
/// 4. *(types only)* dotted, head matches this instance's own name ->
///    `prefix.rest` (self-reference: the member type is supplied by this
///    instance's own/inherited `.type`s, e.g. `configuration.Context`
///    written inside the component instantiated as `configuration`)
/// 5. *(types only)* dotted, head matches a type-param -> `bound.rest`
/// 6. dotted, none of the above -> pass through (types) / error (relations)
/// 7. single segment matching a local `.decl`, or *(types only)* a local
///    `.type` alias declared in this comp -> `prefix.name`
/// 8. *(relations only)* single segment matching a relation declared in an
///    enclosing instance -> that instance's qualified name
/// 9. otherwise -> unchanged, resolved later via the global registry
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
    // declared in an enclosing instance, if one exists (Souffle lexical
    // scoping). Types are excluded: they fall through to the registry.
    if strict && let Some(qualified) = scope.enclosing_decls.get(&key) {
        return Ok(qualified.clone());
    }
    Ok(s.to_string())
}

fn rewrite_rule(rule: &mut FlowLogRule, scope: &Scope<'_>) -> Result<(), ParseError> {
    // Resolve on `raw_name()`, not `name()`: `resolve_qualified` reports its
    // input verbatim in `UnresolvedQualifiedRef`, so feeding it the surface
    // spelling makes that diagnostic echo what the user wrote. The result is
    // canonicalized here: as the directive callers also do: so the
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

/// Replace the `.` in every dotted, inliner-produced relation name with
/// [`INLINER_SEP`], across relation declarations, rule heads, body atoms, and
/// facts.
pub(super) fn normalize_dots(
    relations: &mut [Relation],
    segments: &mut [Segment],
    raw_facts: &mut [FlowLogRule],
) {
    for rel in relations.iter_mut() {
        if rel.name().contains('.') {
            let renamed = rel.raw_name().replace('.', INLINER_SEP);
            rel.set_name(renamed);
        }
    }
    for_each_rule_mut(segments, normalize_rule_dots);
    for fact in raw_facts.iter_mut() {
        normalize_rule_dots(fact);
    }
}

fn normalize_rule_dots(rule: &mut FlowLogRule) {
    let head = rule.head_mut();
    if head.name().contains('.') {
        head.set_name(head.name().replace('.', INLINER_SEP));
    }
    for pred in rule.rhs_mut() {
        if let Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) = pred
            && a.name().contains('.')
        {
            a.set_name(a.name().replace('.', INLINER_SEP));
        }
    }
}

/// Separator for inliner-prefixed relation names, replacing the user's `.`
/// (`c.holds` becomes `c\u{b7}holds`). `\u{b7}` (U+00B7) is in Unicode's
/// XID_Continue, so the result stays a valid Rust 2021 identifier; the FlowLog
/// grammar's `identifier` is ASCII-only, so it can never collide with a
/// user-written name.
const INLINER_SEP: &str = "\u{b7}";

/// Apply `f` to every rule in every segment, including rules nested
/// inside loop/fixpoint blocks.
fn for_each_rule_mut<F>(segments: &mut [Segment], mut f: F)
where
    F: FnMut(&mut FlowLogRule),
{
    for seg in segments.iter_mut() {
        let rules: &mut [FlowLogRule] = match seg {
            Segment::Plain(rs) => rs.as_mut_slice(),
            Segment::Loop(b) | Segment::Fixpoint(b) => b.rules_mut(),
        };
        for rule in rules {
            f(rule);
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;

    use super::*;
    use crate::DataType;
    use crate::Node;
    use crate::Predicate;
    use crate::Program;
    use crate::Rule;
    use crate::StopRelation;
    use crate::assert_err;
    use crate::test_util::assembled;
    use crate::test_util::parse_pair;

    fn init(instance: &str, comp: &str, args: &[&str]) -> InitDecl {
        InitDecl {
            instance: instance.to_string(),
            comp: comp.to_string(),
            args: args.iter().map(|s| (*s).to_string()).collect(),
            span: Span::DUMMY,
        }
    }

    fn comp(name: &str, type_params: &[&str], supertype: Option<SuperRef>) -> CompDecl {
        CompDecl {
            name: name.to_string(),
            type_params: type_params.iter().map(|s| (*s).to_string()).collect(),
            supertype,
            body: vec![],
            span: Span::DUMMY,
        }
    }

    /// Inline `init` against `comps`, discarding the emitted output.
    fn inline(init: InitDecl, comps: &mut HashMap<String, CompDecl>) -> Result<(), ParseError> {
        inline_one(
            "",
            &HashMap::new(),
            &HashMap::new(),
            init,
            comps,
            &mut InlinerOutput::default(),
            &mut TypeRegistry::new(),
        )
    }

    #[test]
    fn inline_of_unknown_component_is_rejected() {
        assert_err!(
            inline(init("c", "Container", &[]), &mut HashMap::new()),
            ParseError::UnknownComponent { .. }
        );
    }

    #[test]
    fn inline_with_wrong_type_arg_count_is_rejected() {
        let mut comps = HashMap::from([("Pair".to_string(), comp("Pair", &["T"], None))]);
        assert_err!(
            inline(init("p", "Pair", &["number", "symbol"]), &mut comps),
            ParseError::ComponentArityMismatch { .. }
        );
    }

    #[test]
    fn inline_with_circular_inheritance_is_rejected() {
        let sref = |n: &str| SuperRef {
            name: n.to_string(),
            args: vec![],
            span: Span::DUMMY,
        };
        let mut comps = HashMap::from([
            ("A".to_string(), comp("A", &[], Some(sref("B")))),
            ("B".to_string(), comp("B", &[], Some(sref("A")))),
        ]);
        assert_err!(
            inline(init("c", "A", &[]), &mut comps),
            ParseError::CircularInheritance { .. }
        );
    }

    /// A rule inside a comp body referencing `cfg.X`, where `cfg` is neither a
    /// nested `.init` instance nor a bound type-param, is an unresolved
    /// qualified reference. Parse a realistic comp, then inline it.
    #[test]
    fn inline_with_unresolved_qualified_ref_is_rejected() {
        let holder = CompDecl::from_parsed_rule(Node::new(
            parse_pair(
                Rule::comp_decl,
                ".comp Holder {\n  .decl R(x: symbol)\n  R(x) :- cfg.X(x).\n}",
            ),
            FileId::new(0),
        ))
        .expect("comp parses");
        let mut comps = HashMap::from([("Holder".to_string(), holder)]);
        assert_err!(
            inline(init("h", "Holder", &[]), &mut comps),
            ParseError::UnresolvedQualifiedRef { .. }
        );
    }

    // --- `.override` validation (`validate_overrides`) ---

    fn raw_decl(name: &str, overridable: bool) -> RawItem {
        RawItem::Decl(RawRelation {
            name: name.to_string(),
            attrs: vec![],
            overridable,
            span: Span::DUMMY,
        })
    }

    /// The `.override <name>` map `validate_overrides` consumes, built through
    /// the real `collect_overrides` collapse.
    fn override_of(name: &str) -> HashMap<String, (Span, String)> {
        collect_overrides(&[RawItem::Override {
            name: name.to_string(),
            span: Span::DUMMY,
        }])
    }

    /// `.override Foo` alongside a *local* `.decl Foo`: the local decl would
    /// shadow the inherited target, making the override ambiguous.
    #[test]
    fn override_that_locally_redeclares_the_target_is_rejected() {
        assert_err!(
            validate_overrides(
                &override_of("Foo"),
                &[raw_decl("Foo", true)],
                &[raw_decl("Foo", true)],
            ),
            ParseError::OverrideRedeclaresRelation { .. }
        );
    }

    /// `.override Foo` with no inherited `Foo` to override.
    #[test]
    fn override_of_an_unknown_relation_is_rejected() {
        assert_err!(
            validate_overrides(&override_of("Foo"), &[], &[]),
            ParseError::OverrideUnknownRelation { .. }
        );
    }

    /// `.override Foo` where the inherited `Foo` was not declared `overridable`.
    #[test]
    fn override_of_a_non_overridable_relation_is_rejected() {
        assert_err!(
            validate_overrides(&override_of("Foo"), &[raw_decl("Foo", false)], &[]),
            ParseError::OverrideOfNonOverridable { .. }
        );
    }

    // --- End-to-end inlining (parse a whole program, inspect the inlined result) ---
    //
    // The inliner rewrites user-written `.` to `\u{b7}` (U+00B7) in prefixed
    // relation names, so a `.init s = Sub` produces facts keyed by `s\u{b7}foo`
    // rather than `s.foo`. These tests assert against the post-inliner form.

    fn find_relation<'a>(program: &'a Program, name: &str) -> &'a Relation {
        program
            .relations()
            .iter()
            .find(|r| r.name() == name)
            .unwrap_or_else(|| panic!("relation `{name}` not found"))
    }

    /// First-column integer values of `rel`'s facts. The override tests assert
    /// by tuple value rather than by parsed-AST structure. Facts are read at the
    /// assembled (pre-type-check) stage, so their columns are still `IntLit`.
    fn fact_numbers(program: &Program, rel: &str) -> Vec<i64> {
        program
            .facts()
            .get(rel)
            .unwrap_or_else(|| panic!("no facts for `{rel}`"))
            .iter()
            .map(|fact| match fact.columns[0].ty() {
                DataType::IntLit => fact.columns[0].text().parse().expect("integer spelling"),
                other => panic!("expected number in `{rel}`, got {other:?}"),
            })
            .collect()
    }

    fn loop_blocks(program: &Program) -> Vec<&crate::segment::LoopBlock> {
        program
            .segments()
            .iter()
            .filter_map(|s| s.as_loop())
            .collect()
    }

    /// The inliner rewrites dotted instance names (`c.R` -> `c\u{b7}R`) on
    /// `name` for Rust ident safety, but leaves `raw_name` carrying the
    /// original literal-dot form: that's what the I/O sinks use for
    /// Souffle-style filenames (`c.R.csv`, not `c\u{b7}R.csv`).
    #[test]
    fn inlined_relation_raw_name_keeps_literal_dot() {
        let src = "
            .comp C {
              .decl R(x: symbol)
              .decl S(x: symbol)
              R(x) :- S(x).
              .output R
            }
            .init c = C
        ";
        let program = assembled(src).expect("assembles");
        let r = find_relation(&program, "c\u{b7}r");
        assert_eq!(r.name(), "c\u{b7}r");
        assert_eq!(r.raw_name(), "c.R");
    }

    /// An attribute typed `instance.Member` resolves even when the nested
    /// `.init` that supplies `Member` is declared *after* the `.decl` in the
    /// comp body: attribute-type resolution is independent of textual order.
    #[test]
    fn member_type_resolves_when_nested_init_follows_decl() {
        let src = "
            .type Value = symbol
            .comp Cfg { .type Context = symbol }
            .comp Analysis<Configuration> {
              .decl RunningThread(ctx:configuration.Context, v:Value)
              .init configuration = Configuration
            }
            .init mainAnalysis = Analysis<Cfg>
        ";
        let program = assembled(src).expect("assembles");
        let r = find_relation(&program, "mainanalysis\u{b7}runningthread");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
    }

    /// A base component declares relations over `configuration.Member` where
    /// `configuration` is the eventual instance of that component itself (no
    /// local `.init`), and the member `.type` is supplied by a concrete
    /// subtype. When the outermost `.init` binds the subtype, the member
    /// resolves to the subtype's `.type`.
    #[test]
    fn self_referential_member_type_from_concrete_subtype() {
        let src = "
            .type Value = symbol
            .type Invo = symbol
            .comp AbstractConfiguration {
              .decl ContextRequest(ctx:configuration.Context, invo:Invo)
            }
            .comp Analysis<Configuration> {
              .init configuration = Configuration
              .decl RunningThread(ctx:configuration.Context, v:Value)
            }
            .comp ConcreteConfiguration : AbstractConfiguration {
              .type Context = symbol
            }
            .init mainAnalysis = Analysis<ConcreteConfiguration>
        ";
        let program = assembled(src).expect("assembles");
        let req = find_relation(
            &program,
            "mainanalysis\u{b7}configuration\u{b7}contextrequest",
        );
        assert_eq!(req.data_type(), vec![DataType::String, DataType::String]);
        let thread = find_relation(&program, "mainanalysis\u{b7}runningthread");
        assert_eq!(thread.data_type(), vec![DataType::String, DataType::String]);
    }

    /// A component-local `.type` alias used as a *bare* (unqualified) attribute
    /// type within the same component resolves against the instance-local alias
    /// table, matching top-level `.type` alias behaviour.
    #[test]
    fn comp_local_type_alias_resolves_as_attr_type() {
        let src = "
            .comp C {
              .type MethodType = symbol
              .decl R(mt:MethodType, i:number)
            }
            .init c = C
        ";
        let program = assembled(src).expect("assembles");
        let r = find_relation(&program, "c\u{b7}r");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::Int32]);
    }

    /// A *bare* member type declared by a concrete subtype resolves in an
    /// inherited base-component `.decl`. Inheritance flattens the base body and
    /// the subtype's `.type` into one comp body, so the subtype's alias is
    /// local: the same mechanism as a plain component-local alias.
    #[test]
    fn bare_member_type_from_concrete_subtype_resolves() {
        let src = "
            .type Invo = symbol
            .comp AbstractConfiguration {
              .decl ContextRequest(ctx:Context, invo:Invo)
            }
            .comp ConcreteConfiguration : AbstractConfiguration {
              .type Context = symbol
            }
            .init c = ConcreteConfiguration
        ";
        let program = assembled(src).expect("assembles");
        let r = find_relation(&program, "c\u{b7}contextrequest");
        assert_eq!(r.data_type(), vec![DataType::String, DataType::String]);
    }

    /// A rule inside one component may reference a relation of a *sibling*
    /// instance declared in the enclosing (global) scope: `basic.SubtypeOf`
    /// inside `main`'s body resolves to the global `basic\u{b7}subtypeof`.
    #[test]
    fn sibling_instance_relation_ref_resolves() {
        let src = "
            .comp Lib { .decl SubtypeOf(a:symbol, b:symbol) }
            .init basic = Lib
            .comp Analysis {
              .decl R(x:symbol)
              R(x) :- basic.SubtypeOf(x, _).
            }
            .init main = Analysis
        ";
        let program = assembled(src).expect("assembles");
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "main\u{b7}r")
            .expect("main\u{b7}r rule");
        let body: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert!(
            body.contains(&"basic\u{b7}subtypeof"),
            "sibling ref should resolve to basic\u{b7}subtypeof, got {body:?}"
        );
    }

    /// An `.output`/`.input` directive *inside* a component may target a
    /// relation declared in the enclosing (global) scope: the directive
    /// resolver falls through to the global relation set.
    #[test]
    fn comp_directive_targets_global_relation() {
        let src = "
            .decl G(x:symbol)
            G(\"a\").
            .comp C {
              .decl L(x:symbol)
              L(x) :- G(x).
              .output G(IO=\"file\",filename=\"G.csv\",delimiter=\"\\t\")
            }
            .init c = C
        ";
        let program = assembled(src).expect("assembles");
        let g = find_relation(&program, "g");
        assert!(
            g.output(),
            ".output of a global relation from inside a comp should apply"
        );
    }

    /// Comp-internal directives bypass `apply_directives` (the inliner sets the
    /// flags directly), so the output/printsize conflict check must run AFTER
    /// both passes: without the post-pass validator two writers would race on
    /// the same `c.R.csv` file.
    #[test]
    fn output_and_printsize_inside_comp_rejected() {
        let err = assembled(
            "
            .comp C {
              .decl Src(x: number)
              .decl R(x: number)
              Src(1).
              R(x) :- Src(x).
              .output R
              .printsize R
            }
            .init c = C
            ",
        )
        .unwrap_err();
        assert!(
            matches!(err, ParseError::OutputAndPrintsizeConflict { .. }),
            "expected OutputAndPrintsizeConflict for comp-internal pair, got {err:?}"
        );
    }

    /// `.override Foo` drops the parent's ground facts.
    #[test]
    fn override_drops_parent_facts() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
              Foo(2).
            }
            .comp Sub : Base {
              .override Foo
              Foo(10).
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        assert_eq!(fact_numbers(&program, "s\u{b7}foo"), vec![10]);
    }

    /// `.override` replaces a derived rule, not just facts.
    #[test]
    fn override_drops_parent_derived_rule() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              .decl Seed(x: number)
              Foo(x) :- Seed(x).
            }
            .comp Sub : Base {
              .override Foo
              Foo(x) :- Seed(x), x > 5.
            }
            .init s = Sub
            .input s.Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        let rules: Vec<_> = program
            .rules()
            .into_iter()
            .filter(|r| r.head().name() == "s\u{b7}foo")
            .collect();
        assert_eq!(rules.len(), 1, "exactly one s\u{b7}foo rule survives");
        let has_compare = rules[0]
            .rhs()
            .iter()
            .any(|p| matches!(p, Predicate::Compare(_)));
        assert!(has_compare, "override's filtered rule should survive");
    }

    /// `overridable` without `.override` is a no-op: the parent's facts remain.
    #[test]
    fn overridable_without_override_keeps_parent_facts() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base { }
            .init s = Sub
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        let tuples = program.facts().get("s\u{b7}foo").expect("s\u{b7}foo facts");
        assert_eq!(tuples.len(), 1);
    }

    /// Diamond inheritance: Mid's override wins through to Bot.
    #[test]
    fn override_propagates_through_inheritance_chain() {
        let src = "
            .comp Top  { .decl Foo(x: number) overridable  Foo(1). }
            .comp Mid1 : Top { .override Foo  Foo(2). }
            .comp Bot  : Mid1 { }
            .init b = Bot
            .output b.Foo
        ";
        let program = assembled(src).expect("assembles");
        assert_eq!(fact_numbers(&program, "b\u{b7}foo"), vec![2]);
    }

    /// Parametric: `.comp X<T>` declares `Foo(x: T) overridable`, a subcomponent
    /// overrides it, and type substitution still flows through.
    #[test]
    fn override_parametric_type_substitution() {
        let src = "
            .comp Base<T> {
              .decl Foo(x: T) overridable
              Foo(0).
            }
            .comp Sub<T> : Base<T> {
              .override Foo
              Foo(42).
            }
            .init s = Sub<number>
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        assert_eq!(fact_numbers(&program, "s\u{b7}foo"), vec![42]);
    }

    /// `.override` with zero replacement rules drops the parent's derivations
    /// and leaves nothing in their place.
    #[test]
    fn override_to_empty_drops_parent_derivations() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base {
              .override Foo
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        assert!(program.facts().get("s\u{b7}foo").is_none());
        assert!(
            program
                .rules()
                .iter()
                .all(|r| r.head().name() != "s\u{b7}foo")
        );
    }

    /// Two `.override Foo` directives are redundant but not an error: they
    /// collapse to a single override.
    #[test]
    fn double_override_is_accepted() {
        let src = "
            .comp Base {
              .decl Foo(x: number) overridable
              Foo(1).
            }
            .comp Sub : Base {
              .override Foo
              .override Foo
              Foo(10).
            }
            .init s = Sub
            .output s.Foo
        ";
        let program = assembled(src).expect("assembles");
        let tuples = program.facts().get("s\u{b7}foo").expect("s\u{b7}foo facts");
        assert_eq!(tuples.len(), 1);
    }

    /// A loop condition may name an inliner-produced relation in its dotted
    /// surface form: condition validation runs before dot normalization, so the
    /// dotted spelling still matches the declaration. After normalization the
    /// declaration carries `\u{b7}` while the condition keeps the user's dot.
    #[test]
    fn loop_until_dotted_inliner_relation_passes_validation() {
        let src = "
            .comp C { .decl Holds() }
            .init c = C
            .decl edge(x: number, y: number)
            .output edge
            edge(1, 2).
            loop until { c.Holds } {
                edge(X, Y) :- edge(Y, X).
            }
        ";
        let program = assembled(src).expect("assembles");
        assert!(
            program
                .relations()
                .iter()
                .any(|r| r.name() == "c\u{b7}holds")
        );
        let block = loop_blocks(&program)[0];
        let names: Vec<&str> = block
            .condition()
            .expect("loop has a condition")
            .until_part()
            .expect("condition has an until part")
            .relations()
            .map(StopRelation::name)
            .collect();
        assert_eq!(names, vec!["c.holds"]);
    }

    /// `.iterative` accepts a dotted inliner-produced name; the directive stores
    /// the canonical dotted spelling while the declaration is normalized to
    /// `\u{b7}`. Same gap family as the loop-until pin above.
    #[test]
    fn iterative_dotted_inliner_relation_keeps_dotted_spelling() {
        let src = "
            .comp C { .decl S(x: number) }
            .init c = C
            .decl edge(x: number)
            .output edge
            fixpoint {
                .iterative c.S
                c.S(X) :- edge(X).
                edge(X) :- c.S(X).
            }
        ";
        let program = assembled(src).expect("assembles");
        assert!(program.relations().iter().any(|r| r.name() == "c\u{b7}s"));
        let block = loop_blocks(&program)[0];
        let names: Vec<&str> = block
            .iterative_relations()
            .iter()
            .map(|d| d.name())
            .collect();
        assert_eq!(names, vec!["c.s"]);
    }

    /// `.plan` inside a `.comp` body permutes the rule's positive atoms at parse
    /// time, so the inlined / prefixed rule reaches the planner already in hint
    /// order, and `plan_pinned` survives inlining.
    #[test]
    fn plan_inside_comp_body() {
        let src = "
            .comp C {
              .decl A(x: number)
              .decl B(x: number)
              .decl D(x: number)
              .decl H(x: number)
              H(X) :- A(X), B(X), D(X).
              .plan (3, 1, 2)
            }
            .init c = C
            .output c.H
        ";
        let program = assembled(src).expect("assembles");
        let rule = program
            .rules()
            .into_iter()
            .find(|r| r.head().name() == "c\u{b7}h")
            .expect("instantiated H rule");
        let names: Vec<&str> = rule.rhs().iter().map(|p| p.name()).collect();
        assert_eq!(names, vec!["c\u{b7}d", "c\u{b7}a", "c\u{b7}b"]);
        assert!(rule.plan_pinned(), "plan_pinned should survive inlining");
    }
}
