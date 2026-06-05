//! Source-to-source lowering passes.
//!
//! These run between parsing and typechecking. They take a fully-inlined
//! [`Program`] and rewrite *surface-level* syntactic sugar to the
//! restricted core that downstream stages already understand.
//!
//! Currently provides, in pipeline order:
//!
//! * [`desugar_expr_atom_args`] — lifts a Soufflé expression in a body
//!   atom's argument position (`R(idx - 1, x)`) to a fresh variable
//!   bound by an equality, so atoms only ever carry variables, constants,
//!   and placeholders.
//! * [`desugar_body_aggregates`] — lifts each Soufflé-style body
//!   aggregate `r = op [expr] : body` into a fresh auxiliary IDB
//!   relation that runs a normal head-position aggregate, and replaces
//!   the original site with a positive atom referencing the aux
//!   relation's result column.
//!
//! Keeping these passes parser-adjacent (rather than typechecker-adjacent)
//! lets every later stage assume rule bodies hold only the four
//! "primitive" predicate kinds: positive atom, negative atom,
//! comparison, fn-call — and that every atom argument is a variable,
//! constant, or placeholder.
//!
//! ## Why a separate aux IDB?
//!
//! Soufflé's body aggregate is, semantically, an aggregation over a
//! subset of the rule's free variables. Lowering it to a head-position
//! aggregate with the grouping variables in the head schema captures
//! exactly that semantics with **zero changes** to the existing
//! aggregation operator, planner, or runtime. The price is one extra
//! relation per call site — cheap, and visible in the IR for debugging.
//!
//! ## Grouping variable discovery
//!
//! For each body aggregate `r = op expr : agg_body` inside an outer
//! rule with head `H` and the other body predicates `P*`:
//!
//! 1. `body_vars`  = free variables of `agg_body`
//! 2. `outer_vars` = vars referenced in `H`, in `P*`, in the result
//!    variable `r`'s outer uses, or in the expressions of *other*
//!    body aggregates that share the outer scope.
//! 3. `groups`     = `body_vars ∩ outer_vars`, in the order each
//!    variable is first encountered in `agg_body`'s positive atoms.
//!
//! This matches Soufflé's implicit grouping rule (a body aggregate's
//! result depends on every outer-scope variable it shares with its
//! body) and keeps the synthesized head deterministic.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::common::Span;
use crate::parser::declaration::{Attribute, ExternFn, Relation};
use crate::parser::error::ParseError;
use crate::parser::logic::{
    Aggregation, AggregationOperator, Arithmetic, Atom, AtomArg, BodyAggregate, BuiltinOperator,
    ComparisonExpr, ComparisonOperator, Factor, FlowLogRule, FnCall, Head, HeadArg, Predicate,
};
use crate::parser::primitive::{ConstType, DataType, TypeRegistry};
use crate::parser::segment::Segment;

/// Lower every `Predicate::BodyAggregate` in `segments` to an
/// auxiliary relation + positive atom rewrite.
///
/// New aux relations are appended to `relations`. New aux rules are
/// inserted into the same segment as the outer rule that referenced
/// them, immediately before that outer rule — preserving the stratum
/// boundaries that `Segment::Loop` / `Segment::Fixpoint` enforce.
///
/// Idempotent in spirit: a second call is a no-op because the rewrite
/// removes every `BodyAggregate` predicate on the first pass.
pub(crate) fn desugar_body_aggregates(
    relations: &mut Vec<Relation>,
    segments: &mut [Segment],
    udfs: &[ExternFn],
    registry: &TypeRegistry,
) -> Result<(), ParseError> {
    // Quick-out so we don't pay the relation-index cost in the common case.
    if !any_body_aggregate(segments) {
        return Ok(());
    }

    let rel_index = RelationIndex::build(relations);
    let udf_index: HashMap<&str, DataType> =
        udfs.iter().map(|u| (u.name(), u.ret_type())).collect();

    let mut counter: usize = 0;
    let mut new_relations: Vec<Relation> = Vec::new();

    for segment in segments.iter_mut() {
        let rules = segment_rules_mut(segment);

        let mut rewritten: Vec<FlowLogRule> = Vec::with_capacity(rules.len());
        for mut rule in rules.drain(..) {
            let aux = desugar_rule(
                &mut rule,
                &rel_index,
                &udf_index,
                registry,
                &mut counter,
                &mut new_relations,
            )?;
            rewritten.extend(aux);
            rewritten.push(rule);
        }
        *rules = rewritten;
    }

    relations.extend(new_relations);
    Ok(())
}

/// True iff any rule (Plain segment or inside a loop block) holds at
/// least one body-aggregate predicate.
fn any_body_aggregate(segments: &[Segment]) -> bool {
    segments.iter().any(|seg| {
        segment_rules(seg)
            .iter()
            .any(|r| r.rhs().iter().any(|p| matches!(p, Predicate::BodyAggregate(_))))
    })
}

/// The rules a segment carries, regardless of whether it is a plain
/// stratum or a loop/fixpoint block.
fn segment_rules(segment: &Segment) -> &[FlowLogRule] {
    match segment {
        Segment::Plain(rules) => rules,
        Segment::Loop(block) | Segment::Fixpoint(block) => block.rules(),
    }
}

/// Mutable counterpart of [`segment_rules`].
fn segment_rules_mut(segment: &mut Segment) -> &mut Vec<FlowLogRule> {
    match segment {
        Segment::Plain(rules) => rules,
        Segment::Loop(block) | Segment::Fixpoint(block) => block.rules_mut(),
    }
}

// =============================================================================
// Per-rule rewrite
// =============================================================================

/// Rewrite one rule, returning the auxiliary rules that should be
/// inserted *before* it in the same segment.
fn desugar_rule(
    rule: &mut FlowLogRule,
    rel_index: &RelationIndex,
    udf_index: &HashMap<&str, DataType>,
    registry: &TypeRegistry,
    counter: &mut usize,
    new_relations: &mut Vec<Relation>,
) -> Result<Vec<FlowLogRule>, ParseError> {
    // Fast exit avoids reparsing var bindings for the 99 % of rules
    // that don't use body aggregates.
    if !rule.rhs().iter().any(|p| matches!(p, Predicate::BodyAggregate(_))) {
        return Ok(Vec::new());
    }

    // Snapshot every variable's (DataType, TypeId) by scanning the
    // outer rule's positive atoms — including the body aggregate's own
    // bodies, so a `min len : Atom(..., len)` can later type-check
    // even if `len` never escapes the aggregate.
    let mut var_types = VarTypes::default();
    var_types.absorb_predicates(rule.rhs(), rel_index);

    // Collect vars referenced in the *outer* scope. Body aggregates
    // contribute only their result variable here — their own body vars
    // stay local unless they appear independently outside.
    let outer_vars = outer_scope_vars(rule);

    let mut aux_rules: Vec<FlowLogRule> = Vec::new();
    let mut replacements: Vec<(usize, Predicate)> = Vec::new();

    for (slot, pred) in rule.rhs().iter().enumerate() {
        let Predicate::BodyAggregate(agg) = pred else {
            continue;
        };

        let lowering = lower_one(
            agg,
            &outer_vars,
            &var_types,
            rel_index,
            udf_index,
            registry,
            counter,
        )?;

        new_relations.push(lowering.aux_relation);
        aux_rules.push(lowering.aux_rule);
        replacements.push((slot, Predicate::PositiveAtom(lowering.replacement)));
    }

    // Apply replacements in place; slice indexing preserves order.
    let rhs = rule.rhs_mut();
    for (slot, new_pred) in replacements {
        rhs[slot] = new_pred;
    }

    Ok(aux_rules)
}

/// Variables visible in the outer rule scope (head + non-aggregate
/// body predicates + each body aggregate's result variable).
///
/// Body aggregates' inner body vars are intentionally excluded — a
/// variable referenced *only* inside a single aggregate body must not
/// become a grouping key.
fn outer_scope_vars(rule: &FlowLogRule) -> HashSet<String> {
    let mut vars = HashSet::new();

    for head_arg in rule.head().head_arguments() {
        for v in head_arg.vars() {
            vars.insert(v.clone());
        }
    }

    for pred in rule.rhs() {
        match pred {
            Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => {
                for arg in a.arguments() {
                    if let AtomArg::Var(v) = arg {
                        vars.insert(v.clone());
                    }
                }
            }
            Predicate::Compare(cmp) => {
                for v in cmp.vars_set() {
                    vars.insert(v.clone());
                }
            }
            Predicate::FnCall(fc) => {
                for v in fn_call_vars(fc) {
                    vars.insert(v);
                }
            }
            Predicate::BodyAggregate(agg) => {
                // Only the *output* of an aggregate is visible outside it.
                vars.insert(agg.result().to_string());
            }
        }
    }

    vars
}

fn fn_call_vars(fc: &FnCall) -> Vec<String> {
    fc.args()
        .iter()
        .flat_map(|a| a.vars().into_iter().cloned())
        .collect()
}

// =============================================================================
// Lowering a single body aggregate
// =============================================================================

struct Lowering {
    aux_relation: Relation,
    aux_rule: FlowLogRule,
    replacement: Atom,
}

fn lower_one(
    agg: &BodyAggregate,
    outer_vars: &HashSet<String>,
    var_types: &VarTypes,
    rel_index: &RelationIndex,
    udf_index: &HashMap<&str, DataType>,
    registry: &TypeRegistry,
    counter: &mut usize,
) -> Result<Lowering, ParseError> {
    let aux_id = *counter;
    *counter += 1;

    // Clone the agg body so we can name its placeholders without
    // disturbing other rules. After this, every positional arg of an
    // atom in the body is either a `Var` or a `Const` — never `_`.
    let mut body = agg.body().to_vec();
    name_placeholders(&mut body, aux_id);

    // Local var table covers vars bound only inside the agg body
    // (including freshly-named placeholders).
    let mut body_vars = var_types.clone();
    body_vars.absorb_predicates(&body, rel_index);

    // Grouping vars: deterministic first-seen order across the agg body.
    let group_vars = collect_group_vars_in(&body, outer_vars);

    // Non-grouping body vars: everything bound inside the body that
    // doesn't escape to the outer scope. Used by `count` to project
    // distinct binding tuples.
    let local_vars = collect_local_body_vars(&body, &group_vars);

    let mut attrs: Vec<Attribute> = Vec::with_capacity(group_vars.len() + 1);
    let mut head_args: Vec<HeadArg> = Vec::with_capacity(group_vars.len() + 1);
    let mut atom_args: Vec<AtomArg> = Vec::with_capacity(group_vars.len() + 1);

    for var in &group_vars {
        let (dt, id) = body_vars.get(var).ok_or_else(|| {
            ParseError::AggregateUntypedVar {
                span: agg.span(),
                var: var.clone(),
            }
        })?;
        attrs.push(Attribute::with_type(var.clone(), dt, id));
        head_args.push(HeadArg::Arith(Arithmetic::new(
            Factor::Var(var.clone()),
            vec![],
        )));
        atom_args.push(AtomArg::Var(var.clone()));
    }

    // Result column: derived from the aggregate operator + expression.
    let result_dt = infer_result_type(agg, &body_vars, udf_index, registry)?;
    let result_id = registry.primitive_id(result_dt);
    attrs.push(Attribute::with_type(
        agg.result().to_string(),
        result_dt,
        result_id,
    ));

    // Head aggregation. For Soufflé `count : Body`, project the
    // distinct bindings of `Body`'s free variables and sum 1 per
    // binding. flowlog's `count(e)` semiring counts distinct
    // `(group, e)` projections, so we use the body's single local var
    // as `e` for the K=1 case and a constant for K=0. K≥2 needs a
    // synthetic per-binding identifier which we don't support yet.
    let head_agg = build_head_aggregation(agg, &local_vars, &body_vars, result_dt)?;
    head_args.push(HeadArg::Aggregation(head_agg));
    atom_args.push(AtomArg::Var(agg.result().to_string()));

    let aux_name = format!("__body_agg_{}", aux_id);
    let aux_relation = Relation::from_components(&aux_name, attrs, Span::DUMMY);
    let aux_head = Head::synth(aux_name.clone(), head_args);
    let aux_rule = FlowLogRule::new(aux_head, body);
    let fingerprint = aux_relation.fingerprint();
    let replacement = Atom::new(&aux_name, atom_args, fingerprint);

    Ok(Lowering {
        aux_relation,
        aux_rule,
        replacement,
    })
}

/// Rename every `AtomArg::Placeholder` to a fresh `__b<aux>_<seq>`
/// variable so the desugared body has only named vars (and constants).
/// Each occurrence becomes a *distinct* variable, matching Soufflé's
/// "fresh anonymous var per `_`" semantics.
fn name_placeholders(body: &mut [Predicate], aux_id: usize) {
    let mut seq: usize = 0;
    for pred in body.iter_mut() {
        if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
            for arg in atom.arguments_mut() {
                if matches!(arg, AtomArg::Placeholder) {
                    *arg = AtomArg::Var(format!("__b{}_{}", aux_id, seq));
                    seq += 1;
                }
            }
        }
    }
}

/// Grouping vars after placeholder renaming. Same algorithm as
/// [`collect_group_vars`] but reads from an already-renamed body.
fn collect_group_vars_in(body: &[Predicate], outer_vars: &HashSet<String>) -> Vec<String> {
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for pred in body {
        match pred {
            Predicate::PositiveAtom(a) | Predicate::NegativeAtom(a) => {
                for arg in a.arguments() {
                    if let AtomArg::Var(v) = arg
                        && outer_vars.contains(v)
                        && seen.insert(v.clone())
                    {
                        out.push(v.clone());
                    }
                }
            }
            Predicate::Compare(cmp) => {
                for v in cmp.vars_set() {
                    if outer_vars.contains(v) && seen.insert(v.clone()) {
                        out.push(v.clone());
                    }
                }
            }
            Predicate::FnCall(fc) => {
                for v in fn_call_vars(fc) {
                    if outer_vars.contains(&v) && seen.insert(v.clone()) {
                        out.push(v);
                    }
                }
            }
            Predicate::BodyAggregate(inner) => {
                if outer_vars.contains(inner.result())
                    && seen.insert(inner.result().to_string())
                {
                    out.push(inner.result().to_string());
                }
            }
        }
    }
    out
}

/// All body vars not in `group_vars`, first-seen order across positive
/// atoms in the body. These are the "binding identity" vars for count.
fn collect_local_body_vars(body: &[Predicate], group_vars: &[String]) -> Vec<String> {
    let group_set: HashSet<&str> = group_vars.iter().map(String::as_str).collect();
    let mut seen: HashSet<String> = HashSet::new();
    let mut out: Vec<String> = Vec::new();
    for pred in body {
        if let Predicate::PositiveAtom(a) = pred {
            for arg in a.arguments() {
                if let AtomArg::Var(v) = arg
                    && !group_set.contains(v.as_str())
                    && seen.insert(v.clone())
                {
                    out.push(v.clone());
                }
            }
        }
    }
    out
}

/// Construct the head-position aggregation the aux rule emits.
///
/// - `min` / `max` / `sum` / `avg`: clone the user-written expression.
/// - `count` with K=0 local body vars: `count(1)` — the aux rule fires
///   at most once per group, so the count is naturally 1 if and only
///   if the body is satisfiable.
/// - `count` with K=1 local body var `v`: `count(v)` — counts the
///   distinct values of `v` per group, which matches Soufflé's
///   "distinct body bindings" semantics when the body has a single
///   non-grouping variable. This covers every body-position `count`
///   that appears in DOOP.
/// - `count` with K≥2 local body vars: rejected with a clear error.
///   flowlog's head-position `count(e)` counts distinct `(group, e)`
///   projections, so a faithful Soufflé `count` over multiple
///   independent body variables would need either a synthetic
///   per-binding identifier or a tuple-keyed semiring — both
///   non-trivial; we punt rather than miscompile.
fn build_head_aggregation(
    agg: &BodyAggregate,
    local_vars: &[String],
    body_vars: &VarTypes,
    declared: DataType,
) -> Result<Aggregation, ParseError> {
    let op = agg.operator();

    if let Some(expr) = agg.expr() {
        return Ok(Aggregation::synth(op, expr.clone()));
    }

    // Only `count` reaches this branch (the grammar enforces an
    // expression for the other operators via `body_agg_expr?` paired
    // with the operator inside `lower_one`). `infer_result_type`
    // already returned `Int32` for `count`, so `declared` is `Int32`
    // and we can pin the synthetic literal to that.
    let init = match local_vars {
        [] => {
            // count : Body with no free body vars → 1 if the body holds.
            Factor::Const(int32_one(declared))
        }
        [single] => {
            // Pinning the var's type isn't necessary here — the
            // typechecker reads it from the var binding directly.
            let _ = body_vars; // silence "unused" if type-pinning is added later
            Factor::Var(single.clone())
        }
        _ => {
            return Err(ParseError::AggregateMissingExpr {
                span: agg.span(),
                op: format!(
                    "count over {} non-grouping body variables (only 0 or 1 supported; \
                     rewrite as `count(<var>)` over a single variable, or restructure the rule)",
                    local_vars.len()
                ),
            });
        }
    };
    Ok(Aggregation::synth(op, Arithmetic::new(init, vec![])))
}

/// Pin a numeric literal `1` to the declared result type. `count` is
/// always `Int32` per [`infer_result_type`], but we keep the match
/// open so future widening (e.g. `count(...): int64`) lands cleanly.
fn int32_one(declared: DataType) -> ConstType {
    match declared {
        DataType::Int8 => ConstType::Int8(1),
        DataType::Int16 => ConstType::Int16(1),
        DataType::Int32 => ConstType::Int32(1),
        DataType::Int64 => ConstType::Int64(1),
        DataType::UInt8 => ConstType::UInt8(1),
        DataType::UInt16 => ConstType::UInt16(1),
        DataType::UInt32 => ConstType::UInt32(1),
        DataType::UInt64 => ConstType::UInt64(1),
        _ => ConstType::Int(1),
    }
}

// =============================================================================
// Type inference helpers
// =============================================================================

#[derive(Default, Clone)]
struct VarTypes {
    /// First-seen type wins. Conflicting bindings are left to the
    /// typechecker (which already produces a precise diagnostic).
    inner: HashMap<String, (DataType, crate::parser::primitive::TypeId)>,
}

impl VarTypes {
    fn absorb_predicates(&mut self, preds: &[Predicate], rel_index: &RelationIndex) {
        for pred in preds {
            if let Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) = pred {
                self.absorb_atom(atom, rel_index);
            }
        }
    }

    fn absorb_atom(&mut self, atom: &Atom, rel_index: &RelationIndex) {
        let Some(cols) = rel_index.lookup(atom.name()) else {
            return;
        };
        for (i, arg) in atom.arguments().iter().enumerate() {
            if let (AtomArg::Var(v), Some(col)) = (arg, cols.get(i)) {
                self.inner.entry(v.clone()).or_insert(*col);
            }
        }
    }

    fn get(&self, v: &str) -> Option<(DataType, crate::parser::primitive::TypeId)> {
        self.inner.get(v).copied()
    }
}

fn infer_result_type(
    agg: &BodyAggregate,
    vars: &VarTypes,
    udf_index: &HashMap<&str, DataType>,
    registry: &TypeRegistry,
) -> Result<DataType, ParseError> {
    // `count` is type-independent in Soufflé; we always emit int32.
    if matches!(agg.operator(), AggregationOperator::Count) {
        return Ok(DataType::Int32);
    }
    let expr = agg.expr().ok_or_else(|| ParseError::AggregateMissingExpr {
        span: agg.span(),
        op: format!("{}", agg.operator()),
    })?;
    infer_arith_type(expr, vars, udf_index, registry).ok_or_else(|| {
        ParseError::AggregateUntypedExpr {
            span: agg.span(),
        }
    })
}

fn infer_arith_type(
    expr: &Arithmetic,
    vars: &VarTypes,
    udf_index: &HashMap<&str, DataType>,
    registry: &TypeRegistry,
) -> Option<DataType> {
    // First non-None factor type wins; matches the typechecker's
    // left-to-right widening for arithmetic.
    if let Some(t) = infer_factor_type(expr.init(), vars, udf_index, registry) {
        return Some(t);
    }
    for (_, f) in expr.rest() {
        if let Some(t) = infer_factor_type(f, vars, udf_index, registry) {
            return Some(t);
        }
    }
    None
}

fn infer_factor_type(
    factor: &Factor,
    vars: &VarTypes,
    udf_index: &HashMap<&str, DataType>,
    registry: &TypeRegistry,
) -> Option<DataType> {
    match factor {
        Factor::Var(v) => vars.get(v).map(|(dt, _)| dt),
        Factor::Const(c) => c.data_type().or(Some(DataType::Int32)),
        Factor::Builtin(bc) => Some(BuiltinOperator::ret_type(bc.op())),
        Factor::FnCall(fc) => udf_index.get(fc.name()).copied(),
        Factor::Cast(c) => {
            // `as(_, T)` always knows its target — resolve via the registry.
            registry
                .lookup(c.target_type())
                .map(|id| registry.root_primitive(id))
        }
    }
}

// =============================================================================
// Expression-valued atom arguments (Soufflé compatibility)
// =============================================================================
//
// Soufflé allows an arithmetic expression directly in a body atom's
// argument position, e.g.
//
//   FormalParam(idx - 1, method, formal)
//   ArrayIndexPointsTo(ord(h), ...)
//
// FlowLog's core relational form only admits a variable, constant, or
// placeholder per column. This pass lifts every such expression argument
// to a fresh variable bound by an equality, inserted immediately *before*
// the atom that uses it:
//
//   __atom_arg_0 = idx - 1,
//   FormalParam(__atom_arg_0, method, formal)
//
// The fresh variable is range-restricted by the (positive) atom it feeds,
// so the equality acts as a filter. Runs before `desugar_body_aggregates`
// and recurses into aggregate bodies, so by the time the body-aggregate /
// typecheck / populate stages run, no `AtomArg::Expr` remains anywhere.

/// Lower every expression-valued atom argument in `segments` to a fresh
/// variable plus a preceding equality. Idempotent in spirit: a second
/// pass is a no-op once all `AtomArg::Expr` have been rewritten.
///
/// An expression inside a *negated* atom is rejected (see
/// [`ParseError::ExprArgInNegatedAtom`]): the lifted variable would be
/// bound only through the equality, which the range-restriction model does
/// not treat as binding.
pub(crate) fn desugar_expr_atom_args(segments: &mut [Segment]) -> Result<(), ParseError> {
    let mut counter: usize = 0;
    for segment in segments.iter_mut() {
        for rule in segment_rules_mut(segment).iter_mut() {
            // Skip the clone+rewrite for the overwhelming majority of
            // rules that carry no expression argument.
            if !predicates_have_expr_arg(rule.rhs()) {
                continue;
            }
            let lifted = lift_predicates(rule.rhs().to_vec(), &mut counter)?;
            rule.set_rhs(lifted);
        }
    }
    Ok(())
}

/// True iff any atom (top-level or inside a body aggregate) carries an
/// `AtomArg::Expr`. Cheap, allocation-free pre-check for the rewrite.
fn predicates_have_expr_arg(preds: &[Predicate]) -> bool {
    preds.iter().any(|pred| match pred {
        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => atom_has_expr_arg(atom),
        Predicate::BodyAggregate(agg) => predicates_have_expr_arg(agg.body()),
        Predicate::Compare(_) | Predicate::FnCall(_) => false,
    })
}

/// True iff `atom` has at least one expression-valued argument.
fn atom_has_expr_arg(atom: &Atom) -> bool {
    atom.arguments().iter().any(|a| matches!(a, AtomArg::Expr(_)))
}

/// Rewrite a predicate list, lifting expression atom arguments. Recurses
/// into body-aggregate inner bodies so their atoms are lifted in the same
/// scope (a grouping variable hidden inside an expression stays visible to
/// the body aggregate's grouping logic).
fn lift_predicates(
    preds: Vec<Predicate>,
    counter: &mut usize,
) -> Result<Vec<Predicate>, ParseError> {
    let mut out: Vec<Predicate> = Vec::with_capacity(preds.len());
    for pred in preds {
        match pred {
            Predicate::PositiveAtom(atom) => {
                let (atom, binders) = lift_atom_args(atom, counter);
                out.extend(binders);
                out.push(Predicate::PositiveAtom(atom));
            }
            Predicate::NegativeAtom(atom) => {
                // A negated atom's lifted variable would be bound only by
                // the synthesized equality, which is not range-restricting.
                if atom_has_expr_arg(&atom) {
                    return Err(ParseError::ExprArgInNegatedAtom {
                        span: atom.span(),
                        atom: atom.to_string(),
                    });
                }
                out.push(Predicate::NegativeAtom(atom));
            }
            Predicate::BodyAggregate(mut agg) => {
                let lifted = lift_predicates(agg.body().to_vec(), counter)?;
                agg.set_body(lifted);
                out.push(Predicate::BodyAggregate(agg));
            }
            other => out.push(other),
        }
    }
    Ok(out)
}

/// Replace each `AtomArg::Expr` in `atom` with a fresh variable, returning
/// the rewritten atom and the equalities that bind those fresh variables
/// (to be spliced in before the atom).
fn lift_atom_args(mut atom: Atom, counter: &mut usize) -> (Atom, Vec<Predicate>) {
    let mut binders: Vec<Predicate> = Vec::new();
    for arg in atom.arguments_mut() {
        if let AtomArg::Expr(expr) = arg {
            let fresh = format!("__atom_arg_{counter}");
            *counter += 1;
            binders.push(eq_binding(&fresh, expr.clone()));
            *arg = AtomArg::Var(fresh);
        }
    }
    (atom, binders)
}

/// `<var> = <expr>` as a comparison predicate.
fn eq_binding(var: &str, expr: Arithmetic) -> Predicate {
    let lhs = Arithmetic::new(Factor::Var(var.to_string()), vec![]);
    Predicate::Compare(ComparisonExpr::synth(
        lhs,
        ComparisonOperator::Equal,
        expr,
        Span::DUMMY,
    ))
}

// =============================================================================
// RelationIndex — name → per-column (DataType, TypeId)
// =============================================================================

struct RelationIndex {
    inner: BTreeMap<String, Vec<(DataType, crate::parser::primitive::TypeId)>>,
}

impl RelationIndex {
    fn build(relations: &[Relation]) -> Self {
        let mut inner = BTreeMap::new();
        for rel in relations {
            let cols: Vec<_> = rel
                .data_type()
                .into_iter()
                .zip(rel.attribute_declared_ids())
                .collect();
            inner.insert(rel.name().to_string(), cols);
        }
        Self { inner }
    }

    fn lookup(&self, name: &str) -> Option<&[(DataType, crate::parser::primitive::TypeId)]> {
        self.inner.get(name).map(Vec::as_slice)
    }
}
