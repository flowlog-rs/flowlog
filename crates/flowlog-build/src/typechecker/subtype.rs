//! Subtype enforcement and `as()` validation. Runs after the
//! primitive-level type check, which works in [`DataType`]s and so
//! can't distinguish `UserId <: number` from `ProductId <: number`.
//!
//! Three rules (all Soufflé 2.x compatible):
//!
//! - **Variable binding**: a variable's binding-site types must have a
//!   [`TypeRegistry::meet`] — sibling subtypes are rejected.
//! - **Head construction**: subtype → parent widens implicitly;
//!   parent → subtype narrowing requires `as()`.
//! - **`as(factor, T)`**: source and target must share a primitive root.
//!
//! On success, every `Factor::Cast` is stripped in place — downstream
//! stages see no casts.

use std::collections::HashMap;
use std::mem;

use super::TypeCheckError;
use crate::common::Span;
use crate::parser::{
    Arithmetic, Atom, AtomArg, ComparisonExpr, Factor, FlowLogRule, HeadArg, Predicate, Program,
};
use crate::parser::{TypeId, TypeRegistry};

type DeclIds = HashMap<String, Vec<TypeId>>;

/// Variable → (inferred TypeId, first-seen span). The TypeId is the
/// meet of all binding-site types seen so far.
type Bindings = HashMap<String, (TypeId, Span)>;

/// Check and lower casts in a single per-rule walk.
pub(super) fn check_and_lower(program: &mut Program) -> Result<(), TypeCheckError> {
    let decls: DeclIds = program
        .relations()
        .iter()
        .map(|r| (r.name().to_string(), r.attribute_declared_ids()))
        .collect();

    let (registry, segments) = program.registry_and_segments_mut();
    for segment in segments.iter_mut() {
        for rule in segment.as_rules_mut() {
            check_rule(rule, registry, &decls)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check_rule(rule, registry, &decls)?;
            }
        }
    }
    Ok(())
}

/// Bind via positive atoms, re-check negated/compared positions and
/// `as()` casts, validate the head, then lower casts in-place.
fn check_rule(
    rule: &mut FlowLogRule,
    reg: &TypeRegistry,
    decls: &DeclIds,
) -> Result<(), TypeCheckError> {
    let mut bindings: Bindings = HashMap::new();

    // Bind via positive atoms first so out-of-order body predicates
    // can resolve their variables.
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtom(atom) = predicate {
            check_atom(atom, decls, reg, &mut bindings, true)?;
        }
    }

    for predicate in rule.rhs() {
        match predicate {
            Predicate::PositiveAtom(_) => {}
            Predicate::NegativeAtom(atom) => {
                check_atom(atom, decls, reg, &mut bindings, false)?;
            }
            Predicate::Compare(cmp) => {
                check_arith_casts(cmp.left(), reg, &bindings)?;
                check_arith_casts(cmp.right(), reg, &bindings)?;
                check_compare(cmp, reg, &bindings)?;
            }
            Predicate::FnCall(fc) => {
                for arg in fc.args() {
                    check_arith_casts(arg, reg, &bindings)?;
                }
            }
        }
    }

    check_head(rule, decls, reg, &bindings)?;
    lower_rule(rule);
    Ok(())
}

/// Walks `atom`'s variable args, refining each binding to the meet
/// with the column's declared type. `bind = true` (positive atoms)
/// inserts on first sight; `bind = false` (negative atoms) skips
/// unbound vars but still rejects subtype mismatches.
fn check_atom(
    atom: &Atom,
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &mut Bindings,
    bind: bool,
) -> Result<(), TypeCheckError> {
    let col_ids = decls.get(atom.name()).ok_or_else(|| {
        TypeCheckError::internal(format!("subtype pass: atom `{}` not declared", atom.name()))
    })?;
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_id = col_ids
            .get(i)
            .copied()
            .ok_or_else(|| TypeCheckError::internal("subtype pass: atom arity mismatch"))?;
        let AtomArg::Var(v) = arg else { continue };
        match bindings.get(v).copied() {
            None if bind => {
                bindings.insert(v.clone(), (col_id, atom.span()));
            }
            None => {}
            Some((existing_id, existing_span)) => {
                let Some(meet) = reg.meet(existing_id, col_id) else {
                    return Err(TypeCheckError::SubtypeMismatch {
                        var: v.clone(),
                        first_ty: reg.name_of(existing_id).to_string(),
                        first_span: existing_span,
                        later_ty: reg.name_of(col_id).to_string(),
                        later_span: atom.span(),
                    });
                };
                if bind && meet != existing_id {
                    bindings.insert(v.clone(), (meet, existing_span));
                }
            }
        }
    }
    Ok(())
}

fn check_head(
    rule: &FlowLogRule,
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let head = rule.head();
    let rel_name = head.name();
    let col_ids = decls.get(rel_name).ok_or_else(|| {
        TypeCheckError::internal(format!(
            "subtype pass: head relation `{rel_name}` not declared"
        ))
    })?;
    for (col, (arg, &expected_id)) in head.head_arguments().iter().zip(col_ids.iter()).enumerate() {
        match arg {
            HeadArg::Var(v) => {
                if let Some(&(found_id, _)) = bindings.get(v)
                    && !reg.is_widening(found_id, expected_id)
                {
                    return Err(TypeCheckError::HeadSubtypeMismatch {
                        span: head.span(),
                        rel: rel_name.to_string(),
                        col,
                        expected: reg.name_of(expected_id).to_string(),
                        found: reg.name_of(found_id).to_string(),
                    });
                }
            }
            HeadArg::Arith(a) => {
                // Widening rule only applies when a single value flows
                // through. Multi-factor arithmetic drops subtype identity.
                if let Some(found_id) = single_var_type(a, reg, bindings)
                    && !reg.is_widening(found_id, expected_id)
                {
                    return Err(TypeCheckError::HeadSubtypeMismatch {
                        span: head.span(),
                        rel: rel_name.to_string(),
                        col,
                        expected: reg.name_of(expected_id).to_string(),
                        found: reg.name_of(found_id).to_string(),
                    });
                }
                check_arith_casts(a, reg, bindings)?;
            }
            HeadArg::Aggregation(agg) => check_arith_casts(agg.arithmetic(), reg, bindings)?,
        }
    }
    Ok(())
}

/// If `a` is a single-factor expression that carries a determinate
/// type identity (a bound variable, or an `as()` cast whose target is
/// in the registry), return its TypeId. Used for the head-widening
/// rule. Multi-factor arithmetic and unbound vars return `None` —
/// arithmetic drops subtype identity, and the primitive pass already
/// validated the underlying types.
fn single_var_type(a: &Arithmetic, reg: &TypeRegistry, bindings: &Bindings) -> Option<TypeId> {
    if !a.rest().is_empty() {
        return None;
    }
    match a.init() {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| id),
        Factor::Cast(c) => reg.lookup(c.target_type()),
        Factor::Const(_) | Factor::FnCall(_) | Factor::Builtin(_) => None,
    }
}

/// Comparison operands with determinate type identity must have a meet.
/// Skipped when either side is a constant, arithmetic expression, or
/// UDF/builtin call (no subtype identity flows through those).
fn check_compare(
    cmp: &ComparisonExpr,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let (Some(l), Some(r)) = (
        single_var_type(cmp.left(), reg, bindings),
        single_var_type(cmp.right(), reg, bindings),
    ) else {
        return Ok(());
    };
    if reg.meet(l, r).is_none() {
        return Err(TypeCheckError::ComparisonSubtypeMismatch {
            span: cmp.span(),
            left_ty: reg.name_of(l).to_string(),
            right_ty: reg.name_of(r).to_string(),
        });
    }
    Ok(())
}

/// Validate every `as()` cast inside an arithmetic expression.
fn check_arith_casts(
    a: &Arithmetic,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    check_factor_casts(a.init(), reg, bindings)?;
    for (_, f) in a.rest() {
        check_factor_casts(f, reg, bindings)?;
    }
    Ok(())
}

fn check_factor_casts(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    match f {
        Factor::Var(_) | Factor::Const(_) => Ok(()),
        Factor::FnCall(fc) => fc
            .args()
            .iter()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::Builtin(bc) => bc
            .args()
            .iter()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::Cast(c) => {
            let target_id =
                reg.lookup(c.target_type())
                    .ok_or_else(|| TypeCheckError::UnknownCastType {
                        span: c.span(),
                        name: c.target_type().to_string(),
                    })?;
            let inner_root = inner_factor_primitive_root(c.inner(), reg, bindings);
            let target_root = reg.root_primitive(target_id);
            if let Some(inner) = inner_root
                && inner != target_root
            {
                return Err(TypeCheckError::IllegalCast {
                    span: c.span(),
                    from: inner.to_string(),
                    to: reg.name_of(target_id).to_string(),
                });
            }
            // Recurse for nested casts: `as(as(x, A), B)`.
            check_factor_casts(c.inner(), reg, bindings)
        }
    }
}

/// Best-effort primitive root of `f` for the same-root check. `None`
/// for cases we can't resolve locally (UDF return, constant); the
/// primitive pass has already validated those, so we just skip.
fn inner_factor_primitive_root(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Option<crate::parser::DataType> {
    match f {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| reg.root_primitive(id)),
        Factor::Const(_) => None,
        Factor::FnCall(_) | Factor::Builtin(_) => None,
        Factor::Cast(c) => reg.lookup(c.target_type()).map(|id| reg.root_primitive(id)),
    }
}

// =============================================================================
// Cast lowering — strip every `Factor::Cast(c)` after the subtype
// check has approved it. Downstream stages never see a cast.
// =============================================================================

fn lower_rule(rule: &mut FlowLogRule) {
    for pred in rule.rhs_mut() {
        match pred {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            Predicate::Compare(cmp) => {
                lower_arith(cmp.left_mut());
                lower_arith(cmp.right_mut());
            }
            Predicate::FnCall(fc) => {
                for a in fc.args_mut() {
                    lower_arith(a);
                }
            }
        }
    }
    for arg in rule.head_mut().head_arguments_mut() {
        match arg {
            HeadArg::Var(_) => {}
            HeadArg::Arith(a) => lower_arith(a),
            HeadArg::Aggregation(agg) => lower_arith(agg.arithmetic_mut()),
        }
    }
}

fn lower_arith(a: &mut Arithmetic) {
    lower_factor(a.init_mut());
    for (_, f) in a.rest_mut() {
        lower_factor(f);
    }
}

fn lower_factor(f: &mut Factor) {
    // Peel nested casts: `as(as(x, A), B)` collapses to `x`.
    loop {
        if let Factor::Cast(c) = f {
            let inner = mem::replace(
                c.inner_mut(),
                Factor::Const(crate::parser::ConstType::Int(0)),
            );
            *f = inner;
            continue;
        }
        break;
    }
    match f {
        Factor::Var(_) | Factor::Const(_) => {}
        Factor::FnCall(fc) => {
            for a in fc.args_mut() {
                lower_arith(a);
            }
        }
        Factor::Builtin(bc) => {
            for a in bc.args_mut() {
                lower_arith(a);
            }
        }
        Factor::Cast(_) => unreachable!("cast was peeled above"),
    }
}
