//! Subtype enforcement and `as()` validation. The primitive check works in
//! [`DataType`]s, so `UserId <: number` and `ProductId <: number` are
//! indistinguishable to it; this pass catches the subtype mismatches it
//! misses, then strips every `Factor::Cast` so downstream stages see none.
//!
//! It rejects, following Souffle 2.x:
//!
//! - a variable bound at two sibling subtypes,
//! - a head column narrowed (parent to subtype) without an `as()`, and
//! - an `as()` between types of different primitive roots.

use std::collections::HashMap;
use std::mem;

use flowlog_error::Span;

use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::ComparisonExpr;
use crate::Constant;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::TupleElem;
use crate::TypeId;
use crate::TypeRegistry;
use crate::error::grammar_bug;

type DeclIds = HashMap<String, Vec<TypeId>>;

/// Variable -> (inferred TypeId, first-seen span). The TypeId is the
/// meet of all binding-site types seen so far.
type Bindings = HashMap<String, (TypeId, Span)>;

/// Check the program's subtype rules and lower every `as()` cast.
pub(crate) fn check_and_lower(program: &mut Program) -> Result<(), ParseError> {
    let decls: DeclIds = program
        .relations()
        .iter()
        .map(|r| (r.name().to_string(), r.attribute_declared_ids()))
        .collect();

    let (registry, segments) = program.registry_and_segments_mut();
    for segment in segments.iter_mut() {
        for rule in segment.as_rules_mut() {
            check_and_lower_rule(rule, registry, &decls)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check_and_lower_rule(rule, registry, &decls)?;
            }
        }
    }
    Ok(())
}

/// Check one rule's subtype rules and lower its `as()` casts.
fn check_and_lower_rule(
    rule: &mut FlowLogRule,
    reg: &TypeRegistry,
    decls: &DeclIds,
) -> Result<(), ParseError> {
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
                check_comparison(cmp, reg, &bindings)?;
            }
        }
    }

    check_head(rule, decls, reg, &bindings)?;
    lower_rule(rule);
    Ok(())
}

/// Refine each variable in `atom` to the meet of its binding-site types,
/// rejecting sibling mismatches. `bind = true` (positive atoms) introduces a
/// new binding on first sight; `bind = false` (negative atoms) only checks
/// variables already bound.
fn check_atom(
    atom: &Atom,
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &mut Bindings,
    bind: bool,
) -> Result<(), ParseError> {
    let col_ids = decls
        .get(atom.name())
        .ok_or_else(|| grammar_bug(format!("subtype pass: atom `{}` not declared", atom.name())))?;
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_id = col_ids
            .get(i)
            .copied()
            .ok_or_else(|| grammar_bug("subtype pass: atom arity mismatch"))?;
        let AtomArg::Var(v) = arg else { continue };
        match bindings.get(v).copied() {
            None if bind => {
                bindings.insert(v.clone(), (col_id, atom.span()));
            }
            None => {}
            Some((existing_id, existing_span)) => {
                let Some(meet) = reg.meet(existing_id, col_id) else {
                    return Err(ParseError::SubtypeMismatch {
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

/// Check that each head argument's type widens into its declared column:
/// narrowing (parent to subtype) needs an explicit `as()`, and every `as()`
/// cast is validated.
fn check_head(
    rule: &FlowLogRule,
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    let head = rule.head();
    let rel_name = head.name();
    let rel_display = head.raw_name().to_string();
    let col_ids = decls.get(rel_name).ok_or_else(|| {
        grammar_bug(format!(
            "subtype pass: head relation `{rel_name}` not declared"
        ))
    })?;
    for (col, (arg, &expected_id)) in head.head_arguments().iter().zip(col_ids.iter()).enumerate() {
        match arg {
            HeadArg::Var(v) => {
                if let Some(&(found_id, _)) = bindings.get(v)
                    && !reg.is_widening(found_id, expected_id)
                {
                    return Err(ParseError::HeadSubtypeMismatch {
                        span: head.span(),
                        rel: rel_display,
                        col,
                        expected: reg.name_of(expected_id).to_string(),
                        found: reg.name_of(found_id).to_string(),
                    });
                }
            }
            HeadArg::Arith(a) => {
                // Widening rule only applies when a single value flows
                // through. Multi-factor arithmetic drops subtype identity.
                // Tuple constructs descend field-wise (see `check_head_widen`).
                if let Err((found_id, expected_field)) =
                    check_head_widen(a, expected_id, reg, bindings)
                {
                    return Err(ParseError::HeadSubtypeMismatch {
                        span: head.span(),
                        rel: rel_display,
                        col,
                        expected: reg.name_of(expected_field).to_string(),
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
/// in the registry), return its TypeId. Multi-factor arithmetic and
/// unbound vars return `None`: arithmetic drops subtype identity, and
/// the primitive pass already validated the underlying types.
fn single_var_type(a: &Arithmetic, reg: &TypeRegistry, bindings: &Bindings) -> Option<TypeId> {
    if !a.rest().is_empty() {
        return None;
    }
    match a.init() {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| id),
        Factor::Cast(c) => reg.lookup(c.target_type()),
        // A projection carries the declared identity of the indexed field:
        // resolve the base tuple's type, then read its field's `TypeId`.
        Factor::TupleProj { tuple, index } => {
            let rec_id = single_var_type(tuple, reg, bindings)?;
            reg.tuple_field_ids(rec_id)?.get(*index).copied()
        }
        // A tuple construct has no single identity (validated field-wise by
        // `check_head_widen`); constants/arithmetic/calls carry none either.
        Factor::Const(_)
        | Factor::FnCall(_)
        | Factor::Builtin(_)
        | Factor::Group(_)
        | Factor::Tuple(_) => None,
    }
}

/// Recursively check that the value produced by head-arg `a` widens into the
/// declared column/field type `expected_id`. A tuple construct flowing into a
/// tuple column descends field-wise, so nominal field identities (which the
/// erased primitive pass collapses to bare roots) are still validated.
/// Returns the `(found, expected)` pair on the first mismatch.
fn check_head_widen(
    a: &Arithmetic,
    expected_id: TypeId,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), (TypeId, TypeId)> {
    if a.rest().is_empty()
        && let Factor::Tuple(lit) = a.init()
        && let Some(field_ids) = reg.tuple_field_ids(expected_id)
    {
        for (elem, &fid) in lit.fields().iter().zip(field_ids) {
            if let TupleElem::Expr(fa) = elem {
                check_head_widen(fa, fid, reg, bindings)?;
            }
        }
        return Ok(());
    }
    if let Some(found_id) = single_var_type(a, reg, bindings)
        && !reg.is_widening(found_id, expected_id)
    {
        return Err((found_id, expected_id));
    }
    Ok(())
}

/// Comparison operands with determinate type identity must have a meet.
/// Skipped when either side is a constant, arithmetic expression, or
/// UDF/builtin call (no subtype identity flows through those).
fn check_comparison(
    cmp: &ComparisonExpr,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    let (Some(l), Some(r)) = (
        single_var_type(cmp.left(), reg, bindings),
        single_var_type(cmp.right(), reg, bindings),
    ) else {
        return Ok(());
    };
    if reg.meet(l, r).is_none() {
        return Err(ParseError::ComparisonSubtypeMismatch {
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
) -> Result<(), ParseError> {
    check_factor_casts(a.init(), reg, bindings)?;
    for (_, f) in a.rest() {
        check_factor_casts(f, reg, bindings)?;
    }
    Ok(())
}

/// Validate every `as()` cast inside a factor.
fn check_factor_casts(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), ParseError> {
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
                    .ok_or_else(|| ParseError::UnknownCastType {
                        span: c.span(),
                        name: c.target_type().to_string(),
                    })?;
            let inner_root = inner_factor_primitive_root(c.inner(), reg, bindings);
            let target_root = reg.root_primitive(target_id);
            if let Some(inner) = inner_root
                && inner != target_root
            {
                return Err(ParseError::IllegalCast {
                    span: c.span(),
                    from: inner.to_string(),
                    to: reg.name_of(target_id).to_string(),
                });
            }
            // Recurse for nested casts: `as(as(x, A), B)`.
            check_factor_casts(c.inner(), reg, bindings)
        }
        Factor::Group(a) => check_arith_casts(a, reg, bindings),
        Factor::Tuple(r) => r
            .exprs()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::TupleProj { tuple, .. } => check_arith_casts(tuple, reg, bindings),
    }
}

/// Best-effort primitive root of `f` for the same-root check. `None`
/// for cases we can't resolve locally (UDF return, constant); the
/// primitive pass has already validated those, so we just skip.
fn inner_factor_primitive_root(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Option<DataType> {
    match f {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| reg.root_primitive(id)),
        Factor::Const(_) => None,
        Factor::FnCall(_) | Factor::Builtin(_) => None,
        Factor::Cast(c) => reg.lookup(c.target_type()).map(|id| reg.root_primitive(id)),
        // A grouped expression drops subtype identity, like multi-factor
        // arithmetic; the primitive pass already validated its contents.
        // Tuples / projections likewise carry no single primitive root here.
        Factor::Group(_) | Factor::Tuple(_) | Factor::TupleProj { .. } => None,
    }
}

// =============================================================================
// Cast lowering: strip every `Factor::Cast(c)` after the subtype
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
        Factor::Group(a) => lower_arith(a),
        Factor::Tuple(r) => {
            for a in r.exprs_mut() {
                lower_arith(a);
            }
        }
        Factor::TupleProj { tuple, .. } => lower_arith(tuple),
        // Drop the cast (`as(as(x, A), B)` collapses to `x`) and re-lower the
        // inner factor, so nested casts unwrap through the recursion.
        Factor::Cast(c) => {
            let inner = mem::replace(
                c.inner_mut(),
                Factor::Const(Constant::new(DataType::IntLit, "0")),
            );
            *f = inner;
            lower_factor(f);
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Factor;
    use crate::HeadArg;
    use crate::ParseError;
    use crate::assert_err;
    use crate::test_util::checked;

    /// `as(x, T)` with an undeclared target type is rejected.
    #[test]
    fn cast_to_unknown_type_rejected() {
        let src = "\
            .decl In(x: number)\n\
            .decl Out(v: number)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(as(x, Nope)) :- In(x).\n";
        assert_err!(checked(src), ParseError::UnknownCastType { .. });
    }

    /// Two aliases of `number` join freely; aliases are transparent.
    #[test]
    fn alias_join_allowed() {
        let src = "\
            .type A = number\n\
            .type B = number\n\
            .decl R(x: A)\n\
            .decl S(x: B)\n\
            .decl Out(x: number)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- R(x), S(x).\n";
        checked(src).expect("alias join must be allowed");
    }

    /// Head narrowing with explicit `as()` is accepted.
    #[test]
    fn head_narrowing_with_cast_accepted() {
        let src = "\
            .type UserId <: number\n\
            .decl Plain(x: number)\n\
            .decl OnlyUsers(u: UserId)\n\
            .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
            .output OnlyUsers\n\
            OnlyUsers(as(x, UserId)) :- Plain(x).\n";
        checked(src).expect("explicit narrowing must be allowed");
    }

    /// Head narrowing without `as()` is rejected, and parentheses around the
    /// variable must not bypass the check (`OnlyUsers((x))` is the same
    /// pass-through as `OnlyUsers(x)`; the parser collapses the group).
    #[test]
    fn head_narrowing_without_cast_rejected_even_parenthesized() {
        for head in ["OnlyUsers(x)", "OnlyUsers((x))"] {
            let src = format!(
                ".type UserId <: number\n\
                 .decl Plain(x: number)\n\
                 .decl OnlyUsers(u: UserId)\n\
                 .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
                 .output OnlyUsers\n\
                 {head} :- Plain(x).\n"
            );
            assert!(
                checked(&src).is_err(),
                "implicit narrowing must be rejected for {head}"
            );
        }
    }

    /// `as()` between two sibling subtypes of the same primitive is allowed:
    /// that's the escape hatch the rule exists for.
    #[test]
    fn sibling_subtype_cast_allowed() {
        let src = "\
            .type UserId    <: number\n\
            .type ProductId <: number\n\
            .decl Friend(a: UserId)\n\
            .decl ProductsForUsers(p: ProductId)\n\
            .input Friend(IO=\"file\", filename=\"Friend.csv\", delimiter=\",\")\n\
            .output ProductsForUsers\n\
            ProductsForUsers(as(x, ProductId)) :- Friend(x).\n";
        checked(src).expect("sibling-subtype cast must be allowed");
    }

    /// After typechecking, every `Factor::Cast` has been lowered to its inner
    /// factor. Downstream stages never see a cast wrapper.
    #[test]
    fn cast_is_lowered_after_typecheck() {
        let src = "\
            .type UserId <: number\n\
            .decl Plain(x: number)\n\
            .decl OnlyUsers(u: UserId)\n\
            .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
            .output OnlyUsers\n\
            OnlyUsers(as(x, UserId)) :- Plain(x).\n";
        let program = checked(src).expect("typecheck must succeed");
        let rule = program.rules()[0];
        for arg in rule.head().head_arguments() {
            if let HeadArg::Arith(a) = arg {
                assert!(
                    !matches!(a.init(), Factor::Cast(_)),
                    "cast should have been lowered after typecheck"
                );
            }
        }
    }
}
