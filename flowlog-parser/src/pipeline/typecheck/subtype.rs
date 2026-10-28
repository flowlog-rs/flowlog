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

use flowlog_common::Span;

use crate::AggregationOperator;
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
use crate::TypeId;
use crate::TypeRegistry;
use crate::Variable;
use crate::error::grammar_bug;

type DeclIds = HashMap<String, Vec<TypeId>>;

/// Variable -> (inferred TypeId, first-seen span). The TypeId is the
/// meet of all binding-site types seen so far.
type Bindings = HashMap<Variable, (TypeId, Span)>;

/// Checks the program's subtype rules and lowers every `as()` cast.
pub(crate) fn check_and_lower(program: &mut Program) -> Result<(), ParseError> {
    let decls: DeclIds = program
        .relations()
        .iter()
        .map(|r| (r.name().to_string(), r.attribute_declared_ids()))
        .collect();

    let (registry, rules) = program.registry_and_rules_mut();
    for rule in rules.iter_mut() {
        check_and_lower_rule(rule, registry, &decls)?;
    }
    Ok(())
}

/// Checks one rule's subtype rules and lowers its `as()` casts.
fn check_and_lower_rule(
    rule: &mut FlowLogRule,
    reg: &TypeRegistry,
    decls: &DeclIds,
) -> Result<(), ParseError> {
    let mut bindings: Bindings = HashMap::new();

    // Bind via positive atoms first, aggregate bodies included, so
    // out-of-order body predicates can resolve their variables.
    bind_positive_atoms(rule.rhs(), decls, reg, &mut bindings)?;
    bind_aggregate_vars(rule.rhs(), reg, &mut bindings)?;
    check_predicates(rule.rhs(), decls, reg, &mut bindings)?;
    check_head(rule, decls, reg, &bindings)?;
    lower_rule(rule);
    Ok(())
}

/// Binds the variables of every positive atom in `predicates`, those in
/// aggregate bodies included.
fn bind_positive_atoms(
    predicates: &[Predicate],
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(atom) => check_atom(atom, decls, reg, bindings, true)?,
            Predicate::Aggregate(aggregate) => {
                bind_positive_atoms(aggregate.body(), decls, reg, bindings)?;
            }
            Predicate::NegativeAtom(_) | Predicate::Compare(_) => {}
        }
    }
    Ok(())
}

/// Binds the variable of each `min` or `max` over a single value to that
/// value's type: the extreme is one of the values. Every other aggregate
/// computes a new number, which carries no subtype, like arithmetic. Nested
/// aggregates are bound first, since a target may read their variables.
fn bind_aggregate_vars(
    predicates: &[Predicate],
    reg: &TypeRegistry,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for predicate in predicates {
        let Predicate::Aggregate(aggregate) = predicate else {
            continue;
        };
        bind_aggregate_vars(aggregate.body(), reg, bindings)?;
        let keeps_identity = match aggregate.operator() {
            AggregationOperator::Min | AggregationOperator::Max => true,
            AggregationOperator::Count | AggregationOperator::Sum | AggregationOperator::Mean => {
                false
            }
        };
        if keeps_identity
            && let Some(var) = aggregate.var()
            && let Some(target) = aggregate.target()
            && let Some(id) = single_var_type(target, reg, bindings)?
        {
            bindings.insert(var.clone(), (id, aggregate.span()));
        }
    }
    Ok(())
}

/// Checks every non-binding predicate in `predicates`, aggregates included.
fn check_predicates(
    predicates: &[Predicate],
    decls: &DeclIds,
    reg: &TypeRegistry,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(_) => {}
            Predicate::NegativeAtom(atom) => {
                check_atom(atom, decls, reg, bindings, false)?;
            }
            Predicate::Compare(cmp) => {
                check_arith_casts(cmp.left(), reg, bindings)?;
                check_arith_casts(cmp.right(), reg, bindings)?;
                check_comparison(cmp, reg, bindings)?;
            }
            Predicate::Aggregate(aggregate) => {
                if let Some(target) = aggregate.target() {
                    check_arith_casts(target, reg, bindings)?;
                }
                check_predicates(aggregate.body(), decls, reg, bindings)?;
            }
        }
    }
    Ok(())
}

/// Refines each variable in `atom` to the meet of its binding-site types,
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

/// Checks that each head argument's type widens into its declared column:
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
                if let Some((found_id, expected_field)) =
                    check_head_widen(a, expected_id, reg, bindings)?
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
        }
    }
    Ok(())
}

/// If `a` is a single-factor expression that carries a determinate
/// type identity (a bound variable, or an `as()` cast), returns its TypeId.
/// Multi-factor arithmetic and unbound vars return `None`: arithmetic drops
/// subtype identity, and the primitive pass already validated the underlying
/// types.
fn single_var_type(
    a: &Arithmetic,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<Option<TypeId>, ParseError> {
    if !a.rest().is_empty() {
        return Ok(None);
    }
    Ok(match a.init() {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| id),
        Factor::Cast(c) => Some(c.target()?.id()),
        // A projection carries the declared identity of the indexed field:
        // resolve the base tuple's type, then read its field's `TypeId`.
        Factor::TupleProj { tuple, index } => single_var_type(tuple, reg, bindings)?
            .and_then(|rec_id| reg.tuple_field_ids(rec_id))
            .and_then(|field_ids| field_ids.get(*index).copied()),
        // A tuple construct has no single identity (validated field-wise by
        // `check_head_widen`); constants/arithmetic/calls carry none either.
        Factor::Const(_)
        | Factor::FnCall(_)
        | Factor::Builtin(_)
        | Factor::Group(_)
        | Factor::Placeholder
        | Factor::Tuple(_)
        | Factor::Aggregate(_) => None,
    })
}

/// Recursively checks that the value produced by head-arg `a` widens into the
/// declared column/field type `expected_id`. A tuple construct flowing into a
/// tuple column descends field-wise, so nominal field identities (which the
/// erased primitive pass collapses to bare roots) are still validated.
/// Returns the `(found, expected)` pair on the first mismatch, `None` if
/// every value widens.
fn check_head_widen(
    a: &Arithmetic,
    expected_id: TypeId,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<Option<(TypeId, TypeId)>, ParseError> {
    if let Some(fields) = a.as_tuple()
        && let Some(field_ids) = reg.tuple_field_ids(expected_id)
    {
        for (fa, &fid) in fields.iter().zip(field_ids) {
            if let Some(mismatch) = check_head_widen(fa, fid, reg, bindings)? {
                return Ok(Some(mismatch));
            }
        }
        return Ok(None);
    }
    if let Some(found_id) = single_var_type(a, reg, bindings)?
        && !reg.is_widening(found_id, expected_id)
    {
        return Ok(Some((found_id, expected_id)));
    }
    Ok(None)
}

/// Checks that comparison operands with determinate type identity have a
/// meet. Skipped when either side is a constant, arithmetic expression, or
/// UDF/builtin call (no subtype identity flows through those).
fn check_comparison(
    cmp: &ComparisonExpr,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    let (Some(l), Some(r)) = (
        single_var_type(cmp.left(), reg, bindings)?,
        single_var_type(cmp.right(), reg, bindings)?,
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

/// Validates every `as()` cast inside an arithmetic expression.
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

/// Validates every `as()` cast inside a factor.
fn check_factor_casts(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    match f {
        Factor::Var(_) | Factor::Const(_) | Factor::Placeholder => Ok(()),
        Factor::FnCall(fc) => fc
            .args()
            .iter()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::Builtin(bc) => bc
            .args()
            .iter()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::Cast(c) => {
            if let Some(inner) = inner_factor_primitive_root(c.inner(), reg, bindings)?
                && &inner != c.target()?.root()
            {
                return Err(ParseError::IllegalCast {
                    span: c.span(),
                    from: inner.to_string(),
                    to: c.target_type().to_string(),
                });
            }
            // Recurse for nested casts: `as(as(x, A), B)`.
            check_factor_casts(c.inner(), reg, bindings)
        }
        Factor::Group(a) => check_arith_casts(a, reg, bindings),
        Factor::Tuple(fields) => fields
            .iter()
            .try_for_each(|a| check_arith_casts(a, reg, bindings)),
        Factor::TupleProj { tuple, .. } => check_arith_casts(tuple, reg, bindings),
        Factor::Aggregate(agg) => Err(grammar_bug(format!(
            "aggregate `{agg}` was not hoisted out of its expression"
        ))),
    }
}

/// Best-effort primitive root of `f` for the same-root check. `None`
/// for cases we can't resolve locally (UDF return, constant); the
/// primitive pass has already validated those, so they are skipped.
fn inner_factor_primitive_root(
    f: &Factor,
    reg: &TypeRegistry,
    bindings: &Bindings,
) -> Result<Option<DataType>, ParseError> {
    Ok(match f {
        Factor::Var(v) => bindings.get(v).map(|&(id, _)| reg.root_primitive(id)),
        Factor::Const(_) | Factor::FnCall(_) | Factor::Builtin(_) => None,
        Factor::Cast(c) => Some(c.target()?.root().clone()),
        // A grouped expression drops subtype identity, like multi-factor
        // arithmetic; the primitive pass already validated its contents.
        // Tuples / projections likewise carry no single primitive root here.
        Factor::Group(_)
        | Factor::Tuple(_)
        | Factor::Placeholder
        | Factor::TupleProj { .. }
        | Factor::Aggregate(_) => None,
    })
}

// =============================================================================
// Cast lowering: strip every `Factor::Cast(c)` after the subtype
// check has approved it. Downstream stages never see a cast.
// =============================================================================

fn lower_rule(rule: &mut FlowLogRule) {
    lower_predicates(rule.rhs_mut());
    for arg in rule.head_mut().head_arguments_mut() {
        match arg {
            HeadArg::Var(_) => {}
            HeadArg::Arith(a) => lower_arith(a),
        }
    }
}

fn lower_predicates(predicates: &mut [Predicate]) {
    for pred in predicates {
        match pred {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            Predicate::Compare(cmp) => {
                lower_arith(cmp.left_mut());
                lower_arith(cmp.right_mut());
            }
            Predicate::Aggregate(aggregate) => {
                if let Some(target) = aggregate.target_mut() {
                    lower_arith(target);
                }
                lower_predicates(aggregate.body_mut());
            }
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
        Factor::Tuple(fields) => {
            for a in fields {
                lower_arith(a);
            }
        }
        Factor::TupleProj { tuple, .. } => lower_arith(tuple),
        // Rule construction hoisted every aggregate into a binding and
        // named every placeholder.
        Factor::Placeholder | Factor::Aggregate(_) => {}
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
    use rstest::rstest;

    use crate::Factor;
    use crate::HeadArg;
    use crate::ParseError;
    use crate::assert_err;
    use crate::test_harness::checked;

    /// `as()` is a runtime no-op, not a conversion, so a variable cast to
    /// another primitive root is rejected, a wider integer included.
    // Cases: target.
    #[rstest]
    #[case::wider_integer("int64")]
    #[case::float("f64")]
    fn variable_cast_across_primitive_roots_rejected(#[case] target: &str) {
        let src = format!(
            ".decl In(x: int32)\n\
             .decl Out(v: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(x) :- In(x), as(x, {target}) > 0.\n"
        );
        assert_err!(
            checked(&src),
            ParseError::IllegalCast { from, to, .. } if from == "int32" && to == target
        );
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
        let rule = &program.rules()[0];
        for arg in rule.head().head_arguments() {
            if let HeadArg::Arith(a) = arg {
                assert!(
                    !matches!(a.init(), Factor::Cast(_)),
                    "cast should have been lowered after typecheck"
                );
            }
        }
    }

    /// Aggregates a user-id column into an order-id column.
    fn order_ids_from(aggregate: &str) -> String {
        format!(
            ".type UserId <: number\n\
             .type OrderId <: number\n\
             .decl Users(x: UserId)\n\
             .decl Out(o: OrderId)\n\
             .input Users(IO=\"file\", filename=\"Users.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(v) :- v = {aggregate}.\n"
        )
    }

    /// `min` and `max` answer one of their values, so the answer keeps the
    /// value's subtype and cannot fill a sibling subtype's column.
    #[rstest]
    #[case("min x : Users(x)")]
    #[case("max x : Users(x)")]
    fn an_extreme_keeps_its_value_subtype(#[case] aggregate: &str) {
        assert_err!(
            checked(&order_ids_from(aggregate)),
            ParseError::HeadSubtypeMismatch { .. }
        );
    }

    /// `count` computes a new number, which carries no subtype, as
    /// arithmetic does.
    #[test]
    fn a_count_carries_no_subtype() {
        checked(&order_ids_from("count : Users(_)")).unwrap();
    }
}
