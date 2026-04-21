//! FlowLog rule-level type checker with constant-type inference.
//!
//! Runs after parse, before stratification. Binds variable types from
//! positive-atom columns, checks every body and head site against the
//! binding map, **and pins every polymorphic literal** to the concrete
//! width derived from its surrounding context. Spans come from the AST,
//! so diagnostics point at the offending expression rather than the
//! enclosing rule.
//!
//! # Two jobs
//!
//! - **Check.** Reject programs whose types don't line up.
//! - **Infer const types.** Every `ConstType::Int(_)` / `Float(_)`
//!   placeholder from the parser is rewritten in place to its concrete
//!   counterpart via [`ConstType::pin`]. After [`check_program`] returns
//!   `Ok`, no polymorphic literal survives anywhere in the program —
//!   catalog, planner, and codegen can call `data_type()` unconditionally.
//!
//! # What we reject
//!
//! - A variable bound to one type but reused as another
//!   (`A(x, _), B(x, _)` where `A` and `B` disagree on column 0).
//! - Arithmetic or comparison between two concrete types that differ
//!   (e.g. `Int32 + Float64`, `x = s` where `x: Int32, s: String`).
//! - Operators applied to an incompatible type: `+-*/%` on `Bool` or
//!   `String`, `cat` on anything non-string, `<`/`>` on `Bool`.
//! - A constant whose family doesn't match the column (`5.0` into
//!   `Int32`, `"x"` into `Bool`).
//! - Calls to undeclared UDFs, wrong arity, or arg of the wrong family.
//! - `sum`/`avg`/`min`/`max` over a non-numeric input, or declared with
//!   an output type that contradicts the op.
//! - A head arity or column type that doesn't match the relation's
//!   `.decl`.
//!
//! # What we allow
//!
//! - Integer literals match any integer column (`Int8`..`UInt64`); float
//!   literals match any float column (`Float32`/`Float64`). The width is
//!   fixed by context and written back by [`ConstType::pin`].
//! - We do **not** range-check integer literals: `300` into a `UInt8`
//!   column passes here and is caught later by the Rust compiler on the
//!   generated code.
//! - Unbound variables in negated atoms, comparisons, or UDF calls —
//!   reported separately by the range-restriction pass, not here.

mod error;

pub use error::TypeCheckError;

use std::collections::HashMap;

use crate::common::source::Span;
use crate::parser::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg,
    ComparisonExpr, ComparisonOperator, ConstType, DataType, Factor, FlowLogRule, FnCall, HeadArg,
    Predicate, Program,
};

/// Type-check every rule and pin each polymorphic literal to its
/// concrete width. Stops at the first failure; on `Ok(())` the program's
/// literals are fully concrete.
pub fn check_program(program: &mut Program) -> Result<(), TypeCheckError> {
    let decls: DeclTypes = program
        .relations()
        .iter()
        .map(|r| (r.name().to_string(), r.data_type()))
        .collect();
    let udfs: UdfSigs = program
        .udfs()
        .iter()
        .map(|u| {
            (
                u.name().to_string(),
                (
                    u.params()
                        .iter()
                        .map(|p| (p.name().to_string(), *p.data_type()))
                        .collect(),
                    u.ret_type(),
                ),
            )
        })
        .collect();

    for segment in program.segments_mut() {
        for rule in segment.as_rules_mut() {
            check_rule(rule, &decls, &udfs)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check_rule(rule, &decls, &udfs)?;
            }
        }
    }

    check_and_pin_facts(program.facts_mut(), &decls)
}

type DeclTypes = HashMap<String, Vec<DataType>>;
type UdfSigs = HashMap<String, (Vec<(String, DataType)>, DataType)>;

/// Var → (first-seen type, first-seen span). Later uses must agree.
type Bindings = HashMap<String, (DataType, Span)>;

/// Numeric literals stay polymorphic within their family (`IntLit` /
/// `FloatLit`) until a concrete context fixes the type. Concrete literals
/// carry their resolved [`DataType`] directly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LitKind {
    IntLit,
    FloatLit,
    Concrete(DataType),
}

impl LitKind {
    fn fits(self, expected: DataType) -> bool {
        match self {
            LitKind::IntLit => expected.is_integer(),
            LitKind::FloatLit => expected.is_float(),
            LitKind::Concrete(t) => t == expected,
        }
    }

    fn is_numeric(self) -> bool {
        match self {
            LitKind::IntLit | LitKind::FloatLit => true,
            LitKind::Concrete(t) => t.is_numeric(),
        }
    }

    /// Representative concrete type for diagnostic rendering **and** for
    /// pinning all-literal expressions that never met a concrete partner.
    fn report_ty(self) -> DataType {
        match self {
            LitKind::IntLit => DataType::Int32,
            LitKind::FloatLit => DataType::Float32,
            LitKind::Concrete(t) => t,
        }
    }
}

/// Combine two operand kinds across an arithmetic operator. `None` on
/// a family mismatch.
fn merge_lit(a: LitKind, b: LitKind) -> Option<LitKind> {
    match (a, b) {
        (x, y) if x == y => Some(x),
        (LitKind::Concrete(t), LitKind::IntLit) | (LitKind::IntLit, LitKind::Concrete(t))
            if t.is_integer() =>
        {
            Some(LitKind::Concrete(t))
        }
        (LitKind::Concrete(t), LitKind::FloatLit) | (LitKind::FloatLit, LitKind::Concrete(t))
            if t.is_float() =>
        {
            Some(LitKind::Concrete(t))
        }
        _ => None,
    }
}

fn check_rule(
    rule: &mut FlowLogRule,
    decls: &DeclTypes,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    // Bind vars first so out-of-order body predicates can resolve them.
    let mut bindings: Bindings = HashMap::new();
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtomPredicate(atom) = predicate {
            bind_atom_vars(atom, decls, &mut bindings)?;
        }
    }

    for predicate in rule.rhs_mut() {
        match predicate {
            Predicate::PositiveAtomPredicate(atom) => pin_atom_consts(atom, decls)?,
            Predicate::NegativeAtomPredicate(atom) => {
                check_atom_uses(atom, decls, &bindings)?;
                pin_atom_consts(atom, decls)?;
            }
            Predicate::ComparePredicate(cmp) => check_comparison(cmp, &bindings, udfs)?,
            Predicate::FnCallPredicate(fc) => {
                // UDF predicate return is always bool; drop the inferred type.
                infer_fn_call_type(fc, &bindings, udfs)?;
                pin_fn_call_args(fc, udfs)?;
            }
        }
    }

    check_head(rule, decls, udfs, &bindings)
}

/// Record each variable's first-seen type; validate const arg families.
/// Consts are pinned separately by [`pin_atom_consts`] so bindings can
/// be populated for the whole rule before any mutation.
fn bind_atom_vars(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &mut Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => match bindings.get(v) {
                None => {
                    bindings.insert(v.clone(), (col_ty, atom.span()));
                }
                Some(&(first_ty, first_span)) if first_ty != col_ty => {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty,
                        first_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
                Some(_) => {}
            },
            AtomArg::Const(c) => {
                if !lit_kind(c)?.fits(col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Check each bound variable matches its column type. Unbound vars are
/// reported separately by the range-restriction pass.
fn check_atom_uses(
    atom: &Atom,
    decls: &DeclTypes,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        match arg {
            AtomArg::Var(v) => {
                if let Some(&(bound_ty, bound_span)) = bindings.get(v) {
                    if bound_ty != col_ty {
                        return Err(TypeCheckError::TypeMismatch {
                            var: v.clone(),
                            first_ty: bound_ty,
                            first_span: bound_span,
                            later_ty: col_ty,
                            later_span: atom.span(),
                        });
                    }
                }
            }
            AtomArg::Const(c) => {
                if !lit_kind(c)?.fits(col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: atom.span(),
                        literal: c.to_string(),
                        expected: col_ty,
                    });
                }
            }
            AtomArg::Placeholder => {}
        }
    }
    Ok(())
}

/// Pin every polymorphic const argument of `atom` to its declared column
/// type. Call after [`bind_atom_vars`] / [`check_atom_uses`] has already
/// validated the family fit.
fn pin_atom_consts(atom: &mut Atom, decls: &DeclTypes) -> Result<(), TypeCheckError> {
    let col_types: Vec<DataType> = {
        let Some(decl) = decls.get(atom.name()) else {
            return Err(TypeCheckError::internal(format!(
                "atom `{}` not declared",
                atom.name()
            )));
        };
        decl.clone()
    };
    for (arg, col_ty) in atom.arguments_mut().iter_mut().zip(col_types.iter()) {
        if let AtomArg::Const(c) = arg {
            if c.is_polymorphic() {
                c.pin(*col_ty);
            }
        }
    }
    Ok(())
}

fn resolve_atom_column(
    atom: &Atom,
    i: usize,
    decls: &DeclTypes,
) -> Result<DataType, TypeCheckError> {
    let decl = decls
        .get(atom.name())
        .ok_or_else(|| TypeCheckError::internal(format!("atom `{}` not declared", atom.name())))?;
    decl.get(i).copied().ok_or_else(|| {
        TypeCheckError::internal(format!(
            "atom `{}` has {} arguments but `.decl` has {}",
            atom.name(),
            atom.arguments().len(),
            decl.len(),
        ))
    })
}

fn check_comparison(
    cmp: &mut ComparisonExpr,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    let left = infer_expr_type(cmp.left(), bindings, udfs)?;
    let right = infer_expr_type(cmp.right(), bindings, udfs)?;
    let op = cmp.operator().clone();
    let span = cmp.span();

    if let (Some(l), Some(r)) = (left, right) {
        if merge_lit(l, r).is_none() {
            return Err(TypeCheckError::ComparisonTypeMismatch {
                span,
                op,
                left: l.report_ty(),
                right: r.report_ty(),
            });
        }
    }

    // Ordering comparisons additionally require an ordered type.
    if !matches!(op, ComparisonOperator::Equal | ComparisonOperator::NotEqual) {
        if let Some(kind) = left.or(right) {
            let is_ordered =
                kind.is_numeric() || matches!(kind, LitKind::Concrete(DataType::String));
            if !is_ordered {
                return Err(TypeCheckError::ComparisonOpNotAllowed {
                    span,
                    op,
                    ty: kind.report_ty(),
                });
            }
        }
    }

    // Pin: both sides unify to the same concrete type. Fall back to the
    // family's representative width when both sides are polymorphic.
    let target = match (left, right) {
        (Some(l), Some(r)) => merge_lit(l, r).map(LitKind::report_ty),
        (Some(k), None) | (None, Some(k)) => Some(k.report_ty()),
        (None, None) => None,
    };
    if let Some(t) = target {
        pin_arith_literals(cmp.left_mut(), t, udfs)?;
        pin_arith_literals(cmp.right_mut(), t, udfs)?;
    }
    Ok(())
}

fn check_head(
    rule: &mut FlowLogRule,
    decls: &DeclTypes,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let head = rule.head_mut();
    let (rel_name, arity, head_span) = (head.name().to_string(), head.arity(), head.span());
    let col_types: Vec<DataType> = {
        let Some(decl) = decls.get(&rel_name) else {
            return Err(TypeCheckError::internal(format!(
                "head relation `{rel_name}` not declared"
            )));
        };
        decl.clone()
    };

    if arity != col_types.len() {
        return Err(TypeCheckError::HeadArity {
            span: head_span,
            rel: rel_name,
            expected: col_types.len(),
            found: arity,
        });
    }

    for (col, (arg, expected)) in head
        .head_arguments_mut()
        .iter_mut()
        .zip(col_types.iter().copied())
        .enumerate()
    {
        match arg {
            HeadArg::Aggregation(agg) => check_aggregation(agg, expected, udfs, bindings)?,
            HeadArg::Var(v) => {
                if let Some(&(found, _)) = bindings.get(v) {
                    if found != expected {
                        return Err(TypeCheckError::HeadColumnType {
                            span: head_span,
                            rel: rel_name.clone(),
                            col,
                            expected,
                            found,
                        });
                    }
                }
            }
            HeadArg::Arith(a) => {
                if let Some(kind) = infer_expr_type(a, bindings, udfs)? {
                    if !kind.fits(expected) {
                        return Err(head_or_literal_mismatch(a, &rel_name, col, expected, kind));
                    }
                }
                pin_arith_literals(a, expected, udfs)?;
            }
        }
    }
    Ok(())
}

/// Bare literal → `LiteralColumnMismatch` (cites the source text);
/// anything else → `HeadColumnType` (cites the inferred type).
fn head_or_literal_mismatch(
    a: &Arithmetic,
    rel: &str,
    col: usize,
    expected: DataType,
    kind: LitKind,
) -> TypeCheckError {
    if let Some(c) = bare_const(a) {
        return TypeCheckError::LiteralColumnMismatch {
            span: a.span(),
            literal: c.to_string(),
            expected,
        };
    }
    TypeCheckError::HeadColumnType {
        span: a.span(),
        rel: rel.to_string(),
        col,
        expected,
        found: kind.report_ty(),
    }
}

fn check_aggregation(
    agg: &mut Aggregation,
    declared: DataType,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let op = *agg.operator();
    let span = agg.span();
    let arg_kind = infer_expr_type(agg.arithmetic(), bindings, udfs)?;

    // `count`'s input type is independent of its declared output.
    if matches!(op, AggregationOperator::Count) {
        if !declared.is_numeric() {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
        if let Some(k) = arg_kind {
            pin_arith_literals(agg.arithmetic_mut(), k.report_ty(), udfs)?;
        }
        return Ok(());
    }

    // sum / avg / min / max: numeric input, output family matches input.
    if let Some(kind) = arg_kind {
        if !kind.is_numeric() {
            return Err(TypeCheckError::AggregationInputNotNumeric {
                span,
                op,
                ty: kind.report_ty(),
            });
        }
        if !kind.fits(declared) {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
    }
    pin_arith_literals(agg.arithmetic_mut(), declared, udfs)?;
    Ok(())
}

/// Infer an expression's kind, merging factor kinds left to right.
/// `None` iff every variable factor is unbound (reported later by the
/// range-restriction pass).
fn infer_expr_type(
    expr: &Arithmetic,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<Option<LitKind>, TypeCheckError> {
    let span = expr.span();
    let mut inferred = infer_factor_type(expr.init(), bindings, udfs)?;

    for (op, factor) in expr.rest() {
        if let Some(k) = infer_factor_type(factor, bindings, udfs)? {
            inferred = match inferred {
                None => Some(k),
                Some(existing) => Some(merge_lit(existing, k).ok_or(
                    TypeCheckError::ArithmeticTypeMismatch {
                        span,
                        left: existing.report_ty(),
                        right: k.report_ty(),
                    },
                )?),
            };
        }
        if let Some(k) = inferred {
            check_arith_op(k, op, span)?;
        }
    }
    Ok(inferred)
}

fn infer_factor_type(
    factor: &Factor,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<Option<LitKind>, TypeCheckError> {
    Ok(match factor {
        Factor::Var(v) => bindings.get(v).map(|&(ty, _)| LitKind::Concrete(ty)),
        Factor::Const(c) => Some(lit_kind(c)?),
        Factor::FnCall(fc) => Some(LitKind::Concrete(infer_fn_call_type(fc, bindings, udfs)?)),
    })
}

fn infer_fn_call_type(
    fc: &FnCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<DataType, TypeCheckError> {
    let (param_types, ret_ty) =
        udfs.get(fc.name())
            .ok_or_else(|| TypeCheckError::UndeclaredUdf {
                span: fc.span(),
                name: fc.name().to_string(),
            })?;

    if fc.args().len() != param_types.len() {
        return Err(TypeCheckError::UdfArity {
            span: fc.span(),
            name: fc.name().to_string(),
            expected: param_types.len(),
            found: fc.args().len(),
        });
    }

    for (arg, (param_name, expected)) in fc.args().iter().zip(param_types.iter()) {
        let Some(kind) = infer_expr_type(arg, bindings, udfs)? else {
            continue;
        };
        if !kind.fits(*expected) {
            return Err(TypeCheckError::UdfArgType {
                span: arg.span(),
                name: fc.name().to_string(),
                param: param_name.clone(),
                expected: *expected,
                found: kind.report_ty(),
            });
        }
    }

    Ok(*ret_ty)
}

fn lit_kind(c: &ConstType) -> Result<LitKind, TypeCheckError> {
    Ok(match c {
        ConstType::Int(_) => LitKind::IntLit,
        ConstType::Float(_) => LitKind::FloatLit,
        _ => LitKind::Concrete(c.data_type().ok_or_else(|| {
            TypeCheckError::internal(format!(
                "lit_kind: polymorphic literal {c:?} escaped Int/Float arms"
            ))
        })?),
    })
}

/// `Some(c)` iff `a` is a single constant with no operators.
fn bare_const(a: &Arithmetic) -> Option<&ConstType> {
    match (a.is_const(), a.init()) {
        (true, Factor::Const(c)) => Some(c),
        _ => None,
    }
}

/// Numeric ops (`+`, `-`, `*`, `/`, `%`) require numeric factors;
/// `cat` requires strings; `Bool` has no arithmetic.
fn check_arith_op(
    kind: LitKind,
    op: &ArithmeticOperator,
    span: Span,
) -> Result<(), TypeCheckError> {
    let is_cat = matches!(op, ArithmeticOperator::Cat);
    let allowed = match kind {
        LitKind::Concrete(DataType::Bool) => false,
        LitKind::Concrete(DataType::String) => is_cat,
        _ => !is_cat,
    };
    if allowed {
        Ok(())
    } else {
        Err(TypeCheckError::ArithmeticOpNotAllowed {
            span,
            op: op.clone(),
            ty: kind.report_ty(),
        })
    }
}

/// Pin every polymorphic literal in `a` to `target`. Recurses into UDF
/// argument expressions using the UDF's declared parameter types — those
/// types are independent of the enclosing expression's `target`.
fn pin_arith_literals(
    a: &mut Arithmetic,
    target: DataType,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    pin_factor(a.init_mut(), target, udfs)?;
    for (_, f) in a.rest_mut() {
        pin_factor(f, target, udfs)?;
    }
    Ok(())
}

fn pin_factor(
    factor: &mut Factor,
    target: DataType,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    match factor {
        Factor::Const(c) => {
            if c.is_polymorphic() {
                c.pin(target);
            }
            Ok(())
        }
        Factor::Var(_) => Ok(()),
        Factor::FnCall(fc) => pin_fn_call_args(fc, udfs),
    }
}

fn pin_fn_call_args(fc: &mut FnCall, udfs: &UdfSigs) -> Result<(), TypeCheckError> {
    // Collected by value so the recursive `pin_arith_literals` below can
    // reborrow `udfs` — holding `&param_types` from `udfs.get(...)` across
    // the recursion would block the reborrow.
    let param_types: Vec<DataType> = udfs
        .get(fc.name())
        .map(|(params, _)| params.iter().map(|(_, ty)| *ty).collect())
        .ok_or_else(|| {
            TypeCheckError::internal(format!(
                "pin_fn_call_args: UDF `{}` not declared",
                fc.name()
            ))
        })?;
    for (arg, pty) in fc.args_mut().iter_mut().zip(param_types.iter()) {
        pin_arith_literals(arg, *pty, udfs)?;
    }
    Ok(())
}

/// Validate each fact tuple's column families against its `.decl` and pin
/// polymorphic literals. Diagnostics cite the fact's head span.
fn check_and_pin_facts(
    facts: &mut HashMap<String, Vec<(Span, Vec<ConstType>)>>,
    decls: &DeclTypes,
) -> Result<(), TypeCheckError> {
    for (rel_name, tuples) in facts.iter_mut() {
        let Some(col_types) = decls.get(rel_name) else {
            return Err(TypeCheckError::internal(format!(
                "fact references undeclared relation `{rel_name}`"
            )));
        };
        for (span, tuple) in tuples.iter_mut() {
            if tuple.len() != col_types.len() {
                return Err(TypeCheckError::HeadArity {
                    span: *span,
                    rel: rel_name.clone(),
                    expected: col_types.len(),
                    found: tuple.len(),
                });
            }
            for (c, col_ty) in tuple.iter_mut().zip(col_types.iter()) {
                if !lit_kind(c)?.fits(*col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: *span,
                        literal: c.to_string(),
                        expected: *col_ty,
                    });
                }
                if c.is_polymorphic() {
                    c.pin(*col_ty);
                }
            }
        }
    }
    Ok(())
}
