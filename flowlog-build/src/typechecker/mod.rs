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
mod subtype;

pub use error::TypeCheckError;

use std::collections::HashMap;

use flowlog_parser::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg, BuiltinCall,
    BuiltinOperator, ComparisonExpr, ComparisonOperator, ConstType, DataType, Factor, FlowLogRule,
    FnCall, HeadArg, Predicate, Program, TupleElem, TupleLit,
};
use flowlog_common::{Config, Span};

/// Type-check every rule and pin each polymorphic literal to its
/// concrete width. Stops at the first failure; on `Ok(())` the program's
/// literals are fully concrete and subtype rules have been enforced.
///
/// `config` is consulted for config-gated builtins (see
/// [`check_builtin_config_requirements`]).
pub fn check_program(program: &mut Program, config: &Config) -> Result<(), TypeCheckError> {
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
                        .map(|p| (p.name().to_string(), p.data_type().clone()))
                        .collect(),
                    u.ret_type(),
                ),
            )
        })
        .collect();
    // Canonical name -> user-facing spelling, for diagnostics only.
    let display: DisplayNames = program
        .relations()
        .iter()
        .map(|r| (r.name().to_string(), r.raw_name().to_string()))
        .collect();

    for segment in program.segments_mut() {
        for rule in segment.as_rules_mut() {
            check_rule(rule, &decls, &udfs, &display)?;
        }
        if let Some(block) = segment.as_loop_mut() {
            for rule in block.rules_mut() {
                check_rule(rule, &decls, &udfs, &display)?;
            }
        }
    }

    check_builtin_config_requirements(program, config)?;

    check_and_pin_facts(program.facts_mut(), &decls, &display)?;

    // Second pass: enforce subtype rules using the type registry. The
    // first pass works in primitive `DataType`s only, so two sibling
    // subtypes of `number` would silently join. This pass tracks each
    // variable's declared `TypeId` and rejects sibling-subtype joins,
    // enforces the asymmetric head-widening rule, and validates `as()`
    // casts. It also lowers every surviving `Factor::Cast` to its
    // inner factor so downstream stages never see a cast.
    subtype::check_and_lower(program, &display)
}

/// Reject built-in calls whose semantics depend on a build flag that
/// isn't enabled — today only `ord(_)`, which needs `--str-intern`.
fn check_builtin_config_requirements(
    program: &Program,
    config: &Config,
) -> Result<(), TypeCheckError> {
    if config.str_intern_enabled() {
        return Ok(());
    }
    fn check_arith(a: &Arithmetic) -> Result<(), TypeCheckError> {
        check_factor(a.init())?;
        for (_, f) in a.rest() {
            check_factor(f)?;
        }
        Ok(())
    }
    fn check_factor(f: &Factor) -> Result<(), TypeCheckError> {
        match f {
            Factor::Var(_) | Factor::Const(_) => Ok(()),
            Factor::FnCall(fc) => fc.args().iter().try_for_each(check_arith),
            Factor::Builtin(bc) => {
                if bc.op() == BuiltinOperator::Ord {
                    return Err(TypeCheckError::OrdRequiresStrIntern { span: bc.span() });
                }
                bc.args().iter().try_for_each(check_arith)
            }
            Factor::Cast(c) => check_factor(c.inner()),
            Factor::Group(a) => check_arith(a),
            Factor::Tuple(r) => r.exprs().try_for_each(check_arith),
            Factor::TupleProj { tuple, .. } => check_arith(tuple),
        }
    }
    for segment in program.segments() {
        for rule in segment.as_rules() {
            for predicate in rule.rhs() {
                match predicate {
                    Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
                    Predicate::Compare(cmp) => {
                        check_arith(cmp.left())?;
                        check_arith(cmp.right())?;
                    }
                    Predicate::FnCall(fc) => fc.args().iter().try_for_each(check_arith)?,
                }
            }
            for head_arg in rule.head().head_arguments() {
                match head_arg {
                    HeadArg::Var(_) => {}
                    HeadArg::Arith(a) => check_arith(a)?,
                    HeadArg::Aggregation(agg) => check_arith(agg.arithmetic())?,
                }
            }
        }
    }
    Ok(())
}

type DeclTypes = HashMap<String, Vec<DataType>>;
/// Canonical relation name -> the user's original spelling. Used only to
/// render names in diagnostics; every lookup and key stays canonical.
type DisplayNames = HashMap<String, String>;

/// User-facing spelling for `canonical`, falling back to the canonical
/// form itself (synthesized names have no separate raw spelling).
fn display_name(display: &DisplayNames, canonical: &str) -> String {
    display
        .get(canonical)
        .cloned()
        .unwrap_or_else(|| canonical.to_string())
}
type UdfSigs = HashMap<String, (Vec<(String, DataType)>, DataType)>;

/// Var → (first-seen type, first-seen span). Later uses must agree.
type Bindings = HashMap<String, (DataType, Span)>;

/// Numeric literals stay polymorphic within their family (`IntLit` /
/// `FloatLit`) until a concrete context fixes the type. Concrete literals
/// carry their resolved [`DataType`] directly.
#[derive(Debug, Clone, PartialEq, Eq)]
enum LitKind {
    IntLit,
    FloatLit,
    Concrete(DataType),
}

impl LitKind {
    fn fits(&self, expected: &DataType) -> bool {
        match self {
            LitKind::IntLit => expected.is_integer(),
            LitKind::FloatLit => expected.is_float(),
            LitKind::Concrete(t) => t == expected,
        }
    }

    fn is_numeric(&self) -> bool {
        match self {
            LitKind::IntLit | LitKind::FloatLit => true,
            LitKind::Concrete(t) => t.is_numeric(),
        }
    }

    /// Representative concrete type for diagnostic rendering **and** for
    /// pinning all-literal expressions that never met a concrete partner.
    fn report_ty(&self) -> DataType {
        match self {
            LitKind::IntLit => DataType::Int32,
            LitKind::FloatLit => DataType::Float32,
            LitKind::Concrete(t) => t.clone(),
        }
    }
}

/// Combine two operand kinds across an arithmetic operator. `None` on
/// a family mismatch.
fn merge_lit(a: &LitKind, b: &LitKind) -> Option<LitKind> {
    match (a, b) {
        (x, y) if x == y => Some(x.clone()),
        (LitKind::Concrete(t), LitKind::IntLit) | (LitKind::IntLit, LitKind::Concrete(t))
            if t.is_integer() =>
        {
            Some(LitKind::Concrete(t.clone()))
        }
        (LitKind::Concrete(t), LitKind::FloatLit) | (LitKind::FloatLit, LitKind::Concrete(t))
            if t.is_float() =>
        {
            Some(LitKind::Concrete(t.clone()))
        }
        _ => None,
    }
}

fn check_rule(
    rule: &mut FlowLogRule,
    decls: &DeclTypes,
    udfs: &UdfSigs,
    display: &DisplayNames,
) -> Result<(), TypeCheckError> {
    // Bind vars first so out-of-order body predicates can resolve them.
    let mut bindings: Bindings = HashMap::new();
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtom(atom) = predicate {
            bind_atom_vars(atom, decls, &mut bindings)?;
        }
    }

    for predicate in rule.rhs_mut() {
        match predicate {
            Predicate::PositiveAtom(atom) => pin_atom_consts(atom, decls)?,
            Predicate::NegativeAtom(atom) => {
                check_atom_uses(atom, decls, &bindings)?;
                pin_atom_consts(atom, decls)?;
            }
            Predicate::Compare(cmp) => check_comparison(cmp, &bindings, udfs)?,
            Predicate::FnCall(fc) => {
                // UDF predicate return is always bool; drop the inferred type.
                infer_fn_call_type(fc, &bindings, udfs)?;
                pin_fn_call_args(fc, udfs)?;
            }
        }
    }

    check_head(rule, decls, udfs, &bindings, display)
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
                Some((first_ty, first_span)) if first_ty != &col_ty => {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: first_ty.clone(),
                        first_span: *first_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
                Some(_) => {}
            },
            AtomArg::Const(c) => {
                if !lit_kind(c)?.fits(&col_ty) {
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
                if let Some((bound_ty, bound_span)) = bindings.get(v)
                    && bound_ty != &col_ty
                {
                    return Err(TypeCheckError::TypeMismatch {
                        var: v.clone(),
                        first_ty: bound_ty.clone(),
                        first_span: *bound_span,
                        later_ty: col_ty,
                        later_span: atom.span(),
                    });
                }
            }
            AtomArg::Const(c) => {
                if !lit_kind(c)?.fits(&col_ty) {
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
        if let AtomArg::Const(c) = arg
            && c.is_polymorphic()
        {
            c.pin(col_ty.clone());
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
    decl.get(i).cloned().ok_or_else(|| {
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

    if let (Some(l), Some(r)) = (&left, &right)
        && merge_lit(l, r).is_none()
    {
        return Err(TypeCheckError::ComparisonTypeMismatch {
            span,
            op,
            left: l.report_ty(),
            right: r.report_ty(),
        });
    }

    // Ordering comparisons additionally require an ordered type.
    if !matches!(op, ComparisonOperator::Equal | ComparisonOperator::NotEqual)
        && let Some(kind) = left.as_ref().or(right.as_ref())
    {
        let is_ordered = kind.is_numeric() || matches!(kind, LitKind::Concrete(DataType::String));
        if !is_ordered {
            return Err(TypeCheckError::ComparisonOpNotAllowed {
                span,
                op,
                ty: kind.report_ty(),
            });
        }
    }

    // Pin: both sides unify to the same concrete type. Fall back to the
    // family's representative width when both sides are polymorphic.
    let target = match (&left, &right) {
        (Some(l), Some(r)) => merge_lit(l, r).map(|k| k.report_ty()),
        (Some(k), None) | (None, Some(k)) => Some(k.report_ty()),
        (None, None) => None,
    };
    if let Some(t) = target {
        pin_arith_literals(cmp.left_mut(), &t, udfs)?;
        pin_arith_literals(cmp.right_mut(), &t, udfs)?;
    }
    Ok(())
}

fn check_head(
    rule: &mut FlowLogRule,
    decls: &DeclTypes,
    udfs: &UdfSigs,
    bindings: &Bindings,
    display: &DisplayNames,
) -> Result<(), TypeCheckError> {
    let head = rule.head_mut();
    let (rel_name, arity, head_span) = (head.name().to_string(), head.arity(), head.span());
    let rel_display = display_name(display, &rel_name);
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
            rel: rel_display,
            expected: col_types.len(),
            found: arity,
        });
    }

    for (col, (arg, expected)) in head
        .head_arguments_mut()
        .iter_mut()
        .zip(col_types.iter().cloned())
        .enumerate()
    {
        match arg {
            HeadArg::Aggregation(agg) => check_aggregation(agg, expected, udfs, bindings)?,
            HeadArg::Var(v) => {
                if let Some((found, _)) = bindings.get(v)
                    && found != &expected
                {
                    return Err(TypeCheckError::HeadColumnType {
                        span: head_span,
                        rel: rel_display.clone(),
                        col,
                        found: found.clone(),
                        expected,
                    });
                }
            }
            HeadArg::Arith(a) => {
                // A bare tuple construct against a tuple column is checked
                // field-wise (so a polymorphic literal fits any same-family
                // field width, exactly as it would in a scalar column) and
                // pinned per field — avoiding the eager width-collapse that
                // `infer_factor_type`'s Tuple arm does for the general case.
                if a.rest().is_empty()
                    && matches!(a.init(), Factor::Tuple(_))
                    && let DataType::FixedTuple(field_types) = &expected
                {
                    let field_types = field_types.clone();
                    if let Factor::Tuple(lit) = a.init_mut() {
                        check_tuple_construct(lit, &field_types, udfs, bindings)?;
                    }
                    continue;
                }
                if let Some(kind) = infer_expr_type(a, bindings, udfs)?
                    && !kind.fits(&expected)
                {
                    return Err(head_or_literal_mismatch(
                        a,
                        &rel_display,
                        col,
                        expected,
                        kind,
                    ));
                }
                pin_arith_literals(a, &expected, udfs)?;
            }
        }
    }
    Ok(())
}

/// Field-wise check + pin of a tuple construct `(e0, …)` against a declared
/// tuple column type. Each field is checked like a scalar column (a
/// polymorphic literal fits any same-family width) and then pinned to its
/// field type — avoiding the eager width-collapse `infer_factor_type` does
/// when it builds a single `Concrete(FixedTuple(..))`. Recurses for nested
/// tuple-literal fields.
fn check_tuple_construct(
    lit: &mut TupleLit,
    expected_fields: &[DataType],
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    if lit.fields().len() != expected_fields.len() {
        return Err(TypeCheckError::TupleConstruct {
            span: lit.span(),
            detail: format!(
                "tuple has {} field(s) but {} value(s) were given",
                expected_fields.len(),
                lit.fields().len()
            ),
        });
    }
    let span = lit.span();
    for (elem, fty) in lit.fields_mut().iter_mut().zip(expected_fields.iter()) {
        match elem {
            TupleElem::Placeholder => {
                return Err(TypeCheckError::TuplePlaceholderInConstruct { span });
            }
            // Nested tuple literal → recurse so its fields get the same
            // literal-width leniency.
            TupleElem::Expr(a) if a.rest().is_empty() && matches!(a.init(), Factor::Tuple(_)) => {
                let DataType::FixedTuple(sub) = fty else {
                    return Err(TypeCheckError::TupleConstruct {
                        span,
                        detail: format!("a tuple literal does not fit field type `{fty}`"),
                    });
                };
                let Factor::Tuple(inner) = a.init_mut() else {
                    unreachable!("guard matched Factor::Tuple")
                };
                check_tuple_construct(inner, sub, udfs, bindings)?;
            }
            TupleElem::Expr(a) => {
                if let Some(kind) = infer_expr_type(a, bindings, udfs)?
                    && !kind.fits(fty)
                {
                    return Err(TypeCheckError::TupleConstruct {
                        span: a.span(),
                        detail: format!(
                            "field value of type `{}` does not fit field type `{fty}`",
                            kind.report_ty()
                        ),
                    });
                }
                pin_arith_literals(a, fty, udfs)?;
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
            pin_arith_literals(agg.arithmetic_mut(), &k.report_ty(), udfs)?;
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
        if !kind.fits(&declared) {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
        }
    }
    pin_arith_literals(agg.arithmetic_mut(), &declared, udfs)?;
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
                Some(existing) => Some(merge_lit(&existing, &k).ok_or(
                    TypeCheckError::ArithmeticTypeMismatch {
                        span,
                        left: existing.report_ty(),
                        right: k.report_ty(),
                    },
                )?),
            };
        }
        if let Some(k) = &inferred {
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
        Factor::Var(v) => bindings.get(v).map(|(ty, _)| LitKind::Concrete(ty.clone())),
        Factor::Const(c) => Some(lit_kind(c)?),
        Factor::FnCall(fc) => Some(LitKind::Concrete(infer_fn_call_type(fc, bindings, udfs)?)),
        Factor::Builtin(bc) => Some(LitKind::Concrete(infer_builtin_call_type(
            bc, bindings, udfs,
        )?)),
        // Primitive layer sees through casts; the `subtype` pass
        // enforces the cast rules separately.
        Factor::Cast(c) => infer_factor_type(c.inner(), bindings, udfs)?,
        Factor::Group(a) => infer_expr_type(a, bindings, udfs)?,
        // Tuple construct `(e0, …)`: the type is the fixed tuple of the
        // components' concrete types. (Destructures are desugared to `TupleProj`s,
        // so a surviving placeholder here is a `_` in a construct — invalid.)
        Factor::Tuple(r) => {
            let mut fields = Vec::with_capacity(r.fields().len());
            for elem in r.fields() {
                match elem {
                    TupleElem::Expr(a) => match infer_expr_type(a, bindings, udfs)? {
                        Some(k) => fields.push(k.report_ty()),
                        // A component is unbound — the range-restriction pass
                        // reports it; we can't determine the tuple type yet.
                        None => return Ok(None),
                    },
                    TupleElem::Placeholder => {
                        return Err(TypeCheckError::TuplePlaceholderInConstruct { span: r.span() });
                    }
                }
            }
            Some(LitKind::Concrete(DataType::FixedTuple(fields)))
        }
        // Projection `tuple.index` (synthesized by the destructure desugar):
        // the type is the indexed field's type. A non-tuple base or an
        // out-of-range index means the user destructured something that isn't a
        // tuple of that shape — a clean user error, not an internal bug.
        Factor::TupleProj { tuple, index } => match infer_expr_type(tuple, bindings, udfs)? {
            Some(LitKind::Concrete(DataType::FixedTuple(fields))) => match fields.get(*index) {
                Some(ty) => Some(LitKind::Concrete(ty.clone())),
                None => {
                    return Err(TypeCheckError::TupleDestructure {
                        span: tuple.span(),
                        detail: format!(
                            "destructure pattern has more than {} field(s)",
                            fields.len()
                        ),
                    });
                }
            },
            Some(other) => {
                return Err(TypeCheckError::TupleDestructure {
                    span: tuple.span(),
                    detail: format!(
                        "cannot destructure `{}`, which is not a tuple",
                        other.report_ty()
                    ),
                });
            }
            None => None,
        },
    })
}

/// Type-check a built-in call against its [`BuiltinOperator`] signature.
/// Arity is already enforced by the parser, so only per-arg types here.
fn infer_builtin_call_type(
    bc: &BuiltinCall,
    bindings: &Bindings,
    udfs: &UdfSigs,
) -> Result<DataType, TypeCheckError> {
    let op = bc.op();
    debug_assert_eq!(
        bc.args().len(),
        op.param_types().len(),
        "parser should enforce builtin arity"
    );

    for (i, (arg, expected)) in bc.args().iter().zip(op.param_types().iter()).enumerate() {
        let Some(kind) = infer_expr_type(arg, bindings, udfs)? else {
            continue;
        };
        if !kind.fits(expected) {
            return Err(TypeCheckError::BuiltinArgType {
                span: arg.span(),
                op,
                arg_index: i,
                expected: expected.clone(),
                found: kind.report_ty(),
            });
        }
    }

    Ok(op.ret_type())
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
        if !kind.fits(expected) {
            return Err(TypeCheckError::UdfArgType {
                span: arg.span(),
                name: fc.name().to_string(),
                param: param_name.clone(),
                expected: expected.clone(),
                found: kind.report_ty(),
            });
        }
    }

    Ok(ret_ty.clone())
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

/// Numeric ops (`+`, `-`, `*`, `/`, `%`) require numeric factors.
/// String / bool factors can't appear in arithmetic.
fn check_arith_op(
    kind: &LitKind,
    op: &ArithmeticOperator,
    span: Span,
) -> Result<(), TypeCheckError> {
    // Arithmetic requires a numeric operand. This rejects `Bool`/`String` and
    // also tuple operands.
    if kind.is_numeric() {
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
    target: &DataType,
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
    target: &DataType,
    udfs: &UdfSigs,
) -> Result<(), TypeCheckError> {
    match factor {
        Factor::Const(c) => {
            if c.is_polymorphic() {
                c.pin(target.clone());
            }
            Ok(())
        }
        Factor::Var(_) => Ok(()),
        Factor::FnCall(fc) => pin_fn_call_args(fc, udfs),
        Factor::Builtin(bc) => pin_builtin_call_args(bc, udfs),
        // Cast asserts its inner has the target's primitive — pin
        // polymorphic literals inside accordingly.
        Factor::Cast(c) => pin_factor(c.inner_mut(), target, udfs),
        Factor::Group(a) => pin_arith_literals(a, target, udfs),
        // Tuple construct: pin each component against its declared field type.
        Factor::Tuple(r) => {
            if let DataType::FixedTuple(field_types) = target {
                for (elem, fty) in r.fields_mut().iter_mut().zip(field_types.iter()) {
                    if let TupleElem::Expr(a) = elem {
                        pin_arith_literals(a, fty, udfs)?;
                    }
                }
            }
            Ok(())
        }
        // Projection's base is a synthesized bound variable — no literals.
        Factor::TupleProj { .. } => Ok(()),
    }
}

/// Pin polymorphic literals in a built-in call against the per-param
/// types on [`BuiltinOperator`].
fn pin_builtin_call_args(bc: &mut BuiltinCall, udfs: &UdfSigs) -> Result<(), TypeCheckError> {
    let op = bc.op();
    for (arg, pty) in bc.args_mut().iter_mut().zip(op.param_types().iter()) {
        pin_arith_literals(arg, pty, udfs)?;
    }
    Ok(())
}

fn pin_fn_call_args(fc: &mut FnCall, udfs: &UdfSigs) -> Result<(), TypeCheckError> {
    // Collected by value so the recursive `pin_arith_literals` below can
    // reborrow `udfs` — holding `&param_types` from `udfs.get(...)` across
    // the recursion would block the reborrow.
    let param_types: Vec<DataType> = udfs
        .get(fc.name())
        .map(|(params, _)| params.iter().map(|(_, ty)| ty.clone()).collect())
        .ok_or_else(|| {
            TypeCheckError::internal(format!(
                "pin_fn_call_args: UDF `{}` not declared",
                fc.name()
            ))
        })?;
    for (arg, pty) in fc.args_mut().iter_mut().zip(param_types.iter()) {
        pin_arith_literals(arg, pty, udfs)?;
    }
    Ok(())
}

/// Validate each fact tuple's column families against its `.decl` and pin
/// polymorphic literals. Diagnostics cite the fact's head span.
fn check_and_pin_facts(
    facts: &mut HashMap<String, Vec<(Span, Vec<ConstType>)>>,
    decls: &DeclTypes,
    display: &DisplayNames,
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
                    rel: display_name(display, rel_name),
                    expected: col_types.len(),
                    found: tuple.len(),
                });
            }
            for (c, col_ty) in tuple.iter_mut().zip(col_types.iter()) {
                if !lit_kind(c)?.fits(col_ty) {
                    return Err(TypeCheckError::LiteralColumnMismatch {
                        span: *span,
                        literal: c.to_string(),
                        expected: col_ty.clone(),
                    });
                }
                if c.is_polymorphic() {
                    c.pin(col_ty.clone());
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    //! Unit tests focus on the typechecker's *second* job — pinning every
    //! polymorphic literal to its concrete width. The 15 integration
    //! fixtures in `tests/errors/typechecker/` cover the check side;
    //! pinning outcomes are not observable through those assertions.

    use super::*;
    use flowlog_common::SourceMap;
    use std::io::Write;

    fn parse_and_check(src: &str) -> Program {
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let mut program =
            Program::parse(&tmp.path().to_string_lossy(), true, &mut sm).expect("parse failed");
        check_program(&mut program, &Config::default()).expect("check failed");
        program
    }

    /// Body-positive atom literal: `Flag(5)` with `.decl Flag(x: int8)` must
    /// pin `5` to `Int8(5)` via `pin_atom_consts`. If that pass becomes a
    /// no-op, catalog calls `data_type()` on a polymorphic `Int` and panics.
    #[test]
    fn body_atom_const_pinned_to_declared_column_width() {
        let src = "\
            .decl Item(x: int8)\n\
            .decl Flag(x: int8)\n\
            .decl Out(x: int8)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .input Flag(IO=\"file\", filename=\"Flag.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x), Flag(5).\n";
        let program = parse_and_check(src);
        let rule = program.rules()[0];
        let flag_atom = match &rule.rhs()[1] {
            Predicate::PositiveAtom(a) => a,
            other => panic!("expected Flag atom, got {other:?}"),
        };
        match &flag_atom.arguments()[0] {
            AtomArg::Const(c) => assert_eq!(c, &ConstType::Int8(5)),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Comparison operand literal: `x > 100` with `x: int16` must pin `100`
    /// to `Int16(100)` via `pin_arith_literals` inside `check_comparison`.
    /// Guards the pin-target selection after `merge_lit` unifies left/right.
    #[test]
    fn comparison_literal_pinned_to_variable_type() {
        let src = "\
            .decl Item(x: int16)\n\
            .decl Big(x: int16)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Big\n\
            Big(x) :- Item(x), x > 100.\n";
        let program = parse_and_check(src);
        let rule = program.rules()[0];
        let cmp = match &rule.rhs()[1] {
            Predicate::Compare(c) => c,
            other => panic!("expected comparison, got {other:?}"),
        };
        match cmp.right().init() {
            Factor::Const(c) => assert_eq!(c, &ConstType::Int16(100)),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Nested UDF call: in `f(1) + x` where `x: int64` and `f: int8 -> int64`,
    /// the `1` must be pinned to the UDF's parameter width (`Int8`), NOT the
    /// enclosing expression's target (`Int64`). A regression using outer
    /// context inside `pin_fn_call_args` would silently widen the literal.
    #[test]
    fn nested_udf_arg_pinned_to_param_type_not_outer_target() {
        let src = "\
            .decl Item(x: int64)\n\
            .decl Flag(x: int64)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Flag\n\
            .extern fn f(a: int8) -> int64\n\
            Flag(f(1) + x) :- Item(x).\n";
        let program = parse_and_check(src);
        let rule = program.rules()[0];
        let head_arith = match &rule.head().head_arguments()[0] {
            HeadArg::Arith(a) => a,
            other => panic!("expected Arith head arg, got {other:?}"),
        };
        let fc = match head_arith.init() {
            Factor::FnCall(fc) => fc,
            other => panic!("expected FnCall factor, got {other:?}"),
        };
        match fc.args()[0].init() {
            Factor::Const(c) => assert_eq!(
                c,
                &ConstType::Int8(1),
                "UDF arg must pin to param type (Int8), not outer target (Int64)"
            ),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Fact tuple literal: `P(5)` with `.decl P(x: uint64)` must pin via
    /// `check_and_pin_facts`. This is a separate code path from rule-body
    /// pinning — a regression here would leak polymorphic literals into
    /// `program.facts()` even though all rule literals are concrete.
    #[test]
    fn fact_tuple_const_pinned_to_declared_column_width() {
        let src = "\
            .decl P(x: uint64)\n\
            .decl Out(x: uint64)\n\
            .output Out\n\
            P(5).\n\
            Out(x) :- P(x).\n";
        let program = parse_and_check(src);
        let p_facts = program.facts().get("p").expect("p facts");
        let (_, tuple) = &p_facts[0];
        assert_eq!(tuple[0], ConstType::UInt64(5));
    }

    /// `merge_lit` is the single source of truth for arithmetic operand
    /// unification. Integration fixtures exercise the rejection paths by
    /// watching for `ArithmeticTypeMismatch`, but a regression that silently
    /// *widened* acceptance (e.g. Int8+Int16 → Int16) would let bad programs
    /// through and only surface as wrong codegen width. Each row below is a
    /// specific bug class.
    #[test]
    fn merge_lit_table() {
        use DataType::*;
        use LitKind::*;

        // Both sides polymorphic: must stay polymorphic so outer context
        // can still pin. Collapsing to Concrete(Int32) would break
        // narrow-width columns consuming `1 + 2`.
        assert_eq!(merge_lit(&IntLit, &IntLit), Some(IntLit));
        assert_eq!(merge_lit(&FloatLit, &FloatLit), Some(FloatLit));

        // Polymorphic meets concrete: concrete wins, picks the exact width.
        assert_eq!(merge_lit(&IntLit, &Concrete(Int8)), Some(Concrete(Int8)));
        assert_eq!(
            merge_lit(&Concrete(UInt16), &IntLit),
            Some(Concrete(UInt16))
        );
        assert_eq!(
            merge_lit(&FloatLit, &Concrete(Float64)),
            Some(Concrete(Float64))
        );

        // Same family, different width: rejects. Any "promote to wider"
        // rule added here would silently accept type-mismatched programs.
        assert_eq!(merge_lit(&Concrete(Int8), &Concrete(Int16)), None);
        assert_eq!(merge_lit(&Concrete(Float32), &Concrete(Float64)), None);

        // Cross-family: rejects.
        assert_eq!(merge_lit(&IntLit, &FloatLit), None);
        assert_eq!(merge_lit(&Concrete(Int32), &Concrete(Float32)), None);
        assert_eq!(merge_lit(&Concrete(Bool), &IntLit), None);
    }

    /// `check_arith_op` rejects every non-numeric type around an
    /// arithmetic op. String concatenation lives in the `cat(a, b)`
    /// built-in (not exercised here — built-ins go through a separate
    /// signature-check path), so a string factor in an arithmetic
    /// expression is always wrong. A regression in any row would
    /// silently flip acceptance for real programs.
    #[test]
    fn check_arith_op_table() {
        use ArithmeticOperator::*;
        use DataType::*;
        use LitKind::Concrete;

        let span = Span::DUMMY;

        // Positive: numeric with numeric ops is fine.
        assert!(check_arith_op(&Concrete(Int32), &Plus, span).is_ok());
        assert!(check_arith_op(&Concrete(Float64), &Multiply, span).is_ok());

        // Negative: numeric op on strings → error.
        assert!(check_arith_op(&Concrete(String), &Plus, span).is_err());

        // Negative: Bool rejects every arithmetic op.
        assert!(check_arith_op(&Concrete(Bool), &Plus, span).is_err());
        assert!(check_arith_op(&Concrete(Bool), &Multiply, span).is_err());
    }

    /// `report_ty` on polymorphic literals returns the default width used
    /// both for diagnostic rendering and for pinning orphan all-literal
    /// expressions (`5 > 10` with no variables). A regression that changed
    /// these defaults would shift every diagnostic's "expected" type AND
    /// silently change what width orphan constants get pinned to.
    #[test]
    fn report_ty_polymorphic_defaults() {
        assert_eq!(LitKind::IntLit.report_ty(), DataType::Int32);
        assert_eq!(LitKind::FloatLit.report_ty(), DataType::Float32);
    }

    // =========================================================================
    // Subtype rules
    // =========================================================================

    fn parse_and_check_result(src: &str) -> Result<Program, TypeCheckError> {
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let mut program = Program::parse(&tmp.path().to_string_lossy(), true, &mut sm)
            .expect("parse should succeed; this test exercises typecheck only");
        check_program(&mut program, &Config::default())?;
        Ok(program)
    }

    /// Two aliases of `number` join freely — aliases are transparent.
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
        parse_and_check_result(src).expect("alias join must be allowed");
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
        parse_and_check_result(src).expect("explicit narrowing must be allowed");
    }

    /// Head narrowing without `as()` is rejected — and parentheses around
    /// the variable must not bypass the check (`OnlyUsers((x))` is the same
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
                parse_and_check_result(&src).is_err(),
                "implicit narrowing must be rejected for {head}"
            );
        }
    }

    /// `as()` between two sibling subtypes of the same primitive is
    /// allowed — that's the escape hatch the rule exists for.
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
        parse_and_check_result(src).expect("sibling-subtype cast must be allowed");
    }

    /// After typechecking, every `Factor::Cast` has been lowered to its
    /// inner factor. Downstream stages never see a cast wrapper.
    #[test]
    fn cast_is_lowered_after_typecheck() {
        let src = "\
            .type UserId <: number\n\
            .decl Plain(x: number)\n\
            .decl OnlyUsers(u: UserId)\n\
            .input Plain(IO=\"file\", filename=\"Plain.csv\", delimiter=\",\")\n\
            .output OnlyUsers\n\
            OnlyUsers(as(x, UserId)) :- Plain(x).\n";
        let program = parse_and_check_result(src).expect("typecheck must succeed");
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

    // ── Tuples ─────────────────────────────────────────────────────────

    /// Construct (`p = (x, y)`) and destructure (`p = (a, b)`) of a tuple
    /// column both type-check against the declared tuple type.
    #[test]
    fn tuple_construct_and_destructure_typecheck() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol)\n\
            .decl Out(p: Pair)\n\
            .decl Back(a: symbol, b: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Back\n\
            Out(p) :- In(x, y), p = (x, y).\n\
            Back(a, b) :- Out(p), p = (a, b).\n";
        parse_and_check_result(src).expect("tuple construct + destructure must type-check");
    }

    /// A construct with the wrong number of fields is rejected (here a 3-field
    /// literal flowing into an arity-2 tuple column).
    #[test]
    fn tuple_construct_wrong_arity_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol, z: symbol)\n\
            .decl Out(p: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(x, y, z), p = (x, y, z).\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "3-field tuple into an arity-2 tuple column must be rejected"
        );
    }

    /// A field of the wrong type is rejected (a `number` field given a symbol).
    #[test]
    fn tuple_field_type_mismatch_rejected() {
        let src = "\
            .type Tv = ( t: symbol, v: number )\n\
            .decl In(s: symbol, n: symbol)\n\
            .decl Out(p: Tv)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(s, n), p = (s, n).\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "a symbol in a `number` tuple field must be rejected"
        );
    }

    /// A tuple literal flowing into a scalar column (and vice-versa) is
    /// rejected — tuples are not interchangeable with their fields.
    #[test]
    fn tuple_vs_scalar_mismatch_rejected() {
        let src = "\
            .decl In(x: symbol, y: symbol)\n\
            .decl Out(p: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(x, y), p = (x, y).\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "a tuple literal into a scalar column must be rejected"
        );
    }

    /// A polymorphic numeric literal in a tuple field whose declared width is
    /// not the family default (here `int64`) must be accepted and pinned — the
    /// same leniency a scalar `int64` column gets. (Regression: an earlier
    /// version collapsed the literal to `Int32` and rejected it.)
    #[test]
    fn tuple_field_non_default_width_literal_accepted() {
        let src = "\
            .type Tv = ( t: symbol, v: int64 )\n\
            .decl In(s: symbol)\n\
            .decl Out(p: Tv)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(s), p = (s, 5).\n";
        parse_and_check_result(src)
            .expect("an int literal in an int64 tuple field must be accepted");
    }

    /// Arithmetic on a tuple operand is rejected at type-check (a clean
    /// diagnostic, not a generated-Rust `Add`-not-satisfied error).
    #[test]
    fn tuple_arithmetic_rejected() {
        let src = "\
            .type Pair = ( a: number, b: number )\n\
            .decl In(x: number, y: number)\n\
            .decl Out(q: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(q) :- In(x, y), p = (x, y), q = p + p.\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "arithmetic on a tuple operand must be rejected"
        );
    }

    /// Destructuring a non-tuple bound variable is a clean user error, not an
    /// internal compiler panic.
    #[test]
    fn destructure_of_non_tuple_is_clean_error() {
        let src = "\
            .decl In(x: symbol)\n\
            .decl Out(a: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(a) :- In(x), x = (a, b).\n";
        match parse_and_check_result(src) {
            Err(TypeCheckError::TupleDestructure { .. }) => {}
            other => panic!("expected a clean TupleDestructure error, got {other:?}"),
        }
    }

    /// `.input` on a relation with a tuple column is rejected (tuples are
    /// constructed by rules, never read from facts) — a clean parse error, not
    /// a codegen panic.
    #[test]
    fn tuple_edb_input_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(p: Pair)\n\
            .decl Out(p: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(p).\n";
        // The `.input` rejection is a ParseError, so it surfaces from parsing.
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        assert!(
            Program::parse(&tmp.path().to_string_lossy(), true, &mut sm).is_err(),
            "`.input` on a tuple-column relation must be rejected"
        );
    }

    /// A destructure with only placeholders against a non-tuple is rejected
    /// (the placeholder still witnesses tuple-ness/arity), not silently
    /// accepted.
    #[test]
    fn placeholder_only_destructure_of_non_tuple_rejected() {
        let src = "\
            .decl In(x: symbol)\n\
            .decl Out(x: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- In(x), x = (_,).\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "`x = (_,)` on a non-tuple `x` must be rejected"
        );
    }

    /// A trailing placeholder past the tuple's arity is rejected, not ignored.
    #[test]
    fn extra_placeholder_past_arity_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol)\n\
            .decl P(p: Pair)\n\
            .decl Out(a: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            P(p)   :- In(x, y), p = (x, y).\n\
            Out(a) :- P(p), p = (a, b, _).\n";
        assert!(
            parse_and_check_result(src).is_err(),
            "a trailing `_` past the tuple's arity must be rejected"
        );
    }

    /// A destructure pattern wider than the tuple is a clean error, not a panic.
    #[test]
    fn over_arity_destructure_is_clean_error() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol, y: symbol)\n\
            .decl P(p: Pair)\n\
            .decl Out(c: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            P(p)   :- In(x, y), p = (x, y).\n\
            Out(c) :- P(p), p = (a, b, c).\n";
        match parse_and_check_result(src) {
            Err(TypeCheckError::TupleDestructure { .. }) => {}
            other => panic!("expected a clean TupleDestructure error, got {other:?}"),
        }
    }
}
