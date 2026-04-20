//! FlowLog rule-level type checker.
//!
//! Runs after parse, before stratification. Binds variable types from
//! positive-atom columns, then checks every body and head site against
//! the binding map. Spans come from the AST, so diagnostics point at the
//! offending expression rather than the enclosing rule.
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
//!   fixed by context.
//! - We do **not** range-check integer literals: `300` into a `UInt8`
//!   column passes here and is caught later by the Rust compiler on the
//!   generated code.
//! - Unbound variables in negated atoms, comparisons, or UDF calls —
//!   reported separately by the range-restriction pass, not here.

mod error;

pub use error::TypeCheckError;

use std::collections::HashMap;

use common::source::Span;
use parser::{
    Aggregation, AggregationOperator, Arithmetic, ArithmeticOperator, Atom, AtomArg,
    ComparisonExpr, ComparisonOperator, ConstType, DataType, ExternFn, Factor, FlowLogRule, FnCall,
    HeadArg, Predicate, Program, Relation,
};

/// Type-check every rule. Stops at the first failure.
pub fn check_program(program: &Program) -> Result<(), TypeCheckError> {
    let decls: DeclMap = program.relations().iter().map(|r| (r.name(), r)).collect();
    let udfs: UdfMap = program.udfs().iter().map(|u| (u.name(), u)).collect();
    for rule in program.rules() {
        check_rule(rule, &decls, &udfs)?;
    }
    Ok(())
}

type DeclMap<'a> = HashMap<&'a str, &'a Relation>;
type UdfMap<'a> = HashMap<&'a str, &'a ExternFn>;

/// Var → (first-seen type, first-seen span). Later uses must agree.
type Bindings = HashMap<String, (DataType, Span)>;

/// Numeric literals stay polymorphic within their family (`IntLit` /
/// `FloatLit`) until a concrete context fixes the type.
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

    /// Representative concrete type for diagnostic rendering.
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

fn check_rule(rule: &FlowLogRule, decls: &DeclMap, udfs: &UdfMap) -> Result<(), TypeCheckError> {
    let mut bindings: Bindings = HashMap::new();

    // Bind variables first so out-of-order body predicates can resolve.
    for predicate in rule.rhs() {
        if let Predicate::PositiveAtomPredicate(atom) = predicate {
            bind_atom_vars(atom, decls, &mut bindings)?;
        }
    }

    for predicate in rule.rhs() {
        match predicate {
            Predicate::PositiveAtomPredicate(_) => {}
            Predicate::NegativeAtomPredicate(atom) => check_atom_uses(atom, decls, &bindings)?,
            Predicate::ComparePredicate(cmp) => check_comparison(cmp, &bindings, udfs)?,
            Predicate::FnCallPredicate(fc) => {
                // UDF predicate return is always bool; drop the inferred type.
                infer_fn_call_type(fc, &bindings, udfs)?;
            }
        }
    }

    check_head(rule, decls, udfs, &bindings)
}

fn bind_atom_vars(
    atom: &Atom,
    decls: &DeclMap,
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
                if !lit_kind(c).fits(col_ty) {
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
    decls: &DeclMap,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    for (i, arg) in atom.arguments().iter().enumerate() {
        let col_ty = resolve_atom_column(atom, i, decls)?;
        if let AtomArg::Var(v) = arg {
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
    }
    Ok(())
}

fn resolve_atom_column(atom: &Atom, i: usize, decls: &DeclMap) -> Result<DataType, TypeCheckError> {
    let decl = decls
        .get(atom.name())
        .ok_or_else(|| TypeCheckError::internal(format!("atom `{}` not declared", atom.name())))?;
    decl.data_type().get(i).copied().ok_or_else(|| {
        TypeCheckError::internal(format!(
            "atom `{}` has {} arguments but `.decl` has {}",
            atom.name(),
            atom.arguments().len(),
            decl.data_type().len(),
        ))
    })
}

fn check_comparison(
    cmp: &ComparisonExpr,
    bindings: &Bindings,
    udfs: &UdfMap,
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
    Ok(())
}

fn check_head(
    rule: &FlowLogRule,
    decls: &DeclMap,
    udfs: &UdfMap,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let head = rule.head();
    let decl = decls.get(head.name()).ok_or_else(|| {
        TypeCheckError::internal(format!("head relation `{}` not declared", head.name()))
    })?;
    let col_types = decl.data_type();

    if head.arity() != col_types.len() {
        return Err(TypeCheckError::HeadArity {
            span: head.span(),
            rel: head.name().to_string(),
            expected: col_types.len(),
            found: head.arity(),
        });
    }

    for (col, (arg, expected)) in head
        .head_arguments()
        .iter()
        .zip(col_types.iter().copied())
        .enumerate()
    {
        match arg {
            HeadArg::Aggregation(agg) => check_aggregation(agg, expected, udfs, bindings)?,
            HeadArg::Var(v) => {
                if let Some(&(found, _)) = bindings.get(v) {
                    if found != expected {
                        return Err(TypeCheckError::HeadColumnType {
                            span: head.span(),
                            rel: head.name().to_string(),
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
                        return Err(head_or_literal_mismatch(
                            a,
                            head.name(),
                            col,
                            expected,
                            kind,
                        ));
                    }
                }
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
    agg: &Aggregation,
    declared: DataType,
    udfs: &UdfMap,
    bindings: &Bindings,
) -> Result<(), TypeCheckError> {
    let op = *agg.operator();
    let span = agg.span();
    let arg_kind = infer_expr_type(agg.arithmetic(), bindings, udfs)?;

    // count: any input; output must be numeric.
    if matches!(op, AggregationOperator::Count) {
        if !declared.is_numeric() {
            return Err(TypeCheckError::AggregationOutputType { span, op, declared });
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
    Ok(())
}

/// Infer an expression's kind, merging factor kinds left to right.
/// `None` iff every variable factor is unbound (reported later by the
/// range-restriction pass).
fn infer_expr_type(
    expr: &Arithmetic,
    bindings: &Bindings,
    udfs: &UdfMap,
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
    udfs: &UdfMap,
) -> Result<Option<LitKind>, TypeCheckError> {
    Ok(match factor {
        Factor::Var(v) => bindings.get(v).map(|&(ty, _)| LitKind::Concrete(ty)),
        Factor::Const(c) => Some(lit_kind(c)),
        Factor::FnCall(fc) => Some(LitKind::Concrete(infer_fn_call_type(fc, bindings, udfs)?)),
    })
}

fn infer_fn_call_type(
    fc: &FnCall,
    bindings: &Bindings,
    udfs: &UdfMap,
) -> Result<DataType, TypeCheckError> {
    let udf = udfs
        .get(fc.name())
        .ok_or_else(|| TypeCheckError::UndeclaredUdf {
            span: fc.span(),
            name: fc.name().to_string(),
        })?;

    if fc.args().len() != udf.params().len() {
        return Err(TypeCheckError::UdfArity {
            span: fc.span(),
            name: fc.name().to_string(),
            expected: udf.params().len(),
            found: fc.args().len(),
        });
    }

    for (arg, param) in fc.args().iter().zip(udf.params()) {
        let expected = *param.data_type();
        let Some(kind) = infer_expr_type(arg, bindings, udfs)? else {
            continue;
        };
        if !kind.fits(expected) {
            return Err(TypeCheckError::UdfArgType {
                span: arg.span(),
                name: fc.name().to_string(),
                param: param.name().to_string(),
                expected,
                found: kind.report_ty(),
            });
        }
    }

    Ok(udf.ret_type())
}

fn lit_kind(c: &ConstType) -> LitKind {
    match c {
        ConstType::Int(_) => LitKind::IntLit,
        ConstType::Float(_) => LitKind::FloatLit,
        ConstType::Text(_) => LitKind::Concrete(DataType::String),
        ConstType::Bool(_) => LitKind::Concrete(DataType::Bool),
    }
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
