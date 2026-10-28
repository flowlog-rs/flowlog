//! Check and pin every rule: its body predicates, then its head (the composer).

use std::collections::HashMap;

use flowlog_common::Span;

use crate::AggregateExpr;
use crate::AggregationOperator;
use crate::Arithmetic;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::Head;
use crate::HeadArg;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::Variable;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::env::DeclTypes;
use crate::pipeline::typecheck::env::PrimitiveEnv;
use crate::pipeline::typecheck::env::UdfSigs;
use crate::pipeline::typecheck::primitive::Bindings;
use crate::pipeline::typecheck::primitive::atom::bind_atom;
use crate::pipeline::typecheck::primitive::atom::check_atom;
use crate::pipeline::typecheck::primitive::atom::pin_atom;
use crate::pipeline::typecheck::primitive::compare::check_and_pin_compare;
use crate::pipeline::typecheck::primitive::expr::infer_expr;
use crate::pipeline::typecheck::primitive::expr::infer_factor;
use crate::pipeline::typecheck::primitive::expr::pin_expr;

/// Check + pin every rule (plain and loop-internal) against the primitive type
/// environment. Per rule: bind variables from positive atoms first (so
/// out-of-order body predicates resolve), then check and pin each predicate,
/// then the head.
pub(super) fn check_and_pin_rules(
    program: &mut Program,
    env: &PrimitiveEnv,
) -> Result<(), ParseError> {
    for rule in program.rules_mut() {
        let mut bindings: Bindings = HashMap::new();
        // Inside an aggregate a variable keeps its rule-wide name: one that
        // also occurs outside is the same variable, so its atoms bind it
        // like any other.
        bind_positive_atoms(rule.rhs(), &env.decls, &mut bindings)?;
        let head = rule.head().clone();
        bind_aggregate_vars(rule.rhs_mut(), Some(&head), env, &mut bindings)?;
        check_and_pin_predicates(rule.rhs_mut(), env, &bindings)?;
        check_and_pin_head(rule, env, &bindings)?;
    }
    Ok(())
}

/// Binds the variables of every positive atom in `predicates`, those in
/// aggregate bodies included.
fn bind_positive_atoms(
    predicates: &[Predicate],
    decls: &DeclTypes,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(atom) => bind_atom(atom, decls, bindings)?,
            Predicate::Aggregate(aggregate) => {
                bind_positive_atoms(aggregate.body(), decls, bindings)?;
            }
            Predicate::NegativeAtom(_) | Predicate::Compare(_) => {}
        }
    }
    Ok(())
}

/// Checks and pins every predicate in `predicates` against `bindings`,
/// aggregates included.
fn check_and_pin_predicates(
    predicates: &mut [Predicate],
    env: &PrimitiveEnv,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(atom) => pin_atom(atom, &env.decls)?,
            Predicate::NegativeAtom(atom) => {
                check_atom(atom, &env.decls, bindings)?;
                pin_atom(atom, &env.decls)?;
            }
            Predicate::Compare(cmp) => check_and_pin_compare(cmp, bindings, &env.udfs)?,
            Predicate::Aggregate(aggregate) => {
                // `bind_aggregate_vars` pinned the variable, and an unbound
                // target left it unpinned for validation to report.
                let op = aggregate.operator();
                let var_type = aggregate.var_type().cloned();
                if let (Some(var_type), Some(target)) = (var_type, aggregate.target_mut()) {
                    let target_type = match infer_expr(target, bindings, &env.udfs)? {
                        // A mean's input is independent of its float
                        // result; the other operators answer in the input's
                        // type.
                        Some(kind) if op == AggregationOperator::Mean => kind.defaulted(),
                        Some(_) | None => var_type,
                    };
                    pin_expr(target, &target_type, bindings, &env.udfs)?;
                }
                check_and_pin_predicates(aggregate.body_mut(), env, bindings)?;
            }
        }
    }
    Ok(())
}

/// Binds the variable of each aggregate in `body` to the type of its value
/// and records that type on the binding. `count` answers an integer and
/// `mean` a float, each of a width its uses pick, like a literal; the other
/// operators answer in their target's type, which is that of a literal when
/// the target is one. The uses are those of `head`, for the rule's body, or
/// of the enclosing aggregate's target, for its body (`head` is `None`).
///
/// Bound in body order, so a later aggregate may read an earlier one's
/// variable, and an aggregate's nested ones first, since its target may read
/// them. A nested variable of a literal kind that its body asks no width of
/// keeps the literal kind, for the enclosing aggregate to pin
/// ([`pin_nested_literals`]).
fn bind_aggregate_vars(
    body: &mut [Predicate],
    head: Option<&Head>,
    env: &PrimitiveEnv,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for i in 0..body.len() {
        let Predicate::Aggregate(aggregate) = &mut body[i] else {
            continue;
        };
        bind_aggregate_vars(aggregate.body_mut(), None, env, bindings)?;
        let op = aggregate.operator();
        let span = aggregate.span();
        let kind = match (op, aggregate.target()) {
            (AggregationOperator::Count, _) => DataType::IntLit,
            (_, Some(target)) => {
                let Some(kind) = infer_expr(target, bindings, &env.udfs)? else {
                    continue;
                };
                if !kind.defaulted().is_numeric() {
                    return Err(ParseError::AggregationInputNotNumeric {
                        span,
                        op,
                        ty: kind.defaulted(),
                    });
                }
                if op == AggregationOperator::Mean {
                    DataType::FloatLit
                } else {
                    kind
                }
            }
            (_, None) => {
                return Err(grammar_bug(format!(
                    "`{op}` aggregate without a target survived lowering"
                )));
            }
        };
        let Some(var) = aggregate.var().cloned() else {
            return Err(grammar_bug(format!(
                "aggregate `{aggregate}` in a rule body binds no variable"
            )));
        };
        let ty = if kind.is_literal() {
            bindings.insert(var.clone(), (kind.clone(), span));
            match first_concrete_use(head, body, &var, &kind, env, bindings)? {
                Some(used) => used,
                None if head.is_none() => continue,
                None => default_aggregate_type(op, &kind),
            }
        } else {
            kind
        };
        bindings.insert(var, (ty.clone(), span));
        if let Predicate::Aggregate(aggregate) = &mut body[i] {
            aggregate.set_var_type(ty.clone());
            pin_nested_literals(aggregate, &ty, env, bindings)?;
        }
    }
    Ok(())
}

/// Pins each variable of a nested aggregate that `aggregate`'s body left at
/// a literal kind: to the width a cast in `aggregate`'s target asks of it
/// ([`first_cast_use`]), else to `ty`, the enclosing aggregate's type, when
/// its target reads the variable and the kind fits `ty`, otherwise to the
/// operator's default. Then pins the literals that each of those holds,
/// likewise.
fn pin_nested_literals(
    aggregate: &mut AggregateExpr,
    ty: &DataType,
    env: &PrimitiveEnv,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    // Cloned, as the loop below borrows the body mutably. Finding every
    // nested variable's cast use first, then pinning, would avoid the clone
    // but read stale types: a cast use reads the types of the siblings
    // pinned before it, as `as((c + d), int64)` reads `c`'s for `d`.
    let target: Option<Arithmetic> = aggregate.target().cloned();
    for predicate in aggregate.body_mut() {
        let Predicate::Aggregate(nested) = predicate else {
            continue;
        };
        let Some(var) = nested.var().cloned() else {
            continue;
        };
        let Some((kind, span)) = bindings.get(&var).cloned() else {
            continue;
        };
        if nested.var_type().is_some() || !kind.is_literal() {
            continue;
        }
        let (read, cast_use) = match &target {
            Some(target) => (
                target.vars().contains(&&var),
                first_cast_use(target, &var, &env.udfs, bindings)?,
            ),
            None => (false, None),
        };
        let nested_ty = match cast_use {
            Some(asked) if kind.fits(&asked) => asked,
            Some(_) | None if read && kind.fits(ty) => ty.clone(),
            Some(_) | None => default_aggregate_type(nested.operator(), &kind),
        };
        bindings.insert(var, (nested_ty.clone(), span));
        nested.set_var_type(nested_ty.clone());
        pin_nested_literals(nested, &nested_ty, env, bindings)?;
    }
    Ok(())
}

/// The type of an aggregate of literal `kind` that no use asks a width of:
/// a `mean` is `float64`, every other operator the kind's default.
fn default_aggregate_type(op: AggregationOperator, kind: &DataType) -> DataType {
    match op {
        AggregationOperator::Mean => DataType::Float64,
        AggregationOperator::Count
        | AggregationOperator::Sum
        | AggregationOperator::Min
        | AggregationOperator::Max => kind.defaulted(),
    }
}

/// The first concrete type of `kind`'s family that a use of `var` asks of
/// it, with `var` bound to `kind`: a column of `head` it fills or an
/// expression of one, then a comparison or aggregate target of `body` it
/// takes part in, those inside aggregates included. Within a use, a cast
/// that takes the type of `var` asks first ([`first_cast_use`]). `None`
/// when every use accepts any width. A use asking another family is
/// skipped here and reported where it is checked.
fn first_concrete_use(
    head: Option<&Head>,
    body: &[Predicate],
    var: &Variable,
    kind: &DataType,
    env: &PrimitiveEnv,
    bindings: &Bindings,
) -> Result<Option<DataType>, ParseError> {
    let reads = |vars: Vec<&Variable>| vars.contains(&var);
    if let Some(head) = head
        && let Some(col_types) = env.decls.get(head.name())
    {
        for (arg, col_type) in head.head_arguments().iter().zip(col_types) {
            let asked = match arg {
                HeadArg::Var(v) if v == var => col_type.clone(),
                HeadArg::Arith(a) if reads(a.vars()) => {
                    match first_cast_use(a, var, &env.udfs, bindings)? {
                        Some(asked) => asked,
                        None => match infer_expr(a, bindings, &env.udfs)? {
                            Some(found) if !found.is_literal() => found,
                            Some(_) | None => col_type.clone(),
                        },
                    }
                }
                HeadArg::Var(_) | HeadArg::Arith(_) => continue,
            };
            if kind.fits(&asked) {
                return Ok(Some(asked));
            }
        }
    }
    for predicate in body {
        let asked = match predicate {
            Predicate::Compare(cmp) => {
                if !(reads(cmp.left().vars()) || reads(cmp.right().vars())) {
                    continue;
                }
                let cast_use = match first_cast_use(cmp.left(), var, &env.udfs, bindings)? {
                    None => first_cast_use(cmp.right(), var, &env.udfs, bindings)?,
                    found => found,
                };
                if cast_use.is_some() {
                    cast_use
                } else if cmp.operator().is_string_constraint() {
                    continue;
                } else {
                    let left = infer_expr(cmp.left(), bindings, &env.udfs)?;
                    let right = infer_expr(cmp.right(), bindings, &env.udfs)?;
                    match (left, right) {
                        (Some(l), Some(r)) => l.merge(&r),
                        (Some(k), None) | (None, Some(k)) => Some(k),
                        (None, None) => None,
                    }
                }
            }
            Predicate::Aggregate(aggregate) => {
                let target = aggregate.target().filter(|target| reads(target.vars()));
                let from_target = match target {
                    Some(target) => match first_cast_use(target, var, &env.udfs, bindings)? {
                        None => infer_expr(target, bindings, &env.udfs)?,
                        found => found,
                    },
                    None => None,
                };
                match from_target {
                    Some(asked) if !asked.is_literal() => Some(asked),
                    Some(_) | None => {
                        first_concrete_use(None, aggregate.body(), var, kind, env, bindings)?
                    }
                }
            }
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => continue,
        };
        if let Some(asked) = asked
            && !asked.is_literal()
            && kind.fits(&asked)
        {
            return Ok(Some(asked));
        }
    }
    Ok(None)
}

/// The type that the first cast in `expr` taking the type of `var` asks of
/// it, with `var` bound to a literal kind: the width another term of the
/// cast's operand fixes, else the cast's target primitive. A cast takes the
/// type of `var` when `var` is a term of its operand ([`takes_type_of`]),
/// and is found anywhere in `expr`, inside calls and tuples too. `None` when
/// no cast takes the type of `var`.
fn first_cast_use(
    expr: &Arithmetic,
    var: &Variable,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<Option<DataType>, ParseError> {
    for factor in expr.factors() {
        if let Some(asked) = factor_cast_use(factor, var, udfs, bindings)? {
            return Ok(Some(asked));
        }
    }
    Ok(None)
}

/// [`first_cast_use`] for a single factor.
fn factor_cast_use(
    factor: &Factor,
    var: &Variable,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<Option<DataType>, ParseError> {
    let nested: &[Arithmetic] = match factor {
        Factor::Cast(cast) if takes_type_of(cast.inner(), var) => {
            return Ok(match infer_factor(cast.inner(), bindings, udfs)? {
                Some(fixed) if !fixed.is_literal() => Some(fixed),
                Some(_) | None => Some(cast.target()?.root().clone()),
            });
        }
        Factor::Cast(cast) => return factor_cast_use(cast.inner(), var, udfs, bindings),
        Factor::Group(a) | Factor::TupleProj { tuple: a, .. } => std::slice::from_ref(a.as_ref()),
        Factor::FnCall(call) => call.args(),
        Factor::Builtin(call) => call.args(),
        Factor::Tuple(fields) => fields,
        Factor::Var(_) | Factor::Const(_) | Factor::Placeholder | Factor::Aggregate(_) => &[],
    };
    for expr in nested {
        if let Some(asked) = first_cast_use(expr, var, udfs, bindings)? {
            return Ok(Some(asked));
        }
    }
    Ok(None)
}

/// Returns `true` if `factor` has the type of `var` because `var` is one of
/// its terms, inside parentheses or not: arithmetic gives all its terms one
/// type, while a call, cast, or tuple has a type of its own.
fn takes_type_of(factor: &Factor, var: &Variable) -> bool {
    match factor {
        Factor::Var(v) => v == var,
        Factor::Group(a) => a.factors().any(|f| takes_type_of(f, var)),
        Factor::Const(_)
        | Factor::FnCall(_)
        | Factor::Builtin(_)
        | Factor::Cast(_)
        | Factor::Tuple(_)
        | Factor::TupleProj { .. }
        | Factor::Placeholder
        | Factor::Aggregate(_) => false,
    }
}

/// Check the head's arity against its `.decl`, then each column against its
/// declared type: a bound variable's type, an arithmetic expression (pinning
/// its literals), or a tuple construct field-wise.
fn check_and_pin_head(
    rule: &mut FlowLogRule,
    env: &PrimitiveEnv,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    let head = rule.head_mut();
    let (rel_name, arity, head_span) = (head.name().to_string(), head.arity(), head.span());
    let rel_display = head.raw_name().to_string();
    let col_types: Vec<DataType> = {
        let Some(decl) = env.decls.get(&rel_name) else {
            return Err(grammar_bug(format!(
                "head relation `{rel_name}` not declared"
            )));
        };
        decl.clone()
    };

    if arity != col_types.len() {
        return Err(ParseError::HeadArity {
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
            HeadArg::Var(v) => {
                if let Some((found, _)) = bindings.get(v)
                    && found != &expected
                {
                    return Err(ParseError::HeadColumnType {
                        span: head_span,
                        rel: rel_display,
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
                // pinned per field, avoiding the eager width-collapse that
                // `infer_factor`'s Tuple arm does for the general case.
                if a.rest().is_empty()
                    && matches!(a.init(), Factor::Tuple(_))
                    && let DataType::FixedTuple(field_types) = &expected
                {
                    let field_types = field_types.clone();
                    let span = a.span();
                    if let Factor::Tuple(fields) = a.init_mut() {
                        check_and_pin_tuple(fields, span, &field_types, &env.udfs, bindings)?;
                    }
                    continue;
                }
                if let Some(kind) = infer_expr(a, bindings, &env.udfs)?
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
                pin_expr(a, &expected, bindings, &env.udfs)?;
            }
        }
    }
    Ok(())
}

/// Field-wise check + pin of a tuple construct `(e0, ...)`, the fields of
/// the literal at `span`, against a declared tuple column type. Each field
/// is checked like a scalar column (a polymorphic literal fits any
/// same-family width) and then pinned to its field type, avoiding the eager
/// width-collapse `infer_factor` does when it builds a single
/// `Concrete(FixedTuple(..))`. Recurses for nested tuple-literal fields.
fn check_and_pin_tuple(
    fields: &mut [Arithmetic],
    span: Span,
    expected_fields: &[DataType],
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    if fields.len() != expected_fields.len() {
        return Err(ParseError::TupleConstruct {
            span,
            detail: format!(
                "tuple has {} field(s) but {} value(s) were given",
                expected_fields.len(),
                fields.len()
            ),
        });
    }
    for (a, fty) in fields.iter_mut().zip(expected_fields.iter()) {
        let field_span = a.span();
        // Nested tuple literal: recurse so its fields get the same
        // literal-width leniency.
        if a.rest().is_empty()
            && let Factor::Tuple(inner) = a.init_mut()
        {
            let DataType::FixedTuple(sub) = fty else {
                return Err(ParseError::TupleConstruct {
                    span,
                    detail: format!("a tuple literal does not fit field type `{fty}`"),
                });
            };
            check_and_pin_tuple(inner, field_span, sub, udfs, bindings)?;
            continue;
        }
        if let Some(kind) = infer_expr(a, bindings, udfs)?
            && !kind.fits(fty)
        {
            return Err(ParseError::TupleConstruct {
                span: a.span(),
                detail: format!(
                    "field value of type `{}` does not fit field type `{fty}`",
                    kind.defaulted()
                ),
            });
        }
        pin_expr(a, fty, bindings, udfs)?;
    }
    Ok(())
}

/// A bare literal yields `LiteralColumnMismatch` (cites the source text);
/// anything else yields `HeadColumnType` (cites the inferred type).
fn head_or_literal_mismatch(
    a: &Arithmetic,
    rel: &str,
    col: usize,
    expected: DataType,
    kind: DataType,
) -> ParseError {
    // A bare literal is a single constant factor with no operators.
    if let (true, Factor::Const(c)) = (a.is_const(), a.init()) {
        return ParseError::LiteralColumnMismatch {
            span: a.span(),
            literal: c.to_string(),
            expected,
        };
    }
    ParseError::HeadColumnType {
        span: a.span(),
        rel: rel.to_string(),
        col,
        expected,
        found: kind.defaulted(),
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::AggregationOperator;
    use crate::DataType;
    use crate::ParseError;
    use crate::Predicate;
    use crate::assert_err;
    use crate::test_harness::checked;

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
        checked(src).expect("tuple construct + destructure must type-check");
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
            checked(src).is_err(),
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
            checked(src).is_err(),
            "a symbol in a `number` tuple field must be rejected"
        );
    }

    /// A tuple literal flowing into a scalar column (and vice-versa) is
    /// rejected: tuples are not interchangeable with their fields.
    #[test]
    fn tuple_vs_scalar_mismatch_rejected() {
        let src = "\
            .decl In(x: symbol, y: symbol)\n\
            .decl Out(p: symbol)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(x, y), p = (x, y).\n";
        assert!(
            checked(src).is_err(),
            "a tuple literal into a scalar column must be rejected"
        );
    }

    /// A polymorphic numeric literal in a tuple field whose declared width is
    /// not the family default (here `int64`) must be accepted and pinned: the
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
        checked(src).expect("an int literal in an int64 tuple field must be accepted");
    }

    // --- Aggregate variable types ---

    /// The type pinned on each aggregate variable of the first rule, in the
    /// order of the variables.
    fn var_types(decls: &str, rule: &str) -> Result<Vec<Option<DataType>>, ParseError> {
        let src = format!(
            ".decl Node(x: int16)\n\
             .decl Edge(x: int16, y: int16)\n\
             .decl Name(x: int16, s: symbol)\n\
             .input Node(IO=\"file\", filename=\"Node.csv\", delimiter=\",\")\n\
             .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
             .input Name(IO=\"file\", filename=\"Name.csv\", delimiter=\",\")\n\
             {decls}\n.output Out\n{rule}\n"
        );
        let program = checked(&src)?;
        let mut types = Vec::new();
        collect_var_types(program.rules()[0].rhs(), &mut types);
        Ok(types)
    }

    /// Pushes the type pinned on each aggregate in `body`, an outer one
    /// before those it holds: the order of their variables.
    fn collect_var_types(body: &[Predicate], out: &mut Vec<Option<DataType>>) {
        for predicate in body {
            if let Predicate::Aggregate(aggregate) = predicate {
                out.push(aggregate.var_type().cloned());
                collect_var_types(aggregate.body(), out);
            }
        }
    }

    /// `count` and `mean` take the width their first use asks for, like
    /// a literal, and default when no use asks; the other operators answer
    /// in their target's type.
    // Cases: declaration of Out, rule, variable type.
    #[rstest]
    #[case(
        ".decl Out(x: int16, c: int64)",
        "Out(x, c) :- Node(x), c = count : Edge(x, _).",
        DataType::Int64
    )]
    #[case(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), count : Edge(x, _) > 3.",
        DataType::Int32
    )]
    #[case(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), count : Edge(x, _) > x.",
        DataType::Int16
    )]
    #[case(
        ".decl Out(x: int16, a: float)",
        "Out(x, a) :- Node(x), a = mean y : Edge(x, y).",
        DataType::Float32
    )]
    #[case(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), mean y : Edge(x, y) > 1.5.",
        DataType::Float64
    )]
    #[case(
        ".decl Out(x: int16, s: int16)",
        "Out(x, s) :- Node(x), s = sum y : Edge(x, y).",
        DataType::Int16
    )]
    #[case(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), min y : Edge(x, y) > 0.",
        DataType::Int16
    )]
    fn aggregate_variables_take_the_type_their_operator_and_uses_give(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] expected: DataType,
    ) {
        assert_eq!(var_types(decls, rule).unwrap(), [Some(expected)]);
    }

    /// A nested aggregate's variable takes the width its comparisons ask
    /// for, else the enclosing aggregate's type when the target reads it,
    /// else its default.
    // Cases: declaration of Out, rule, outer type, nested type.
    #[rstest]
    #[case(
        ".decl Out(x: int16, s: int64)",
        "Out(x, s) :- Node(x), s = sum count : Edge(y, _) : Edge(x, y).",
        DataType::Int64,
        DataType::Int64
    )]
    #[case(
        ".decl Out(x: int16, c: int64)",
        "Out(x, c) :- Node(x), c = count : { Edge(x, y), count : Edge(y, _) > y }.",
        DataType::Int64,
        DataType::Int16
    )]
    #[case(
        ".decl Out(x: int16, c: int64)",
        "Out(x, c) :- Node(x), c = count : { Edge(x, y), count : Edge(y, _) > 2 }.",
        DataType::Int64,
        DataType::Int32
    )]
    #[case(
        ".decl Out(x: int16, a: float)",
        "Out(x, a) :- Node(x), a = mean count : Edge(y, _) : Edge(x, y).",
        DataType::Float32,
        DataType::Int32
    )]
    fn nested_aggregate_variables_take_the_type_their_uses_give(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] outer: DataType,
        #[case] nested: DataType,
    ) {
        assert_eq!(var_types(decls, rule).unwrap(), [Some(outer), Some(nested)]);
    }

    /// A use inside another aggregate's braces or target asks a width like
    /// any other.
    // Cases: declaration of Out, rule, first type, second type.
    #[rstest]
    #[case(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), c = count : Edge(x, _), count : { Edge(x, y), y > c } > 0.",
        DataType::Int16,
        DataType::Int32
    )]
    #[case(
        ".decl Out(x: int16, s: int16)",
        "Out(x, s) :- Node(x), c = count : Edge(x, _), s = sum y + c : Edge(x, y).",
        DataType::Int16,
        DataType::Int16
    )]
    fn an_aggregate_variable_used_inside_another_aggregate_takes_that_width(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] first: DataType,
        #[case] second: DataType,
    ) {
        assert_eq!(var_types(decls, rule).unwrap(), [Some(first), Some(second)]);
    }

    /// A cast picks the width of an aggregate variable in its operand, even
    /// inside a call or string constraint, whose own type says nothing of
    /// that width.
    // Cases: declaration of Out, rule, variable types.
    #[rstest]
    #[case::in_a_call(
        ".decl Out(x: int16, s: symbol)",
        "Out(x, to_string(as(c, int64))) :- Node(x), c = count : Edge(x, _).",
        vec![DataType::Int64]
    )]
    #[case::in_a_comparison(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), c = count : Edge(x, _), as(c, int64) bshl 31 > 0.",
        vec![DataType::Int64]
    )]
    #[case::in_a_string_constraint(
        ".decl Out(x: int16)",
        "Out(x) :- Node(x), c = count : Edge(x, _), \
         contains(\"1\", to_string(as(c, int64))).",
        vec![DataType::Int64]
    )]
    #[case::two_counts_in_one_operand(
        ".decl Out(x: int16, s: symbol)",
        "Out(x, to_string(as((c + d), int64))) :- Node(x), \
         c = count : Edge(x, _), d = count : Edge(_, x).",
        vec![DataType::Int64, DataType::Int64]
    )]
    #[case::nested_in_the_enclosing_target(
        ".decl Out(x: int16, s: int32)",
        "Out(x, s) :- Node(x), \
         s = sum strlen(to_string(as(count : Edge(y, _), int64))) : Edge(x, y).",
        vec![DataType::Int32, DataType::Int64]
    )]
    fn a_cast_picks_the_width_of_an_aggregate_variable_in_its_operand(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] expected: Vec<DataType>,
    ) {
        let expected: Vec<Option<DataType>> = expected.into_iter().map(Some).collect();
        assert_eq!(var_types(decls, rule).unwrap(), expected);
    }

    /// A cast over a call has the call's type, not the type of a variable in
    /// the call's arguments, so it leaves that variable's width alone.
    #[test]
    fn a_cast_over_a_call_leaves_an_aggregate_variable_in_its_arguments_alone() {
        let types = var_types(
            ".extern fn widen(a: int32) -> int64\n.decl Out(x: int16, s: symbol)",
            "Out(x, to_string(as(widen(c), int64))) :- Node(x), c = count : Edge(x, _).",
        );
        assert_eq!(types.unwrap(), [Some(DataType::Int32)]);
    }

    /// A `count` answer flowing into a column of another family is a head
    /// type error, not a silent conversion.
    #[test]
    fn a_count_into_a_string_column_is_rejected() {
        assert_err!(
            var_types(
                ".decl Out(x: int16, c: symbol)",
                "Out(x, c) :- Node(x), c = count : Edge(x, _).",
            ),
            ParseError::HeadColumnType { .. }
        );
    }

    #[test]
    fn a_sum_over_strings_is_rejected() {
        assert_err!(
            var_types(
                ".decl Out(x: int16)",
                "Out(x) :- Node(x), sum s : Name(x, s) > 0.",
            ),
            ParseError::AggregationInputNotNumeric {
                op: AggregationOperator::Sum,
                ..
            }
        );
    }
}
