//! Check and pin every rule: its body predicates, then its head (the composer).

use std::collections::HashMap;

use crate::Aggregation;
use crate::AggregationOperator;
use crate::Arithmetic;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::HeadArg;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::TupleElem;
use crate::TupleLit;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::env::PrimitiveEnv;
use crate::pipeline::typecheck::env::UdfSigs;
use crate::pipeline::typecheck::primitive::Bindings;
use crate::pipeline::typecheck::primitive::atom::bind_atom;
use crate::pipeline::typecheck::primitive::atom::check_atom;
use crate::pipeline::typecheck::primitive::atom::pin_atom;
use crate::pipeline::typecheck::primitive::compare::check_and_pin_compare;
use crate::pipeline::typecheck::primitive::expr::infer_expr;
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
        for predicate in rule.rhs() {
            if let Predicate::PositiveAtom(atom) = predicate {
                bind_atom(atom, &env.decls, &mut bindings)?;
            }
        }

        for predicate in rule.rhs_mut() {
            match predicate {
                Predicate::PositiveAtom(atom) => pin_atom(atom, &env.decls)?,
                Predicate::NegativeAtom(atom) => {
                    check_atom(atom, &env.decls, &bindings)?;
                    pin_atom(atom, &env.decls)?;
                }
                Predicate::Compare(cmp) => check_and_pin_compare(cmp, &bindings, &env.udfs)?,
            }
        }

        check_and_pin_head(rule, env, &bindings)?;
    }
    Ok(())
}

/// Check the head's arity against its `.decl`, then each column against its
/// declared type: a bound variable's type, an arithmetic expression (pinning
/// its literals), a tuple construct field-wise, or an aggregation.
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
            HeadArg::Aggregation(agg) => {
                check_and_pin_aggregation(agg, expected, &env.udfs, bindings)?
            }
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
                    if let Factor::Tuple(lit) = a.init_mut() {
                        check_and_pin_tuple(lit, &field_types, &env.udfs, bindings)?;
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

/// Field-wise check + pin of a tuple construct `(e0, ...)` against a declared
/// tuple column type. Each field is checked like a scalar column (a
/// polymorphic literal fits any same-family width) and then pinned to its
/// field type, avoiding the eager width-collapse `infer_factor` does when it
/// builds a single `Concrete(FixedTuple(..))`. Recurses for nested
/// tuple-literal fields.
fn check_and_pin_tuple(
    lit: &mut TupleLit,
    expected_fields: &[DataType],
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    if lit.fields().len() != expected_fields.len() {
        return Err(ParseError::TupleConstruct {
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
                return Err(ParseError::TuplePlaceholderInConstruct { span });
            }
            // Nested tuple literal: recurse so its fields get the same
            // literal-width leniency.
            TupleElem::Expr(a) if a.rest().is_empty() && matches!(a.init(), Factor::Tuple(_)) => {
                let DataType::FixedTuple(sub) = fty else {
                    return Err(ParseError::TupleConstruct {
                        span,
                        detail: format!("a tuple literal does not fit field type `{fty}`"),
                    });
                };
                let Factor::Tuple(inner) = a.init_mut() else {
                    return Err(grammar_bug("tuple guard matched but init is not a tuple"));
                };
                check_and_pin_tuple(inner, sub, udfs, bindings)?;
            }
            TupleElem::Expr(a) => {
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
        }
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

/// Check an aggregation's input and output against the declared column and pin
/// its input expression. `count` allows any numeric output; `sum` / `avg` /
/// `min` / `max` require a numeric input whose family matches the output.
fn check_and_pin_aggregation(
    agg: &mut Aggregation,
    declared: DataType,
    udfs: &UdfSigs,
    bindings: &Bindings,
) -> Result<(), ParseError> {
    let op = *agg.operator();
    let span = agg.span();
    let arg_kind = infer_expr(agg.arithmetic(), bindings, udfs)?;

    // `count`'s input type is independent of its declared output.
    if matches!(op, AggregationOperator::Count) {
        if !declared.is_numeric() {
            return Err(ParseError::AggregationOutputType { span, op, declared });
        }
        if let Some(k) = arg_kind {
            pin_expr(agg.arithmetic_mut(), &k.defaulted(), bindings, udfs)?;
        }
        return Ok(());
    }

    // sum / avg / min / max: numeric input, output family matches input.
    if let Some(kind) = arg_kind {
        if !kind.defaulted().is_numeric() {
            return Err(ParseError::AggregationInputNotNumeric {
                span,
                op,
                ty: kind.defaulted(),
            });
        }
        if !kind.fits(&declared) {
            return Err(ParseError::AggregationOutputType { span, op, declared });
        }
    }
    pin_expr(agg.arithmetic_mut(), &declared, bindings, udfs)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::test_util::checked;

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
}
