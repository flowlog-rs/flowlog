//! Check and pin every rule: its body predicates, then its head (the composer).

use std::collections::HashMap;

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
use crate::pipeline::typecheck::env::DeclTypes;
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
        // Inside an aggregate a variable keeps its rule-wide name: one that
        // also occurs outside is the same variable, so its atoms bind it
        // like any other.
        bind_positive_atoms(rule.rhs(), &env.decls, &mut bindings)?;
        bind_aggregate_vars(rule, env, &mut bindings)?;
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

/// Binds each aggregate's variable to the type of its value and
/// records that type on the binding. `count` answers an integer and
/// `mean` a float, each of a width its uses pick, like a literal; the
/// other operators answer in their target's type, which is that of a
/// literal when the target is one. Bound in body order, so a later
/// aggregate may read an earlier one's variable.
fn bind_aggregate_vars(
    rule: &mut FlowLogRule,
    env: &PrimitiveEnv,
    bindings: &mut Bindings,
) -> Result<(), ParseError> {
    for i in 0..rule.rhs().len() {
        let Predicate::Aggregate(aggregate) = &rule.rhs()[i] else {
            continue;
        };
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
        let Some(var) = aggregate.var().map(String::from) else {
            return Err(grammar_bug(format!(
                "aggregate `{aggregate}` in a rule body binds no variable"
            )));
        };
        let ty = if kind.is_literal() {
            bindings.insert(var.clone(), (kind.clone(), span));
            let used = first_concrete_use(rule, &var, &kind, env, bindings)?;
            used.unwrap_or_else(|| match op {
                AggregationOperator::Mean => DataType::Float64,
                AggregationOperator::Count
                | AggregationOperator::Sum
                | AggregationOperator::Min
                | AggregationOperator::Max => kind.defaulted(),
            })
        } else {
            kind
        };
        bindings.insert(var, (ty.clone(), span));
        if let Predicate::Aggregate(aggregate) = &mut rule.rhs_mut()[i] {
            aggregate.set_var_type(ty);
        }
    }
    Ok(())
}

/// The first concrete type of `kind`'s family that a use of `var` asks of
/// it, with `var` bound to `kind`: a head column it fills or an expression of
/// one, then a comparison it takes part in. `None` when every use accepts any
/// width. A use asking another family is skipped here and reported where it
/// is checked.
fn first_concrete_use(
    rule: &FlowLogRule,
    var: &str,
    kind: &DataType,
    env: &PrimitiveEnv,
    bindings: &Bindings,
) -> Result<Option<DataType>, ParseError> {
    let reads = |vars: Vec<&String>| vars.iter().any(|v| *v == var);
    if let Some(col_types) = env.decls.get(rule.head().name()) {
        for (arg, col_type) in rule.head().head_arguments().iter().zip(col_types) {
            let asked = match arg {
                HeadArg::Var(v) if v == var => col_type.clone(),
                HeadArg::Arith(a) if reads(a.vars()) => match infer_expr(a, bindings, &env.udfs)? {
                    Some(found) if !found.is_literal() => found,
                    Some(_) | None => col_type.clone(),
                },
                HeadArg::Var(_) | HeadArg::Arith(_) => continue,
            };
            if kind.fits(&asked) {
                return Ok(Some(asked));
            }
        }
    }
    for predicate in rule.rhs() {
        let Predicate::Compare(cmp) = predicate else {
            continue;
        };
        if cmp.operator().is_string_constraint()
            || !(reads(cmp.left().vars()) || reads(cmp.right().vars()))
        {
            continue;
        }
        let left = infer_expr(cmp.left(), bindings, &env.udfs)?;
        let right = infer_expr(cmp.right(), bindings, &env.udfs)?;
        let merged = match (left, right) {
            (Some(l), Some(r)) => l.merge(&r),
            (Some(k), None) | (None, Some(k)) => Some(k),
            (None, None) => None,
        };
        if let Some(asked) = merged
            && !asked.is_literal()
            && kind.fits(&asked)
        {
            return Ok(Some(asked));
        }
    }
    Ok(None)
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

    /// The type pinned on the first aggregate variable of the first rule.
    fn var_type(decls: &str, rule: &str) -> Result<DataType, ParseError> {
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
        Ok(program.rules()[0]
            .rhs()
            .iter()
            .find_map(|p| match p {
                Predicate::Aggregate(aggregate) => aggregate.var_type().cloned(),
                Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) | Predicate::Compare(_) => {
                    None
                }
            })
            .expect("the rule aggregates"))
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
        assert_eq!(var_type(decls, rule).unwrap(), expected);
    }

    /// A `count` answer flowing into a column of another family is a head
    /// type error, not a silent conversion.
    #[test]
    fn a_count_into_a_string_column_is_rejected() {
        assert_err!(
            var_type(
                ".decl Out(x: int16, c: symbol)",
                "Out(x, c) :- Node(x), c = count : Edge(x, _).",
            ),
            ParseError::HeadColumnType { .. }
        );
    }

    #[test]
    fn a_sum_over_strings_is_rejected() {
        assert_err!(
            var_type(
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
