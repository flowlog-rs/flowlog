//! Writes an inference's result into the program: each polymorphic literal
//! is pinned to its concrete width, and each aggregate's variable is given
//! its type. Runs after substitution, so a literal a definition carried
//! into several uses is pinned once per use, by that use's context.
//!
//! Every context's type comes from the inferred variable types and the
//! declarations: a head argument's or an atom argument's from its column,
//! a comparison's from its sides, a call's from its parameters, an
//! aggregate target's from the aggregate. Inference accepted the program,
//! so no context disagrees with what it holds; only a literal's spelling
//! can still fail, when it does not fit its width.

use flowlog_common::Span;

use crate::AggregationOperator;
use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::BuiltinCall;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::FnCall;
use crate::HeadArg;
use crate::InlineFact;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::TupleElem;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::declarations::Declarations;
use crate::pipeline::typecheck::declarations::UdfSigs;
use crate::pipeline::typecheck::infer::VariableTypes;

/// Pins every literal of `program` and types every aggregate variable, as
/// inference found them: `types` holds each rule's, in the program's rule
/// order.
pub(super) fn pin(program: &mut Program, types: &[VariableTypes]) -> Result<(), ParseError> {
    let decls = Declarations::from_program(program);
    for (index, rule) in program.rules_mut().iter_mut().enumerate() {
        let vars = types
            .get(index)
            .ok_or_else(|| grammar_bug(format!("rule {index} was not inferred")))?;
        pin_rule(rule, vars, &decls)?;
    }
    for (rel_name, facts) in program.facts_mut() {
        let Some(columns) = decls.relations.get(rel_name) else {
            return Err(grammar_bug(format!(
                "fact references undeclared relation `{rel_name}`"
            )));
        };
        pin_facts(facts, columns)?;
    }
    Ok(())
}

/// Pins `rule`'s body, then each head argument to its column.
fn pin_rule(
    rule: &mut FlowLogRule,
    vars: &VariableTypes,
    decls: &Declarations,
) -> Result<(), ParseError> {
    pin_body(rule.rhs_mut(), vars, decls)?;
    let head = rule.head_mut();
    let Some(columns) = decls.relations.get(head.name()).cloned() else {
        return Err(grammar_bug(format!(
            "head relation `{}` not declared",
            head.name()
        )));
    };
    for (arg, column) in head.head_arguments_mut().iter_mut().zip(&columns) {
        match arg {
            HeadArg::Var(_) => {}
            HeadArg::Arith(a) => pin_expr(a, column, vars, &decls.udfs)?,
        }
    }
    Ok(())
}

/// Pins each predicate of `body`: an atom's constants to their columns, a
/// comparison's literals to the type its sides share, and an aggregate's
/// target to its answer's type (a `mean`'s to the target's own), its
/// variable given that type, and its body pinned in turn.
fn pin_body(
    body: &mut [Predicate],
    vars: &VariableTypes,
    decls: &Declarations,
) -> Result<(), ParseError> {
    for predicate in body {
        match predicate {
            Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => pin_atom(atom, decls)?,
            Predicate::Compare(cmp) => {
                let span = cmp.span();
                let target = match (
                    type_of(cmp.left(), vars, &decls.udfs)?,
                    type_of(cmp.right(), vars, &decls.udfs)?,
                ) {
                    (Some(left), Some(right)) => left.merge(&right).ok_or_else(|| {
                        grammar_bug(format!("comparison at {span:?} disagrees after inference"))
                    })?,
                    (Some(one), None) | (None, Some(one)) => one,
                    (None, None) => continue,
                };
                let target = target.defaulted();
                pin_expr(cmp.left_mut(), &target, vars, &decls.udfs)?;
                pin_expr(cmp.right_mut(), &target, vars, &decls.udfs)?;
            }
            Predicate::Aggregate(aggregate) => {
                let Some(var) = aggregate.var() else {
                    return Err(grammar_bug(format!(
                        "aggregate `{aggregate}` in a rule body binds no variable"
                    )));
                };
                let Some(ty) = vars.get(var).cloned() else {
                    return Err(grammar_bug(format!("aggregate `{aggregate}` has no type")));
                };
                aggregate.set_var_type(ty.clone());
                let op = aggregate.operator();
                if let Some(target) = aggregate.target_mut() {
                    // A mean's input is independent of its float answer;
                    // the other operators answer in the input's type.
                    let target_ty = match op {
                        AggregationOperator::Mean => {
                            type_of(target, vars, &decls.udfs)?.map(|ty| ty.defaulted())
                        }
                        AggregationOperator::Count
                        | AggregationOperator::Sum
                        | AggregationOperator::Min
                        | AggregationOperator::Max => Some(ty),
                    };
                    if let Some(target_ty) = target_ty {
                        pin_expr(target, &target_ty, vars, &decls.udfs)?;
                    }
                }
                pin_body(aggregate.body_mut(), vars, decls)?;
            }
        }
    }
    Ok(())
}

/// Pins each polymorphic constant argument of `atom` to its column.
fn pin_atom(atom: &mut Atom, decls: &Declarations) -> Result<(), ParseError> {
    let span = atom.span();
    let Some(columns) = decls.relations.get(atom.name()).cloned() else {
        return Err(grammar_bug(format!("atom `{}` not declared", atom.name())));
    };
    for (arg, column) in atom.arguments_mut().iter_mut().zip(&columns) {
        if let AtomArg::Const(constant) = arg
            && constant.is_polymorphic()
        {
            constant.pin(column.clone(), span)?;
        }
    }
    Ok(())
}

/// Pins each polymorphic column of `facts` to its `.decl` column.
fn pin_facts(facts: &mut [InlineFact], columns: &[DataType]) -> Result<(), ParseError> {
    for fact in facts {
        for (constant, column) in fact.columns.iter_mut().zip(columns) {
            if constant.is_polymorphic() {
                constant.pin(column.clone(), fact.span)?;
            }
        }
    }
    Ok(())
}

/// The type of `expr` over the inferred `vars`, a literal family where only
/// literals decide it; `None` when a variable of it has no type.
fn type_of(
    expr: &Arithmetic,
    vars: &VariableTypes,
    udfs: &UdfSigs,
) -> Result<Option<DataType>, ParseError> {
    let mut ty = type_of_factor(expr.init(), vars, udfs)?;
    for (_, factor) in expr.rest() {
        let Some(next) = type_of_factor(factor, vars, udfs)? else {
            continue;
        };
        ty = Some(match ty {
            None => next,
            Some(ty) => ty.merge(&next).ok_or_else(|| {
                grammar_bug(format!(
                    "`{expr}` mixes `{ty}` and `{next}` after inference"
                ))
            })?,
        });
    }
    Ok(ty)
}

fn type_of_factor(
    factor: &Factor,
    vars: &VariableTypes,
    udfs: &UdfSigs,
) -> Result<Option<DataType>, ParseError> {
    Ok(match factor {
        Factor::Var(var) => vars.get(var).cloned(),
        Factor::Const(constant) => Some(constant.ty().clone()),
        Factor::FnCall(call) => Some(
            udfs.get(call.name())
                .map(|(_, ret)| ret.clone())
                .ok_or_else(|| grammar_bug(format!("UDF `{}` not declared", call.name())))?,
        ),
        Factor::Builtin(call) => Some(call.op().ret_type()),
        Factor::Cast(cast) => type_of_factor(cast.inner(), vars, udfs)?,
        Factor::Group(a) => type_of(a, vars, udfs)?,
        Factor::Tuple(lit) => {
            let mut fields = Vec::with_capacity(lit.fields().len());
            for elem in lit.fields() {
                let TupleElem::Expr(a) = elem else {
                    return Err(grammar_bug(format!(
                        "`_` in the tuple `{lit}` after inference"
                    )));
                };
                let Some(ty) = type_of(a, vars, udfs)? else {
                    return Ok(None);
                };
                fields.push(ty);
            }
            Some(DataType::FixedTuple(fields))
        }
        Factor::TupleProj { tuple, index } => match type_of(tuple, vars, udfs)? {
            Some(DataType::FixedTuple(fields)) => {
                Some(fields.get(*index).cloned().ok_or_else(|| {
                    grammar_bug(format!("projection `{factor}` is out of its tuple's range"))
                })?)
            }
            Some(other) => {
                return Err(grammar_bug(format!(
                    "projection `{factor}` of `{other}`, which is not a tuple"
                )));
            }
            None => None,
        },
        Factor::Aggregate(aggregate) => {
            return Err(grammar_bug(format!(
                "aggregate `{aggregate}` was not hoisted out of its expression"
            )));
        }
    })
}

/// Pins every polymorphic literal in `a` to `target`. A call's arguments
/// are pinned to its parameters, which are independent of `target`.
fn pin_expr(
    a: &mut Arithmetic,
    target: &DataType,
    vars: &VariableTypes,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    let span = a.span();
    pin_factor(a.init_mut(), target, span, vars, udfs)?;
    for (_, factor) in a.rest_mut() {
        pin_factor(factor, target, span, vars, udfs)?;
    }
    Ok(())
}

fn pin_factor(
    factor: &mut Factor,
    target: &DataType,
    span: Span,
    vars: &VariableTypes,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    match factor {
        Factor::Const(constant) => {
            if constant.is_polymorphic() {
                constant.pin(target.clone(), span)?;
            }
            Ok(())
        }
        Factor::Var(_) => Ok(()),
        Factor::FnCall(call) => pin_fn_call(call, vars, udfs),
        Factor::Builtin(call) => pin_builtin_call(call, vars, udfs),
        // A cast asserts its inner has the target's primitive, so the
        // literals inside are pinned to it.
        Factor::Cast(cast) => pin_factor(cast.inner_mut(), target, span, vars, udfs),
        Factor::Group(a) => pin_expr(a, target, vars, udfs),
        Factor::Tuple(lit) => {
            if let DataType::FixedTuple(fields) = target {
                for (elem, field) in lit.fields_mut().iter_mut().zip(fields) {
                    if let TupleElem::Expr(a) = elem {
                        pin_expr(a, field, vars, udfs)?;
                    }
                }
            }
            Ok(())
        }
        // A projection's tuple is a variable, or a tuple of variables and
        // literals pinned where it was constructed.
        Factor::TupleProj { tuple, .. } => match type_of(tuple, vars, udfs)? {
            Some(ty) => pin_expr(tuple, &ty.defaulted(), vars, udfs),
            None => Ok(()),
        },
        Factor::Aggregate(aggregate) => Err(grammar_bug(format!(
            "aggregate `{aggregate}` was not hoisted out of its expression"
        ))),
    }
}

/// Pins each argument of a built-in call to its own type: for a
/// fixed-type parameter that is the declared type, and for a parameter of
/// several allowed types whatever the argument is.
fn pin_builtin_call(
    call: &mut BuiltinCall,
    vars: &VariableTypes,
    udfs: &UdfSigs,
) -> Result<(), ParseError> {
    for arg in call.args_mut() {
        if let Some(ty) = type_of(arg, vars, udfs)? {
            pin_expr(arg, &ty.defaulted(), vars, udfs)?;
        }
    }
    Ok(())
}

fn pin_fn_call(call: &mut FnCall, vars: &VariableTypes, udfs: &UdfSigs) -> Result<(), ParseError> {
    // Collected by value so the recursive `pin_expr` below can reborrow
    // `udfs`; holding `&params` from `udfs.get(...)` across the recursion
    // would block it.
    let params: Vec<DataType> = udfs
        .get(call.name())
        .map(|(params, _)| params.iter().map(|(_, ty)| ty.clone()).collect())
        .ok_or_else(|| grammar_bug(format!("UDF `{}` not declared", call.name())))?;
    for (arg, param) in call.args_mut().iter_mut().zip(&params) {
        pin_expr(arg, param, vars, udfs)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::AtomArg;
    use crate::Constant;
    use crate::DataType;
    use crate::Factor;
    use crate::HeadArg;
    use crate::Predicate;
    use crate::test_harness::checked;

    /// The constant at the top of the head argument `col` of the first
    /// rule.
    fn head_constant(src: &str, col: usize) -> Constant {
        let program = checked(src).expect("type-check should succeed");
        let rule = &program.rules()[0];
        let HeadArg::Arith(a) = &rule.head().head_arguments()[col] else {
            panic!(
                "expected Arith head arg, got {:?}",
                rule.head().head_arguments()[col]
            );
        };
        let Factor::Const(c) = a.init() else {
            panic!("expected Const, got {:?}", a.init());
        };
        c.clone()
    }

    /// Body-positive atom literal: `Flag(5)` with `.decl Flag(x: int8)` must
    /// pin `5` to `Int8(5)`. If that pass becomes a no-op, catalog calls
    /// `data_type()` on a polymorphic `Int` and panics.
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
        let program = checked(src).expect("type-check should succeed");
        let rule = &program.rules()[0];
        let Predicate::PositiveAtom(flag) = &rule.rhs()[1] else {
            panic!("expected Flag atom, got {:?}", rule.rhs()[1]);
        };
        match &flag.arguments()[0] {
            AtomArg::Const(c) => assert_eq!(c, &Constant::new(DataType::Int8, "5")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Comparison operand literal: `x > 100` with `x: int16` must pin `100` to
    /// `Int16(100)`.
    #[test]
    fn comparison_literal_pinned_to_variable_type() {
        let src = "\
            .decl Item(x: int16)\n\
            .decl Big(x: int16)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Big\n\
            Big(x) :- Item(x), x > 100.\n";
        let program = checked(src).expect("type-check should succeed");
        let rule = &program.rules()[0];
        let Predicate::Compare(cmp) = &rule.rhs()[1] else {
            panic!("expected comparison, got {:?}", rule.rhs()[1]);
        };
        match cmp.right().init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int16, "100")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// A literal component of a destructure is pinned to its field's width,
    /// and keeps it in the filter the destructure expands to.
    #[test]
    fn destructure_component_literal_pinned_to_field_type() {
        let src = "\
            .type Pair = ( a: int16, b: int16 )\n\
            .decl In(x: int16, y: int16)\n\
            .decl P(p: Pair)\n\
            .decl Out(a: int16)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            P(p) :- In(x, y), p = (x, y).\n\
            Out(a) :- P(p), (a, 7) = p.\n";
        let program = checked(src).expect("type-check should succeed");
        let Predicate::Compare(filter) = &program.rules()[1].rhs()[1] else {
            panic!("expected the filter, got {:?}", program.rules()[1].rhs());
        };
        match filter.right().init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int16, "7")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// Nested UDF call: in `f(1) + x` where `x: int64` and `f: int8 -> int64`,
    /// the `1` must be pinned to the UDF's parameter width (`Int8`), not the
    /// enclosing expression's target (`Int64`).
    #[test]
    fn nested_udf_arg_pinned_to_param_type_not_outer_target() {
        let src = "\
            .decl Item(x: int64)\n\
            .decl Flag(x: int64)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Flag\n\
            .extern fn f(a: int8) -> int64\n\
            Flag(f(1) + x) :- Item(x).\n";
        let program = checked(src).expect("type-check should succeed");
        let HeadArg::Arith(a) = &program.rules()[0].head().head_arguments()[0] else {
            panic!("expected Arith head arg");
        };
        let Factor::FnCall(fc) = a.init() else {
            panic!("expected FnCall factor, got {:?}", a.init());
        };
        match fc.args()[0].init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int8, "1")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// A literal in a tuple field is pinned to the field's width, here one
    /// other than the family default.
    #[test]
    fn tuple_field_literal_pinned_to_field_width() {
        let src = "\
            .type Tv = ( t: symbol, v: int64 )\n\
            .decl In(s: symbol)\n\
            .decl Out(p: Tv)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(p) :- In(s), p = (s, 5).\n";
        let program = checked(src).expect("type-check should succeed");
        let HeadArg::Arith(a) = &program.rules()[0].head().head_arguments()[0] else {
            panic!("expected Arith head arg");
        };
        let Some(lit) = a.as_tuple() else {
            panic!("expected a tuple literal, got {a}");
        };
        let field: Vec<String> = lit.exprs().map(ToString::to_string).collect();
        assert_eq!(field, ["s", "5"]);
        let Some(crate::TupleElem::Expr(v)) = lit.fields().get(1) else {
            panic!("expected a second field");
        };
        match v.init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int64, "5")),
            other => panic!("expected Const, got {other:?}"),
        }
    }

    /// A value with no variables, substituted into two columns, is pinned
    /// once per column.
    // Cases: column, pinned constant.
    #[rstest]
    #[case(0, Constant::new(DataType::Int16, "1"))]
    #[case(1, Constant::new(DataType::Int64, "1"))]
    fn a_definition_with_no_variables_is_pinned_per_use(
        #[case] col: usize,
        #[case] pinned: Constant,
    ) {
        let src = "\
            .decl In(x: int16)\n\
            .decl Out(a: int16, b: int64)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(t, t) :- In(x), t = 1.\n";
        assert_eq!(head_constant(src, col), pinned);
    }

    /// Fact tuple literal: `P(5)` with `.decl P(x: uint64)` must pin, a
    /// separate path from rule-body pinning.
    #[test]
    fn fact_tuple_const_pinned_to_declared_column_width() {
        let src = "\
            .decl P(x: uint64)\n\
            .decl Out(x: uint64)\n\
            .output Out\n\
            P(5).\n\
            Out(x) :- P(x).\n";
        let program = checked(src).expect("type-check should succeed");
        let p_facts = program.facts().get("p").expect("p facts");
        assert_eq!(p_facts[0].columns[0], Constant::new(DataType::UInt64, "5"));
    }
}
