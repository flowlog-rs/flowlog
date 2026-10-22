//! Type inference over each rule as written.
//!
//! Every construct states one kind of fact, that two types are equal, and
//! [`Solver::unify`] enforces it:
//!
//! - an atom's argument has its column's type;
//! - the two sides of `=`, `!=`, or an ordering are of one type, a tuple
//!   literal field by field; `_` in a tuple literal beside `=` matches any
//!   field;
//! - the operands of an arithmetic operator are of one numeric type, which
//!   is the expression's;
//! - a call's argument has its parameter's type;
//! - `count` answers an integer, `mean` a float, and `sum`, `min`, `max`
//!   in their target's numeric type;
//! - a head argument has its column's type.
//!
//! A literal is a variable of its family, so `5` takes whatever width its
//! context asks, and `int32` (`float32` for a float, `float64` for a
//! `mean`) when nothing does. A rule variable is a variable nothing
//! constrains until a column or expression does; one nothing ever
//! constrains has no type, and the range-restriction pass reports it.
//!
//! The constraints of a rule are generated in reading order, the body left
//! to right and the head last, so the first use that disagrees with what
//! came before is the one reported, together with where the type was
//! fixed.
//!
//! A variable that an equality defines as a value with no variables
//! (`t = 1 + 2`, or `u = t * 2` once `t` is) names that value, not a typed
//! cell: each use of it is typed on its own, so `t = 1` may fill an `int8`
//! column and an `int64` one in the same rule. The substitution stage
//! inlines such a definition into its uses.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::Config;
use flowlog_common::Span;

use crate::AggregateExpr;
use crate::AggregationOperator;
use crate::Arithmetic;
use crate::Atom;
use crate::AtomArg;
use crate::BuiltinCall;
use crate::BuiltinOperator;
use crate::ComparisonExpr;
use crate::ComparisonOperator;
use crate::DataType;
use crate::Factor;
use crate::FlowLogRule;
use crate::FnCall;
use crate::Head;
use crate::HeadArg;
use crate::InlineFact;
use crate::ParseError;
use crate::Predicate;
use crate::Program;
use crate::TupleElem;
use crate::TupleLit;
use crate::error::grammar_bug;
use crate::pipeline::typecheck::declarations::Declarations;
use crate::pipeline::typecheck::solver::Kind;
use crate::pipeline::typecheck::solver::Solver;
use crate::pipeline::typecheck::solver::TypeTerm;

/// The type of each variable of one rule that has one, aggregate
/// variables included. A variable defined as a value with no variables is
/// absent, each use of it having its own type.
pub(super) type VariableTypes = HashMap<String, DataType>;

/// Infers the types of every rule's variables, in the program's rule order,
/// and checks every rule, fact, and `ord` use. Stops at the first error.
/// `config` gates config-dependent builtins (`ord`) and is updated with
/// `ord`'s serial-load requirement.
pub(super) fn infer(
    program: &Program,
    config: &mut Config,
) -> Result<Vec<VariableTypes>, ParseError> {
    let decls = Declarations::from_program(program);
    let mut rules = Vec::with_capacity(program.rules().len());
    for rule in program.rules() {
        rules.push(RuleInference::new(&decls).infer(rule)?);
    }
    for (rel_name, facts) in program.facts() {
        check_facts(rel_name, facts, &decls)?;
    }
    check_ord(program, config)?;
    Ok(rules)
}

// =============================================================================
// RuleInference
// =============================================================================

/// The inference of one rule.
struct RuleInference<'d> {
    decls: &'d Declarations,
    solver: Solver,
    /// Each variable's term, and where the rule first mentions it.
    vars: HashMap<String, (TypeTerm, Span)>,
    /// The term of each variable a definition with no variables defines,
    /// instantiated afresh for each use.
    closed: HashMap<String, TypeTerm>,
}

impl<'d> RuleInference<'d> {
    fn new(decls: &'d Declarations) -> Self {
        Self {
            decls,
            solver: Solver::default(),
            vars: HashMap::new(),
            closed: HashMap::new(),
        }
    }

    /// Infers `rule`, returning the type of each of its variables that has
    /// one.
    fn infer(mut self, rule: &FlowLogRule) -> Result<VariableTypes, ParseError> {
        let binders: HashSet<String> = rule.bound_vars().into_iter().map(String::from).collect();
        self.constrain_body(rule.rhs(), &binders)?;
        self.constrain_head(rule.head())?;
        let Self { solver, vars, .. } = self;
        Ok(vars
            .into_iter()
            .filter_map(|(name, (ty, _))| solver.resolve(&ty).map(|ty| (name, ty)))
            .collect())
    }

    /// Generates the constraints of `body`, given the variables `binders`
    /// bind around and in it: the rule's atoms and aggregates, plus the
    /// enclosing aggregates' for an aggregate's body.
    fn constrain_body(
        &mut self,
        body: &[Predicate],
        binders: &HashSet<String>,
    ) -> Result<(), ParseError> {
        let definitions = closed_definitions(body, binders, &self.closed);
        for (_, var, value) in &definitions {
            let term = self.term_of(value)?;
            self.closed.insert((*var).to_string(), term);
        }
        let defining: HashSet<usize> = definitions.iter().map(|(index, ..)| *index).collect();
        for (index, predicate) in body.iter().enumerate() {
            match predicate {
                Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                    self.constrain_atom(atom)?
                }
                Predicate::Compare(_) if defining.contains(&index) => {}
                Predicate::Compare(cmp) => self.constrain_compare(cmp)?,
                Predicate::Aggregate(aggregate) => self.constrain_aggregate(aggregate, binders)?,
            }
        }
        Ok(())
    }

    /// Each argument of `atom` has its column's type.
    fn constrain_atom(&mut self, atom: &Atom) -> Result<(), ParseError> {
        let columns = self
            .decls
            .relations
            .get(atom.name())
            .ok_or_else(|| grammar_bug(format!("atom `{}` not declared", atom.name())))?;
        if atom.arity() != columns.len() {
            return Err(ParseError::AtomArity {
                span: atom.span(),
                rel: atom.raw_name().to_string(),
                expected: columns.len(),
                found: atom.arity(),
            });
        }
        for (arg, column) in atom.arguments().iter().zip(columns) {
            match arg {
                AtomArg::Var(var) => {
                    let (term, first_span) = self.variable_term(var, atom.span());
                    if self.solver.unify(&term, &TypeTerm::of(column)).is_err() {
                        return Err(ParseError::TypeMismatch {
                            var: var.clone(),
                            first_ty: self.solver.describe(&term, column),
                            first_span,
                            later_ty: column.clone(),
                            later_span: atom.span(),
                        });
                    }
                }
                AtomArg::Const(constant) => {
                    if !constant.ty().fits(column) {
                        return Err(ParseError::LiteralColumnMismatch {
                            span: atom.span(),
                            literal: constant.to_string(),
                            expected: column.clone(),
                        });
                    }
                }
                AtomArg::Placeholder => {}
            }
        }
        Ok(())
    }

    /// The two sides of `cmp` are of one type; `match` and `contains`
    /// take strings, and an ordering an ordered type.
    fn constrain_compare(&mut self, cmp: &ComparisonExpr) -> Result<(), ParseError> {
        let span = cmp.span();
        let op = cmp.operator().clone();
        if op.is_string_constraint() {
            for side in [cmp.left(), cmp.right()] {
                let term = self.term_of(side)?;
                if self
                    .solver
                    .unify(&term, &TypeTerm::Scalar(DataType::String))
                    .is_err()
                {
                    return Err(ParseError::ComparisonOpNotAllowed {
                        span,
                        op,
                        ty: self.solver.describe(&term, &DataType::String),
                    });
                }
            }
            return Ok(());
        }
        let left = self.operand_term(cmp.left())?;
        let right = self.operand_term(cmp.right())?;
        if self.solver.unify(&left, &right).is_err() {
            return Err(self.equality_error(cmp, &left, &right));
        }
        if op.is_ordering() && self.solver.constrain(&left, Kind::Ordered).is_err() {
            return Err(ParseError::ComparisonOpNotAllowed {
                span,
                op,
                ty: self.solver.describe(&left, &DataType::Int32),
            });
        }
        Ok(())
    }

    /// The error for the two sides of `cmp` not being of one type. A tuple
    /// literal beside `=` is a destructure: of a non-tuple, or of a tuple of
    /// another width, it is a [`ParseError::TupleDestructure`]; otherwise a
    /// field, or the sides themselves, disagree.
    fn equality_error(
        &self,
        cmp: &ComparisonExpr,
        left: &TypeTerm,
        right: &TypeTerm,
    ) -> ParseError {
        let span = cmp.span();
        let sides = [(cmp.left(), left, right), (cmp.right(), right, left)];
        for (side, _, other) in sides {
            let Some(pattern) = side.as_tuple() else {
                continue;
            };
            match self.solver.resolve(other) {
                Some(DataType::FixedTuple(fields)) if fields.len() != pattern.fields().len() => {
                    return ParseError::TupleDestructure {
                        span,
                        detail: format!(
                            "pattern has {} field(s) but the tuple has {}",
                            pattern.fields().len(),
                            fields.len()
                        ),
                    };
                }
                Some(DataType::FixedTuple(_)) | None => {}
                Some(other) => {
                    return ParseError::TupleDestructure {
                        span,
                        detail: format!("cannot destructure `{other}`, which is not a tuple"),
                    };
                }
            }
        }
        let shown = |ty: &TypeTerm, like: &TypeTerm| {
            let like = self.solver.describe(like, &DataType::Int32);
            self.solver.describe(ty, &like)
        };
        ParseError::ComparisonTypeMismatch {
            span,
            op: cmp.operator().clone(),
            left: shown(left, right),
            right: shown(right, left),
        }
    }

    /// The constraints of `aggregate`: its body's, then its target's, and
    /// its variable has its answer's type.
    fn constrain_aggregate(
        &mut self,
        aggregate: &AggregateExpr,
        binders: &HashSet<String>,
    ) -> Result<(), ParseError> {
        let mut inner = binders.clone();
        inner.extend(aggregate.bound_vars().into_iter().map(String::from));
        self.constrain_body(aggregate.body(), &inner)?;

        let span = aggregate.span();
        let op = aggregate.operator();
        let answer = match op {
            AggregationOperator::Count => self.solver.fresh(Kind::Integer),
            AggregationOperator::Mean => {
                self.numeric_target_term(aggregate)?;
                self.solver
                    .fresh_with_default(Kind::Float, Some(DataType::Float64))
            }
            AggregationOperator::Sum | AggregationOperator::Min | AggregationOperator::Max => {
                self.numeric_target_term(aggregate)?
            }
        };
        let Some(var) = aggregate.var() else {
            return Err(grammar_bug(format!(
                "aggregate `{aggregate}` in a rule body binds no variable"
            )));
        };
        let (term, first_span) = self.variable_term(var, span);
        if self.solver.unify(&term, &answer).is_err() {
            let later_ty = self.solver.describe(&answer, &DataType::Int32);
            return Err(ParseError::TypeMismatch {
                var: var.to_string(),
                first_ty: self.solver.describe(&term, &later_ty),
                first_span,
                later_ty,
                later_span: span,
            });
        }
        Ok(())
    }

    /// The term of `aggregate`'s target, which must be numeric.
    fn numeric_target_term(&mut self, aggregate: &AggregateExpr) -> Result<TypeTerm, ParseError> {
        let op = aggregate.operator();
        let Some(target) = aggregate.target() else {
            return Err(grammar_bug(format!(
                "`{op}` aggregate without a target survived lowering"
            )));
        };
        let term = self.term_of(target)?;
        if self.solver.constrain(&term, Kind::Numeric).is_err() {
            return Err(ParseError::AggregationInputNotNumeric {
                span: aggregate.span(),
                op,
                ty: self.solver.describe(&term, &DataType::Int32),
            });
        }
        Ok(term)
    }

    /// Each argument of `head` has its column's type.
    fn constrain_head(&mut self, head: &Head) -> Result<(), ParseError> {
        let rel = head.raw_name().to_string();
        let columns =
            self.decls.relations.get(head.name()).ok_or_else(|| {
                grammar_bug(format!("head relation `{}` not declared", head.name()))
            })?;
        if head.arity() != columns.len() {
            return Err(ParseError::HeadArity {
                span: head.span(),
                rel,
                expected: columns.len(),
                found: head.arity(),
            });
        }
        for (col, (arg, column)) in head.head_arguments().iter().zip(columns).enumerate() {
            let (term, span) = match arg {
                HeadArg::Var(var) => (self.variable_term(var, head.span()).0, head.span()),
                HeadArg::Arith(a) => (self.term_of(a)?, a.span()),
            };
            if self.solver.unify(&term, &TypeTerm::of(column)).is_ok() {
                continue;
            }
            if let HeadArg::Arith(a) = arg {
                if let (true, Factor::Const(constant)) = (a.is_const(), a.init()) {
                    return Err(ParseError::LiteralColumnMismatch {
                        span,
                        literal: constant.to_string(),
                        expected: column.clone(),
                    });
                }
                if let Some(lit) = a.as_tuple() {
                    return Err(self.tuple_construct_error(lit, &term, column));
                }
            }
            return Err(ParseError::HeadColumnType {
                span,
                rel,
                col,
                expected: column.clone(),
                found: self.solver.describe(&term, column),
            });
        }
        Ok(())
    }

    /// The error for the tuple literal `lit`, of term `term`, not fitting
    /// `column`: the wrong field count, a field that does not fit, or no
    /// tuple column at all.
    fn tuple_construct_error(
        &mut self,
        lit: &TupleLit,
        term: &TypeTerm,
        column: &DataType,
    ) -> ParseError {
        let DataType::FixedTuple(fields) = column else {
            return ParseError::TupleConstruct {
                span: lit.span(),
                detail: format!("a tuple literal does not fit `{column}`"),
            };
        };
        if fields.len() != lit.fields().len() {
            return ParseError::TupleConstruct {
                span: lit.span(),
                detail: format!(
                    "tuple has {} field(s) but {} value(s) were given",
                    fields.len(),
                    lit.fields().len()
                ),
            };
        }
        // The fields before the clash unified already, so re-unifying each
        // in order fails first at the one that does not fit.
        let TypeTerm::Tuple(terms) = term else {
            return grammar_bug(format!(
                "tuple literal `{lit}` has the non-tuple term {term:?}"
            ));
        };
        for ((elem, field_term), field) in lit.fields().iter().zip(terms).zip(fields) {
            if self.solver.unify(field_term, &TypeTerm::of(field)).is_err() {
                let span = match elem {
                    TupleElem::Expr(a) => a.span(),
                    TupleElem::Placeholder => lit.span(),
                };
                return ParseError::TupleConstruct {
                    span,
                    detail: format!(
                        "field value of type `{}` does not fit field type `{field}`",
                        self.solver.describe(field_term, field)
                    ),
                };
            }
        }
        grammar_bug(format!(
            "tuple literal `{lit}` clashed with `{column}` at no field"
        ))
    }

    /// The term of the variable `var`, and where the rule first mentions
    /// it: a fresh instance for a variable defined as a value with no
    /// variables, otherwise its one term, created unconstrained at `span`
    /// on first mention.
    fn variable_term(&mut self, var: &str, span: Span) -> (TypeTerm, Span) {
        if let Some(closed) = self.closed.get(var) {
            let closed = closed.clone();
            return (self.solver.instantiate(&closed), span);
        }
        let (term, first_span) = self
            .vars
            .entry(var.to_string())
            .or_insert_with(|| (self.solver.fresh(Kind::Any), span));
        (term.clone(), *first_span)
    }

    /// The term of a side of `=`: a tuple literal there is a pattern, whose
    /// `_` matches any field.
    fn operand_term(&mut self, a: &Arithmetic) -> Result<TypeTerm, ParseError> {
        match a.as_tuple() {
            Some(lit) => self.tuple_term(lit, true),
            None => self.term_of(a),
        }
    }

    /// The term of an expression: its factors are of one type, numeric
    /// across an arithmetic operator and integer across a bitwise one.
    fn term_of(&mut self, a: &Arithmetic) -> Result<TypeTerm, ParseError> {
        let span = a.span();
        let term = self.factor_term(a.init(), span)?;
        for (op, factor) in a.rest() {
            let next = self.factor_term(factor, span)?;
            if self.solver.unify(&term, &next).is_err() {
                let left = self.solver.describe(&term, &DataType::Int32);
                let right = self.solver.describe(&next, &left);
                return Err(ParseError::ArithmeticTypeMismatch { span, left, right });
            }
            let kind = if op.is_bitwise() {
                Kind::Integer
            } else {
                Kind::Numeric
            };
            if self.solver.constrain(&term, kind).is_err() {
                return Err(ParseError::ArithmeticOpNotAllowed {
                    span,
                    op: op.clone(),
                    ty: self.solver.describe(&term, &DataType::Int32),
                });
            }
        }
        Ok(term)
    }

    /// The term of a factor at `span`, the expression it is part of.
    fn factor_term(&mut self, factor: &Factor, span: Span) -> Result<TypeTerm, ParseError> {
        Ok(match factor {
            Factor::Var(var) => self.variable_term(var, span).0,
            Factor::Const(constant) => match constant.ty() {
                DataType::IntLit => self.solver.fresh(Kind::Integer),
                DataType::FloatLit => self.solver.fresh(Kind::Float),
                concrete => TypeTerm::of(concrete),
            },
            Factor::FnCall(call) => self.udf_call_term(call)?,
            Factor::Builtin(call) => self.builtin_call_term(call)?,
            // Inference sees through a cast; the subtype pass checks it.
            Factor::Cast(cast) => self.factor_term(cast.inner(), span)?,
            Factor::Group(a) => self.term_of(a)?,
            Factor::Tuple(lit) => self.tuple_term(lit, false)?,
            // Substitution, which follows inference, is the only source of
            // projections.
            Factor::TupleProj { .. } => {
                return Err(grammar_bug(format!(
                    "projection `{factor}` reached inference, which precedes substitution"
                )));
            }
            Factor::Aggregate(aggregate) => {
                return Err(grammar_bug(format!(
                    "aggregate `{aggregate}` was not hoisted out of its expression"
                )));
            }
        })
    }

    /// The term of a tuple literal. A `_` is a field of any type when the
    /// literal is a `pattern`, beside `=`; elsewhere the literal constructs
    /// a value and has no field to leave open.
    fn tuple_term(&mut self, lit: &TupleLit, pattern: bool) -> Result<TypeTerm, ParseError> {
        let mut fields = Vec::with_capacity(lit.fields().len());
        for elem in lit.fields() {
            fields.push(match elem {
                TupleElem::Expr(a) => self.term_of(a)?,
                TupleElem::Placeholder if pattern => self.solver.fresh(Kind::Any),
                TupleElem::Placeholder => {
                    return Err(ParseError::TuplePlaceholderInConstruct { span: lit.span() });
                }
            });
        }
        Ok(TypeTerm::Tuple(fields))
    }

    /// The term of a UDF call: each argument has its parameter's type, and
    /// the call its return type.
    fn udf_call_term(&mut self, call: &FnCall) -> Result<TypeTerm, ParseError> {
        let (params, ret) =
            self.decls
                .udfs
                .get(call.name())
                .ok_or_else(|| ParseError::UndeclaredUdf {
                    span: call.span(),
                    name: call.name().to_string(),
                })?;
        if call.args().len() != params.len() {
            return Err(ParseError::UdfArity {
                span: call.span(),
                name: call.name().to_string(),
                expected: params.len(),
                found: call.args().len(),
            });
        }
        for (arg, (param, expected)) in call.args().iter().zip(params) {
            let term = self.term_of(arg)?;
            if self.solver.unify(&term, &TypeTerm::of(expected)).is_err() {
                return Err(ParseError::UdfArgType {
                    span: arg.span(),
                    name: call.name().to_string(),
                    param: param.clone(),
                    expected: expected.clone(),
                    found: self.solver.describe(&term, expected),
                });
            }
        }
        Ok(TypeTerm::of(ret))
    }

    /// The term of a built-in call: each argument has its parameter's
    /// type, or, for a parameter of several allowed types, one of them;
    /// the call has the operator's return type.
    fn builtin_call_term(&mut self, call: &BuiltinCall) -> Result<TypeTerm, ParseError> {
        let op = call.op();
        for (index, (arg, allowed)) in call.args().iter().zip(op.param_allowed_types()).enumerate()
        {
            let term = self.term_of(arg)?;
            let fits = match allowed {
                [only] => self.solver.unify(&term, &TypeTerm::of(only)).is_ok(),
                _ => self.solver.admits_any(&term, allowed),
            };
            if !fits {
                return Err(ParseError::BuiltinArgType {
                    span: arg.span(),
                    op,
                    arg_index: index,
                    expected: allowed.to_vec(),
                    found: self.solver.describe(&term, &DataType::Int32),
                });
            }
        }
        Ok(TypeTerm::of(&op.ret_type()))
    }
}

/// The definitions in `body` of a variable as a value with no variables but
/// those of such definitions, as `(index, variable, value)`, in an order
/// that defines each before the values that read it: `t = 1` before
/// `u = t * 2`. `closed` holds the variables the enclosing bodies so
/// define. A variable `binders` bind, a variable its own value reads, and
/// a value with a placeholder define nothing this way.
fn closed_definitions<'a>(
    body: &'a [Predicate],
    binders: &HashSet<String>,
    closed: &HashMap<String, TypeTerm>,
) -> Vec<(usize, &'a str, &'a Arithmetic)> {
    let mut defined: HashSet<&str> = closed.keys().map(String::as_str).collect();
    let mut definitions: Vec<(usize, &str, &Arithmetic)> = Vec::new();
    loop {
        let mut progressed = false;
        for (index, predicate) in body.iter().enumerate() {
            let Predicate::Compare(cmp) = predicate else {
                continue;
            };
            if *cmp.operator() != ComparisonOperator::Equal
                || definitions.iter().any(|(i, ..)| *i == index)
            {
                continue;
            }
            let sides = [(cmp.left(), cmp.right()), (cmp.right(), cmp.left())];
            let Some((var, value)) = sides.into_iter().find_map(|(var_side, value)| {
                if !var_side.is_var() {
                    return None;
                }
                let var = var_side.vars()[0].as_str();
                let value_vars = value.vars();
                let defines = !binders.contains(var)
                    && !defined.contains(var)
                    && !value.has_placeholder()
                    && value_vars
                        .iter()
                        .all(|v| *v != var && defined.contains(v.as_str()));
                defines.then_some((var, value))
            }) else {
                continue;
            };
            defined.insert(var);
            definitions.push((index, var, value));
            progressed = true;
        }
        if !progressed {
            return definitions;
        }
    }
}

// =============================================================================
// Facts and `ord`
// =============================================================================

/// Each column of each fact of `rel_name` fits its `.decl`.
fn check_facts(
    rel_name: &str,
    facts: &[InlineFact],
    decls: &Declarations,
) -> Result<(), ParseError> {
    let Some(columns) = decls.relations.get(rel_name) else {
        return Err(grammar_bug(format!(
            "fact references undeclared relation `{rel_name}`"
        )));
    };
    for fact in facts {
        if fact.columns.len() != columns.len() {
            return Err(ParseError::HeadArity {
                span: fact.span,
                rel: fact.raw_name.clone(),
                expected: columns.len(),
                found: fact.columns.len(),
            });
        }
        for (constant, column) in fact.columns.iter().zip(columns) {
            if !constant.ty().fits(column) {
                return Err(ParseError::LiteralColumnMismatch {
                    span: fact.span,
                    literal: constant.to_string(),
                    expected: column.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Check `ord(_)` usage across every rule: reject it without `--str-intern`
/// ([`ParseError::OrdRequiresStrIntern`]), otherwise set
/// [`Config::serialize_load`] so the loader interns serially and `ord`
/// stays deterministic across worker counts.
fn check_ord(program: &Program, config: &mut Config) -> Result<(), ParseError> {
    fn arith(a: &Arithmetic) -> Option<Span> {
        factor(a.init()).or_else(|| a.rest().iter().find_map(|(_, f)| factor(f)))
    }
    fn factor(f: &Factor) -> Option<Span> {
        match f {
            Factor::Var(_) | Factor::Const(_) => None,
            Factor::FnCall(fc) => fc.args().iter().find_map(arith),
            Factor::Builtin(bc) if bc.op() == BuiltinOperator::Ord => Some(bc.span()),
            Factor::Builtin(bc) => bc.args().iter().find_map(arith),
            Factor::Cast(c) => factor(c.inner()),
            Factor::Group(a) => arith(a),
            Factor::Tuple(r) => r.exprs().find_map(arith),
            Factor::TupleProj { tuple, .. } => arith(tuple),
            // Rule lowering hoisted every aggregate into a binding.
            Factor::Aggregate(_) => None,
        }
    }
    fn body(predicates: &[Predicate]) -> Option<Span> {
        predicates.iter().find_map(|predicate| match predicate {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
            Predicate::Compare(cmp) => arith(cmp.left()).or_else(|| arith(cmp.right())),
            Predicate::Aggregate(aggregate) => aggregate
                .target()
                .and_then(arith)
                .or_else(|| body(aggregate.body())),
        })
    }

    let ord_span = program.rules().iter().find_map(|rule| {
        body(rule.rhs()).or_else(|| {
            rule.head()
                .head_arguments()
                .iter()
                .find_map(|head_arg| match head_arg {
                    HeadArg::Var(_) => None,
                    HeadArg::Arith(a) => arith(a),
                })
        })
    });

    if let Some(span) = ord_span {
        if !config.str_intern_enabled() {
            return Err(ParseError::OrdRequiresStrIntern { span });
        }
        config.serialize_load = true;
    }
    Ok(())
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

    // --- Atoms ---

    /// A body atom whose argument count disagrees with its `.decl` is a user
    /// error, whichever way it is wrong and wherever the atom sits: too few
    /// arguments must not type-check, and too many must not surface as an
    /// internal error.
    // Cases: body, (expected, found).
    #[rstest]
    #[case("Edge(x)", (2, 1))]
    #[case("Edge(x, y, z)", (2, 3))]
    #[case("Edge(x, y), !Edge(y)", (2, 1))]
    #[case("Edge(x, y), !Edge(y, x, x)", (2, 3))]
    #[case("Edge(x, y), n = count : { Edge(x) }", (2, 1))]
    fn body_atom_arity_mismatch_is_rejected(
        #[case] body: &str,
        #[case] (expected, found): (usize, usize),
    ) {
        let src = format!(
            ".decl Edge(a: int32, b: int32)\n\
             .decl Out(a: int32)\n\
             .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(x) :- {body}.\n"
        );
        match checked(&src) {
            Err(ParseError::AtomArity {
                rel,
                expected: e,
                found: f,
                ..
            }) => assert_eq!((rel.as_str(), e, f), ("Edge", expected, found)),
            other => panic!("expected AtomArity, got {other:?}"),
        }
    }

    /// A variable bound by two atoms must have one type; the error cites
    /// both atoms.
    #[test]
    fn a_variable_bound_at_two_types_is_rejected() {
        let src = "\
            .decl A(x: int16)\n\
            .decl B(x: int32)\n\
            .decl Out(x: int16)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x), B(x).\n";
        match checked(src) {
            Err(ParseError::TypeMismatch {
                var,
                first_ty,
                first_span,
                later_ty,
                later_span,
            }) => {
                assert_eq!(
                    (var.as_str(), first_ty, later_ty),
                    ("x", DataType::Int16, DataType::Int32)
                );
                assert_eq!(&src[first_span.range()], "A(x)");
                assert_eq!(&src[later_span.range()], "B(x)");
            }
            other => panic!("expected TypeMismatch, got {other:?}"),
        }
    }

    // --- Expressions ---

    /// Arithmetic mixing an integer and a float (`int32 + 5.0`) is rejected.
    #[test]
    fn arithmetic_mixing_int_and_float_rejected() {
        let src = "\
            .decl Score(pts: int32)\n\
            .decl Out(v: int32)\n\
            .input Score(IO=\"file\", filename=\"Score.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(pts + 5.0) :- Score(pts).\n";
        assert_err!(checked(src), ParseError::ArithmeticTypeMismatch { .. });
    }

    /// Numeric operators take numbers and bitwise operators integers;
    /// strings, booleans, and floats under a bitwise operator are rejected.
    // Cases: column type, operator, accepted.
    #[rstest]
    #[case("int32", "+", true)]
    #[case("f64", "*", true)]
    #[case("f64", "^", true)]
    #[case("int32", "band", true)]
    #[case("uint64", "bshru", true)]
    #[case("f64", "band", false)]
    #[case("f32", "bshl", false)]
    #[case("symbol", "+", false)]
    #[case("symbol", "bor", false)]
    #[case("bool", "+", false)]
    #[case("bool", "*", false)]
    fn an_arithmetic_operator_takes_the_types_it_is_defined_on(
        #[case] ty: &str,
        #[case] op: &str,
        #[case] accepted: bool,
    ) {
        let src = format!(
            ".decl In(x: {ty})\n\
             .decl Out(v: {ty})\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(x {op} x) :- In(x).\n"
        );
        match checked(&src) {
            Ok(_) => assert!(accepted, "`{ty} {op} {ty}` must be rejected"),
            Err(ParseError::ArithmeticOpNotAllowed { .. }) => {
                assert!(!accepted, "`{ty} {op} {ty}` must be accepted");
            }
            other => panic!("expected Ok or ArithmeticOpNotAllowed, got {other:?}"),
        }
    }

    /// A call to a function with no `.extern fn` declaration is rejected.
    #[test]
    fn undeclared_udf_call_rejected() {
        let src = "\
            .decl In(x: int32)\n\
            .decl Out(v: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(f(x)) :- In(x).\n";
        assert_err!(checked(src), ParseError::UndeclaredUdf { .. });
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
        assert_err!(checked(src), ParseError::ArithmeticOpNotAllowed { .. });
    }

    /// `ord()` needs `--str-intern`; the gate rejects it in a plain rule and
    /// inside an aggregate alike.
    // Cases: rule.
    #[rstest]
    #[case("Out(ord(s)) :- In(s).")]
    #[case("Out(n) :- In(s), n = count : { In(t), ord(t) > 0 }.")]
    fn ord_requires_str_intern(#[case] rule: &str) {
        let src = format!(
            ".decl In(s: symbol)\n\
             .decl Out(n: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        assert_err!(checked(&src), ParseError::OrdRequiresStrIntern { .. });
    }

    // --- Tuples ---

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

    /// A variable constructed as a tuple of the wrong width or field type,
    /// or against a scalar column, is a head type error naming both types.
    // Cases: declarations, rule, (expected, found).
    #[rstest]
    #[case(
        ".type Pair = ( a: symbol, b: symbol )\n.decl In(x: symbol, y: symbol, z: symbol)\n.decl Out(p: Pair)",
        "Out(p) :- In(x, y, z), p = (x, y, z).",
        (
            DataType::FixedTuple(vec![DataType::String, DataType::String]),
            DataType::FixedTuple(vec![DataType::String, DataType::String, DataType::String])
        )
    )]
    #[case(
        ".type Tv = ( t: symbol, v: number )\n.decl In(s: symbol, n: symbol)\n.decl Out(p: Tv)",
        "Out(p) :- In(s, n), p = (s, n).",
        (
            DataType::FixedTuple(vec![DataType::String, DataType::Int32]),
            DataType::FixedTuple(vec![DataType::String, DataType::String])
        )
    )]
    #[case(
        ".decl In(x: symbol, y: symbol)\n.decl Out(p: symbol)",
        "Out(p) :- In(x, y), p = (x, y).",
        (
            DataType::String,
            DataType::FixedTuple(vec![DataType::String, DataType::String])
        )
    )]
    fn a_tuple_variable_must_fit_its_column(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] (expected, found): (DataType, DataType),
    ) {
        let src = format!(
            "{decls}\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        match checked(&src) {
            Err(ParseError::HeadColumnType {
                expected: e,
                found: f,
                ..
            }) => assert_eq!((e, f), (expected, found)),
            other => panic!("expected HeadColumnType, got {other:?}"),
        }
    }

    /// A tuple literal in the head is rejected with the wrong number of
    /// fields, a field of the wrong type, or against a scalar column, each
    /// named.
    // Cases: declarations, rule, detail of the error.
    #[rstest]
    #[case(
        ".type Pair = ( a: symbol, b: symbol )\n.decl In(x: symbol, y: symbol, z: symbol)\n.decl Out(p: Pair)",
        "Out((x, y, z)) :- In(x, y, z).",
        "tuple has 2 field(s) but 3 value(s) were given"
    )]
    #[case(
        ".type Tv = ( t: symbol, v: number )\n.decl In(s: symbol, n: symbol)\n.decl Out(p: Tv)",
        "Out((s, n)) :- In(s, n).",
        "field value of type `string` does not fit field type `int32`"
    )]
    #[case(
        ".decl In(x: symbol, y: symbol)\n.decl Out(p: symbol)",
        "Out((x, y)) :- In(x, y).",
        "a tuple literal does not fit `string`"
    )]
    fn a_tuple_construct_must_fit_its_column(
        #[case] decls: &str,
        #[case] rule: &str,
        #[case] detail: &str,
    ) {
        let src = format!(
            "{decls}\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             {rule}\n"
        );
        match checked(&src) {
            Err(ParseError::TupleConstruct { detail: found, .. }) => assert_eq!(found, detail),
            other => panic!("expected TupleConstruct, got {other:?}"),
        }
    }

    /// `_` constructs nothing: a placeholder outside a destructure's pattern
    /// is rejected.
    #[test]
    fn a_placeholder_in_a_construct_is_rejected() {
        let src = "\
            .type Pair = ( a: symbol, b: symbol )\n\
            .decl In(x: symbol)\n\
            .decl Out(p: Pair)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out((x, _)) :- In(x).\n";
        assert_err!(checked(src), ParseError::TuplePlaceholderInConstruct { .. });
    }

    /// A program around `rule`: `In(x, y)` feeds `P` with `(x, y)` tuples of
    /// two `int16` fields, and `Out(a: int16)` is the output.
    fn pairs(rule: &str) -> String {
        format!(
            ".type Pair = ( a: int16, b: int16 )\n\
             .decl In(x: int16, y: int16)\n\
             .decl P(p: Pair)\n\
             .decl Out(a: int16)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             P(p) :- In(x, y), p = (x, y).\n\
             {rule}\n"
        )
    }

    /// A destructure's pattern must have exactly its tuple's field count:
    /// wider or narrower, on either side of `=`, with its components used,
    /// unused, or `_`, inside an aggregate, and against a tuple an
    /// assignment constructs.
    // Cases: rule, pattern width.
    #[rstest]
    #[case("Out(c) :- P(p), p = (a, b, c).", 3)]
    #[case("Out(a) :- P(p), (a, b, c) = p.", 3)]
    #[case("Out(a) :- P(p), (a, b, _) = p.", 3)]
    #[case("Out(1) :- P(p), (_, _, _) = p.", 3)]
    #[case("Out(a) :- P(p), (a,) = p.", 1)]
    #[case("Out(1) :- P(p), (_,) = p.", 1)]
    #[case("Out(n) :- n = count : { P(p), (a, b, c) = p }.", 3)]
    #[case("Out(a) :- In(x, y), q = (x, y), (a, b, c) = q.", 3)]
    fn pattern_width_must_equal_the_tuple_width(#[case] rule: &str, #[case] width: usize) {
        match checked(&pairs(rule)) {
            Err(ParseError::TupleDestructure { detail, .. }) => assert_eq!(
                detail,
                format!("pattern has {width} field(s) but the tuple has 2")
            ),
            other => panic!("expected TupleDestructure, got {other:?}"),
        }
    }

    /// Destructuring a non-tuple is rejected, even with a pattern of
    /// placeholders only.
    // Cases: rule.
    #[rstest]
    #[case("Out(a) :- In(x, y), x = (a, b).")]
    #[case("Out(x) :- In(x, y), x = (_,).")]
    fn destructuring_a_non_tuple_is_rejected(#[case] rule: &str) {
        match checked(&pairs(rule)) {
            Err(ParseError::TupleDestructure { detail, .. }) => {
                assert_eq!(detail, "cannot destructure `int16`, which is not a tuple");
            }
            other => panic!("expected TupleDestructure, got {other:?}"),
        }
    }

    /// The error points at the destructure in the source.
    #[test]
    fn destructure_error_spans_the_comparison() {
        let src = pairs("Out(a) :- P(p), (a, b, c) = p.");
        let pattern = "(a, b, c) = p";
        let start = src.find(pattern).expect("pattern is in the source");
        match checked(&src) {
            Err(ParseError::TupleDestructure { span, .. }) => assert_eq!(
                (span.start() as usize, span.end() as usize),
                (start, start + pattern.len())
            ),
            other => panic!("expected TupleDestructure, got {other:?}"),
        }
    }

    /// A fresh component takes its field's type, so a head column of
    /// another width rejects it; through another destructure's component
    /// too, whichever order the body writes them in.
    // Cases: body.
    #[rstest]
    #[case("P(p), (a, _) = p")]
    #[case("O(o), (x, inner) = o, (a, b) = inner")]
    #[case("O(o), (a, b) = inner, (x, inner) = o")]
    fn a_fresh_component_takes_its_field_type(#[case] body: &str) {
        let src = format!(
            ".type Pair = ( a: int16, b: int16 )\n\
             .type Outer = ( x: int32, inner: Pair )\n\
             .decl In(x: int32, a: int16, b: int16)\n\
             .decl P(p: Pair)\n\
             .decl O(o: Outer)\n\
             .decl Out(a: int32)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             P(p) :- In(x, a, b), p = (a, b).\n\
             O(o) :- In(x, a, b), o = (x, (a, b)).\n\
             Out(a) :- {body}.\n"
        );
        match checked(&src) {
            Err(ParseError::HeadColumnType {
                expected, found, ..
            }) => assert_eq!((expected, found), (DataType::Int32, DataType::Int16)),
            other => panic!("expected HeadColumnType, got {other:?}"),
        }
    }

    /// A component of another type family than its field is rejected; the
    /// error shows both tuples, so the field stands out.
    #[test]
    fn a_component_of_another_family_is_rejected() {
        match checked(&pairs("Out(a) :- P(p), (a, 1.5) = p.")) {
            Err(ParseError::ComparisonTypeMismatch { left, right, .. }) => assert_eq!(
                (left, right),
                (
                    DataType::FixedTuple(vec![DataType::Int16, DataType::Float32]),
                    DataType::FixedTuple(vec![DataType::Int16, DataType::Int16])
                )
            ),
            other => panic!("expected ComparisonTypeMismatch, got {other:?}"),
        }
    }

    /// Two tuple literals beside `=` unify field by field, so a pattern
    /// against a construct defines the pattern's fresh components.
    #[test]
    fn a_pattern_against_a_construct_defines_its_components() {
        checked(&pairs("Out(a) :- In(x, y), (a, b) = (x, y).")).expect("type-checks");
    }

    // --- Definitions ---

    /// A variable defined as a value with no variables is typed per use,
    /// so it fills columns of two widths, directly or through another such
    /// definition; a variable defined over a typed variable has that one
    /// type, which a column of another width rejects.
    // Cases: definitions, accepted.
    #[rstest]
    #[case("t = 1", true)]
    #[case("t = 1 + 2", true)]
    #[case("u = 1, t = u * 2", true)]
    #[case("t = u * 2, u = 1", true)]
    #[case("t = x + 1", false)]
    #[case("u = x, t = u + 1", false)]
    fn only_a_definition_with_no_variables_fills_columns_of_two_widths(
        #[case] definitions: &str,
        #[case] accepted: bool,
    ) {
        let src = format!(
            ".decl In(x: int16)\n\
             .decl Out(a: int16, b: int64)\n\
             .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
             .output Out\n\
             Out(t, t) :- In(x), {definitions}.\n"
        );
        match checked(&src) {
            Ok(_) => assert!(accepted, "`{definitions}` must be rejected"),
            Err(ParseError::HeadColumnType {
                expected, found, ..
            }) => {
                assert!(!accepted, "`{definitions}` must be accepted");
                assert_eq!((expected, found), (DataType::Int64, DataType::Int16));
            }
            other => panic!("expected Ok or HeadColumnType, got {other:?}"),
        }
    }

    /// An equality of a variable an atom binds is a test, not a definition,
    /// so its literal takes the variable's type.
    #[test]
    fn an_equality_of_a_bound_variable_is_a_test() {
        let src = "\
            .decl In(x: int16)\n\
            .decl Out(a: int16)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- In(x), x = \"a\".\n";
        assert_err!(checked(src), ParseError::ComparisonTypeMismatch { .. });
    }

    /// A polymorphic numeric literal in a tuple field whose declared width is
    /// not the family default (here `int64`) must be accepted: the same
    /// leniency a scalar `int64` column gets.
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

    /// `count` and `mean` take the width a use asks for, like a literal,
    /// and default when no use asks; the other operators answer in their
    /// target's type.
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
    /// directly or through a definition, else its default.
    // Cases: declaration of Out, rule, outer type, nested type.
    #[rstest]
    #[case(
        ".decl Out(x: int16, s: int64)",
        "Out(x, s) :- Node(x), s = sum count : Edge(y, _) : Edge(x, y).",
        DataType::Int64,
        DataType::Int64
    )]
    #[case(
        ".decl Out(x: int16, s: int64)",
        "Out(x, s) :- Node(x), s = sum t : { Edge(x, y), c = count : Edge(y, _), t = c + 1 }.",
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
