//! Arithmetic expression signatures for FlowLog Datalog programs.

use std::fmt;

use flowlog_parser::Arithmetic;
use flowlog_parser::ArithmeticOperator;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::Constant;
use flowlog_parser::Factor;
use flowlog_parser::TupleElem;

use crate::catalog::AtomArgumentSignature;
use crate::catalog::CatalogError;

/// A factor in an arithmetic expression with variables resolved to their
/// concrete positions within atoms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FactorPos {
    /// A variable reference identified by its atom and argument position.
    Var(AtomArgumentSignature),
    Const(Constant),
    FnCall {
        name: String,
        args: Vec<ArithmeticPos>,
    },
    /// An engine built-in call (Souffle-style intrinsic).
    Builtin {
        op: BuiltinOperator,
        args: Vec<ArithmeticPos>,
    },
    /// Parenthesised sub-expression, preserving its grouping.
    Group(Box<ArithmeticPos>),
    /// Tuple construction `(e0, e1, ...)` (one column). Only constructs
    /// reach here; destructures become [`FactorPos::TupleProj`] first.
    Tuple {
        fields: Vec<ArithmeticPos>,
    },
    /// Projection of tuple component `index` (`tuple.index`).
    TupleProj {
        tuple: Box<ArithmeticPos>,
        index: usize,
    },
}

impl FactorPos {
    /// Returns the argument signature if this factor is a single variable.
    pub(crate) fn as_var_signature(&self) -> Option<&AtomArgumentSignature> {
        match self {
            FactorPos::Var(atom_arg_signature) => Some(atom_arg_signature),
            FactorPos::Const(_)
            | FactorPos::FnCall { .. }
            | FactorPos::Builtin { .. }
            | FactorPos::Group(_)
            | FactorPos::Tuple { .. }
            | FactorPos::TupleProj { .. } => None,
        }
    }

    /// Returns all argument signatures referenced in this factor
    /// (including nested in FnCall / Builtin / Tuple / TupleProj args).
    pub(crate) fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        match self {
            FactorPos::Var(sig) => vec![sig],
            FactorPos::Const(_) => vec![],
            FactorPos::FnCall { args, .. } | FactorPos::Builtin { args, .. } => {
                args.iter().flat_map(|a| a.signatures()).collect()
            }
            FactorPos::Group(a) => a.signatures(),
            FactorPos::Tuple { fields } => fields.iter().flat_map(|a| a.signatures()).collect(),
            FactorPos::TupleProj { tuple, .. } => tuple.signatures(),
        }
    }

    /// Transforms every variable in this factor using `f`, recursing into
    /// FnCall / Builtin / Tuple / TupleProj args.
    pub(crate) fn map_vars(&self, f: &impl Fn(&AtomArgumentSignature) -> FactorPos) -> FactorPos {
        match self {
            FactorPos::Var(sig) => f(sig),
            FactorPos::Const(c) => FactorPos::Const(c.clone()),
            FactorPos::FnCall { name, args } => FactorPos::FnCall {
                name: name.clone(),
                args: args.iter().map(|a| a.map_vars(f)).collect(),
            },
            FactorPos::Builtin { op, args } => FactorPos::Builtin {
                op: *op,
                args: args.iter().map(|a| a.map_vars(f)).collect(),
            },
            FactorPos::Group(a) => FactorPos::Group(Box::new(a.map_vars(f))),
            FactorPos::Tuple { fields } => FactorPos::Tuple {
                fields: fields.iter().map(|a| a.map_vars(f)).collect(),
            },
            FactorPos::TupleProj { tuple, index } => FactorPos::TupleProj {
                tuple: Box::new(tuple.map_vars(f)),
                index: *index,
            },
        }
    }
}

impl fmt::Display for FactorPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FactorPos::Var(sig) => write!(f, "{sig}"),
            FactorPos::Const(c) => write!(f, "{c}"),
            FactorPos::FnCall { name, args } => {
                let args_str = args
                    .iter()
                    .map(ArithmeticPos::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{name}({args_str})")
            }
            FactorPos::Builtin { op, args } => {
                let args_str = args
                    .iter()
                    .map(ArithmeticPos::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{op}({args_str})")
            }
            FactorPos::Group(a) => write!(f, "({a})"),
            FactorPos::Tuple { fields } => {
                let inner = fields
                    .iter()
                    .map(ArithmeticPos::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{inner}]")
            }
            FactorPos::TupleProj { tuple, index } => write!(f, "({tuple}).{index}"),
        }
    }
}

/// Positional arithmetic expression with variables resolved to their
/// concrete argument signatures.
#[derive(Clone, Hash, PartialEq, Eq, Debug)]
pub(crate) struct ArithmeticPos {
    /// The left-most factor.
    init: FactorPos,
    /// Subsequent `(operator, right operand)` steps, applied left to right.
    rest: Vec<(ArithmeticOperator, FactorPos)>,
}

impl ArithmeticPos {
    pub(crate) fn new(init: FactorPos, rest: Vec<(ArithmeticOperator, FactorPos)>) -> Self {
        Self { init, rest }
    }

    pub(crate) fn from_var_signature(signature: AtomArgumentSignature) -> Self {
        ArithmeticPos {
            init: FactorPos::Var(signature),
            rest: vec![],
        }
    }

    /// Resolves a parsed expression's variables against `var_signatures`,
    /// consumed in source order.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the signature count does not match the
    /// expression or a tuple placeholder survived parser lowering.
    pub(crate) fn from_arithmetic(
        arith: &Arithmetic,
        var_signatures: &[AtomArgumentSignature],
    ) -> Result<Self, CatalogError> {
        fn take_signatures<'a>(
            var_signatures: &'a [AtomArgumentSignature],
            var_id: &mut usize,
            count: usize,
        ) -> Result<&'a [AtomArgumentSignature], CatalogError> {
            let start = *var_id;
            let end = start.checked_add(count).ok_or_else(|| {
                CatalogError::internal(format!(
                    "arithmetic signature range overflowed: start={start}, count={count}"
                ))
            })?;
            let signatures = var_signatures.get(start..end).ok_or_else(|| {
                CatalogError::internal(format!(
                    "arithmetic needs signatures {start}..{end}, but only {} exist",
                    var_signatures.len()
                ))
            })?;
            *var_id = end;
            Ok(signatures)
        }

        fn map_factor(
            factor: &Factor,
            var_signatures: &[AtomArgumentSignature],
            var_id: &mut usize,
        ) -> Result<FactorPos, CatalogError> {
            // Both `FnCall` and `Builtin` carry their args as
            // `Vec<Arithmetic>` and consume `num_vars` signatures per arg
            // in source order. Walk once, reuse for both arms.
            let map_call_args = |args: &[Arithmetic],
                                 var_id: &mut usize|
             -> Result<Vec<ArithmeticPos>, CatalogError> {
                args.iter()
                    .map(|arg| {
                        let num_vars = arg.vars().len();
                        let sigs = take_signatures(var_signatures, var_id, num_vars)?;
                        ArithmeticPos::from_arithmetic(arg, sigs)
                    })
                    .collect()
            };
            Ok(match factor {
                Factor::Var(_) => {
                    let sig = take_signatures(var_signatures, var_id, 1)?[0];
                    FactorPos::Var(sig)
                }
                Factor::Const(c) => FactorPos::Const(c.clone()),
                Factor::FnCall(fc) => FactorPos::FnCall {
                    name: fc.name().to_string(),
                    args: map_call_args(fc.args(), var_id)?,
                },
                Factor::Builtin(bc) => FactorPos::Builtin {
                    op: bc.op(),
                    args: map_call_args(bc.args(), var_id)?,
                },
                // Cast is identity at runtime: strip the wrapper and
                // map the inner factor directly. The typechecker has
                // already validated subtype compatibility by this point.
                Factor::Cast(c) => return map_factor(c.inner(), var_signatures, var_id),
                Factor::Group(a) => {
                    let num_vars = a.vars().len();
                    let sigs = take_signatures(var_signatures, var_id, num_vars)?;
                    FactorPos::Group(Box::new(ArithmeticPos::from_arithmetic(a, sigs)?))
                }
                // Tuple fields consume signatures in source order, like call
                // arguments. Destructure placeholders become projections before
                // catalog construction.
                Factor::Tuple(r) => {
                    let fields = r
                        .fields()
                        .iter()
                        .map(|elem| -> Result<ArithmeticPos, CatalogError> {
                            match elem {
                                TupleElem::Expr(a) => {
                                    let num_vars = a.vars().len();
                                    let sigs = take_signatures(var_signatures, var_id, num_vars)?;
                                    ArithmeticPos::from_arithmetic(a, sigs)
                                }
                                TupleElem::Placeholder => Err(CatalogError::internal(
                                    "tuple placeholder reached catalog after parser lowering",
                                )),
                            }
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    FactorPos::Tuple { fields }
                }
                Factor::TupleProj { tuple, index } => {
                    let num_vars = tuple.vars().len();
                    let sigs = take_signatures(var_signatures, var_id, num_vars)?;
                    FactorPos::TupleProj {
                        tuple: Box::new(ArithmeticPos::from_arithmetic(tuple, sigs)?),
                        index: *index,
                    }
                }
            })
        }

        let mut var_id = 0usize;

        let init = map_factor(arith.init(), var_signatures, &mut var_id)?;
        let rest = arith
            .rest()
            .iter()
            .map(|(op, factor)| Ok((op.clone(), map_factor(factor, var_signatures, &mut var_id)?)))
            .collect::<Result<Vec<_>, CatalogError>>()?;
        if var_id != var_signatures.len() {
            return Err(CatalogError::internal(format!(
                "arithmetic consumed {var_id} of {} variable signatures",
                var_signatures.len()
            )));
        }

        Ok(ArithmeticPos { init, rest })
    }

    /// Returns the initial (left-most) factor.
    #[inline]
    pub(crate) fn init(&self) -> &FactorPos {
        &self.init
    }

    /// Returns the sequence of operators and factors after the initial term.
    #[inline]
    pub(crate) fn rest(&self) -> &[(ArithmeticOperator, FactorPos)] {
        &self.rest
    }

    /// The signature of a bare variable reference, a plain column selectable
    /// by index, or `None` for a *computed* value (projection, builtin,
    /// arithmetic, ...) that must be materialized.
    #[inline]
    pub(crate) fn plain_var(&self) -> Option<AtomArgumentSignature> {
        if self.rest.is_empty() {
            self.init.as_var_signature().copied()
        } else {
            None
        }
    }

    /// Returns all variable signatures referenced, in source order.
    pub(crate) fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        let mut sigs = self.init.signatures();
        for (_, factor) in &self.rest {
            sigs.extend(factor.signatures());
        }
        sigs
    }

    /// Transforms every variable in this expression using `f`, recursing
    /// into call arguments.
    pub(crate) fn map_vars(
        &self,
        f: &impl Fn(&AtomArgumentSignature) -> FactorPos,
    ) -> ArithmeticPos {
        let init = self.init.map_vars(f);
        let rest = self
            .rest
            .iter()
            .map(|(op, factor)| (op.clone(), factor.map_vars(f)))
            .collect();
        ArithmeticPos::new(init, rest)
    }
}

impl fmt::Display for ArithmeticPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.init)?;
        for (op, factor) in &self.rest {
            write!(f, " {op} {factor}")?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_error::SourceMap;
    use flowlog_parser::DataType;
    use flowlog_parser::Predicate;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::catalog::AtomSignature;

    fn parsed_left_arithmetic(left: &str) -> Arithmetic {
        // Arithmetic constructors are private to flowlog-parser, so use its
        // public pipeline to obtain a checked expression.
        let source = format!(
            "\
.extern fn combine(a: int32, b: int32) -> int32
.decl A(a: int32, b: int32, c: int32)
.decl Out()
.input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")
.output Out
Out() :- A(x, y, z), {left} > 0.
"
        );
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(source.as_bytes()).expect("write source");
        let mut source_map = SourceMap::new();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut source_map,
            &mut Config::default(),
        )
        .expect("parse rule");
        program.rules()[0]
            .rhs()
            .iter()
            .find_map(|predicate| match predicate {
                Predicate::Compare(comparison) => Some(comparison.left().clone()),
                Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
            })
            .expect("comparison expression")
    }

    #[test]
    fn from_arithmetic_consumes_nested_variables_in_source_order() {
        let arithmetic = parsed_left_arithmetic("combine(x, y + z)");
        let atom = AtomSignature::new(true, 0);
        let x = AtomArgumentSignature::new(atom, 0);
        let y = AtomArgumentSignature::new(atom, 1);
        let z = AtomArgumentSignature::new(atom, 2);

        let positional =
            ArithmeticPos::from_arithmetic(&arithmetic, &[x, y, z]).expect("matching signatures");

        assert_eq!(
            positional,
            ArithmeticPos::new(
                FactorPos::FnCall {
                    name: "combine".into(),
                    args: vec![
                        ArithmeticPos::from_var_signature(x),
                        ArithmeticPos::new(
                            FactorPos::Var(y),
                            vec![(ArithmeticOperator::Plus, FactorPos::Var(z))],
                        ),
                    ],
                },
                Vec::new(),
            )
        );
    }

    #[test]
    fn from_arithmetic_rejects_signature_count_mismatch_in_both_directions() {
        let arithmetic = parsed_left_arithmetic("combine(x, y + z)");
        let atom = AtomSignature::new(true, 0);
        let signatures = [
            AtomArgumentSignature::new(atom, 0),
            AtomArgumentSignature::new(atom, 1),
            AtomArgumentSignature::new(atom, 2),
            AtomArgumentSignature::new(atom, 3),
        ];

        let too_few = ArithmeticPos::from_arithmetic(&arithmetic, &signatures[..2])
            .expect_err("two signatures cannot cover three variables");
        assert_eq!(
            too_few.to_string(),
            "internal compiler error at stage `catalog`: arithmetic needs signatures 1..3, but \
             only 2 exist"
        );

        let too_many = ArithmeticPos::from_arithmetic(&arithmetic, &signatures)
            .expect_err("four signatures cannot map three variables");
        assert_eq!(
            too_many.to_string(),
            "internal compiler error at stage `catalog`: arithmetic consumed 3 of 4 variable \
             signatures"
        );
    }

    #[test]
    fn plain_var_accepts_only_an_uncomputed_variable() {
        let signature = AtomArgumentSignature::new(AtomSignature::new(true, 0), 1);
        let direct = ArithmeticPos::from_var_signature(signature);
        let computed = ArithmeticPos::new(
            FactorPos::Var(signature),
            vec![(
                ArithmeticOperator::Plus,
                FactorPos::Const(Constant::new(DataType::Int32, "1")),
            )],
        );
        let projected = ArithmeticPos::new(
            FactorPos::TupleProj {
                tuple: Box::new(direct.clone()),
                index: 0,
            },
            Vec::new(),
        );

        assert_eq!(direct.plain_var(), Some(signature));
        assert_eq!(computed.plain_var(), None);
        assert_eq!(projected.plain_var(), None);
    }

    #[test]
    fn map_vars_rewrites_every_nested_variable() {
        let source_atom = AtomSignature::new(true, 0);
        let source_0 = AtomArgumentSignature::new(source_atom, 0);
        let source_1 = AtomArgumentSignature::new(source_atom, 1);
        let source_2 = AtomArgumentSignature::new(source_atom, 2);
        let expression = ArithmeticPos::new(
            FactorPos::Tuple {
                fields: vec![
                    ArithmeticPos::new(
                        FactorPos::FnCall {
                            name: "f".into(),
                            args: vec![ArithmeticPos::from_var_signature(source_0)],
                        },
                        Vec::new(),
                    ),
                    ArithmeticPos::new(
                        FactorPos::TupleProj {
                            tuple: Box::new(ArithmeticPos::new(
                                FactorPos::Builtin {
                                    op: BuiltinOperator::ToString,
                                    args: vec![ArithmeticPos::new(
                                        FactorPos::Group(Box::new(
                                            ArithmeticPos::from_var_signature(source_1),
                                        )),
                                        Vec::new(),
                                    )],
                                },
                                Vec::new(),
                            )),
                            index: 1,
                        },
                        Vec::new(),
                    ),
                ],
            },
            vec![(ArithmeticOperator::Plus, FactorPos::Var(source_2))],
        );
        let target_atom = AtomSignature::new(false, 2);
        let target_0 = AtomArgumentSignature::new(target_atom, 10);
        let target_1 = AtomArgumentSignature::new(target_atom, 11);
        let target_2 = AtomArgumentSignature::new(target_atom, 12);

        let mapped = expression.map_vars(&|signature| {
            FactorPos::Var(AtomArgumentSignature::new(
                target_atom,
                signature.argument_id() + 10,
            ))
        });

        assert_eq!(
            mapped,
            ArithmeticPos::new(
                FactorPos::Tuple {
                    fields: vec![
                        ArithmeticPos::new(
                            FactorPos::FnCall {
                                name: "f".into(),
                                args: vec![ArithmeticPos::from_var_signature(target_0)],
                            },
                            Vec::new(),
                        ),
                        ArithmeticPos::new(
                            FactorPos::TupleProj {
                                tuple: Box::new(ArithmeticPos::new(
                                    FactorPos::Builtin {
                                        op: BuiltinOperator::ToString,
                                        args: vec![ArithmeticPos::new(
                                            FactorPos::Group(Box::new(
                                                ArithmeticPos::from_var_signature(target_1),
                                            )),
                                            Vec::new(),
                                        )],
                                    },
                                    Vec::new(),
                                )),
                                index: 1,
                            },
                            Vec::new(),
                        ),
                    ],
                },
                vec![(ArithmeticOperator::Plus, FactorPos::Var(target_2))],
            )
        );
    }
}
