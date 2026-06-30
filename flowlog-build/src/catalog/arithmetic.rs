//! Arithmetic expression signatures for FlowLog Datalog programs.

use std::fmt;
use std::slice;

use flowlog_parser::Arithmetic;
use flowlog_parser::ArithmeticOperator;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::ConstType;
use flowlog_parser::Factor;
use flowlog_parser::TupleElem;

use crate::catalog::AtomArgumentSignature;

/// A factor in an arithmetic expression with variables resolved to their
/// concrete positions within atoms.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FactorPos {
    /// A variable reference identified by its atom and argument position
    Var(AtomArgumentSignature),
    /// A constant value (integer, string, etc.)
    Const(ConstType),
    /// A user-defined function call.
    FnCall {
        name: String,
        args: Vec<ArithmeticPos>,
    },
    /// An engine built-in call (Soufflé-style intrinsic).
    Builtin {
        op: BuiltinOperator,
        args: Vec<ArithmeticPos>,
    },
    /// Parenthesised sub-expression, preserving its grouping.
    Group(Box<ArithmeticPos>),
    /// Tuple construction `(e0, e1, …)` (one column). Only constructs reach
    /// here — destructures are desugared to [`FactorPos::TupleProj`] before catalog.
    Tuple { fields: Vec<ArithmeticPos> },
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

    /// Transform every variable in this factor using `f`, recursing into
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
#[derive(Clone, Hash, PartialEq, Eq)]
pub(crate) struct ArithmeticPos {
    /// The initial factor (left-most term)
    init: FactorPos,
    /// Subsequent operations and factors (operator, right operand)
    rest: Vec<(ArithmeticOperator, FactorPos)>,
}

impl ArithmeticPos {
    /// Creates a new positional arithmetic expression.
    pub(crate) fn new(init: FactorPos, rest: Vec<(ArithmeticOperator, FactorPos)>) -> Self {
        Self { init, rest }
    }

    /// Creates a simple ArithmeticPos for a single variable from an AtomArgumentSignature.
    pub(crate) fn from_var_signature(signature: AtomArgumentSignature) -> Self {
        ArithmeticPos {
            init: FactorPos::Var(signature),
            rest: vec![],
        }
    }

    /// Constructs a positional arithmetic expression from a parsed arithmetic expression.
    pub(crate) fn from_arithmetic(
        arith: &Arithmetic,
        var_signatures: &[AtomArgumentSignature],
    ) -> Self {
        fn map_factor(
            factor: &Factor,
            var_signatures: &[AtomArgumentSignature],
            var_id: &mut usize,
        ) -> FactorPos {
            // Both `FnCall` and `Builtin` carry their args as
            // `Vec<Arithmetic>` and consume `num_vars` signatures per arg
            // in source order. Walk once, reuse for both arms.
            let map_call_args = |args: &[Arithmetic], var_id: &mut usize| -> Vec<ArithmeticPos> {
                args.iter()
                    .map(|arg| {
                        let num_vars = arg.vars().len();
                        let sigs = &var_signatures[*var_id..*var_id + num_vars];
                        *var_id += num_vars;
                        ArithmeticPos::from_arithmetic(arg, sigs)
                    })
                    .collect()
            };
            match factor {
                Factor::Var(_) => {
                    let sig = var_signatures[*var_id];
                    *var_id += 1;
                    FactorPos::Var(sig)
                }
                Factor::Const(c) => FactorPos::Const(c.clone()),
                Factor::FnCall(fc) => FactorPos::FnCall {
                    name: fc.name().to_string(),
                    args: map_call_args(fc.args(), var_id),
                },
                Factor::Builtin(bc) => FactorPos::Builtin {
                    op: bc.op(),
                    args: map_call_args(bc.args(), var_id),
                },
                // Cast is identity at runtime — strip the wrapper and
                // map the inner factor directly. The typechecker has
                // already validated subtype compatibility by this point.
                Factor::Cast(c) => map_factor(c.inner(), var_signatures, var_id),
                Factor::Group(a) => {
                    let num_vars = a.vars().len();
                    let sigs = &var_signatures[*var_id..*var_id + num_vars];
                    *var_id += num_vars;
                    FactorPos::Group(Box::new(ArithmeticPos::from_arithmetic(a, sigs)))
                }
                // A tuple construct: each element is an expression (placeholders
                // only occur in destructures, which are desugared to `TupleProj`s
                // before catalog construction). Consume each element's variable
                // signatures in source order, mirroring call-arg handling.
                Factor::Tuple(r) => {
                    let fields = r
                        .fields()
                        .iter()
                        .map(|elem| match elem {
                            TupleElem::Expr(a) => {
                                let num_vars = a.vars().len();
                                let sigs = &var_signatures[*var_id..*var_id + num_vars];
                                *var_id += num_vars;
                                ArithmeticPos::from_arithmetic(a, sigs)
                            }
                            TupleElem::Placeholder => unreachable!(
                                "tuple placeholder reached catalog; destructures \
                                 are desugared and constructs reject `_`"
                            ),
                        })
                        .collect();
                    FactorPos::Tuple { fields }
                }
                Factor::TupleProj { tuple, index } => FactorPos::TupleProj {
                    tuple: Box::new(
                        map_call_args(slice::from_ref(tuple.as_ref()), var_id)
                            .pop()
                            .expect("proj lowering yields exactly one arithmetic"),
                    ),
                    index: *index,
                },
            }
        }

        let mut var_id = 0usize;

        let init = map_factor(arith.init(), var_signatures, &mut var_id);
        let rest = arith
            .rest()
            .iter()
            .map(|(op, factor)| (op.clone(), map_factor(factor, var_signatures, &mut var_id)))
            .collect();

        ArithmeticPos { init, rest }
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

    /// Returns a vector of all variable signatures referenced in this expression, in order.
    pub(crate) fn signatures(&self) -> Vec<&AtomArgumentSignature> {
        let mut sigs = self.init.signatures();
        for (_, factor) in &self.rest {
            sigs.extend(factor.signatures());
        }
        sigs
    }

    /// Transform every variable in this expression using `f`, recursing into FnCall args.
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

impl fmt::Debug for ArithmeticPos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Delegate Debug to Display
        fmt::Display::fmt(self, f)
    }
}
