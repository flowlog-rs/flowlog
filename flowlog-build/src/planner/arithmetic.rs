//! Arithmetic expression representation for query planning in FlowLog Datalog programs.

use std::fmt;

use crate::catalog::{ArithmeticPos, FactorPos};
use crate::parser::{ArithmeticOperator, BuiltinOperator, ConstType};
use crate::planner::TransformationArgument;

/// Represents a basic factor in an arithmetic expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum FactorArgument {
    /// Variable reference to locate the value
    Var(TransformationArgument),

    /// Constant/literal value
    Const(ConstType),

    /// User-defined function call.
    FnCall {
        name: String,
        args: Vec<ArithmeticArgument>,
    },

    /// Engine built-in call (Soufflé-style intrinsic).
    Builtin {
        op: BuiltinOperator,
        args: Vec<ArithmeticArgument>,
    },

    /// Parenthesised sub-expression, preserving its grouping.
    Group(Box<ArithmeticArgument>),

    /// Tuple construction `(e0, e1, …)`.
    Tuple { fields: Vec<ArithmeticArgument> },

    /// Tuple component projection `tuple.index`.
    TupleProj {
        tuple: Box<ArithmeticArgument>,
        index: usize,
    },
}

impl FactorArgument {
    /// Returns all transformation arguments referenced in this factor
    /// (including nested in FnCall / Builtin args).
    pub(crate) fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        match self {
            Self::Var(arg) => vec![arg],
            Self::Const(_) => vec![],
            Self::FnCall { args, .. } | Self::Builtin { args, .. } => args
                .iter()
                .flat_map(|a| a.transformation_arguments())
                .collect(),
            Self::Group(a) => a.transformation_arguments(),
            Self::Tuple { fields } => fields
                .iter()
                .flat_map(|a| a.transformation_arguments())
                .collect(),
            Self::TupleProj { tuple, .. } => tuple.transformation_arguments(),
        }
    }
}

impl fmt::Display for FactorArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(transformation_arg) => write!(f, "{transformation_arg}"),
            Self::Const(constant) => write!(f, "{constant}"),
            Self::FnCall { name, args } => {
                let args_str = args
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{name}({args_str})")
            }
            Self::Builtin { op, args } => {
                let args_str = args
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "{op}({args_str})")
            }
            Self::Group(a) => write!(f, "({a})"),
            Self::Tuple { fields } => {
                let inner = fields
                    .iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(f, "[{inner}]")
            }
            Self::TupleProj { tuple, index } => write!(f, "({tuple}).{index}"),
        }
    }
}

/// Represents a complete arithmetic expression
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct ArithmeticArgument {
    /// The initial factor in the expression
    pub(crate) init: FactorArgument,

    /// Additional operations and factors (e.g., + 5, * x)
    pub(crate) rest: Vec<(ArithmeticOperator, FactorArgument)>,
}

impl ArithmeticArgument {
    /// Creates an ArithmeticArgument from an ArithmeticPos and a list of transformation arguments.
    pub(crate) fn from_arithmeticpos(
        arithmetic: &ArithmeticPos,
        var_arguments: &[TransformationArgument],
    ) -> Self {
        fn map_factor(
            factor: &FactorPos,
            var_arguments: &[TransformationArgument],
            var_id: &mut usize,
        ) -> FactorArgument {
            // Shared between `FnCall` and `Builtin`: consume `signatures().len()`
            // slots per arg from the running `var_arguments` cursor.
            let map_call_args =
                |args: &[ArithmeticPos], var_id: &mut usize| -> Vec<ArithmeticArgument> {
                    args.iter()
                        .map(|arg| {
                            let num_vars = arg.signatures().len();
                            let sub_args = &var_arguments[*var_id..*var_id + num_vars];
                            *var_id += num_vars;
                            ArithmeticArgument::from_arithmeticpos(arg, sub_args)
                        })
                        .collect()
                };
            match factor {
                FactorPos::Var(_) => {
                    let var = var_arguments[*var_id];
                    *var_id += 1;
                    FactorArgument::Var(var)
                }
                FactorPos::Const(constant) => FactorArgument::Const(constant.clone()),
                FactorPos::FnCall { name, args } => FactorArgument::FnCall {
                    name: name.clone(),
                    args: map_call_args(args, var_id),
                },
                FactorPos::Builtin { op, args } => FactorArgument::Builtin {
                    op: *op,
                    args: map_call_args(args, var_id),
                },
                FactorPos::Group(a) => {
                    let num_vars = a.signatures().len();
                    let sub_args = &var_arguments[*var_id..*var_id + num_vars];
                    *var_id += num_vars;
                    FactorArgument::Group(Box::new(ArithmeticArgument::from_arithmeticpos(
                        a, sub_args,
                    )))
                }
                FactorPos::Tuple { fields } => FactorArgument::Tuple {
                    fields: map_call_args(fields, var_id),
                },
                FactorPos::TupleProj { tuple, index } => FactorArgument::TupleProj {
                    tuple: Box::new(
                        map_call_args(std::slice::from_ref(tuple), var_id)
                            .pop()
                            .expect("proj lowering yields exactly one arithmetic"),
                    ),
                    index: *index,
                },
            }
        }

        let mut var_id = 0;

        let init = map_factor(arithmetic.init(), var_arguments, &mut var_id);
        let rest = arithmetic
            .rest()
            .iter()
            .map(|(op, factor)| (op.clone(), map_factor(factor, var_arguments, &mut var_id)))
            .collect();

        Self { init, rest }
    }

    /// Returns the initial factor of the arithmetic expression.
    pub(crate) fn init(&self) -> &FactorArgument {
        &self.init
    }

    /// Returns all operations and factors after the initial factor.
    pub(crate) fn rest(&self) -> &[(ArithmeticOperator, FactorArgument)] {
        &self.rest
    }

    /// Returns all transformation arguments referenced in this arithmetic expression.
    pub(crate) fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        let mut args = self.init.transformation_arguments();
        for (_, factor) in &self.rest {
            args.extend(factor.transformation_arguments());
        }
        args
    }
}

impl fmt::Display for ArithmeticArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.init)?;

        for (op, factor) in &self.rest {
            write!(f, " {op} {factor}")?;
        }

        Ok(())
    }
}
