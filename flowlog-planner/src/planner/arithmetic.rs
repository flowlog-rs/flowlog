//! Arithmetic expressions as a transformation computes them: the same
//! shapes the catalog parses, with every column resolved to a slot of the
//! input collection.

use std::fmt;

use flowlog_parser::ArithmeticOperator;
use flowlog_parser::BuiltinOperator;
use flowlog_parser::Constant;

use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::FactorPos;
use crate::planner::TransformationArgument;

/// Represents a basic factor in an arithmetic expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FactorArgument {
    /// Variable reference to locate the value
    Var(TransformationArgument),

    /// Constant/literal value
    Const(Constant),

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
    /// `factor` with each column replaced by the slot `column` gives for
    /// it, or `None` when some column has none.
    pub(crate) fn from_factor_pos(
        factor: &FactorPos,
        column: &mut impl FnMut(&AtomArgumentSignature) -> Option<TransformationArgument>,
    ) -> Option<Self> {
        let mut args = |args: &[ArithmeticPos]| {
            args.iter()
                .map(|arg| ArithmeticArgument::from_arithmetic_pos(arg, column))
                .collect::<Option<Vec<_>>>()
        };
        Some(match factor {
            FactorPos::Var(signature) => Self::Var(column(signature)?),
            FactorPos::Const(constant) => Self::Const(constant.clone()),
            FactorPos::FnCall { name, args: inner } => Self::FnCall {
                name: name.clone(),
                args: args(inner)?,
            },
            FactorPos::Builtin { op, args: inner } => Self::Builtin {
                op: *op,
                args: args(inner)?,
            },
            FactorPos::Group(inner) => Self::Group(Box::new(
                ArithmeticArgument::from_arithmetic_pos(inner, column)?,
            )),
            FactorPos::Tuple { fields } => Self::Tuple {
                fields: args(fields)?,
            },
            FactorPos::TupleProj { tuple, index } => Self::TupleProj {
                tuple: Box::new(ArithmeticArgument::from_arithmetic_pos(tuple, column)?),
                index: *index,
            },
        })
    }

    /// Returns all transformation arguments referenced in this factor
    /// (including nested in FnCall / Builtin args).
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
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
        let list = |args: &[ArithmeticArgument]| {
            args.iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ")
        };
        match self {
            Self::Var(slot) => write!(f, "{slot}"),
            Self::Const(constant) => write!(f, "{constant}"),
            Self::FnCall { name, args } => write!(f, "{name}({})", list(args)),
            Self::Builtin { op, args } => write!(f, "{op}({})", list(args)),
            Self::Group(inner) => write!(f, "({inner})"),
            Self::Tuple { fields } => write!(f, "[{}]", list(fields)),
            Self::TupleProj { tuple, index } => write!(f, "({tuple}).{index}"),
        }
    }
}

/// Represents a complete arithmetic expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ArithmeticArgument {
    /// The initial factor in the expression
    pub init: FactorArgument,

    /// Additional operations and factors (e.g., + 5, * x)
    pub rest: Vec<(ArithmeticOperator, FactorArgument)>,
}

impl ArithmeticArgument {
    /// `expr` with each column replaced by the slot `column` gives for
    /// it, or `None` when some column has none. Columns are asked for in
    /// source order.
    pub(crate) fn from_arithmetic_pos(
        expr: &ArithmeticPos,
        column: &mut impl FnMut(&AtomArgumentSignature) -> Option<TransformationArgument>,
    ) -> Option<Self> {
        Some(Self {
            init: FactorArgument::from_factor_pos(expr.init(), column)?,
            rest: expr
                .rest()
                .iter()
                .map(|(op, factor)| {
                    Some((op.clone(), FactorArgument::from_factor_pos(factor, column)?))
                })
                .collect::<Option<Vec<_>>>()?,
        })
    }

    /// `expr` with its columns replaced by `slots` in source order, one
    /// per column.
    ///
    /// # Panics
    ///
    /// Panics if `slots` has fewer entries than `expr` has columns, which
    /// the catalog rules out.
    pub(crate) fn from_arithmetic_pos_in_order(
        expr: &ArithmeticPos,
        slots: &[TransformationArgument],
    ) -> Self {
        let mut slots = slots.iter().copied();
        Self::from_arithmetic_pos(expr, &mut |_| slots.next())
            .expect("Planner error: a slot for every column")
    }

    /// Returns the initial factor of the arithmetic expression.
    pub fn init(&self) -> &FactorArgument {
        &self.init
    }

    /// Returns all operations and factors after the initial factor.
    pub fn rest(&self) -> &[(ArithmeticOperator, FactorArgument)] {
        &self.rest
    }

    /// Returns all transformation arguments referenced in this arithmetic expression.
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
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
