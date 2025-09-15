//! Arithmetic expression representation for query planning in Macaron Datalog programs.

use crate::argument::TransformationArgument;
use catalog::arithmetic::{ArithmeticPos, FactorPos};
use parser::{ArithmeticOperator, ConstType};
use std::fmt;

/// Represents a basic factor in an arithmetic expression
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FactorArgument {
    /// Variable reference to locate the value
    Var(TransformationArgument),

    /// Constant/literal value
    Const(ConstType),
}

impl FactorArgument {
    /// Returns the transformation argument referenced by this factor, if it exists.
    pub fn transformation_argument(&self) -> Option<&TransformationArgument> {
        match self {
            Self::Var(transformation_arg) => Some(transformation_arg),
            Self::Const(_) => None,
        }
    }
}

impl fmt::Display for FactorArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(transformation_arg) => write!(f, "{}", transformation_arg),
            Self::Const(constant) => write!(f, "{}", constant),
        }
    }
}

/// Represents a complete arithmetic expression
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct ArithmeticArgument {
    /// The initial factor in the expression
    pub init: FactorArgument,

    /// Additional operations and factors (e.g., + 5, * x)
    pub rest: Vec<(ArithmeticOperator, FactorArgument)>,
}

impl ArithmeticArgument {
    /// Creates an ArithmeticArgument from an ArithmeticPos and a list of transformation arguments.
    pub fn from_arithmeticpos(
        arithmetic: &ArithmeticPos,
        var_arguments: &[TransformationArgument],
    ) -> Self {
        let mut var_id = 0;

        // Convert the initial factor
        let init = match arithmetic.init() {
            FactorPos::Var(_) => {
                let var_signature = &var_arguments[var_id];
                var_id += 1;
                FactorArgument::Var(*var_signature)
            }
            FactorPos::Const(constant) => FactorArgument::Const(constant.clone()),
        };

        // Convert the rest of the factors
        let rest = arithmetic
            .rest()
            .iter()
            .map(|(op, factor)| {
                let factor = match factor {
                    FactorPos::Var(_) => {
                        let var_signature = &var_arguments[var_id];
                        var_id += 1;
                        FactorArgument::Var(*var_signature)
                    }
                    FactorPos::Const(constant) => FactorArgument::Const(constant.clone()),
                };
                (op.clone(), factor)
            })
            .collect();

        Self { init, rest }
    }

    /// Returns the initial factor of the arithmetic expression.
    pub fn init(&self) -> &FactorArgument {
        &self.init
    }

    /// Returns all operations and factors after the initial factor.
    pub fn rest(&self) -> &[(ArithmeticOperator, FactorArgument)] {
        &self.rest
    }

    /// Checks if this expression is just a single factor with no operations.
    pub fn is_literal(&self) -> bool {
        self.rest.is_empty()
    }

    /// Returns all transformation arguments referenced in this arithmetic expression.
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        let mut args = Vec::new();

        // Add transformation argument from the initial factor if it exists
        if let Some(arg) = self.init.transformation_argument() {
            args.push(arg);
        }

        // Add transformation arguments from all remaining factors
        for (_, factor) in &self.rest {
            if let Some(arg) = factor.transformation_argument() {
                args.push(arg);
            }
        }

        args
    }

    /// Creates a new ArithmeticArgument with all join arguments flipped.
    pub fn jn_flip(&self) -> Self {
        // Flip the initial factor if it's a variable
        let init = match &self.init {
            FactorArgument::Var(arg) => FactorArgument::Var(arg.jn_flip()),
            FactorArgument::Const(constant) => FactorArgument::Const(constant.clone()),
        };

        // Flip all remaining factors if they're variables
        let rest = self
            .rest
            .iter()
            .map(|(op, factor)| {
                let factor = match factor {
                    FactorArgument::Var(arg) => FactorArgument::Var(arg.jn_flip()),
                    FactorArgument::Const(constant) => FactorArgument::Const(constant.clone()),
                };
                (op.clone(), factor)
            })
            .collect();

        Self { init, rest }
    }
}

impl fmt::Display for ArithmeticArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.init)?;

        for (op, factor) in &self.rest {
            write!(f, " {} {}", op, factor)?;
        }

        Ok(())
    }
}
