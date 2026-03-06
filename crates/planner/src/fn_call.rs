//! FnCall predicate argument representation for query planning in FlowLog Datalog programs.

use crate::{argument::TransformationArgument, arithmetic::ArithmeticArgument};
use catalog::FnCallPredicatePos;
use std::fmt;

/// Represents a fn_call predicate in a query plan.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FnCallPredicateArgument {
    /// The function name.
    name: String,
    /// The arguments arithmetic expression.
    args: Vec<ArithmeticArgument>,
}

impl FnCallPredicateArgument {
    /// Creates a new FnCallPredicateArgument from constituent parts.
    pub fn from_fn_call_pos(
        fn_call_pos: &FnCallPredicatePos,
        var_arguments_per_arg: &[Vec<TransformationArgument>],
    ) -> Self {
        let args = fn_call_pos
            .args()
            .iter()
            .zip(var_arguments_per_arg.iter())
            .map(|(pos, var_args)| ArithmeticArgument::from_arithmeticpos(pos, var_args))
            .collect();
        Self {
            name: fn_call_pos.name().to_string(),
            args,
        }
    }

    /// Returns the function name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the resolved arguments.
    pub fn args(&self) -> &[ArithmeticArgument] {
        &self.args
    }

    /// Returns all transformation arguments referenced in this fn_call predicate.
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        self.args
            .iter()
            .flat_map(|a| a.transformation_arguments())
            .collect()
    }
}

impl fmt::Display for FnCallPredicateArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        write!(f, "{}({})", self.name, args_str)
    }
}
