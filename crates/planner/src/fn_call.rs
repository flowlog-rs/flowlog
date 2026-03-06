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
    /// Whether the UDF result is negated.
    is_negated: bool,
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
            is_negated: fn_call_pos.is_negated(),
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

    /// Whether the UDF result is negated.
    pub fn is_negated(&self) -> bool {
        self.is_negated
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
        if self.is_negated {
            write!(f, "!{}({})", self.name, args_str)
        } else {
            write!(f, "{}({})", self.name, args_str)
        }
    }
}
