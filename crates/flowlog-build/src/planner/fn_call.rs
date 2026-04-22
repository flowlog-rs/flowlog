//! FnCall predicate argument representation for query planning in FlowLog Datalog programs.

use crate::catalog::FnCallPredicatePos;
use crate::planner::{ArithmeticArgument, TransformationArgument};
use std::fmt;

/// Represents a fn_call predicate in a query plan.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct FnCallPredicateArgument {
    /// The function name.
    name: String,
    /// The arguments arithmetic expression.
    args: Vec<ArithmeticArgument>,
    /// Whether the UDF result is negated.
    is_negated: bool,
}

impl FnCallPredicateArgument {
    /// Creates a new FnCallPredicateArgument from constituent parts.
    pub(crate) fn from_fn_call_pos(
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
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the resolved arguments.
    pub(crate) fn args(&self) -> &[ArithmeticArgument] {
        &self.args
    }

    /// Whether the UDF result is negated.
    pub(crate) fn is_negated(&self) -> bool {
        self.is_negated
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
