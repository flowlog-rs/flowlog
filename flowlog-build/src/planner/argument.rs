//! Argument types for query planning transformations in FlowLog Datalog program.

use std::fmt;

use crate::planner::ArithmeticArgument;
use crate::planner::FactorArgument;

/// Represents different types of transformation arguments used in query planning.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub(crate) enum TransformationArgument {
    /// Key-Value transformation from input stream (key/value, index)
    KV((bool, usize)),

    /// Join transformation from input streams (left/right, key/value, index)
    Jn((bool, bool, usize)),
}

impl TransformationArgument {
    /// Converts ArithmeticArgument(s) to TransformationArgument(s).
    /// Each ArithmeticArgument is expected to contain only a single variable reference.
    pub(crate) fn from_arithmetic_arguments(arith_args: Vec<ArithmeticArgument>) -> Vec<Self> {
        arith_args
            .into_iter()
            .map(|arith_arg| {
                // Assert that there are no additional arithmetic operations
                assert!(
                    arith_arg.rest.is_empty(),
                    "Planner error: expected simple variable reference in ArithmeticArgument, but found operations: {:?}",
                    arith_arg.rest
                );

                match arith_arg.init {
                    FactorArgument::Var(trans_arg) => trans_arg,
                    _ => panic!(
                        "Planner error: expected only variable references in ArithmeticArgument, got: {:?}",
                        arith_arg.init
                    ),
                }
            })
            .collect()
    }
}

impl fmt::Display for TransformationArgument {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::KV((key_or_value, id)) => {
                let prefix = if *key_or_value { 'K' } else { 'V' };
                write!(f, "{}{}", prefix, id)
            }
            Self::Jn((left_or_right, key_or_value, id)) => {
                let side = if *left_or_right { 'L' } else { 'R' };
                let prefix = if *key_or_value { 'K' } else { 'V' };
                write!(f, "{}{}{}", side, prefix, id)
            }
        }
    }
}
