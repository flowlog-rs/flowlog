//! Argument types for query planning transformations in Macaron Datalog program.

use crate::{ArithmeticArgument, FactorArgument};
use std::fmt;

/// Represents different types of transformation arguments used in query planning.
#[derive(Debug, Copy, Clone, Hash, PartialEq, Eq)]
pub enum TransformationArgument {
    /// Key-Value transformation from input stream (key/value, index)
    KV((bool, usize)),

    /// Join transformation from input streams (left/right, key/value, index)
    Jn((bool, bool, usize)),
}

impl TransformationArgument {
    /// Extracts key-value indices from a KV variant.
    pub fn kv_indices(&self) -> (bool, usize) {
        match self {
            Self::KV(indices) => *indices,
            _ => panic!(
                "Planner error: TransformationArgument::kv_indices expects KV variant, got: {:?}",
                self
            ),
        }
    }

    /// Extracts join indices from a Jn variant.
    pub fn jn_indices(&self) -> (bool, bool, usize) {
        match self {
            Self::Jn(indices) => *indices,
            _ => panic!(
                "Planner error: TransformationArgument::jn_indices expects Jn variant, got: {:?}",
                self
            ),
        }
    }

    /// Converts ArithmeticArgument(s) to TransformationArgument(s).
    /// Each ArithmeticArgument is expected to contain only a single variable reference.
    pub fn from_arithmetic_arguments(arith_args: Vec<ArithmeticArgument>) -> Vec<Self> {
        arith_args
            .into_iter()
            .map(|arith_arg| {
                // Assert that there are no additional arithmetic operations
                if !arith_arg.rest.is_empty() {
                    panic!(
                        "Planner error: expected simple variable reference in ArithmeticArgument, but found operations: {:?}",
                        arith_arg.rest
                    );
                }

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
                let prefix = if *key_or_value { "value" } else { "key" };
                write!(f, "[{}, {}]", prefix, id)
            }
            Self::Jn((left_or_right, key_or_value, id)) => {
                let side = if *left_or_right { "right" } else { "left" };
                let prefix = if *key_or_value { "value" } else { "key" };
                write!(f, "[{}, {}, {}]", side, prefix, id)
            }
        }
    }
}
