//! Argument types for query planning transformations in Macaron Datalog program.

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

    /// Flips the left/right flag in a Jn variant.
    pub fn jn_flip(&self) -> Self {
        match self {
            Self::Jn((left_or_right, key_or_value, id)) => {
                Self::Jn((!left_or_right, *key_or_value, *id))
            }
            _ => panic!(
                "Planner error: TransformationArgument::jn_flip expects Jn variant, got: {:?}",
                self
            ),
        }
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
