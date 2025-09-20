//! Collection types for query planning in Macaron Datalog programs.

use crate::TransformationFlow;
use catalog::ArithmeticPos;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Identifies a data collection and tracks its transformation structure.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct CollectionSignature(pub u64);

impl CollectionSignature {
    pub fn from_unary(input: Self, flow: &TransformationFlow) -> Self {
        let mut hasher = DefaultHasher::new();
        "unary".hash(&mut hasher);
        input.0.hash(&mut hasher);
        flow.hash(&mut hasher);
        Self(hasher.finish())
    }

    pub fn from_join(left: Self, right: Self, flow: &TransformationFlow) -> Self {
        let mut hasher = DefaultHasher::new();
        "join".hash(&mut hasher);
        left.0.hash(&mut hasher);
        right.0.hash(&mut hasher);
        flow.hash(&mut hasher);
        Self(hasher.finish())
    }
    pub fn from_neg_join(left: Self, right: Self, flow: &TransformationFlow) -> Self {
        let mut hasher = DefaultHasher::new();
        "anti_join".hash(&mut hasher);
        left.0.hash(&mut hasher);
        right.0.hash(&mut hasher);
        flow.hash(&mut hasher);
        Self(hasher.finish())
    }
}

impl fmt::Display for CollectionSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Collection {
    /// A signature identifying the collection type and lineage
    signature: Arc<CollectionSignature>,

    /// Key argument signatures (empty for row-only collections)
    key_argument_signatures: Vec<ArithmeticPos>,

    /// Value argument signatures
    value_argument_signatures: Vec<ArithmeticPos>,
}

impl Collection {
    /// Creates a new collection with the given signature and argument signatures.
    pub fn new(
        signature: CollectionSignature,
        key_argument_signatures: &[ArithmeticPos],
        value_argument_signatures: &[ArithmeticPos],
    ) -> Self {
        Self {
            signature: Arc::new(signature),
            key_argument_signatures: key_argument_signatures.to_vec(),
            value_argument_signatures: value_argument_signatures.to_vec(),
        }
    }

    /// Returns the arity as (key_count, value_count).
    pub fn arity(&self) -> (usize, usize) {
        (
            self.key_argument_signatures.len(),
            self.value_argument_signatures.len(),
        )
    }

    /// Returns `true` if this collection has a key-value structure.
    pub fn is_kv(&self) -> bool {
        !self.key_argument_signatures.is_empty()
    }

    /// Returns `true` if this collection has only keys (no values).
    pub fn is_k_only(&self) -> bool {
        self.value_argument_signatures.is_empty()
    }

    /// Returns the collection signature.
    pub fn signature(&self) -> &Arc<CollectionSignature> {
        &self.signature
    }

    /// Returns references to both key and value argument signatures.
    pub fn kv_argument_signatures(&self) -> (&[ArithmeticPos], &[ArithmeticPos]) {
        (
            &self.key_argument_signatures,
            &self.value_argument_signatures,
        )
    }

    /// Returns the key argument signatures.
    pub fn key_argument_signatures(&self) -> &[ArithmeticPos] {
        &self.key_argument_signatures
    }

    /// Returns the value argument signatures.
    pub fn value_argument_signatures(&self) -> &[ArithmeticPos] {
        &self.value_argument_signatures
    }
}

impl fmt::Display for Collection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_kv() {
            write!(
                f,
                "{}({}: {})",
                self.signature,
                self.key_argument_signatures
                    .iter()
                    .map(|sig| sig.to_string())
                    .collect::<Vec<_>>()
                    .join(", "),
                self.value_argument_signatures
                    .iter()
                    .map(|sig| sig.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        } else {
            write!(
                f,
                "{}({})",
                self.signature,
                self.value_argument_signatures
                    .iter()
                    .map(|sig| sig.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        }
    }
}
