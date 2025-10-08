//! Collection types used during planning.
//!
//! A Collection models a relation of tuples represented by key/value
//! positions (as `ArithmeticPos`). Collections can be row-based (no keys) or
//! key/value-based and are identified by a fingerprint.

use catalog::{ArithmeticPos, AtomArgumentSignature};
use std::fmt;

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Collection {
    /// A fingerprint identifying the collection type and lineage
    fingerprint: u64,

    /// Key argument signatures (empty for row-only collections)
    key_argument_signatures: Vec<ArithmeticPos>,

    /// Value argument signatures
    value_argument_signatures: Vec<ArithmeticPos>,
}

impl Collection {
    /// Creates a new collection with the given fingerprint and argument signatures.
    pub fn new(
        fingerprint: u64,
        key_argument_signatures: &[ArithmeticPos],
        value_argument_signatures: &[ArithmeticPos],
    ) -> Self {
        Self {
            fingerprint,
            key_argument_signatures: key_argument_signatures.to_vec(),
            value_argument_signatures: value_argument_signatures.to_vec(),
        }
    }

    /// Creates a collection from an atom with the given argument signatures and fingerprint.
    pub fn from_atom(
        atom_argument_signatures: &[AtomArgumentSignature],
        atom_fingerprint: u64,
    ) -> Self {
        // Convert each AtomArgumentSignature to ArithmeticPos
        let value_argument_signatures: Vec<ArithmeticPos> = atom_argument_signatures
            .iter()
            .map(|sig| ArithmeticPos::from_var_signature(*sig))
            .collect();

        // Create collection with no keys (row-based) and all arguments as values
        Self {
            fingerprint: atom_fingerprint,
            key_argument_signatures: Vec::new(), // No keys for row-based atom
            value_argument_signatures,
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

    /// Returns the collection fingerprint.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
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
        let key = self
            .key_argument_signatures
            .iter()
            .map(|sig| sig.to_string())
            .collect::<Vec<_>>()
            .join(", ");
        let value = self
            .value_argument_signatures
            .iter()
            .map(|sig| sig.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "0x{:16x}, key:({}), value:({})",
            self.fingerprint, key, value
        )
    }
}
