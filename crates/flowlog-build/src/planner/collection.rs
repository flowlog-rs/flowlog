//! Collection types used during planning.
//!
//! A Collection models a relation of tuples represented by key/value
//! positions (as `ArithmeticPos`). Collections can be row-based (no keys) or
//! key/value-based and are identified by a fingerprint.

use crate::catalog::{ArithmeticPos, AtomArgumentSignature};
use std::fmt;

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Collection {
    /// A fingerprint identifying the collection type and lineage
    fingerprint: u64,

    /// Hierarchical name describing how this collection was built from EDBs
    /// (e.g. `(reach ⋈[y] arc)` or `π[x](σ[x > 0](arc))`). Used for
    /// log/debug rendering. Empty for internal placeholders.
    name: String,

    /// Key argument signatures (empty for row-only collections)
    key_argument_signatures: Vec<ArithmeticPos>,

    /// Value argument signatures
    value_argument_signatures: Vec<ArithmeticPos>,
}

impl Collection {
    /// Creates a new collection with the given fingerprint, name, and argument signatures.
    pub fn new(
        fingerprint: u64,
        name: String,
        key_argument_signatures: &[ArithmeticPos],
        value_argument_signatures: &[ArithmeticPos],
    ) -> Self {
        Self {
            fingerprint,
            name,
            key_argument_signatures: key_argument_signatures.to_vec(),
            value_argument_signatures: value_argument_signatures.to_vec(),
        }
    }

    /// Creates a collection from an atom with the given argument signatures and fingerprint.
    /// The atom's name is used as the hierarchical name (atoms are EDB leaves).
    pub fn from_atom(
        atom_argument_signatures: &[AtomArgumentSignature],
        atom_fingerprint: u64,
        atom_name: String,
    ) -> Self {
        // Convert each AtomArgumentSignature to ArithmeticPos
        let value_argument_signatures: Vec<ArithmeticPos> = atom_argument_signatures
            .iter()
            .map(|sig| ArithmeticPos::from_var_signature(*sig))
            .collect();

        // Create collection with no keys (row-based) and all arguments as values
        Self {
            fingerprint: atom_fingerprint,
            name: atom_name,
            key_argument_signatures: Vec::new(), // No keys for row-based atom
            value_argument_signatures,
        }
    }

    /// Returns the arity as (key_count, value_count).
    #[inline]
    pub fn arity(&self) -> (usize, usize) {
        (
            self.key_argument_signatures.len(),
            self.value_argument_signatures.len(),
        )
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
    /// Canonical form: `<name> [0x{:016x}], key:(..), value:(..)`.
    /// When `name` is empty (internal placeholder), only the hex form appears.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let k = self
            .key_argument_signatures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        let v = self
            .value_argument_signatures
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        if self.name.is_empty() {
            write!(f, "0x{:016x}, key:({}), value:({})", self.fingerprint, k, v)
        } else {
            write!(
                f,
                "{} [0x{:016x}], key:({}), value:({})",
                self.name, self.fingerprint, k, v
            )
        }
    }
}
