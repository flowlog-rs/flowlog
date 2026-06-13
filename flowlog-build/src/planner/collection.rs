//! Collection types used during planning.
//!
//! A Collection models a relation of tuples represented by key/value
//! positions (as `ArithmeticPos`). Collections can be row-based (no keys) or
//! key/value-based and are identified by a fingerprint.

use crate::catalog::ArithmeticPos;
use std::fmt;

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub(crate) struct Collection {
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
    pub(crate) fn new(
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

    /// Returns the arity as (key_count, value_count).
    #[inline]
    pub(crate) fn arity(&self) -> (usize, usize) {
        (
            self.key_argument_signatures.len(),
            self.value_argument_signatures.len(),
        )
    }

    /// Returns `true` if this collection has only keys (no values).
    pub(crate) fn is_k_only(&self) -> bool {
        self.value_argument_signatures.is_empty()
    }

    /// Returns the collection fingerprint.
    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
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
