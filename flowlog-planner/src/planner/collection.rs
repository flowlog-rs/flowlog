//! Collection types used during planning.
//!
//! A Collection models a relation of tuples represented by a key/value
//! layout of argument positions. Collections can be row-based (no keys) or
//! key/value-based. A fingerprint says which plan node a collection is;
//! a canonical form says what rows it holds.

use std::fmt;

use crate::planner::CanonicalForm;
use crate::planner::KeyValueLayout;

/// Represents a data collection with key-value structure.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct Collection {
    /// Identity of this collection in the plan graph, which readers name
    /// their inputs by. It is the lineage fingerprint the rule planner
    /// built the collection under, so equal fingerprints mean the same
    /// operation over the same inputs at the same body positions, and
    /// nothing about rows: the same rows reached through another plan, or
    /// the same step planned by another rule, carry a different
    /// fingerprint.
    fingerprint: u64,

    /// Hierarchical name describing how this collection was built from EDBs
    /// (e.g. `(reach ⋈[y] arc)` or `π[x](σ[x > 0](arc))`). Used for
    /// log/debug rendering. Empty for internal placeholders.
    name: String,

    /// Key and value argument signatures; the key is empty for a row-only
    /// collection.
    kv_layout: KeyValueLayout,

    /// The query this collection computes, independent of the plan that
    /// built it. Where the fingerprint says which node this is, the form
    /// says what rows it holds, so equal forms are what sharing acts on.
    canonical: CanonicalForm,
}

impl Collection {
    /// Creates a new collection with the given fingerprint, name, layout,
    /// and canonical form.
    pub(crate) fn new(
        fingerprint: u64,
        name: String,
        kv_layout: KeyValueLayout,
        canonical: CanonicalForm,
    ) -> Self {
        Self {
            fingerprint,
            name,
            kv_layout,
            canonical,
        }
    }

    /// Returns the arity as (key_count, value_count).
    #[inline]
    pub fn arity(&self) -> (usize, usize) {
        (self.kv_layout.key().len(), self.kv_layout.value().len())
    }

    /// Returns `true` if this collection has only keys (no values).
    pub fn is_k_only(&self) -> bool {
        self.kv_layout.value().is_empty()
    }

    /// Returns the collection fingerprint.
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Returns the query this collection computes.
    pub(crate) fn canonical(&self) -> &CanonicalForm {
        &self.canonical
    }
}

impl fmt::Display for Collection {
    /// Canonical form: `<name> [0x{:016x}], key:(..), value:(..)`.
    /// When `name` is empty (internal placeholder), only the hex form appears.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.name.is_empty() {
            write!(f, "0x{:016x}, {}", self.fingerprint, self.kv_layout)
        } else {
            write!(
                f,
                "{} [0x{:016x}], {}",
                self.name, self.fingerprint, self.kv_layout
            )
        }
    }
}
