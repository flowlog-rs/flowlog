//! Atom signatures for Macaron Datalog programs.

use std::fmt;

/// A signature uniquely identifying an atom occurring in a RHS position per rule.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd)]
pub struct AtomSignature {
    is_positive: bool,
    rhs_id: usize,
}

impl AtomSignature {
    /// Create a new atom signature.
    #[inline]
    pub fn new(is_positive: bool, rhs_id: usize) -> Self {
        Self {
            is_positive,
            rhs_id,
        }
    }

    /// Returns true if the atom is positive.
    #[inline]
    pub fn is_positive(&self) -> bool {
        self.is_positive
    }

    /// Returns the RHS id.
    #[inline]
    pub fn rhs_id(&self) -> usize {
        self.rhs_id
    }

    /// Returns the negated signature (flips positivity, keeps id).
    #[inline]
    pub fn negated(self) -> Self {
        Self {
            is_positive: !self.is_positive,
            ..self
        }
    }
}

impl fmt::Display for AtomSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // e.g. !1
        write!(
            f,
            "{}{}",
            if self.is_positive { "" } else { "!" },
            self.rhs_id
        )
    }
}

/// A signature referencing a specific argument within an atom.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct AtomArgumentSignature {
    atom_signature: AtomSignature,
    argument_id: usize,
}

impl AtomArgumentSignature {
    /// Create a new atom-argument signature.
    #[inline]
    pub fn new(atom_signature: AtomSignature, argument_id: usize) -> Self {
        Self {
            atom_signature,
            argument_id,
        }
    }

    /// Returns true if the underlying atom is positive.
    #[inline]
    pub fn is_positive(&self) -> bool {
        self.atom_signature.is_positive()
    }

    /// Reference to the underlying atom signature.
    #[inline]
    pub fn atom_signature(&self) -> &AtomSignature {
        &self.atom_signature
    }
}

impl fmt::Display for AtomArgumentSignature {
    // e.g. !2.0
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.atom_signature, self.argument_id)
    }
}
