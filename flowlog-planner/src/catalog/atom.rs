//! Body-atom and atom-argument signatures used by the catalog.

use std::fmt;

/// Identifies one body atom by polarity and its zero-based index among
/// atoms with that polarity.
///
/// In `Out(x) :- A(x, y), C(y), !B(y).`, `A` is `0`, `C` is `1`,
/// and `B` is `!0`. Positive and negative atoms use separate counters;
/// these are not positions in the complete rule body.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub(crate) struct AtomSignature {
    is_positive: bool,
    rhs_id: usize,
}

impl AtomSignature {
    #[inline]
    pub(crate) fn new(is_positive: bool, rhs_id: usize) -> Self {
        Self {
            is_positive,
            rhs_id,
        }
    }

    #[inline]
    pub(crate) fn is_positive(&self) -> bool {
        self.is_positive
    }

    /// Index of the atom within its polarity's body atoms, not within
    /// the whole rule body.
    #[inline]
    pub(crate) fn rhs_id(&self) -> usize {
        self.rhs_id
    }
}

impl fmt::Display for AtomSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}{}",
            if self.is_positive { "" } else { "!" },
            self.rhs_id
        )
    }
}

/// Identifies one zero-based argument position inside a body atom.
///
/// In `Out(x) :- A(x, y), C(y), !B(y).`, `0.1` identifies `y` in
/// `A(x, y)`, while `!0.0` identifies `y` in `!B(y)`.
#[derive(Clone, Copy, Hash, PartialEq, Eq, Ord, PartialOrd, Debug)]
pub(crate) struct AtomArgumentSignature {
    atom_signature: AtomSignature,
    argument_id: usize,
}

impl AtomArgumentSignature {
    #[inline]
    pub(crate) fn new(atom_signature: AtomSignature, argument_id: usize) -> Self {
        Self {
            atom_signature,
            argument_id,
        }
    }

    #[inline]
    pub(crate) fn is_positive(&self) -> bool {
        self.atom_signature.is_positive()
    }

    #[inline]
    pub(crate) fn atom_signature(&self) -> &AtomSignature {
        &self.atom_signature
    }

    /// Index of the argument within the atom (zero-based).
    #[inline]
    pub(crate) fn argument_id(&self) -> usize {
        self.argument_id
    }
}

impl fmt::Display for AtomArgumentSignature {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}.{}", self.atom_signature, self.argument_id)
    }
}
