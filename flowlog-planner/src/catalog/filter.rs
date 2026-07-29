//! One rule's local filters: variable-equality, constant, and
//! placeholder constraints on atom argument positions. Iteration follows
//! argument-signature order.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;

use flowlog_parser::Constant;

use crate::catalog::AtomArgumentSignature;

/// Constraints a single rule imposes on its atom argument positions.
///
/// Examples use `atom.argument` signatures: `0.1` is argument 1 of
/// positive atom 0, while `!0.1` is argument 1 of negative atom 0.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Filters {
    /// Maps a repeated variable occurrence to the variable's first
    /// occurrence within the same atom.
    ///
    /// For `A(x, x)`, this contains `0.1 -> 0.0`: the second argument
    /// must equal the first.
    var_eq_map: BTreeMap<AtomArgumentSignature, AtomArgumentSignature>,

    /// Maps an argument position to the constant it must equal.
    ///
    /// For `A(x, 5)`, this contains `0.1 -> 5`.
    const_map: BTreeMap<AtomArgumentSignature, Constant>,

    /// Argument positions holding a placeholder (`_`).
    ///
    /// For `A(x, _)`, this contains `0.1`; that position is projected
    /// away without an equality predicate.
    placeholder_set: BTreeSet<AtomArgumentSignature>,
}

impl Filters {
    pub(crate) fn new(
        var_eq_map: BTreeMap<AtomArgumentSignature, AtomArgumentSignature>,
        const_map: BTreeMap<AtomArgumentSignature, Constant>,
        placeholder_set: BTreeSet<AtomArgumentSignature>,
    ) -> Self {
        Self {
            var_eq_map,
            const_map,
            placeholder_set,
        }
    }

    #[inline]
    pub(crate) fn var_eq_map(&self) -> &BTreeMap<AtomArgumentSignature, AtomArgumentSignature> {
        &self.var_eq_map
    }

    #[inline]
    pub(crate) fn const_map(&self) -> &BTreeMap<AtomArgumentSignature, Constant> {
        &self.const_map
    }

    #[inline]
    pub(crate) fn placeholder_set(&self) -> &BTreeSet<AtomArgumentSignature> {
        &self.placeholder_set
    }

    /// Returns `true` if `arg` is constrained by any filter kind.
    pub(crate) fn is_const_or_var_eq_or_placeholder(&self, arg: &AtomArgumentSignature) -> bool {
        self.var_eq_map.contains_key(arg)
            || self.const_map.contains_key(arg)
            || self.placeholder_set.contains(arg)
    }

    /// Returns `true` if there are no constraints of any kind.
    #[inline]
    pub(crate) fn is_empty(&self) -> bool {
        self.var_eq_map.is_empty() && self.const_map.is_empty() && self.placeholder_set.is_empty()
    }
}

impl fmt::Display for Filters {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_empty() {
            return writeln!(f, "Filters: (empty)");
        }

        writeln!(f, "Filters:")?;

        if !self.var_eq_map.is_empty() {
            writeln!(f, "  Variable Equality Constraints:")?;
            for (var, target) in &self.var_eq_map {
                writeln!(f, "    {} = {}", var, target)?;
            }
        }

        if !self.const_map.is_empty() {
            writeln!(f, "  Constant Constraints:")?;
            for (var, constant) in &self.const_map {
                writeln!(f, "    {} = {}", var, constant)?;
            }
        }

        if !self.placeholder_set.is_empty() {
            writeln!(f, "  Placeholder Variables:")?;
            for placeholder in &self.placeholder_set {
                writeln!(f, "    {}", placeholder)?;
            }
        }

        Ok(())
    }
}
