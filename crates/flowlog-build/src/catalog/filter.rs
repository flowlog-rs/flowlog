//! Filter expression for FlowLog Datalog programs.

use crate::catalog::AtomArgumentSignature;
use crate::parser::ConstType;
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Base constraint filters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Filters {
    /// Maps variables to other variables they must equal
    var_eq_map: HashMap<AtomArgumentSignature, AtomArgumentSignature>,

    /// Maps variables to constant values they must equal
    const_map: HashMap<AtomArgumentSignature, ConstType>,

    /// Set of variables that are treated as placeholders
    placeholder_set: HashSet<AtomArgumentSignature>,
}

impl Filters {
    /// Creates a filters with the specified constraints.
    pub(crate) fn new(
        var_eq_map: HashMap<AtomArgumentSignature, AtomArgumentSignature>,
        const_map: HashMap<AtomArgumentSignature, ConstType>,
        placeholder_set: HashSet<AtomArgumentSignature>,
    ) -> Self {
        Self {
            var_eq_map,
            const_map,
            placeholder_set,
        }
    }

    /// Returns a reference to the variable equality constraints map.
    #[inline]
    pub(crate) fn var_eq_map(&self) -> &HashMap<AtomArgumentSignature, AtomArgumentSignature> {
        &self.var_eq_map
    }

    /// Returns a reference to the constant equality constraints map.
    #[inline]
    pub(crate) fn const_map(&self) -> &HashMap<AtomArgumentSignature, ConstType> {
        &self.const_map
    }

    /// Returns a reference to the placeholder variables set.
    #[inline]
    pub(crate) fn placeholder_set(&self) -> &HashSet<AtomArgumentSignature> {
        &self.placeholder_set
    }

    /// Returns if the given args has any constraints.
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

        // Variable equality constraints
        if !self.var_eq_map.is_empty() {
            writeln!(f, "  Variable Equality Constraints:")?;
            for (var, target) in &self.var_eq_map {
                writeln!(f, "    {} = {}", var, target)?;
            }
        }

        // Constant equality constraints
        if !self.const_map.is_empty() {
            writeln!(f, "  Constant Constraints:")?;
            for (var, constant) in &self.const_map {
                writeln!(f, "    {} = {}", var, constant)?;
            }
        }

        // Placeholder variables
        if !self.placeholder_set.is_empty() {
            writeln!(f, "  Placeholder Variables:")?;
            for placeholder in &self.placeholder_set {
                writeln!(f, "    {}", placeholder)?;
            }
        }

        Ok(())
    }
}
