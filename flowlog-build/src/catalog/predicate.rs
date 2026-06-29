//! Predicate filter structs for planner transformations.

use std::fmt;

use crate::catalog::{AtomArgumentSignature, ComparisonExprPos};
use flowlog_parser::ConstType;

/// Write `" and "` before every term except the first. Toggles `first` to
/// `false` after running, so callers can reuse it across heterogeneous
/// term iterators without re-implementing the bookkeeping.
fn write_and_sep(f: &mut fmt::Formatter<'_>, first: &mut bool) -> fmt::Result {
    if !*first {
        write!(f, " and ")?;
    }
    *first = false;
    Ok(())
}

/// Predicate filters for a Join (or Anti-Join) to Key-Value transformation.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct JoinPredicates {
    pub(crate) compare_exprs: Vec<ComparisonExprPos>,
}

impl JoinPredicates {
    pub(crate) fn is_empty(&self) -> bool {
        self.compare_exprs.is_empty()
    }
}

impl fmt::Display for JoinPredicates {
    /// Joined with " and ". Empty when [`Self::is_empty`] is true.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for c in &self.compare_exprs {
            write_and_sep(f, &mut first)?;
            write!(f, "{}", c)?;
        }
        Ok(())
    }
}

/// Predicate filters for a Key-Value to Key-Value transformation.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub(crate) struct KvPredicates {
    pub(crate) const_eq: Vec<(AtomArgumentSignature, ConstType)>,
    pub(crate) var_eq: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
    pub(crate) compare_exprs: Vec<ComparisonExprPos>,
}

impl KvPredicates {
    pub(crate) fn is_empty(&self) -> bool {
        self.const_eq.is_empty() && self.var_eq.is_empty() && self.compare_exprs.is_empty()
    }
}

impl fmt::Display for KvPredicates {
    /// Joined with " and ". Empty when [`Self::is_empty`] is true.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (sig, c) in &self.const_eq {
            write_and_sep(f, &mut first)?;
            write!(f, "{} = {:?}", sig, c)?;
        }
        for (l, r) in &self.var_eq {
            write_and_sep(f, &mut first)?;
            write!(f, "{} = {}", l, r)?;
        }
        for c in &self.compare_exprs {
            write_and_sep(f, &mut first)?;
            write!(f, "{}", c)?;
        }
        Ok(())
    }
}
