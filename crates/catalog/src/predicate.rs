//! Predicate filter structs for planner transformations.

use std::fmt;

use crate::atom::AtomArgumentSignature;
use crate::compare::ComparisonExprPos;
use crate::fn_call::FnCallPredicatePos;
use parser::ConstType;

/// Predicate filters for a Join (or Anti-Join) to Key-Value transformation.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct JoinPredicates {
    pub compare_exprs: Vec<ComparisonExprPos>,
    pub fn_call_preds: Vec<FnCallPredicatePos>,
}

impl JoinPredicates {
    pub fn is_empty(&self) -> bool {
        self.compare_exprs.is_empty() && self.fn_call_preds.is_empty()
    }
}

impl fmt::Display for JoinPredicates {
    /// Joined with " and ". Empty when [`Self::is_empty`] is true.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for c in &self.compare_exprs {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{}", c)?;
            first = false;
        }
        for fc in &self.fn_call_preds {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{}", fc)?;
            first = false;
        }
        Ok(())
    }
}

/// Predicate filters for a Key-Value to Key-Value transformation.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct KvPredicates {
    pub const_eq: Vec<(AtomArgumentSignature, ConstType)>,
    pub var_eq: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
    pub compare_exprs: Vec<ComparisonExprPos>,
    pub fn_call_preds: Vec<FnCallPredicatePos>,
}

impl KvPredicates {
    pub fn is_empty(&self) -> bool {
        self.const_eq.is_empty()
            && self.var_eq.is_empty()
            && self.compare_exprs.is_empty()
            && self.fn_call_preds.is_empty()
    }
}

impl fmt::Display for KvPredicates {
    /// Joined with " and ". Empty when [`Self::is_empty`] is true.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut first = true;
        for (sig, c) in &self.const_eq {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{} = {:?}", sig, c)?;
            first = false;
        }
        for (l, r) in &self.var_eq {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{} = {}", l, r)?;
            first = false;
        }
        for c in &self.compare_exprs {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{}", c)?;
            first = false;
        }
        for fc in &self.fn_call_preds {
            if !first {
                write!(f, " and ")?;
            }
            write!(f, "{}", fc)?;
            first = false;
        }
        Ok(())
    }
}
