//! Predicate filter structs for planner transformations.

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

/// Predicate filters for a Key-Value to Key-Value transformation.
#[derive(Default, Clone, PartialEq, Eq, Hash, Debug)]
pub struct KvPredicates {
    pub const_eq: Vec<(AtomArgumentSignature, ConstType)>,
    pub var_eq: Vec<(AtomArgumentSignature, AtomArgumentSignature)>,
    pub compare_exprs: Vec<ComparisonExprPos>,
    pub fn_call_preds: Vec<FnCallPredicatePos>,
}
