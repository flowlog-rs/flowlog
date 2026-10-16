//! The aggregate step: plans an aggregate's braces as a body of their own,
//! keyed by the variables it reads from the enclosing body, and appends its
//! value to the atom covering it.

use super::RulePlanner;
use crate::catalog::Catalog;
use crate::optimizer::Optimizer;
use crate::planner::PlanError;

impl RulePlanner {
    /// Takes aggregate `index`: plans its braces, keyed by the variables it
    /// reads from this body, and appends its value to the atom covering it,
    /// then folds to a fixpoint.
    // TODO: lands with the `Aggregate` transformation; until then an
    // aggregate cannot be planned.
    pub(crate) fn aggregate(
        &mut self,
        catalog: &mut Catalog,
        index: usize,
        _optimizer: &mut Optimizer,
    ) -> Result<(), PlanError> {
        Err(PlanError::internal(format!(
            "aggregate {} of rule `{}` cannot be planned yet",
            catalog.aggregate_predicate(index)?,
            catalog.rule()
        )))
    }
}
