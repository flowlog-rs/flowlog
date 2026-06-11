//! Constraints representation for query planning in FlowLog Datalog programs.

use crate::common::{compute_fp, compute_fp_unordered};
use crate::parser::ConstType;
use crate::planner::TransformationArgument;
use std::fmt;
use std::sync::Arc;

/// Represents constraints in a query plan.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub(crate) struct Constraints {
    /// Constraints that equate a variable (transformation arg) to a constant value (e.g., `x = 5`)
    constant_eq_constraints: Arc<Vec<(TransformationArgument, ConstType)>>,

    /// Constraints that equate two variables (transformation args), e.g., `x = y`
    variable_eq_constraints: Arc<Vec<(TransformationArgument, TransformationArgument)>>,
}

impl Constraints {
    /// Creates a new Constraints instance.
    pub(crate) fn new(
        constant_eq_constraints: Vec<(TransformationArgument, ConstType)>,
        variable_eq_constraints: Vec<(TransformationArgument, TransformationArgument)>,
    ) -> Self {
        Self {
            constant_eq_constraints: Arc::new(constant_eq_constraints),
            variable_eq_constraints: Arc::new(variable_eq_constraints),
        }
    }

    /// Returns the constant equality constraints.
    pub(crate) fn constant_eq_constraints(&self) -> &Arc<Vec<(TransformationArgument, ConstType)>> {
        &self.constant_eq_constraints
    }

    /// Returns the variable equality constraints.
    pub(crate) fn variable_eq_constraints(
        &self,
    ) -> &Arc<Vec<(TransformationArgument, TransformationArgument)>> {
        &self.variable_eq_constraints
    }

    /// Checks if this constraint set is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.constant_eq_constraints.is_empty() && self.variable_eq_constraints.is_empty()
    }

    /// Order-independent content fingerprint of the constraint set, for
    /// cross-rule arrangement sharing. Equality constraints are conjunctive —
    /// `x = 5, y = z` keeps the same rows as `y = z, x = 5` — so each kind is
    /// hashed as an order-independent multiset, with the two kinds kept separate
    /// so a constant- and a variable-equality can never collide.
    pub(crate) fn content_key(&self) -> u64 {
        compute_fp((
            "constraints_content_v1",
            compute_fp_unordered(self.constant_eq_constraints.iter()),
            compute_fp_unordered(self.variable_eq_constraints.iter()),
        ))
    }
}

impl fmt::Display for Constraints {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut constraints = Vec::new();

        // Format constant constraints like `x = 3`
        for (arg, constant) in self.constant_eq_constraints.iter() {
            constraints.push(format!("{} = {}", arg, constant));
        }

        // Format variable equality constraints like `x = y`
        for (left_arg, right_arg) in self.variable_eq_constraints.iter() {
            constraints.push(format!("{} = {}", left_arg, right_arg));
        }

        // Join all constraints with ", " and write them on a single line
        write!(f, "{}", constraints.join(", "))
    }
}

#[cfg(test)]
mod content_key_tests {
    use super::*;

    fn arg(idx: usize) -> TransformationArgument {
        TransformationArgument::KV((false, idx))
    }

    /// Equality constraints are conjunctive, so the order the planner records
    /// them in must not change the content key (else two rules with the same
    /// filters in a different order would fail to share).
    #[test]
    fn content_key_ignores_order() {
        let c1 = Constraints::new(
            vec![(arg(0), ConstType::Int(5)), (arg(1), ConstType::Int(7))],
            vec![(arg(0), arg(2))],
        );
        let c2 = Constraints::new(
            // same constants, reversed
            vec![(arg(1), ConstType::Int(7)), (arg(0), ConstType::Int(5))],
            vec![(arg(0), arg(2))],
        );
        assert_eq!(
            c1.content_key(),
            c2.content_key(),
            "the same constraints in a different order must share a content key"
        );
    }

    /// A genuinely different constraint set must not collapse onto another.
    #[test]
    fn content_key_distinguishes_real_differences() {
        let base = Constraints::new(vec![(arg(0), ConstType::Int(5))], vec![]);
        // Different constant value.
        assert_ne!(
            base.content_key(),
            Constraints::new(vec![(arg(0), ConstType::Int(6))], vec![]).content_key(),
            "constant value must matter"
        );
        // Different column.
        assert_ne!(
            base.content_key(),
            Constraints::new(vec![(arg(1), ConstType::Int(5))], vec![]).content_key(),
            "constrained column must matter"
        );
        // A constant-equality and a variable-equality with the same shape must
        // not collide (the two kinds are hashed separately).
        assert_ne!(
            Constraints::new(vec![(arg(0), ConstType::Int(1))], vec![]).content_key(),
            Constraints::new(vec![], vec![(arg(0), arg(1))]).content_key(),
            "constant-eq and variable-eq must not collide"
        );
    }
}
