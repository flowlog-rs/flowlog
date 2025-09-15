//! Constraints representation for query planning in Macaron Datalog programs.

use crate::argument::TransformationArgument;
use parser::ConstType;
use std::fmt;
use std::sync::Arc;

/// Represents constraints in a query plan.
#[derive(Debug, Hash, Clone, PartialEq, Eq)]
pub struct Constraints {
    /// Constraints that equate a variable (transformation arg) to a constant value (e.g., `x = 5`)
    constant_eq_constraints: Arc<Vec<(TransformationArgument, ConstType)>>,

    /// Constraints that equate two variables (transformation args), e.g., `x = y`
    variable_eq_constraints: Arc<Vec<(TransformationArgument, TransformationArgument)>>,
}

impl Constraints {
    /// Creates a new Constraints instance.
    pub fn new(
        constant_eq_constraints: Vec<(TransformationArgument, ConstType)>,
        variable_eq_constraints: Vec<(TransformationArgument, TransformationArgument)>,
    ) -> Self {
        Self {
            constant_eq_constraints: Arc::new(constant_eq_constraints),
            variable_eq_constraints: Arc::new(variable_eq_constraints),
        }
    }

    /// Returns the constant equality constraints.
    pub fn constant_eq_constraints(&self) -> &Arc<Vec<(TransformationArgument, ConstType)>> {
        &self.constant_eq_constraints
    }

    /// Returns the variable equality constraints.
    pub fn variable_eq_constraints(
        &self,
    ) -> &Arc<Vec<(TransformationArgument, TransformationArgument)>> {
        &self.variable_eq_constraints
    }

    /// Checks if this constraint set is empty.
    pub fn is_empty(&self) -> bool {
        self.constant_eq_constraints.is_empty() && self.variable_eq_constraints.is_empty()
    }

    /// Returns all transformation arguments referenced in these constraints.
    pub fn transformation_arguments(&self) -> Vec<&TransformationArgument> {
        let mut args = Vec::new();

        // Collect arguments from constant constraints
        for (arg, _) in self.constant_eq_constraints.iter() {
            args.push(arg);
        }

        // Collect arguments from variable constraints
        for (left_arg, right_arg) in self.variable_eq_constraints.iter() {
            args.push(left_arg);
            args.push(right_arg);
        }

        args
    }

    /// Creates a new Constraints instance with all join arguments flipped.
    pub fn jn_flip(&self) -> Self {
        let constant_eq_constraints = self
            .constant_eq_constraints
            .iter()
            .map(|(arg, constant)| (arg.jn_flip(), constant.clone()))
            .collect();

        let variable_eq_constraints = self
            .variable_eq_constraints
            .iter()
            .map(|(left_arg, right_arg)| (left_arg.jn_flip(), right_arg.jn_flip()))
            .collect();

        Self::new(constant_eq_constraints, variable_eq_constraints)
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
