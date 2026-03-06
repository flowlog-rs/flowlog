//! FnCall predicate signatures for FlowLog Datalog programs.

use crate::arithmetic::ArithmeticPos;
use std::fmt;

/// A fn_call predicate with variables resolved to their concrete positions.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct FnCallPredicatePos {
    name: String,
    args: Vec<ArithmeticPos>,
    is_negated: bool,
}

impl FnCallPredicatePos {
    /// Construct a new positional fn_call predicate.
    pub fn new(name: String, args: Vec<ArithmeticPos>, is_negated: bool) -> Self {
        Self { name, args, is_negated }
    }

    /// Returns the function name.
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the argument arithmetic positions.
    #[inline]
    pub fn args(&self) -> &[ArithmeticPos] {
        &self.args
    }

    /// Whether the UDF result is negated.
    #[inline]
    pub fn is_negated(&self) -> bool {
        self.is_negated
    }
}

impl fmt::Display for FnCallPredicatePos {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let args_str = self
            .args
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(", ");
        if self.is_negated {
            write!(f, "[!{}({})]", self.name, args_str)
        } else {
            write!(f, "[{}({})]", self.name, args_str)
        }
    }
}
