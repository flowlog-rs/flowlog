//! Comparison expressions for Macaron Datalog programs.
//!
//! - [`ComparisonOperator`]: `== | ≠ | > | ≥ | < | ≤`
//! - [`ComparisonExpr`]: `{left} {op} {right}`
//!
//! # Example
//! ```rust
//! use parser::logic::{Arithmetic, ComparisonExpr, ComparisonOperator, Factor};
//! use parser::primitive::ConstType;
//! let lhs = Arithmetic::new(Factor::Var("age".into()), vec![]);
//! let rhs = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
//! let cmp = ComparisonExpr::new(lhs, ComparisonOperator::GreaterEqualThan, rhs);
//! assert_eq!(cmp.to_string(), "age ≥ 18");
//! ```

use super::Arithmetic;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashSet;
use std::fmt;

/// Comparison operator.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ComparisonOperator {
    Equal,            // ==
    NotEqual,         // ≠
    GreaterThan,      // >
    GreaterEqualThan, // ≥
    LessThan,         // <
    LessEqualThan,    // ≤
}

impl ComparisonOperator {
    #[must_use]
    #[inline]
    pub fn is_equal(&self) -> bool {
        matches!(self, Self::Equal)
    }
}

impl fmt::Display for ComparisonOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sym = match self {
            Self::Equal => "==",
            Self::NotEqual => "≠",
            Self::GreaterThan => ">",
            Self::GreaterEqualThan => "≥",
            Self::LessThan => "<",
            Self::LessEqualThan => "≤",
        };
        write!(f, "{sym}")
    }
}

impl Lexeme for ComparisonOperator {
    /// Parse a comparison operator from the grammar.
    ///
    /// # Panics
    /// Panics if the rule is not one of the expected operator tokens.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let op = parsed_rule
            .into_inner()
            .next()
            .expect("Parser error: comparison operator missing inner token");
        match op.as_rule() {
            Rule::equal => Self::Equal,
            Rule::not_equal => Self::NotEqual,
            Rule::greater_than => Self::GreaterThan,
            Rule::greater_equal_than => Self::GreaterEqualThan,
            Rule::less_than => Self::LessThan,
            Rule::less_equal_than => Self::LessEqualThan,
            other => panic!("Parser error: unknown comparison operator: {:?}", other),
        }
    }
}

/// `{left} {op} {right}` boolean comparison.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ComparisonExpr {
    left: Arithmetic,
    operator: ComparisonOperator,
    right: Arithmetic,
}

impl ComparisonExpr {
    /// Create a new comparison.
    #[must_use]
    pub fn new(left: Arithmetic, operator: ComparisonOperator, right: Arithmetic) -> Self {
        Self {
            left,
            operator,
            right,
        }
    }

    /// Left-hand expression.
    #[must_use]
    #[inline]
    pub fn left(&self) -> &Arithmetic {
        &self.left
    }

    /// Operator.
    #[must_use]
    #[inline]
    pub fn operator(&self) -> &ComparisonOperator {
        &self.operator
    }

    /// Right-hand expression.
    #[must_use]
    #[inline]
    pub fn right(&self) -> &Arithmetic {
        &self.right
    }

    /// Unique variables referenced on either side (deduplicated).
    #[must_use]
    pub fn vars_set(&self) -> HashSet<&String> {
        self.left
            .vars_set()
            .union(&self.right.vars_set())
            .cloned()
            .collect()
    }

    /// Variables from the left expression (order preserved, duplicates kept).
    #[must_use]
    pub fn left_vars(&self) -> Vec<&String> {
        self.left.vars()
    }

    /// Variables from the right expression (order preserved, duplicates kept).
    #[must_use]
    pub fn right_vars(&self) -> Vec<&String> {
        self.right.vars()
    }
}

impl fmt::Display for ComparisonExpr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} {} {}", self.left, self.operator, self.right)
    }
}

impl Lexeme for ComparisonExpr {
    /// Parse `arithmetic ~ comparison_operator ~ arithmetic`.
    ///
    /// # Panics
    /// Panics if any of the three parts are missing.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        let left_pair = inner
            .next()
            .expect("Parser error: comparison missing left expression");
        let op_pair = inner
            .next()
            .expect("Parser error: comparison missing operator");
        let right_pair = inner
            .next()
            .expect("Parser error: comparison missing right expression");

        let left = Arithmetic::from_parsed_rule(left_pair);
        let operator = ComparisonOperator::from_parsed_rule(op_pair);
        let right = Arithmetic::from_parsed_rule(right_pair);

        Self::new(left, operator, right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Arithmetic, Factor};
    use crate::primitive::ConstType;
    use ComparisonOperator::*;

    fn var(name: &str) -> Arithmetic {
        Arithmetic::new(Factor::Var(name.into()), vec![])
    }
    fn iconst(v: i32) -> Arithmetic {
        Arithmetic::new(Factor::Const(ConstType::Integer(v)), vec![])
    }

    #[test]
    fn operator_display_and_predicate() {
        assert_eq!(Equal.to_string(), "==");
        assert_eq!(NotEqual.to_string(), "≠");
        assert_eq!(GreaterThan.to_string(), ">");
        assert_eq!(GreaterEqualThan.to_string(), "≥");
        assert_eq!(LessThan.to_string(), "<");
        assert_eq!(LessEqualThan.to_string(), "≤");
        assert!(Equal.is_equal());
        assert!(!NotEqual.is_equal());
    }

    #[test]
    fn expr_basics_and_display() {
        let e1 = ComparisonExpr::new(var("x"), Equal, var("y"));
        assert_eq!(e1.to_string(), "x == y");

        let e2 = ComparisonExpr::new(var("age"), GreaterEqualThan, iconst(18));
        assert_eq!(e2.to_string(), "age ≥ 18");

        let e3 = ComparisonExpr::new(iconst(0), LessThan, var("score"));
        assert_eq!(e3.to_string(), "0 < score");
    }

    #[test]
    fn vars_accessors() {
        let e = ComparisonExpr::new(var("x"), NotEqual, var("y"));
        let set = e.vars_set();
        assert_eq!(set.len(), 2);
        let x_str = "x".to_string();
        let y_str = "y".to_string();
        assert!(set.contains(&x_str));
        assert!(set.contains(&y_str));

        assert_eq!(e.left_vars(), vec![&"x".to_string()]);
        assert_eq!(e.right_vars(), vec![&"y".to_string()]);

        let dedup = ComparisonExpr::new(var("x"), Equal, var("x"));
        assert_eq!(dedup.vars_set().len(), 1);
    }

    #[test]
    fn clone_hash_eq() {
        let a = ComparisonExpr::new(var("n"), LessEqualThan, iconst(10));
        let b = a.clone();
        assert_eq!(a, b);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(a);
        set.insert(b);
        assert_eq!(set.len(), 1);
    }
}
