//! Rule heads for Macaron Datalog programs.
//!
//! - [`HeadArg`]: `Var | Arith | Aggregation`
//! - [`Head`]: `rel(arg1, ..., argN)`
//!
//! # Example
//! ```rust
//! use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
//! use parser::primitive::ConstType;
//! let inc = Arithmetic::new(
//!     Factor::Var("Y".into()),
//!     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(10)))],
//! );
//! let head = Head::new("result".into(), vec![HeadArg::Var("X".into()), HeadArg::Arith(inc)]);
//! assert_eq!(head.to_string(), "result(X, Y + 10)");
//! ```

use super::{Aggregation, Arithmetic};
use crate::{error::ParserError, Lexeme, Result, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Argument in a rule head.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// Pass-through variable.
    Var(String),
    /// Arithmetic expression.
    Arith(Arithmetic),
    /// Aggregation (e.g., `count(X)`).
    Aggregation(Aggregation),
}

impl HeadArg {
    /// Variables referenced by this argument (order preserved, duplicates kept).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(v) => vec![v],
            Self::Arith(a) => a.vars(),
            Self::Aggregation(agg) => agg.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(v) => write!(f, "{v}"),
            Self::Arith(a) => write!(f, "{a}"),
            Self::Aggregation(agg) => write!(f, "{agg}"),
        }
    }
}

impl Lexeme for HeadArg {
    /// Parse a head argument from the grammar.
    ///
    /// Optimization: if the arithmetic is a single variable (`is_var()`), emit `Var` instead of `Arith`.
    ///
    /// # Return errors
    /// Return errors on unknown/unsupported inner rule.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Result<Self> {
        // Defensive: head_arg inner must be arithmetic_expr or aggregate_expr.
        let inner = parsed_rule
            .into_inner()
            .next()
            .ok_or_else(|| ParserError::IncompleteHeadArg("inner token".to_string()))?;

        match inner.as_rule() {
            Rule::arithmetic_expr => {
                let arith = Arithmetic::from_parsed_rule(inner)?;
                if arith.is_var() {
                    // `init()` must be a `Factor::Var` when `is_var()` is true.
                    let name = arith
                        .init()
                        .vars()
                        .into_iter()
                        .next()
                        .expect("Parser bug: is_var() but no variable in init")
                        .clone();
                    Ok(Self::Var(name))
                } else {
                    Ok(Self::Arith(arith))
                }
            }
            Rule::aggregate_expr => Ok(Self::Aggregation(Aggregation::from_parsed_rule(inner)?)),
            other => Err(ParserError::UnexpectedRule(
                "head argument".to_string(),
                format!("{:?}", other),
            )),
        }
    }
}

/// `rel(arg1, ..., argN)`
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Head {
    name: String,
    head_arguments: Vec<HeadArg>,
}

impl Head {
    /// Create a new rule head.
    #[must_use]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        Self {
            name,
            head_arguments,
        }
    }

    /// Relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Arguments.
    #[must_use]
    #[inline]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    /// Arity (number of arguments).
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        let mut first = true;
        for arg in &self.head_arguments {
            if !first {
                write!(f, ", ")?;
            }
            first = false;
            write!(f, "{arg}")?;
        }
        write!(f, ")")
    }
}

impl Lexeme for Head {
    /// Parse `relation_name "(" (head_arg ("," head_arg)*)? ")"`.
    ///
    /// # Return errors
    /// Return errors if the name or argument list is malformed/missing.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Result<Self> {
        let mut inner = parsed_rule.into_inner();

        // Defensive: head begins with relation_name per grammar.
        let name = inner
            .next()
            .ok_or_else(|| ParserError::IncompleteHead("relation name".to_string()))?
            .as_str()
            .to_string();

        let mut args = Vec::new();
        for pair in inner {
            if pair.as_rule() == Rule::head_arg {
                args.push(HeadArg::from_parsed_rule(pair)?);
            }
        }

        Ok(Self::new(name, args))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{ArithmeticOperator, Factor};
    use crate::primitive::ConstType;

    fn var_arg(s: &str) -> HeadArg {
        HeadArg::Var(s.into())
    }
    fn arith_var(s: &str) -> Arithmetic {
        Arithmetic::new(Factor::Var(s.into()), vec![])
    }
    fn arith_plus_x_5() -> Arithmetic {
        Arithmetic::new(
            Factor::Var("X".into()),
            vec![(
                ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(5)),
            )],
        )
    }

    #[test]
    fn headarg_vars_and_display() {
        let v = var_arg("P");
        assert_eq!(v.vars(), vec![&"P".to_string()]);
        assert_eq!(v.to_string(), "P");

        let a = HeadArg::Arith(arith_plus_x_5());
        assert_eq!(a.vars(), vec![&"X".to_string()]);
        assert_eq!(a.to_string(), "X + 5");

        let agg = HeadArg::Aggregation(Aggregation::new(
            super::super::AggregationOperator::Count,
            arith_var("X"),
        ));
        assert_eq!(agg.vars(), vec![&"X".to_string()]);
        assert_eq!(agg.to_string(), "count(X)");
    }

    #[test]
    fn head_basics() {
        let h = Head::new("person".into(), vec![var_arg("Name"), var_arg("Age")]);
        assert_eq!(h.name(), "person");
        assert_eq!(h.arity(), 2);
        assert_eq!(h.to_string(), "person(Name, Age)");
    }

    #[test]
    fn head_nullary_and_mixed() {
        let nullary = Head::new("flag".into(), vec![]);
        assert_eq!(nullary.arity(), 0);
        assert_eq!(nullary.to_string(), "flag()");

        let mixed = Head::new(
            "computed".into(),
            vec![var_arg("X"), HeadArg::Arith(arith_plus_x_5())],
        );
        assert_eq!(mixed.to_string(), "computed(X, X + 5)");
    }

    #[test]
    fn head_clone_hash_eq() {
        let h = Head::new("t".into(), vec![var_arg("X")]);
        let c = h.clone();
        assert_eq!(h, c);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(h);
        set.insert(c);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn headarg_arith_only_constants_has_no_vars() {
        let a = HeadArg::Arith(Arithmetic::new(
            Factor::Const(ConstType::Integer(10)),
            vec![(
                ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(5)),
            )],
        ));
        assert!(a.vars().is_empty());
        assert_eq!(a.to_string(), "10 + 5");
    }
}
