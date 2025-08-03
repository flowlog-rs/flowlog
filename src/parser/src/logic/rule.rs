//! FlowLog rule structures.

use super::{Factor, Head, HeadArg, Predicate};
use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Represents a complete FlowLog rule.
///
/// A rule consists of a head (what is derived) and a body of predicates (conditions).
/// Rules follow the structure: `head :- body.` where the head is derived when all
/// predicates in the body are satisfied.
///
/// Rules can optionally be marked for planning optimization with the `.plan` suffix.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{FLRule, Head, HeadArg, Predicate};
///
/// // Create rule: result(X) :- input(X).
/// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
/// let body = vec![Predicate::BoolPredicate(true)];
/// let rule = FLRule::new(head, body, false);
///
/// assert!(!rule.is_planning());
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FLRule {
    head: Head,
    rhs: Vec<Predicate>,
    is_planning: bool,
}

impl fmt::Display for FLRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|pred| pred.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl FLRule {
    /// Creates a new rule.
    #[must_use]
    pub fn new(head: Head, rhs: Vec<Predicate>, is_planning: bool) -> Self {
        Self {
            head,
            rhs,
            is_planning,
        }
    }

    /// Returns the head of this rule.
    #[must_use]
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Returns the right-hand side (body) predicates of this rule.
    #[must_use]
    pub fn rhs(&self) -> &[Predicate] {
        &self.rhs
    }

    /// Checks if this rule is marked for planning optimization.
    #[must_use]
    pub fn is_planning(&self) -> bool {
        self.is_planning
    }

    /// Checks if this rule contains any boolean predicates.
    #[must_use]
    pub fn is_boolean(&self) -> bool {
        self.rhs.iter().any(|pred| pred.is_boolean())
    }

    /// Returns the predicate at the given index in the right-hand side
    pub fn get(&self, i: usize) -> &Predicate {
        &self.rhs[i]
    }

    /// Extracts constant values from the head of a boolean rule.
    ///
    /// This method is used during boolean fact extraction to get the constant
    /// values that should be inserted into the boolean facts map.
    ///
    /// # Returns
    ///
    /// A vector of `Const` values representing the constants in the rule head.
    ///
    /// # Panics
    ///
    /// Panics if the head contains any non-constant arguments, indicating the rule
    /// is not actually a boolean rule.
    ///
    /// # Examples
    ///
    /// For a rule like `fact(1, "hello") :- True.`, this returns `[Const::Int(1), Const::Str("hello")]`.
    #[must_use]
    pub fn extract_constants_from_head(&self) -> Vec<ConstType> {
        let head_args = self.head.head_arguments();
        let mut constants = Vec::new();

        for arg in head_args {
            match arg {
                HeadArg::Var(_) => {
                    panic!("Boolean rule head must contain only constants: {self}")
                }
                HeadArg::GroupBy(_) => {
                    panic!("Boolean rule head must contain only constants: {self}")
                }
                HeadArg::Arith(arith) => {
                    // Only allow simple constant arithmetic expressions
                    if arith.is_const() {
                        if let Factor::Const(c) = arith.init() {
                            constants.push(c.clone());
                        }
                    } else {
                        panic!("Boolean rule head must contain only constants: {self}");
                    }
                }
            }
        }

        constants
    }
}

impl Lexeme for FLRule {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        // Parse head
        let head = Head::from_parsed_rule(inner.next().unwrap());

        // Parse predicates
        let predicates_rule = inner.next().unwrap();
        let rhs: Vec<Predicate> = predicates_rule
            .into_inner()
            .map(Predicate::from_parsed_rule)
            .collect();

        // Check for optimization directive
        let is_planning = inner.next().is_some(); // If there's an optimize rule

        Self::new(head, rhs, is_planning)
    }
}
