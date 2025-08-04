//! FlowLog rule structures.
//!
//! This module provides types for representing complete FlowLog rules, which are the
//! fundamental building blocks of logic programs. Rules define logical implications
//! that specify how new facts can be derived from existing facts and conditions.
//!
//! # Overview
//!
//! The module defines the [`FLRule`] struct which represents complete logical rules
//! consisting of:
//! - **Head**: What fact is derived when the rule fires
//! - **Body**: Conditions that must be satisfied for the rule to apply
//! - **Planning flag**: Optional optimization directive for rule processing
//!
//! # Rule Structure
//!
//! FlowLog rules follow the standard logical implication syntax:
//! ```text
//! head :- body.
//! ```
//!
//! Where:
//! - `head` is a single atom defining what is derived
//! - `body` is a comma-separated list of predicates (conditions)
//! - The rule fires when all body predicates are satisfied
//!
//! # Rule Types
//!
//! ## Fact Rules
//! Rules with only boolean `True` predicates effectively define facts:
//! ```text
//! person("Alice", 25) :- True.
//! ```
//!
//! ## Query Rules
//! Rules used for querying with complex conditions:
//! ```text
//! result(X, Y) :- relation1(X, Z), relation2(Z, Y), !blocked(X).
//! ```
//!
//! # Planning Optimization
//!
//! Rules can be marked for planning optimization, which enables advanced
//! query optimization strategies during rule evaluation.
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
//! use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
//! use parser::primitive::ConstType;
//!
//! // Simple derivation rule: adult(Person) :- person(Person, Age), Age >= 18
//! let adult_head = Head::new("adult".to_string(), vec![
//!     HeadArg::Var("Person".to_string())
//! ]);
//!
//! let person_atom = Atom::new("person", vec![
//!     AtomArg::Var("Person".to_string()),
//!     AtomArg::Var("Age".to_string())
//! ]);
//!
//! let age_var = Arithmetic::new(Factor::Var("Age".to_string()), vec![]);
//! let adult_age = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
//! let age_check = ComparisonExpr::new(age_var, ComparisonOperator::GreaterEqualThan, adult_age);
//!
//! let adult_body = vec![
//!     Predicate::PositiveAtomPredicate(person_atom),
//!     Predicate::ComparePredicate(age_check)
//! ];
//!
//! let adult_rule = FLRule::new(adult_head, adult_body, false);
//! assert_eq!(adult_rule.to_string(), "adult(Person) :- person(Person, Age), Age ≥ 18.");
//!
//! // Fact rule: connected() :- True
//! let fact_head = Head::new("connected".to_string(), vec![]);
//! let fact_body = vec![Predicate::BoolPredicate(true)];
//! let fact_rule = FLRule::new(fact_head, fact_body, false);
//! assert!(fact_rule.is_boolean());
//!
//! // Planning-optimized rule
//! let opt_head = Head::new("optimized".to_string(), vec![HeadArg::Var("X".to_string())]);
//! let opt_body = vec![Predicate::BoolPredicate(true)];
//! let opt_rule = FLRule::new(opt_head, opt_body, true);
//! assert!(opt_rule.is_planning());
//! ```

use super::{Factor, Head, HeadArg, Predicate};
use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Represents a complete FlowLog rule.
///
/// A FlowLog rule encapsulates a logical implication that defines how new facts
/// can be derived from existing facts and conditions. Rules are the core mechanism
/// for expressing logical relationships and computational processes in FlowLog programs.
///
/// # Structure
///
/// A rule consists of three main components:
/// - **Head**: A single [`Head`] defining what fact is derived when the rule fires
/// - **Body (RHS)**: A vector of [`Predicate`]s that must all be satisfied
/// - **Planning flag**: A boolean indicating whether planning optimization is enabled
///
/// # Rule Types by Purpose
///
/// ## Fact Definition Rules
/// Rules that define ground facts, typically with boolean `True` bodies:
/// ```text
/// person("Alice", 25) :- True.
/// ```
///
/// ## Constraint Rules
/// Rules that enforce conditions with negation and comparisons:
/// ```text
/// valid_user(U) :- user(U), !blocked(U), reputation(U, R), R > 10.
/// ```
///
/// # Planning Optimization
///
/// Rules can be marked for planning optimization, which enables:
/// - Advanced query reordering for performance
/// - Cost-based optimization strategies
/// - Specialized execution paths for complex queries
///
/// # Boolean Rules
///
/// Rules containing boolean predicates are often used for fact insertion
/// and testing. The `extract_constants_from_head()` method supports
/// extracting constant values from boolean fact rules.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
/// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
/// use parser::primitive::ConstType;
///
/// // Complex derivation rule: eligible(Student, Course) :-
/// //   enrolled(Student, Course), grade(Student, Course, G), G >= 70, !dropped(Student)
/// let eligible_head = Head::new("eligible".to_string(), vec![
///     HeadArg::Var("Student".to_string()),
///     HeadArg::Var("Course".to_string())
/// ]);
///
/// let enrolled_atom = Atom::new("enrolled", vec![
///     AtomArg::Var("Student".to_string()),
///     AtomArg::Var("Course".to_string())
/// ]);
///
/// let grade_atom = Atom::new("grade", vec![
///     AtomArg::Var("Student".to_string()),
///     AtomArg::Var("Course".to_string()),
///     AtomArg::Var("G".to_string())
/// ]);
///
/// let grade_var = Arithmetic::new(Factor::Var("G".to_string()), vec![]);
/// let passing_grade = Arithmetic::new(Factor::Const(ConstType::Integer(70)), vec![]);
/// let grade_check = ComparisonExpr::new(grade_var, ComparisonOperator::GreaterEqualThan, passing_grade);
///
/// let dropped_atom = Atom::new("dropped", vec![
///     AtomArg::Var("Student".to_string())
/// ]);
///
/// let eligible_body = vec![
///     Predicate::PositiveAtomPredicate(enrolled_atom),
///     Predicate::PositiveAtomPredicate(grade_atom),
///     Predicate::ComparePredicate(grade_check),
///     Predicate::NegatedAtomPredicate(dropped_atom)
/// ];
///
/// let eligible_rule = FLRule::new(eligible_head, eligible_body, false);
///
/// assert_eq!(eligible_rule.head().name(), "eligible");
/// assert_eq!(eligible_rule.head().arity(), 2);
/// assert_eq!(eligible_rule.rhs().len(), 4);
/// assert!(!eligible_rule.is_boolean());
/// assert!(!eligible_rule.is_planning());
///
/// // Boolean fact rule: config("debug", true) :- True
/// let config_head = Head::new("config".to_string(), vec![
///     HeadArg::Arith(Arithmetic::new(Factor::Const(ConstType::Text("debug".to_string())), vec![])),
///     HeadArg::Arith(Arithmetic::new(Factor::Const(ConstType::Integer(1)), vec![])) // representing true
/// ]);
/// let config_body = vec![Predicate::BoolPredicate(true)];
/// let config_rule = FLRule::new(config_head, config_body, false);
///
/// assert!(config_rule.is_boolean());
/// let constants = config_rule.extract_constants_from_head();
/// assert_eq!(constants.len(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FLRule {
    head: Head,
    rhs: Vec<Predicate>,
    is_planning: bool,
}

/// Provides string representation for FlowLog rules.
///
/// Formats rules in standard FlowLog syntax: `head :- body.`
/// where the body predicates are comma-separated.
///
/// # Format Structure
///
/// - **Head**: Single predicate or fact being derived
/// - **Separator**: ` :- ` (space-colon-dash-space)  
/// - **Body**: Comma-separated list of predicates
/// - **Terminator**: `.` (period)
///
/// # Examples
///
/// ```rust
/// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
/// use std::fmt::Write;
///
/// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
/// let atom = Atom::new("input", vec![AtomArg::Var("X".to_string())]);
/// let body = vec![
///     Predicate::PositiveAtomPredicate(atom),
///     Predicate::BoolPredicate(true)
/// ];
/// let rule = FLRule::new(head, body, false);
///
/// let formatted = format!("{}", rule);
/// assert!(formatted.contains("result(X) :- input(X), true."));
/// ```
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
    /// Creates a new FlowLog rule with the specified components.
    ///
    /// # Arguments
    ///
    /// * `head` - The [`Head`] of the rule that defines what fact is derived
    /// * `rhs` - A vector of [`Predicate`]s forming the rule body (right-hand side)
    /// * `is_planning` - Whether planning optimization is enabled for this rule
    ///
    /// # Returns
    ///
    /// A new [`FLRule`] instance representing the logical rule
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
    ///
    /// // Simple fact rule: user("alice") :- True.
    /// let head = Head::new("user".to_string(), vec![
    ///     HeadArg::Var("alice".to_string())
    /// ]);
    /// let body = vec![Predicate::BoolPredicate(true)];
    /// let fact_rule = FLRule::new(head, body, false);
    ///
    /// // Derivation rule with planning: result(X) :- input(X), condition(X).plan
    /// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
    /// let input_atom = Atom::new("input", vec![AtomArg::Var("X".to_string())]);
    /// let condition_atom = Atom::new("condition", vec![AtomArg::Var("X".to_string())]);
    /// let body = vec![
    ///     Predicate::PositiveAtomPredicate(input_atom),
    ///     Predicate::PositiveAtomPredicate(condition_atom)
    /// ];
    /// let derivation_rule = FLRule::new(head, body, true);
    ///
    /// assert!(derivation_rule.is_planning());
    /// assert_eq!(derivation_rule.rhs().len(), 2);
    /// ```
    #[must_use]
    pub fn new(head: Head, rhs: Vec<Predicate>, is_planning: bool) -> Self {
        Self {
            head,
            rhs,
            is_planning,
        }
    }

    /// Returns a reference to the rule's head.
    ///
    /// The head defines what fact is derived when this rule fires.
    /// It consists of a relation name and a list of arguments that
    /// can be variables or arithmetic expressions.
    ///
    /// # Returns
    ///
    /// A reference to the [`Head`] of this rule
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate};
    ///
    /// let head = Head::new("output".to_string(), vec![
    ///     HeadArg::Var("X".to_string()),
    ///     HeadArg::Var("Y".to_string())
    /// ]);
    /// let rule = FLRule::new(head, vec![Predicate::BoolPredicate(true)], false);
    ///
    /// assert_eq!(rule.head().name(), "output");
    /// assert_eq!(rule.head().arity(), 2);
    /// ```
    #[must_use]
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Returns a reference to the rule's body (right-hand side predicates).
    ///
    /// The body contains all conditions that must be satisfied for the rule
    /// to fire. Predicates are evaluated in the order they appear in the vector.
    ///
    /// # Returns
    ///
    /// A slice of [`Predicate`]s forming the rule body
    ///
    /// # Predicate Types
    ///
    /// The body can contain various types of predicates:
    /// - [`Predicate::PositiveAtomPredicate`] - Positive atomic facts
    /// - [`Predicate::NegatedAtomPredicate`] - Negated atomic facts  
    /// - [`Predicate::ComparePredicate`] - Arithmetic comparisons
    /// - [`Predicate::BoolPredicate`] - Boolean constants (True/False)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
    /// use parser::logic::{ComparisonExpr, ComparisonOperator, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
    ///
    /// let input_atom = Atom::new("input", vec![AtomArg::Var("X".to_string())]);
    /// let blocked_atom = Atom::new("blocked", vec![AtomArg::Var("X".to_string())]);
    ///
    /// let x_var = Arithmetic::new(Factor::Var("X".to_string()), vec![]);
    /// let threshold = Arithmetic::new(Factor::Const(ConstType::Integer(10)), vec![]);
    /// let comparison = ComparisonExpr::new(x_var, ComparisonOperator::GreaterThan, threshold);
    ///
    /// let body = vec![
    ///     Predicate::PositiveAtomPredicate(input_atom),
    ///     Predicate::NegatedAtomPredicate(blocked_atom),
    ///     Predicate::ComparePredicate(comparison)
    /// ];
    ///
    /// let rule = FLRule::new(head, body, false);
    ///
    /// assert_eq!(rule.rhs().len(), 3);
    /// // First predicate is positive atom
    /// if let Predicate::PositiveAtomPredicate(atom) = &rule.rhs()[0] {
    ///     assert_eq!(atom.name(), "input");
    /// }
    /// ```
    #[must_use]
    pub fn rhs(&self) -> &[Predicate] {
        &self.rhs
    }

    /// Checks if planning optimization is enabled for this rule.
    ///
    /// Planning optimization enables advanced query reordering and execution
    /// strategies for improved performance on complex rules. Rules marked with
    /// the `.plan` suffix have this flag set to true.
    ///
    /// # Returns
    ///
    /// `true` if planning optimization is enabled, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate};
    ///
    /// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
    /// let body = vec![Predicate::BoolPredicate(true)];
    ///
    /// let regular_rule = FLRule::new(head.clone(), body.clone(), false);
    /// let planning_rule = FLRule::new(head, body, true);
    ///
    /// assert!(!regular_rule.is_planning());
    /// assert!(planning_rule.is_planning());
    /// ```
    #[must_use]
    pub fn is_planning(&self) -> bool {
        self.is_planning
    }

    /// Checks if this rule contains any boolean predicates in its body.
    ///
    /// Boolean rules are typically used for fact insertion and simple
    /// assertions. This method returns true if any predicate in the body
    /// is a [`Predicate::BoolPredicate`].
    ///
    /// # Returns
    ///
    /// `true` if any body predicate is boolean, `false` otherwise
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
    ///
    /// let head = Head::new("config".to_string(), vec![
    ///     HeadArg::Var("setting".to_string())
    /// ]);
    ///
    /// // Boolean rule: config("setting") :- True.
    /// let boolean_body = vec![Predicate::BoolPredicate(true)];
    /// let boolean_rule = FLRule::new(head.clone(), boolean_body, false);
    ///
    /// // Mixed rule: config(X) :- input(X), True.
    /// let input_atom = Atom::new("input", vec![AtomArg::Var("X".to_string())]);
    /// let mixed_body = vec![
    ///     Predicate::PositiveAtomPredicate(input_atom),
    ///     Predicate::BoolPredicate(true)
    /// ];
    /// let mixed_rule = FLRule::new(head, mixed_body, false);
    ///
    /// assert!(boolean_rule.is_boolean());
    /// assert!(mixed_rule.is_boolean()); // Contains boolean predicate
    /// ```
    #[must_use]
    pub fn is_boolean(&self) -> bool {
        self.rhs.iter().any(|pred| pred.is_boolean())
    }

    /// Retrieves the predicate at the specified index in the rule body.
    ///
    /// Provides indexed access to body predicates for traversing rule
    /// structure systematically. This method is useful for examining
    /// individual predicates during rule analysis or execution.
    ///
    /// # Arguments
    ///
    /// * `i` - The zero-based index of the predicate to retrieve
    ///
    /// # Returns
    ///
    /// A reference to the [`Predicate`] at the given index
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds (>= the number of body predicates)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate, Atom, AtomArg};
    ///
    /// let head = Head::new("result".to_string(), vec![HeadArg::Var("X".to_string())]);
    /// let atom1 = Atom::new("input", vec![AtomArg::Var("X".to_string())]);
    /// let atom2 = Atom::new("valid", vec![AtomArg::Var("X".to_string())]);
    /// let body = vec![
    ///     Predicate::PositiveAtomPredicate(atom1),
    ///     Predicate::PositiveAtomPredicate(atom2)
    /// ];
    /// let rule = FLRule::new(head, body, false);
    ///
    /// // Access first predicate
    /// if let Predicate::PositiveAtomPredicate(atom) = rule.get(0) {
    ///     assert_eq!(atom.name(), "input");
    /// }
    ///
    /// // Access second predicate
    /// if let Predicate::PositiveAtomPredicate(atom) = rule.get(1) {
    ///     assert_eq!(atom.name(), "valid");
    /// }
    /// ```
    pub fn get(&self, i: usize) -> &Predicate {
        &self.rhs[i]
    }

    /// Extracts constant values from the rule head arguments.
    ///
    /// This method is particularly useful for boolean fact rules where
    /// the head contains constant values that need to be extracted for
    /// database insertion or fact matching operations.
    ///
    /// # Returns
    ///
    /// A [`Vec<ConstType>`] containing all constant values found in the
    /// head arguments, preserving their order and types
    ///
    /// # Constant Extraction Process
    ///
    /// 1. **Head Argument Traversal**: Examines each argument in the head
    /// 2. **Arithmetic Expression Analysis**: For arithmetic arguments, recursively
    ///    searches for constant factors and operands
    /// 3. **Constant Collection**: Collects all [`ConstType`] values found
    /// 4. **Order Preservation**: Maintains the order of constants as they appear
    ///
    /// # Supported Constant Types
    ///
    /// - [`ConstType::Integer`] - Integer literals
    /// - [`ConstType::Text`] - String literals
    ///
    /// # Use Cases
    ///
    /// - **Fact Insertion**: Extracting values for database operations
    /// - **Configuration Reading**: Getting constant configuration values
    /// - **Test Case Generation**: Extracting expected values from test rules
    /// - **Static Analysis**: Analyzing constant usage patterns
    ///
    /// # Panics
    ///
    /// Panics if the head contains any non-constant arguments, indicating the rule
    /// is not actually a boolean rule with only constant values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{FLRule, Head, HeadArg, Predicate, Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// // Rule: config("timeout", 300, true) :- True.
    /// let head = Head::new("config".to_string(), vec![
    ///     HeadArg::Arith(Arithmetic::new(
    ///         Factor::Const(ConstType::Text("timeout".to_string())), vec![]
    ///     )),
    ///     HeadArg::Arith(Arithmetic::new(
    ///         Factor::Const(ConstType::Integer(300)), vec![]
    ///     ))
    /// ]);
    /// let body = vec![Predicate::BoolPredicate(true)];
    /// let rule = FLRule::new(head, body, false);
    ///
    /// let constants = rule.extract_constants_from_head();
    /// assert_eq!(constants.len(), 2);
    ///
    /// if let ConstType::Text(name) = &constants[0] {
    ///     assert_eq!(name, "timeout");
    /// }
    /// if let ConstType::Integer(value) = constants[1] {
    ///     assert_eq!(value, 300);
    /// }
    /// ```
    #[must_use]
    pub fn extract_constants_from_head(&self) -> Vec<ConstType> {
        let head_args = self.head.head_arguments();
        let mut constants = Vec::new();

        for arg in head_args {
            match arg {
                HeadArg::Var(_) => {
                    panic!("Boolean rule head must contain only constants: {self}")
                }
                HeadArg::Aggregation(_) => {
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

/// Enables parsing FlowLog rules from grammar tokens.
///
/// Implements the [`Lexeme`] trait to support parsing rules from
/// Pest grammar parse trees. This implementation handles the complete
/// rule parsing pipeline from tokens to structured rule objects.
///
/// # Parsing Process
///
/// 1. **Head Parsing**: Extracts and parses the rule head
/// 2. **Body Parsing**: Parses all body predicates in sequence
/// 3. **Optimization Detection**: Checks for planning optimization directives
/// 4. **Rule Construction**: Assembles the complete rule structure
///
/// # Grammar Integration
///
/// Expects parse tree structure corresponding to the FlowLog grammar rule:
/// ```pest
/// rule = { head ~ ":-" ~ predicates ~ optimize? ~ "." }
/// ```
///
/// # Examples
///
/// The parser handles various rule formats:
/// ```text
/// // Simple fact rule
/// person("Alice") :- True.
///
/// // Complex derivation rule
/// eligible(S, C) :- enrolled(S, C), grade(S, C, G), G >= 70.
///
/// // Planning optimized rule  
/// complex_query(X) :- condition1(X), condition2(X), condition3(X).plan
/// ```
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Arithmetic, Atom, AtomArg, ComparisonExpr, ComparisonOperator};

    // Helper functions for creating test components
    fn var_arg(name: &str) -> AtomArg {
        AtomArg::Var(name.to_string())
    }

    #[allow(dead_code)]
    fn int_arg(value: i32) -> AtomArg {
        AtomArg::Const(ConstType::Integer(value))
    }

    #[allow(dead_code)]
    fn str_arg(value: &str) -> AtomArg {
        AtomArg::Const(ConstType::Text(value.to_string()))
    }

    fn simple_atom(name: &str, args: Vec<AtomArg>) -> Atom {
        Atom::new(name, args)
    }

    fn head_arg_var(name: &str) -> HeadArg {
        HeadArg::Var(name.to_string())
    }

    fn head_arg_const(value: ConstType) -> HeadArg {
        HeadArg::Arith(Arithmetic::new(Factor::Const(value), vec![]))
    }

    fn simple_head(name: &str, args: Vec<HeadArg>) -> Head {
        Head::new(name.to_string(), args)
    }

    fn positive_atom_predicate(name: &str, args: Vec<AtomArg>) -> Predicate {
        Predicate::PositiveAtomPredicate(simple_atom(name, args))
    }

    fn negated_atom_predicate(name: &str, args: Vec<AtomArg>) -> Predicate {
        Predicate::NegatedAtomPredicate(simple_atom(name, args))
    }

    fn bool_predicate(value: bool) -> Predicate {
        Predicate::BoolPredicate(value)
    }

    fn simple_comparison() -> Predicate {
        let left = Arithmetic::new(Factor::Var("X".to_string()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
        let comp = ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right);
        Predicate::ComparePredicate(comp)
    }

    #[test]
    fn test_rule_creation() {
        let head = simple_head("result", vec![head_arg_var("X")]);
        let body = vec![positive_atom_predicate("input", vec![var_arg("X")])];
        let rule = FLRule::new(head, body, false);

        assert!(!rule.is_planning());
        assert!(!rule.is_boolean());
        assert_eq!(rule.head().name(), "result");
        assert_eq!(rule.rhs().len(), 1);
    }

    #[test]
    fn test_rule_with_planning() {
        let head = simple_head("optimized", vec![head_arg_var("Y")]);
        let body = vec![positive_atom_predicate("source", vec![var_arg("Y")])];
        let rule = FLRule::new(head, body, true);

        assert!(rule.is_planning());
        assert!(!rule.is_boolean());
    }

    #[test]
    fn test_rule_with_boolean_predicate() {
        let head = simple_head("fact", vec![head_arg_const(ConstType::Integer(42))]);
        let body = vec![bool_predicate(true)];
        let rule = FLRule::new(head, body, false);

        assert!(!rule.is_planning());
        assert!(rule.is_boolean());
    }

    #[test]
    fn test_rule_accessors() {
        let head = simple_head("test", vec![head_arg_var("A"), head_arg_var("B")]);
        let body = vec![
            positive_atom_predicate("rel1", vec![var_arg("A")]),
            negated_atom_predicate("rel2", vec![var_arg("B")]),
        ];
        let rule = FLRule::new(head.clone(), body.clone(), true);

        assert_eq!(rule.head(), &head);
        assert_eq!(rule.rhs(), &body);
        assert!(rule.is_planning());
        assert_eq!(rule.rhs().len(), 2);
    }

    #[test]
    fn test_rule_get_predicate() {
        let head = simple_head("multi", vec![head_arg_var("X")]);
        let body = vec![
            positive_atom_predicate("first", vec![var_arg("X")]),
            simple_comparison(),
            bool_predicate(false),
        ];
        let rule = FLRule::new(head, body, false);

        // Test accessing individual predicates
        match rule.get(0) {
            Predicate::PositiveAtomPredicate(atom) => assert_eq!(atom.name(), "first"),
            _ => panic!("Expected positive atom predicate"),
        }

        match rule.get(1) {
            Predicate::ComparePredicate(_) => {} // Expected
            _ => panic!("Expected comparison predicate"),
        }

        match rule.get(2) {
            Predicate::BoolPredicate(false) => {} // Expected
            _ => panic!("Expected false boolean predicate"),
        }
    }

    #[test]
    fn test_rule_display() {
        // Simple rule
        let head = simple_head("result", vec![head_arg_var("X")]);
        let body = vec![positive_atom_predicate("input", vec![var_arg("X")])];
        let rule = FLRule::new(head, body, false);
        assert_eq!(rule.to_string(), "result(X) :- input(X).");

        // Rule with multiple predicates
        let head2 = simple_head("complex", vec![head_arg_var("A"), head_arg_var("B")]);
        let body2 = vec![
            positive_atom_predicate("rel1", vec![var_arg("A")]),
            negated_atom_predicate("rel2", vec![var_arg("B")]),
            bool_predicate(true),
        ];
        let rule2 = FLRule::new(head2, body2, false);
        assert_eq!(
            rule2.to_string(),
            "complex(A, B) :- rel1(A), !rel2(B), true."
        );

        // Rule with comparison
        let head3 = simple_head("filtered", vec![head_arg_var("X")]);
        let body3 = vec![
            positive_atom_predicate("data", vec![var_arg("X")]),
            simple_comparison(),
        ];
        let rule3 = FLRule::new(head3, body3, false);
        assert_eq!(rule3.to_string(), "filtered(X) :- data(X), X > 5.");
    }

    #[test]
    fn test_rule_extract_constants_from_head() {
        // Boolean rule with constant head
        let head = simple_head(
            "facts",
            vec![
                head_arg_const(ConstType::Integer(42)),
                head_arg_const(ConstType::Text("hello".to_string())),
            ],
        );
        let body = vec![bool_predicate(true)];
        let rule = FLRule::new(head, body, false);

        let constants = rule.extract_constants_from_head();
        assert_eq!(constants.len(), 2);
        assert_eq!(constants[0], ConstType::Integer(42));
        assert_eq!(constants[1], ConstType::Text("hello".to_string()));
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn test_rule_extract_constants_from_head_with_variable_panics() {
        let head = simple_head(
            "invalid",
            vec![
                head_arg_const(ConstType::Integer(1)),
                head_arg_var("X"), // This should cause panic
            ],
        );
        let body = vec![bool_predicate(true)];
        let rule = FLRule::new(head, body, false);

        let _ = rule.extract_constants_from_head(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn test_rule_extract_constants_from_head_with_aggregation_panics() {
        use crate::logic::{Aggregation, AggregationOperator};

        let arith = Arithmetic::new(Factor::Var("X".to_string()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Sum, arith);
        let head = simple_head(
            "invalid",
            vec![
                head_arg_const(ConstType::Integer(1)),
                HeadArg::Aggregation(agg), // This should cause panic
            ],
        );
        let body = vec![bool_predicate(true)];
        let rule = FLRule::new(head, body, false);

        let _ = rule.extract_constants_from_head(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn test_rule_extract_constants_from_head_with_complex_arithmetic_panics() {
        let complex_arith = Arithmetic::new(
            Factor::Var("X".to_string()),
            vec![(
                crate::logic::ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(1)),
            )],
        );
        let head = simple_head("invalid", vec![HeadArg::Arith(complex_arith)]);
        let body = vec![bool_predicate(true)];
        let rule = FLRule::new(head, body, false);

        let _ = rule.extract_constants_from_head(); // Should panic
    }

    #[test]
    fn test_rule_clone_hash() {
        let head = simple_head("test", vec![head_arg_var("X")]);
        let body = vec![positive_atom_predicate("input", vec![var_arg("X")])];
        let rule = FLRule::new(head, body, false);
        let cloned = rule.clone();
        assert_eq!(rule, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(rule.clone());
        set.insert(cloned);
        assert_eq!(set.len(), 1); // Should be deduplicated
    }

    #[test]
    fn test_rule_is_boolean_variants() {
        // Rule with no boolean predicates
        let head1 = simple_head("non_bool", vec![head_arg_var("X")]);
        let body1 = vec![
            positive_atom_predicate("input", vec![var_arg("X")]),
            simple_comparison(),
        ];
        let rule1 = FLRule::new(head1, body1, false);
        assert!(!rule1.is_boolean());

        // Rule with boolean predicate
        let head2 = simple_head("with_bool", vec![head_arg_var("X")]);
        let body2 = vec![
            positive_atom_predicate("input", vec![var_arg("X")]),
            bool_predicate(true),
        ];
        let rule2 = FLRule::new(head2, body2, false);
        assert!(rule2.is_boolean());

        // Rule with only boolean predicate
        let head3 = simple_head("only_bool", vec![head_arg_const(ConstType::Integer(1))]);
        let body3 = vec![bool_predicate(false)];
        let rule3 = FLRule::new(head3, body3, false);
        assert!(rule3.is_boolean());
    }

    #[test]
    fn test_rule_complex_example() {
        // Complex rule: derived(Person, Age) :- person(Person, Age), Age > 18, !blocked(Person).
        let head = simple_head("derived", vec![head_arg_var("Person"), head_arg_var("Age")]);

        let left = Arithmetic::new(Factor::Var("Age".to_string()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
        let age_check = ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right);

        let body = vec![
            positive_atom_predicate("person", vec![var_arg("Person"), var_arg("Age")]),
            Predicate::ComparePredicate(age_check),
            negated_atom_predicate("blocked", vec![var_arg("Person")]),
        ];

        let rule = FLRule::new(head, body, false);

        assert_eq!(rule.head().name(), "derived");
        assert_eq!(rule.head().arity(), 2);
        assert_eq!(rule.rhs().len(), 3);
        assert!(!rule.is_boolean());
        assert!(!rule.is_planning());

        let expected_display =
            "derived(Person, Age) :- person(Person, Age), Age > 18, !blocked(Person).";
        assert_eq!(rule.to_string(), expected_display);
    }
}
