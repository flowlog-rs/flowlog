//! Predicate types for FlowLog rule bodies.
//!
//! This module provides types for representing predicates in FlowLog rule bodies.
//! Predicates define the conditions that must be satisfied for a rule to fire,
//! forming the antecedent part of logical implications. They specify constraints,
//! relationships, and conditions that determine when rules are applicable.
//!
//! # Overview
//!
//! The module defines the [`Predicate`] enum which encompasses all types of
//! conditions that can appear in rule bodies:
//! - **Positive atoms**: Facts that must be true in the knowledge base
//! - **Negated atoms**: Facts that must be false (negation as failure)
//! - **Comparison expressions**: Mathematical and logical comparisons
//! - **Boolean literals**: Direct true/false conditions
//!
//! # Predicate Types
//!
//! ## Atom Predicates
//!
//! Atom predicates reference facts in the knowledge base:
//! - **Positive atoms** (`edge(X, Y)`): Require the fact to exist
//! - **Negated atoms** (`!edge(X, Y)`): Require the fact to not exist
//!
//! ## Comparison Predicates
//!
//! Comparison predicates evaluate mathematical and logical relationships:
//! - **Arithmetic comparisons**: `X > 5`, `Age >= 18`, `Count == 0`
//! - **Variable relationships**: `X < Y`, `Price * Quantity > Budget`
//!
//! ## Boolean Predicates
//!
//! Boolean predicates provide direct logical values:
//! - **True literals**: Always satisfied conditions
//! - **False literals**: Never satisfied conditions (useful for testing)
//!
//! # Usage in Rules
//!
//! Predicates appear in rule bodies to specify when rules should fire:
//! ```text
//! head(...) :- predicate1, predicate2, !predicate3, X > Y.
//! ```
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{Predicate, Atom, AtomArg, ComparisonExpr, ComparisonOperator};
//! use parser::logic::{Arithmetic, Factor};
//! use parser::primitive::ConstType;
//!
//! // Positive atom predicate: person(Name, Age)
//! let person_atom = Atom::new("person", vec![
//!     AtomArg::Var("Name".to_string()),
//!     AtomArg::Var("Age".to_string())
//! ]);
//! let positive_pred = Predicate::PositiveAtomPredicate(person_atom);
//! assert_eq!(positive_pred.to_string(), "person(Name, Age)");
//!
//! // Negated atom predicate: !blocked(User)
//! let blocked_atom = Atom::new("blocked", vec![
//!     AtomArg::Var("User".to_string())
//! ]);
//! let negated_pred = Predicate::NegatedAtomPredicate(blocked_atom);
//! assert_eq!(negated_pred.to_string(), "!blocked(User)");
//!
//! // Comparison predicate: Age >= 18
//! let age_var = Arithmetic::new(Factor::Var("Age".to_string()), vec![]);
//! let adult_age = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
//! let age_check = ComparisonExpr::new(age_var, ComparisonOperator::GreaterEqualThan, adult_age);
//! let comparison_pred = Predicate::ComparePredicate(age_check);
//! assert_eq!(comparison_pred.to_string(), "Age ≥ 18");
//!
//! // Boolean predicate: True
//! let bool_pred = Predicate::BoolPredicate(true);
//! assert_eq!(bool_pred.to_string(), "true");
//! assert!(bool_pred.is_boolean());
//! ```

use super::{Atom, AtomArg, ComparisonExpr};
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// Represents a predicate in a rule body.
///
/// Predicates define the conditions that must be satisfied for a rule to fire,
/// forming the logical constraints that determine rule applicability. Each predicate
/// type serves different purposes in expressing conditions and relationships within
/// the knowledge base.
///
/// # Variants
///
/// - **`PositiveAtomPredicate(Atom)`**: Requires the specified atom to exist in the
///   knowledge base. This represents positive facts that must be true for the rule to apply.
/// - **`NegatedAtomPredicate(Atom)`**: Requires the specified atom to NOT exist in the
///   knowledge base, implementing negation as failure semantics.
/// - **`ComparePredicate(ComparisonExpr)`**: Evaluates mathematical or logical comparisons
///   between arithmetic expressions, enabling numeric and relational constraints.
/// - **`BoolPredicate(bool)`**: Represents direct boolean literals (true/false) for
///   unconditional success or failure conditions.
///
/// # Logical Semantics
///
/// ## Positive Atoms
/// A positive atom predicate succeeds if there exists a matching fact in the knowledge base.
/// Variables in the atom are unified with values from matching facts.
///
/// ## Negated Atoms
/// A negated atom predicate succeeds if NO matching fact exists in the knowledge base.
/// This implements "negation as failure" - absence of proof is treated as proof of absence.
///
/// ## Comparisons
/// Comparison predicates evaluate arithmetic expressions and test relationships like
/// equality, ordering, and numeric constraints. They succeed if the comparison evaluates to true.
///
/// ## Booleans
/// Boolean predicates provide direct logical values, useful for testing, debugging,
/// and expressing unconditional constraints.
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Predicate, Atom, AtomArg, ComparisonExpr, ComparisonOperator};
/// use parser::logic::{Arithmetic, Factor};
/// use parser::primitive::ConstType;
///
/// // Positive atom: student(Person)
/// let student_atom = Atom::new("student", vec![
///     AtomArg::Var("Person".to_string())
/// ]);
/// let student_pred = Predicate::PositiveAtomPredicate(student_atom.clone());
/// assert_eq!(student_pred.name(), "student");
/// assert_eq!(student_pred.arguments().len(), 1);
/// assert!(!student_pred.is_boolean());
///
/// // Negated atom: !graduated(Person)
/// let graduated_atom = Atom::new("graduated", vec![
///     AtomArg::Var("Person".to_string())
/// ]);
/// let not_graduated = Predicate::NegatedAtomPredicate(graduated_atom);
/// assert_eq!(not_graduated.to_string(), "!graduated(Person)");
///
/// // Comparison: GPA > 3.5
/// let gpa_var = Arithmetic::new(Factor::Var("GPA".to_string()), vec![]);
/// let threshold = Arithmetic::new(Factor::Const(ConstType::Integer(35)), vec![]); // 3.5 * 10
/// let gpa_check = ComparisonExpr::new(gpa_var, ComparisonOperator::GreaterThan, threshold);
/// let comparison = Predicate::ComparePredicate(gpa_check);
/// assert!(!comparison.is_boolean());
///
/// // Boolean literal: True
/// let always_true = Predicate::BoolPredicate(true);
/// assert!(always_true.is_boolean());
/// assert_eq!(always_true.to_string(), "true");
///
/// // Boolean literal: False
/// let always_false = Predicate::BoolPredicate(false);
/// assert!(always_false.is_boolean());
/// assert_eq!(always_false.to_string(), "false");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// A positive atom (e.g., edge(X, Y))
    ///
    /// Represents a fact that must exist in the knowledge base for the rule to fire.
    /// Variables in the atom are unified with values from matching facts, enabling
    /// data flow and pattern matching in rule evaluation.
    PositiveAtomPredicate(Atom),

    /// A negated atom (e.g., !edge(X, Y))
    ///
    /// Represents a fact that must NOT exist in the knowledge base, implementing
    /// negation as failure semantics. The predicate succeeds only if no matching
    /// fact can be found, regardless of variable bindings.
    NegatedAtomPredicate(Atom),

    /// A comparison expression (e.g., X > Y)
    ///
    /// Evaluates arithmetic expressions and tests mathematical or logical relationships.
    /// Supports equality, inequality, and ordering comparisons between computed values
    /// and enables numeric constraints in rule conditions.
    ComparePredicate(ComparisonExpr),

    /// A boolean literal (True or False)
    ///
    /// Provides direct logical values for unconditional success or failure.
    /// Useful for testing, debugging, default cases, and expressing conditions
    /// that are always true or always false.
    BoolPredicate(bool),
}

impl Predicate {
    /// Returns the arguments of this predicate if it's an atom or negated atom.
    ///
    /// This method extracts the argument list from atom-based predicates, providing
    /// access to the variables, constants, and placeholders that define the atom's
    /// structure. It's essential for variable analysis, unification, and rule processing.
    ///
    /// # Returns
    ///
    /// A vector of references to [`AtomArg`] values representing the atom's arguments.
    ///
    /// # Applicability
    ///
    /// This method is only valid for:
    /// - `PositiveAtomPredicate`: Returns arguments of the positive atom
    /// - `NegatedAtomPredicate`: Returns arguments of the negated atom
    ///
    /// # Panics
    ///
    /// Panics if called on:
    /// - `ComparePredicate`: Comparison expressions don't have atom arguments
    /// - `BoolPredicate`: Boolean literals don't have atom arguments
    ///
    /// Use pattern matching or `is_boolean()` to check predicate type before calling.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Predicate, Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// // Positive atom with mixed arguments
    /// let person_atom = Atom::new("person", vec![
    ///     AtomArg::Var("Name".to_string()),
    ///     AtomArg::Const(ConstType::Integer(25)),
    ///     AtomArg::Placeholder
    /// ]);
    /// let person_pred = Predicate::PositiveAtomPredicate(person_atom);
    ///
    /// let args = person_pred.arguments();
    /// assert_eq!(args.len(), 3);
    /// assert!(args[0].is_var());
    /// assert!(args[1].is_const());
    /// assert!(args[2].is_placeholder());
    ///
    /// // Negated atom
    /// let blocked_atom = Atom::new("blocked", vec![
    ///     AtomArg::Var("User".to_string())
    /// ]);
    /// let blocked_pred = Predicate::NegatedAtomPredicate(blocked_atom);
    /// let blocked_args = blocked_pred.arguments();
    /// assert_eq!(blocked_args.len(), 1);
    /// assert!(blocked_args[0].is_var());
    /// ```
    pub fn arguments(&self) -> Vec<&AtomArg> {
        match self {
            Self::PositiveAtomPredicate(atom) => atom.arguments().iter().collect(),
            Self::NegatedAtomPredicate(atom) => atom.arguments().iter().collect(),
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get arguments from a comparison predicate")
            }
            Self::BoolPredicate(_) => unreachable!("Cannot get arguments from a true predicate"),
        }
    }

    /// Returns the name of this predicate if it's an atom or negated atom.
    ///
    /// This method extracts the relation name from atom-based predicates, providing
    /// the identifier used for fact matching and rule resolution. The relation name
    /// determines which facts in the knowledge base are relevant for this predicate.
    ///
    /// # Returns
    ///
    /// A string slice containing the relation name.
    ///
    /// # Applicability
    ///
    /// This method is only valid for:
    /// - `PositiveAtomPredicate`: Returns the name of the positive atom
    /// - `NegatedAtomPredicate`: Returns the name of the negated atom
    ///
    /// # Panics
    ///
    /// Panics if called on:
    /// - `ComparePredicate`: Comparison expressions don't have relation names
    /// - `BoolPredicate`: Boolean literals don't have relation names
    ///
    /// Use pattern matching or type checking before calling this method.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Predicate, Atom, AtomArg};
    ///
    /// // Positive atom predicate
    /// let student_atom = Atom::new("student", vec![
    ///     AtomArg::Var("Person".to_string())
    /// ]);
    /// let student_pred = Predicate::PositiveAtomPredicate(student_atom);
    /// assert_eq!(student_pred.name(), "student");
    ///
    /// // Negated atom predicate
    /// let graduated_atom = Atom::new("graduated", vec![
    ///     AtomArg::Var("Person".to_string())
    /// ]);
    /// let not_graduated = Predicate::NegatedAtomPredicate(graduated_atom);
    /// assert_eq!(not_graduated.name(), "graduated");
    ///
    /// // Safe usage with pattern matching
    /// match &student_pred {
    ///     Predicate::PositiveAtomPredicate(_) | Predicate::NegatedAtomPredicate(_) => {
    ///         println!("Atom predicate: {}", student_pred.name());
    ///     }
    ///     _ => println!("Non-atom predicate"),
    /// }
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            Self::PositiveAtomPredicate(atom) | Self::NegatedAtomPredicate(atom) => atom.name(),
            Self::ComparePredicate(_) => {
                unreachable!("Cannot get name from a comparison predicate")
            }
            Self::BoolPredicate(_) => unreachable!("Cannot get name from a boolean predicate"),
        }
    }

    /// Checks if this predicate is a boolean literal.
    ///
    /// Returns `true` if this predicate is a `BoolPredicate`, `false` for all other
    /// predicate types. This method is useful for distinguishing boolean literals
    /// from other predicate types without pattern matching.
    ///
    /// # Returns
    ///
    /// `true` if the predicate is a `BoolPredicate`, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Predicate, Atom, AtomArg, ComparisonExpr, ComparisonOperator};
    /// use parser::logic::{Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// // Boolean predicates
    /// let true_pred = Predicate::BoolPredicate(true);
    /// let false_pred = Predicate::BoolPredicate(false);
    /// assert!(true_pred.is_boolean());
    /// assert!(false_pred.is_boolean());
    ///
    /// // Atom predicates
    /// let atom = Atom::new("test", vec![AtomArg::Var("X".to_string())]);
    /// let positive_pred = Predicate::PositiveAtomPredicate(atom.clone());
    /// let negated_pred = Predicate::NegatedAtomPredicate(atom);
    /// assert!(!positive_pred.is_boolean());
    /// assert!(!negated_pred.is_boolean());
    ///
    /// // Comparison predicate
    /// let left = Arithmetic::new(Factor::Var("X".to_string()), vec![]);
    /// let right = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
    /// let comp = ComparisonExpr::new(left, ComparisonOperator::Equal, right);
    /// let comp_pred = Predicate::ComparePredicate(comp);
    /// assert!(!comp_pred.is_boolean());
    ///
    /// // Usage in control flow
    /// if !true_pred.is_boolean() {
    ///     println!("Relation name: {}", true_pred.name()); // Would panic!
    /// }
    /// ```
    pub fn is_boolean(&self) -> bool {
        matches!(&self, Predicate::BoolPredicate(_))
    }
}

impl fmt::Display for Predicate {
    /// Formats the predicate for display using standard logical notation.
    ///
    /// Each predicate type is formatted according to its logical meaning:
    /// - **Positive atoms**: Standard predicate notation `relation(args)`
    /// - **Negated atoms**: Negation prefix notation `!relation(args)`
    /// - **Comparisons**: Infix mathematical notation `left op right`
    /// - **Booleans**: Lowercase literal values `true` or `false`
    ///
    /// The formatting is suitable for rule display, debugging, query representation,
    /// and generating human-readable FlowLog programs.
    ///
    /// # Format Examples
    ///
    /// ```rust
    /// use parser::logic::{Predicate, Atom, AtomArg, ComparisonExpr, ComparisonOperator};
    /// use parser::logic::{Arithmetic, Factor};
    /// use parser::primitive::ConstType;
    ///
    /// // Positive atom: person("Alice", 30)
    /// let person_atom = Atom::new("person", vec![
    ///     AtomArg::Const(ConstType::Text("Alice".to_string())),
    ///     AtomArg::Const(ConstType::Integer(30))
    /// ]);
    /// let person_pred = Predicate::PositiveAtomPredicate(person_atom);
    /// assert_eq!(person_pred.to_string(), "person(\"Alice\", 30)");
    ///
    /// // Negated atom: !blocked(User)
    /// let blocked_atom = Atom::new("blocked", vec![
    ///     AtomArg::Var("User".to_string())
    /// ]);
    /// let blocked_pred = Predicate::NegatedAtomPredicate(blocked_atom);
    /// assert_eq!(blocked_pred.to_string(), "!blocked(User)");
    ///
    /// // Comparison: Age >= 18
    /// let age_var = Arithmetic::new(Factor::Var("Age".to_string()), vec![]);
    /// let adult_threshold = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
    /// let age_check = ComparisonExpr::new(age_var, ComparisonOperator::GreaterEqualThan, adult_threshold);
    /// let comp_pred = Predicate::ComparePredicate(age_check);
    /// assert_eq!(comp_pred.to_string(), "Age ≥ 18");
    ///
    /// // Boolean literals
    /// let true_pred = Predicate::BoolPredicate(true);
    /// let false_pred = Predicate::BoolPredicate(false);
    /// assert_eq!(true_pred.to_string(), "true");
    /// assert_eq!(false_pred.to_string(), "false");
    ///
    /// // Nullary atom: connected()
    /// let nullary_atom = Atom::new("connected", vec![]);
    /// let nullary_pred = Predicate::PositiveAtomPredicate(nullary_atom);
    /// assert_eq!(nullary_pred.to_string(), "connected()");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PositiveAtomPredicate(atom) => write!(f, "{atom}"),
            Self::NegatedAtomPredicate(atom) => write!(f, "!{atom}"),
            Self::ComparePredicate(expr) => write!(f, "{expr}"),
            Self::BoolPredicate(boolean) => write!(f, "{boolean}"),
        }
    }
}

impl Lexeme for Predicate {
    /// Parses a predicate from a pest parsing rule.
    ///
    /// This method constructs a `Predicate` from parsed grammar tokens, handling
    /// the conversion from textual FlowLog syntax to the appropriate predicate type.
    /// It dispatches to specialized parsing logic based on the rule type.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed predicate rule
    ///
    /// # Returns
    ///
    /// A new `Predicate` instance representing the parsed condition.
    ///
    /// # Grammar Mapping
    ///
    /// The method handles multiple grammar rules:
    /// - `Rule::predicate` → Wrapper rule, delegates to inner predicate type
    /// - `Rule::atom` → `Predicate::PositiveAtomPredicate`
    /// - `Rule::neg_atom` → `Predicate::NegatedAtomPredicate`
    /// - `Rule::compare_expr` → `Predicate::ComparePredicate`
    /// - `Rule::boolean` → `Predicate::BoolPredicate`
    ///
    /// # Parsing Process
    ///
    /// ## Atom Parsing
    /// - **Positive atoms**: Direct parsing of atom structure
    /// - **Negated atoms**: Unwraps negation syntax and parses the inner atom
    ///
    /// ## Comparison Parsing
    /// Delegates to `ComparisonExpr::from_parsed_rule` for arithmetic expression parsing.
    ///
    /// ## Boolean Parsing
    /// Converts string literals "True"/"False" to boolean values.
    ///
    /// # Examples
    ///
    /// For input text like:
    /// - `"person(X, Y)"` → `PositiveAtomPredicate(Atom { ... })`
    /// - `"!blocked(User)"` → `NegatedAtomPredicate(Atom { ... })`
    /// - `"Age > 18"` → `ComparePredicate(ComparisonExpr { ... })`
    /// - `"True"` → `BoolPredicate(true)`
    /// - `"False"` → `BoolPredicate(false)`
    ///
    /// # Grammar Structure
    ///
    /// Expected grammar structures:
    /// ```text
    /// predicate = { atom | neg_atom | compare_expr | boolean }
    /// neg_atom = { "!" ~ atom }
    /// boolean = { "True" | "False" }
    /// ```
    ///
    /// # Error Handling
    ///
    /// The method panics on:
    /// - Unknown rule types that don't match expected predicate patterns
    /// - Invalid boolean literals other than "True" or "False"
    /// - Malformed grammar structures
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for predicate");

        match inner.as_rule() {
            Rule::atom => {
                let atom = Atom::from_parsed_rule(inner);
                Self::PositiveAtomPredicate(atom)
            }
            Rule::neg_atom => {
                // The negated atom rule has an extra layer: neg_atom >> { "!" ~ atom }
                let inner_rule = inner.into_inner().next().unwrap();
                let negated_atom = Atom::from_parsed_rule(inner_rule);
                Self::NegatedAtomPredicate(negated_atom)
            }
            Rule::compare_expr => {
                let compare_expr = ComparisonExpr::from_parsed_rule(inner);
                Self::ComparePredicate(compare_expr)
            }
            Rule::boolean => {
                let value = inner.as_str();
                match value {
                    "True" => Self::BoolPredicate(true),
                    "False" => Self::BoolPredicate(false),
                    _ => unreachable!("Invalid boolean literal: {}", value),
                }
            }
            _ => unreachable!("Invalid predicate type: {:?}", inner.as_rule()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::logic::{Arithmetic, ComparisonOperator, Factor};
    use crate::primitive::ConstType;

    // Helper functions for creating test predicates
    fn simple_atom(name: &str, args: Vec<AtomArg>) -> Atom {
        Atom::new(name, args)
    }

    fn var_arg(name: &str) -> AtomArg {
        AtomArg::Var(name.to_string())
    }

    fn int_arg(value: i32) -> AtomArg {
        AtomArg::Const(ConstType::Integer(value))
    }

    fn str_arg(value: &str) -> AtomArg {
        AtomArg::Const(ConstType::Text(value.to_string()))
    }

    fn simple_comparison() -> ComparisonExpr {
        let left = Arithmetic::new(Factor::Var("X".to_string()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
        ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right)
    }

    #[test]
    fn test_positive_atom_predicate() {
        let atom = simple_atom("edge", vec![var_arg("X"), var_arg("Y")]);
        let pred = Predicate::PositiveAtomPredicate(atom.clone());

        assert_eq!(pred.name(), "edge");
        assert!(!pred.is_boolean());
        assert_eq!(pred.to_string(), "edge(X, Y)");

        let args = pred.arguments();
        assert_eq!(args.len(), 2);
        assert_eq!(args[0], &var_arg("X"));
        assert_eq!(args[1], &var_arg("Y"));
    }

    #[test]
    fn test_negated_atom_predicate() {
        let atom = simple_atom("forbidden", vec![str_arg("test")]);
        let pred = Predicate::NegatedAtomPredicate(atom.clone());

        assert_eq!(pred.name(), "forbidden");
        assert!(!pred.is_boolean());
        assert_eq!(pred.to_string(), "!forbidden(\"test\")");

        let args = pred.arguments();
        assert_eq!(args.len(), 1);
        assert_eq!(args[0], &str_arg("test"));
    }

    #[test]
    fn test_compare_predicate() {
        let comp = simple_comparison();
        let pred = Predicate::ComparePredicate(comp.clone());

        assert!(!pred.is_boolean());
        assert_eq!(pred.to_string(), "X > 5");
    }

    #[test]
    fn test_bool_predicate_true() {
        let pred = Predicate::BoolPredicate(true);
        assert!(pred.is_boolean());
        assert_eq!(pred.to_string(), "true");
    }

    #[test]
    fn test_bool_predicate_false() {
        let pred = Predicate::BoolPredicate(false);
        assert!(pred.is_boolean());
        assert_eq!(pred.to_string(), "false");
    }

    #[test]
    #[should_panic(expected = "Cannot get arguments from a comparison predicate")]
    fn test_compare_predicate_arguments_panics() {
        let comp = simple_comparison();
        let pred = Predicate::ComparePredicate(comp);
        let _ = pred.arguments(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Cannot get arguments from a true predicate")]
    fn test_bool_predicate_arguments_panics() {
        let pred = Predicate::BoolPredicate(true);
        let _ = pred.arguments(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Cannot get name from a comparison predicate")]
    fn test_compare_predicate_name_panics() {
        let comp = simple_comparison();
        let pred = Predicate::ComparePredicate(comp);
        let _ = pred.name(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Cannot get name from a boolean predicate")]
    fn test_bool_predicate_name_panics() {
        let pred = Predicate::BoolPredicate(false);
        let _ = pred.name(); // Should panic
    }

    #[test]
    fn test_predicate_clone_hash() {
        let pred = Predicate::PositiveAtomPredicate(simple_atom("test", vec![var_arg("X")]));
        let cloned = pred.clone();
        assert_eq!(pred, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(pred.clone());
        set.insert(cloned);
        set.insert(Predicate::BoolPredicate(true));
        assert_eq!(set.len(), 2); // Should be deduplicated
    }

    #[test]
    fn test_predicate_display_formats() {
        // Test various display formats
        let pos_atom = Predicate::PositiveAtomPredicate(simple_atom(
            "person",
            vec![str_arg("Alice"), int_arg(30)],
        ));
        assert_eq!(pos_atom.to_string(), "person(\"Alice\", 30)");

        let neg_atom =
            Predicate::NegatedAtomPredicate(simple_atom("blocked", vec![var_arg("User")]));
        assert_eq!(neg_atom.to_string(), "!blocked(User)");

        // Comparison with different operators
        let left = Arithmetic::new(Factor::Var("age".to_string()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
        let comp_gte = ComparisonExpr::new(
            left.clone(),
            ComparisonOperator::GreaterEqualThan,
            right.clone(),
        );
        let comp_pred = Predicate::ComparePredicate(comp_gte);
        assert_eq!(comp_pred.to_string(), "age ≥ 18");

        let comp_eq = ComparisonExpr::new(left, ComparisonOperator::Equal, right);
        let comp_pred_eq = Predicate::ComparePredicate(comp_eq);
        assert_eq!(comp_pred_eq.to_string(), "age == 18");
    }

    #[test]
    fn test_predicate_complex_examples() {
        // Complex atom with multiple arguments
        let complex_atom = simple_atom(
            "hasProperty",
            vec![
                var_arg("Object"),
                str_arg("color"),
                var_arg("Value"),
                int_arg(1),
            ],
        );
        let complex_pred = Predicate::PositiveAtomPredicate(complex_atom);
        assert_eq!(
            complex_pred.to_string(),
            "hasProperty(Object, \"color\", Value, 1)"
        );
        assert_eq!(complex_pred.name(), "hasProperty");
        assert_eq!(complex_pred.arguments().len(), 4);

        // Complex comparison with arithmetic
        let complex_left = Arithmetic::new(
            Factor::Var("salary".to_string()),
            vec![(
                crate::logic::ArithmeticOperator::Multiply,
                Factor::Const(ConstType::Integer(12)),
            )],
        );
        let complex_right = Arithmetic::new(Factor::Const(ConstType::Integer(100000)), vec![]);
        let complex_comp =
            ComparisonExpr::new(complex_left, ComparisonOperator::GreaterThan, complex_right);
        let complex_comp_pred = Predicate::ComparePredicate(complex_comp);
        assert_eq!(complex_comp_pred.to_string(), "salary * 12 > 100000");
    }

    #[test]
    fn test_predicate_nullary_atom() {
        // Test nullary (zero-argument) atom
        let nullary = simple_atom("flag", vec![]);
        let pred = Predicate::PositiveAtomPredicate(nullary);

        assert_eq!(pred.name(), "flag");
        assert_eq!(pred.to_string(), "flag()");
        assert!(pred.arguments().is_empty());
    }

    #[test]
    fn test_predicate_all_comparison_operators() {
        let left = Arithmetic::new(Factor::Var("x".to_string()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(10)), vec![]);

        let operators_and_symbols = [
            (ComparisonOperator::Equal, "x == 10"),
            (ComparisonOperator::NotEqual, "x ≠ 10"),
            (ComparisonOperator::GreaterThan, "x > 10"),
            (ComparisonOperator::GreaterEqualThan, "x ≥ 10"),
            (ComparisonOperator::LessThan, "x < 10"),
            (ComparisonOperator::LessEqualThan, "x ≤ 10"),
        ];

        for (op, expected_str) in operators_and_symbols {
            let comp = ComparisonExpr::new(left.clone(), op, right.clone());
            let pred = Predicate::ComparePredicate(comp);
            assert_eq!(pred.to_string(), expected_str);
            assert!(!pred.is_boolean());
        }
    }
}
