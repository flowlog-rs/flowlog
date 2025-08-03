//! FlowLog rule components and structures.
//!
//! This module defines the core components of FlowLog rules, including:
//! - Rule heads with arguments (variables, arithmetic expressions, aggregates)
//! - Rule bodies with predicates (atoms, negations, comparisons, booleans)
//!
//! FlowLog rules follow the structure: `head :- body.`
//! Where the head defines what is derived and the body specifies the conditions.

use pest::iterators::Pair;
use std::fmt;

use super::{
    expression::{Arithmetic, Atom, ComparisonExpr, Factor},
    AtomArg, Const, Lexeme, Rule,
};

/// Represents an argument in a rule head.
///
/// Rule head arguments define what values are produced by the rule.
/// They can be simple variables, computed arithmetic expressions,
/// or aggregate functions (currently unimplemented).
///
/// # Examples
///
/// ```rust
/// use parser::rule::HeadArg;
/// use parser::expression::{Arithmetic, Factor};
/// use parser::primitive::Const;
/// use parser::expression::ArithmeticOperator;
///
/// // Simple variable argument
/// let var_arg = HeadArg::Var("X".to_string());
///
/// // Arithmetic expression argument: X + 1
/// let arith = Arithmetic::new(
///     Factor::Var("X".to_string()),
///     vec![(ArithmeticOperator::Plus, Factor::Const(Const::Integer(1)))]
/// );
/// let arith_arg = HeadArg::Arith(arith);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// A variable argument (e.g., X, Y)
    Var(String),
    /// An arithmetic expression (e.g., X + Y, X * 2)
    Arith(Arithmetic),
    /// An aggregate function (e.g., count(X), sum(X + Y))
    ///
    /// # Note
    ///
    /// Currently unimplemented and will panic if used.
    GroupBy(Arithmetic),
}

impl HeadArg {
    /// Returns a vector of references to all variables in this head argument.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::rule::HeadArg;
    ///
    /// let var_arg = HeadArg::Var("X".to_string());
    /// assert_eq!(var_arg.vars(), vec![&"X".to_string()]);
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if called on a `GroupBy` variant (currently unimplemented).
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(var) => vec![var],
            Self::Arith(arith) => arith.vars(),
            Self::GroupBy(_arith) => todo!("GroupBy unimplemented"),
        }
    }
}

impl fmt::Display for HeadArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Arith(arith) => write!(f, "{arith}"),
            Self::GroupBy(arith) => write!(f, "{arith}"),
        }
    }
}

impl Lexeme for HeadArg {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for head_arg");

        // Process the rule based on its type
        match inner.as_rule() {
            Rule::arithmetic_expr => {
                // Parse as arithmetic (which also handles variables)
                let arithmetic = Arithmetic::from_parsed_rule(inner);

                // Check if it's a simple variable
                if arithmetic.is_var() {
                    // Extract the variable name and create a Var variant
                    let var_name = arithmetic.init().vars()[0].to_string();
                    Self::Var(var_name)
                } else {
                    Self::Arith(arithmetic)
                }
            }
            Rule::aggregate_expr => {
                // TODO: Implement aggregate/groupby parsing
                todo!("Aggregate parsing not implemented yet")
            }
            _ => unreachable!("Unexpected rule in HeadArg: {:?}", inner.as_rule()),
        }
    }
}

/// Represents the head of a rule (e.g., "result(X, Y + Z)").
///
/// The head defines what relation and values are produced when the rule fires.
/// It consists of a relation name and a list of arguments that can be variables,
/// arithmetic expressions, or aggregate functions.
///
/// # Examples
///
/// ```rust
/// use parser::rule::{Head, HeadArg};
///
/// // Create head: person(X, Y)
/// let args = vec![
///     HeadArg::Var("X".to_string()),
///     HeadArg::Var("Y".to_string()),
/// ];
/// let head = Head::new("person".to_string(), args);
/// assert_eq!(head.arity(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Head {
    /// Name of the relation being defined
    name: String,
    /// Arguments in the head
    head_arguments: Vec<HeadArg>,
}

impl Head {
    /// Creates a new rule head.
    #[must_use]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        Self {
            name,
            head_arguments,
        }
    }

    /// Returns the name of the relation being defined.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the arguments in this rule head.
    #[must_use]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    /// Returns the arity (number of arguments) of this rule head.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.head_arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Lexeme for Head {
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let name = inner.next().unwrap().as_str().to_string();

        let mut head_arguments = Vec::new();
        // Process remaining rules as head arguments directly
        for arg_rule in inner {
            head_arguments.push(HeadArg::from_parsed_rule(arg_rule));
        }

        Self::new(name, head_arguments)
    }
}

/// Represents a predicate in a rule body.
///
/// Predicates define the conditions that must be satisfied for a rule to fire.
/// They can be positive atoms (facts that must be true), negated atoms (facts
/// that must be false), comparison expressions, or boolean literals.
///
/// # Examples
///
/// ```rust
/// use parser::rule::Predicate;
/// use parser::expression::{Atom, AtomArg};
///
/// // Positive atom: edge(X, Y)
/// let atom = Atom::new("edge", vec![
///     AtomArg::Var("X".to_string()),
///     AtomArg::Var("Y".to_string())
/// ]);
/// let positive_pred = Predicate::PositiveAtomPredicate(atom.clone());
///
/// // Negated atom: !edge(X, Y)
/// let negated_pred = Predicate::NegatedAtomPredicate(atom);
///
/// // Boolean literal
/// let bool_pred = Predicate::BoolPredicate(true);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Predicate {
    /// A positive atom (e.g., edge(X, Y))
    PositiveAtomPredicate(Atom),
    /// A negated atom (e.g., !edge(X, Y))
    NegatedAtomPredicate(Atom),
    /// A comparison expression (e.g., X > Y)
    ComparePredicate(ComparisonExpr),
    /// A boolean literal (True or False)
    BoolPredicate(bool),
}

impl Predicate {
    /// Returns the arguments of this predicate if it's an atom or negated atom
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
    /// # Panics
    ///
    /// Panics if called on a comparison or boolean predicate.
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

    pub fn is_boolean(&self) -> bool {
        matches!(&self, Predicate::BoolPredicate(_))
    }
}

impl fmt::Display for Predicate {
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
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        match parsed_rule.as_rule() {
            Rule::predicate => {
                // predicate is a wrapper that contains the actual predicate type
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                Self::from_parsed_rule(inner_rule)
            }
            Rule::atom => {
                let atom = Atom::from_parsed_rule(parsed_rule);
                Self::PositiveAtomPredicate(atom)
            }
            Rule::neg_atom => {
                // The negated atom rule has an extra layer: neg_atom >> { "!" ~ atom }
                let inner_rule = parsed_rule.into_inner().next().unwrap();
                let negated_atom = Atom::from_parsed_rule(inner_rule);
                Self::NegatedAtomPredicate(negated_atom)
            }
            Rule::compare_expr => {
                let compare_expr = ComparisonExpr::from_parsed_rule(parsed_rule);
                Self::ComparePredicate(compare_expr)
            }
            Rule::boolean => {
                let value = parsed_rule.as_str();
                match value {
                    "True" => Self::BoolPredicate(true),
                    "False" => Self::BoolPredicate(false),
                    _ => unreachable!("Invalid boolean literal: {}", value),
                }
            }
            _ => unreachable!("Invalid predicate type: {:?}", parsed_rule.as_rule()),
        }
    }
}

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
/// use parser::rule::{FLRule, Head, HeadArg, Predicate};
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
    pub fn extract_constants_from_head(&self) -> Vec<Const> {
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
