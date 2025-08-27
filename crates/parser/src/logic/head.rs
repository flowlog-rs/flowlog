//! Rule head components for Datalog programs (Macaron engine).
//!
//! This module provides types for representing rule heads in Datalog programs parsed by Macaron.
//! Rule heads define what facts are derived when a rule fires, specifying the
//! relation name and the arguments that will be produced. They form the consequent
//! part of logical rules and determine the output structure of rule evaluation.
//!
//! # Overview
//!
//! The module defines two main types:
//! - [`HeadArg`]: Arguments that can appear in rule heads (variables, arithmetic expressions, aggregates)
//! - [`Head`]: Complete rule heads with relation names and argument lists
//!
//! # Rule Head Structure
//!
//! A rule head follows the pattern: `relation_name(arg1, arg2, ..., argN)`
//!
//! Arguments can be:
//! - **Variables**: Simple variable references that pass through values
//! - **Arithmetic expressions**: Computed values using mathematical operations
//! - **Aggregate functions**: Statistical operations for data summarization
//!
//! # Usage in Rules
//!
//! Rule heads appear in contexts like:
//! - **Fact derivation**: `derived_fact(X, Y + 1) :- base_fact(X, Y)`
//! - **Data transformation**: `processed(Name, Age * 2) :- person(Name, Age)`
//! - **Projection**: `result(X) :- complex_relation(X, _, _)`
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
//! use parser::primitive::ConstType;
//!
//! // Simple head with variables: person(X, Y)
//! let simple_head = Head::new("person".to_string(), vec![
//!     HeadArg::Var("X".to_string()),
//!     HeadArg::Var("Y".to_string()),
//! ]);
//! assert_eq!(simple_head.to_string(), "person(X, Y)");
//!
//! // Head with arithmetic: result(X, Y + 10)
//! let arith_expr = Arithmetic::new(
//!     Factor::Var("Y".to_string()),
//!     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(10)))]
//! );
//! let computed_head = Head::new("result".to_string(), vec![
//!     HeadArg::Var("X".to_string()),
//!     HeadArg::Arith(arith_expr),
//! ]);
//! assert_eq!(computed_head.to_string(), "result(X, Y + 10)");
//!
//! // Nullary head (no arguments): flag()
//! let nullary_head = Head::new("flag".to_string(), vec![]);
//! assert_eq!(nullary_head.to_string(), "flag()");
//! ```

use super::{Aggregation, Arithmetic};
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::fmt;

/// Represents an argument in a rule head.
///
/// Rule head arguments define what values are produced when a rule fires,
/// specifying how data flows from the rule body to the derived fact. Each
/// argument type serves different purposes in data processing and transformation.
///
/// # Variants
///
/// - **`Var(String)`**: Variable arguments that directly pass through values from
///   the rule body. These create a direct binding between body variables and head positions.
/// - **`Arith(Arithmetic)`**: Arithmetic expression arguments that compute new values
///   using mathematical operations on variables and constants from the rule body.
/// - **`Aggregation(Aggregation)`**: Aggregate function arguments for statistical operations
///   such as counting, summing, or finding maximum values.
///
/// # Data Flow Patterns
///
/// Arguments enable different data transformation patterns:
/// - **Pass-through**: Variables directly transfer values without modification
/// - **Computation**: Arithmetic expressions transform input values using mathematical operations
/// - **Aggregation**: Statistical functions that summarize multiple input rows
///
/// # Examples
///
/// ```rust
/// use parser::logic::{HeadArg, Arithmetic, Factor, ArithmeticOperator};
/// use parser::primitive::ConstType;
///
/// // Simple variable argument - direct pass-through
/// let var_arg = HeadArg::Var("PersonName".to_string());
/// assert_eq!(var_arg.vars(), vec![&"PersonName".to_string()]);
/// assert_eq!(var_arg.to_string(), "PersonName");
///
/// // Arithmetic expression argument - computed value
/// let arith_expr = Arithmetic::new(
///     Factor::Var("Age".to_string()),
///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
/// );
/// let arith_arg = HeadArg::Arith(arith_expr);
/// assert_eq!(arith_arg.vars(), vec![&"Age".to_string()]);
/// assert_eq!(arith_arg.to_string(), "Age + 1");
///
/// // Complex arithmetic with multiple variables
/// let complex_expr = Arithmetic::new(
///     Factor::Var("Price".to_string()),
///     vec![
///         (ArithmeticOperator::Multiply, Factor::Var("Quantity".to_string())),
///         (ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5)))
///     ]
/// );
/// let complex_arg = HeadArg::Arith(complex_expr);
/// let vars = complex_arg.vars();
/// assert_eq!(vars.len(), 2);
/// assert!(vars.contains(&&"Price".to_string()));
/// assert!(vars.contains(&&"Quantity".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum HeadArg {
    /// A variable argument (e.g., X, Y)
    ///
    /// Variable arguments create direct bindings between rule body variables
    /// and head positions, enabling pass-through of values without modification.
    /// They are the most common type of head argument and form the basis of
    /// relational data flow in logic programming.
    Var(String),

    /// An arithmetic expression (e.g., X + Y, X * 2)
    ///
    /// Arithmetic arguments enable computation of derived values using
    /// mathematical operations on variables and constants. They allow rules
    /// to transform input data and generate computed results.
    Arith(Arithmetic),

    /// An aggregate function (e.g., count(X), sum(X + Y))
    ///
    /// Aggregate arguments perform statistical operations that summarize multiple
    /// input rows into single values. Common aggregates include counting,
    /// summing, finding minimums/maximums, and averaging.
    Aggregation(Aggregation),
}

impl HeadArg {
    /// Returns a vector of references to all variables in this head argument.
    ///
    /// This method performs variable dependency analysis, identifying which rule body
    /// variables are required to compute this head argument. The analysis supports:
    /// - **Variables**: Returns the single variable name
    /// - **Arithmetic expressions**: Returns all variables used in the expression
    /// - **Aggregates**: Returns all variables used in the aggregated expression
    ///
    /// The order of variables reflects their appearance order in the expression,
    /// and duplicates are preserved to maintain dependency information.
    ///
    /// # Returns
    ///
    /// A vector of string references representing all variables needed for this argument.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Variable argument
    /// let var_arg = HeadArg::Var("PersonId".to_string());
    /// let vars = var_arg.vars();
    /// assert_eq!(vars.len(), 1);
    /// assert_eq!(vars[0], &"PersonId".to_string());
    ///
    /// // Arithmetic argument with single variable
    /// let simple_arith = Arithmetic::new(
    ///     Factor::Var("Age".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    /// let arith_arg = HeadArg::Arith(simple_arith);
    /// let vars = arith_arg.vars();
    /// assert_eq!(vars.len(), 1);
    /// assert_eq!(vars[0], &"Age".to_string());
    ///
    /// // Arithmetic argument with multiple variables
    /// let multi_arith = Arithmetic::new(
    ///     Factor::Var("X".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Plus, Factor::Var("Y".to_string())),
    ///         (ArithmeticOperator::Multiply, Factor::Var("Z".to_string()))
    ///     ]
    /// );
    /// let multi_arg = HeadArg::Arith(multi_arith);
    /// let vars = multi_arg.vars();
    /// assert_eq!(vars.len(), 3);
    /// assert!(vars.contains(&&"X".to_string()));
    /// assert!(vars.contains(&&"Y".to_string()));
    /// assert!(vars.contains(&&"Z".to_string()));
    ///
    /// // Arithmetic with only constants (no variables)
    /// let const_arith = Arithmetic::new(
    ///     Factor::Const(ConstType::Integer(10)),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(5)))]
    /// );
    /// let const_arg = HeadArg::Arith(const_arith);
    /// assert!(const_arg.vars().is_empty());
    /// ```
    #[must_use]
    pub fn vars(&self) -> Vec<&String> {
        match self {
            Self::Var(var) => vec![var],
            Self::Arith(arith) => arith.vars(),
            Self::Aggregation(agg) => agg.vars(),
        }
    }
}

impl fmt::Display for HeadArg {
    /// Formats the head argument for display.
    ///
    /// Each argument type is formatted according to its semantic meaning:
    /// - **Variables**: Displayed as their name without modification
    /// - **Arithmetic expressions**: Formatted using standard mathematical notation
    /// - **Aggregates**: Displayed using standard function notation (e.g., "count(X)")
    ///
    /// The formatting is suitable for rule display, debugging, and generating
    /// human-readable representations of Macaron programs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Variable argument
    /// let var = HeadArg::Var("PersonName".to_string());
    /// assert_eq!(var.to_string(), "PersonName");
    ///
    /// // Simple arithmetic
    /// let simple_arith = Arithmetic::new(
    ///     Factor::Var("Count".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    /// let arith_arg = HeadArg::Arith(simple_arith);
    /// assert_eq!(arith_arg.to_string(), "Count + 1");
    ///
    /// // Complex arithmetic
    /// let complex_arith = Arithmetic::new(
    ///     Factor::Var("Base".to_string()),
    ///     vec![
    ///         (ArithmeticOperator::Multiply, Factor::Const(ConstType::Integer(2))),
    ///         (ArithmeticOperator::Plus, Factor::Var("Offset".to_string()))
    ///     ]
    /// );
    /// let complex_arg = HeadArg::Arith(complex_arith);
    /// assert_eq!(complex_arg.to_string(), "Base * 2 + Offset");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Arith(arith) => write!(f, "{arith}"),
            Self::Aggregation(agg) => write!(f, "{agg}"),
        }
    }
}

impl Lexeme for HeadArg {
    /// Parses a head argument from a pest parsing rule.
    ///
    /// This method constructs a `HeadArg` from parsed grammar tokens, handling
    /// the conversion from textual Macaron syntax to the appropriate argument type.
    /// It performs intelligent parsing to distinguish between simple variables
    /// and complex arithmetic expressions.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed head argument rule
    ///
    /// # Returns
    ///
    /// A new `HeadArg` instance representing the parsed argument.
    ///
    /// # Parsing Logic
    ///
    /// The parser handles different argument types:
    /// 1. **Arithmetic expressions**: Parsed first to handle all mathematical constructs
    /// 2. **Simple variables**: Detected when arithmetic expressions contain only a single variable
    /// 3. **Complex expressions**: Remain as `Arith` variants for computed values
    /// 4. **Aggregates**: Parsed from aggregate expressions with operators and arithmetic
    ///
    /// # Optimization
    ///
    /// Simple variables are optimized from `Arith(variable)` to `Var(name)` for
    /// efficiency and cleaner representation.
    ///
    /// # Grammar Mapping
    ///
    /// - `Rule::arithmetic_expr` → `HeadArg::Var` (if simple variable) or `HeadArg::Arith`
    /// - `Rule::aggregate_expr` → `HeadArg::Aggregation` - Creates aggregation with operator and expression
    ///
    /// # Examples
    ///
    /// For input tokens like:
    /// - `"X"` → `HeadArg::Var("X".to_string())`
    /// - `"Count + 1"` → `HeadArg::Arith(Arithmetic { ... })`
    /// - `"Price * Quantity"` → `HeadArg::Arith(Arithmetic { ... })`
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The parsed rule contains no inner rules
    /// - The inner rule type is not recognized
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
            Rule::aggregate_expr => Self::Aggregation(Aggregation::from_parsed_rule(inner)),
            _ => unreachable!("Unexpected rule in HeadArg: {:?}", inner.as_rule()),
        }
    }
}

/// Represents the head of a rule (e.g., "result(X, Y + Z)").
///
/// The head defines what relation and values are produced when a rule fires,
/// forming the consequent part of logical implications. It specifies both the
/// target relation name and the structure of facts that will be derived.
///
/// # Structure
///
/// A rule head consists of:
/// - **Name**: A string identifier for the relation being derived
/// - **Arguments**: A list of [`HeadArg`] values defining the output structure
///
/// The structure follows the pattern: `relation_name(arg1, arg2, ..., argN)`
///
///
/// # Arity and Schema
///
/// The arity (number of arguments) defines the schema of the derived relation:
/// - **Nullary heads** (arity 0): Derive propositional facts like `connected()`
/// - **Unary heads** (arity 1): Derive single-attribute facts like `student(X)`
/// - **N-ary heads** (arity N): Derive multi-attribute relations
///
/// # Argument Types
///
/// Head arguments support various data processing patterns:
/// - **Variables**: Direct data pass-through from rule body
/// - **Arithmetic**: Computed values using mathematical expressions
/// - **Aggregates**: Summarized values from grouped data using statistical functions
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
/// use parser::primitive::ConstType;
///
/// // Simple head with variables: person(Name, Age)
/// let person_head = Head::new("person".to_string(), vec![
///     HeadArg::Var("Name".to_string()),
///     HeadArg::Var("Age".to_string()),
/// ]);
/// assert_eq!(person_head.name(), "person");
/// assert_eq!(person_head.arity(), 2);
/// assert_eq!(person_head.to_string(), "person(Name, Age)");
///
/// // Head with computation: adult(Person, Age + 1)
/// let age_increment = Arithmetic::new(
///     Factor::Var("Age".to_string()),
///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
/// );
/// let adult_head = Head::new("adult".to_string(), vec![
///     HeadArg::Var("Person".to_string()),
///     HeadArg::Arith(age_increment),
/// ]);
/// assert_eq!(adult_head.to_string(), "adult(Person, Age + 1)");
///
/// // Nullary head: flag()
/// let flag_head = Head::new("flag".to_string(), vec![]);
/// assert_eq!(flag_head.arity(), 0);
/// assert_eq!(flag_head.to_string(), "flag()");
///
/// // Complex computation: cost(Item, Price * Quantity + Tax)
/// let cost_calc = Arithmetic::new(
///     Factor::Var("Price".to_string()),
///     vec![
///         (ArithmeticOperator::Multiply, Factor::Var("Quantity".to_string())),
///         (ArithmeticOperator::Plus, Factor::Var("Tax".to_string()))
///     ]
/// );
/// let cost_head = Head::new("cost".to_string(), vec![
///     HeadArg::Var("Item".to_string()),
///     HeadArg::Arith(cost_calc),
/// ]);
/// assert_eq!(cost_head.to_string(), "cost(Item, Price * Quantity + Tax)");
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
    ///
    /// This is the primary constructor for building rule heads programmatically.
    /// The relation name should follow Macaron naming conventions, and arguments
    /// define the structure and computation of the derived facts.
    ///
    /// # Arguments
    ///
    /// * `name` - The relation name as a string
    /// * `head_arguments` - A vector of [`HeadArg`] values defining the output structure
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Simple fact head: student(Person)
    /// let student_head = Head::new("student".to_string(), vec![
    ///     HeadArg::Var("Person".to_string())
    /// ]);
    /// assert_eq!(student_head.name(), "student");
    /// assert_eq!(student_head.arity(), 1);
    ///
    /// // Computed head: next_year(Person, Age + 1)
    /// let age_expr = Arithmetic::new(
    ///     Factor::Var("Age".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    /// let computed_head = Head::new("next_year".to_string(), vec![
    ///     HeadArg::Var("Person".to_string()),
    ///     HeadArg::Arith(age_expr),
    /// ]);
    /// assert_eq!(computed_head.arity(), 2);
    ///
    /// // Nullary head: completed()
    /// let nullary_head = Head::new("completed".to_string(), vec![]);
    /// assert_eq!(nullary_head.arity(), 0);
    /// ```
    #[must_use]
    pub fn new(name: String, head_arguments: Vec<HeadArg>) -> Self {
        Self {
            name,
            head_arguments,
        }
    }

    /// Returns the name of the relation being defined.
    ///
    /// The relation name identifies what type of fact this head produces.
    /// It's used for rule matching, fact organization, and query resolution
    /// in the knowledge base.
    ///
    /// # Returns
    ///
    /// A string slice containing the relation name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Head, HeadArg};
    ///
    /// let employee_head = Head::new("employee".to_string(), vec![
    ///     HeadArg::Var("Name".to_string()),
    ///     HeadArg::Var("Department".to_string())
    /// ]);
    /// assert_eq!(employee_head.name(), "employee");
    ///
    /// let derived_head = Head::new("derived_fact".to_string(), vec![]);
    /// assert_eq!(derived_head.name(), "derived_fact");
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the arguments in this rule head.
    ///
    /// The arguments define the structure of facts that will be derived when
    /// the rule fires. Each argument specifies how a position in the derived
    /// fact is computed from the rule body.
    ///
    /// # Returns
    ///
    /// A slice of [`HeadArg`] values representing the head arguments.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// let head = Head::new("result".to_string(), vec![
    ///     HeadArg::Var("Input".to_string()),
    ///     HeadArg::Arith(Arithmetic::new(
    ///         Factor::Var("Value".to_string()),
    ///         vec![(ArithmeticOperator::Multiply, Factor::Const(ConstType::Integer(2)))]
    ///     ))
    /// ]);
    ///
    /// let args = head.head_arguments();
    /// assert_eq!(args.len(), 2);
    ///
    /// // Check first argument is a variable
    /// match &args[0] {
    ///     HeadArg::Var(name) => assert_eq!(name, "Input"),
    ///     _ => panic!("Expected variable argument"),
    /// }
    ///
    /// // Check second argument is arithmetic
    /// match &args[1] {
    ///     HeadArg::Arith(_) => {}, // Expected
    ///     _ => panic!("Expected arithmetic argument"),
    /// }
    /// ```
    #[must_use]
    pub fn head_arguments(&self) -> &[HeadArg] {
        &self.head_arguments
    }

    /// Returns the arity (number of arguments) of this rule head.
    ///
    /// The arity defines the schema of the derived relation and must match
    /// the expected arity for the relation name. It's used for type checking,
    /// rule validation, and ensuring consistency in the knowledge base.
    ///
    /// # Returns
    ///
    /// The number of arguments as a `usize`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Nullary head (0 arguments)
    /// let nullary = Head::new("flag".to_string(), vec![]);
    /// assert_eq!(nullary.arity(), 0);
    ///
    /// // Unary head (1 argument)
    /// let unary = Head::new("student".to_string(), vec![
    ///     HeadArg::Var("Person".to_string())
    /// ]);
    /// assert_eq!(unary.arity(), 1);
    ///
    /// // Binary head (2 arguments)
    /// let binary = Head::new("knows".to_string(), vec![
    ///     HeadArg::Var("Person1".to_string()),
    ///     HeadArg::Var("Person2".to_string())
    /// ]);
    /// assert_eq!(binary.arity(), 2);
    ///
    /// // Ternary head with computation (3 arguments)
    /// let total_expr = Arithmetic::new(
    ///     Factor::Var("Price".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Var("Tax".to_string()))]
    /// );
    /// let ternary = Head::new("order".to_string(), vec![
    ///     HeadArg::Var("Customer".to_string()),
    ///     HeadArg::Var("Item".to_string()),
    ///     HeadArg::Arith(total_expr)
    /// ]);
    /// assert_eq!(ternary.arity(), 3);
    /// ```
    #[must_use]
    pub fn arity(&self) -> usize {
        self.head_arguments.len()
    }
}

impl fmt::Display for Head {
    /// Formats the rule head for display using standard logical notation.
    ///
    /// The format follows the standard pattern: `relation_name(arg1, arg2, ..., argN)`
    /// where arguments are separated by commas and spaces. This produces output
    /// suitable for rule display, debugging, and generating human-readable
    /// Macaron programs.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Head, HeadArg, Arithmetic, Factor, ArithmeticOperator};
    /// use parser::primitive::ConstType;
    ///
    /// // Nullary head
    /// let nullary = Head::new("connected".to_string(), vec![]);
    /// assert_eq!(nullary.to_string(), "connected()");
    ///
    /// // Unary head with variable
    /// let unary = Head::new("student".to_string(), vec![
    ///     HeadArg::Var("Person".to_string())
    /// ]);
    /// assert_eq!(unary.to_string(), "student(Person)");
    ///
    /// // Binary head with variables
    /// let binary = Head::new("parent".to_string(), vec![
    ///     HeadArg::Var("Parent".to_string()),
    ///     HeadArg::Var("Child".to_string())
    /// ]);
    /// assert_eq!(binary.to_string(), "parent(Parent, Child)");
    ///
    /// // Head with arithmetic computation
    /// let increment_expr = Arithmetic::new(
    ///     Factor::Var("Age".to_string()),
    ///     vec![(ArithmeticOperator::Plus, Factor::Const(ConstType::Integer(1)))]
    /// );
    /// let computed = Head::new("next_age".to_string(), vec![
    ///     HeadArg::Var("Person".to_string()),
    ///     HeadArg::Arith(increment_expr)
    /// ]);
    /// assert_eq!(computed.to_string(), "next_age(Person, Age + 1)");
    ///
    /// // Complex head with multiple computations
    /// let price_calc = Arithmetic::new(
    ///     Factor::Var("Base".to_string()),
    ///     vec![(ArithmeticOperator::Multiply, Factor::Var("Rate".to_string()))]
    /// );
    /// let complex = Head::new("pricing".to_string(), vec![
    ///     HeadArg::Var("Product".to_string()),
    ///     HeadArg::Arith(price_calc),
    ///     HeadArg::Var("Currency".to_string())
    /// ]);
    /// assert_eq!(complex.to_string(), "pricing(Product, Base * Rate, Currency)");
    /// ```
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
    /// Parses a rule head from a pest parsing rule.
    ///
    /// This method constructs a `Head` from parsed grammar tokens, extracting
    /// the relation name and parsing each head argument. It handles the conversion
    /// from textual Macaron syntax to the structured head representation.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed head rule
    ///
    /// # Returns
    ///
    /// A new `Head` instance representing the parsed rule head.
    ///
    /// # Grammar Structure
    ///
    /// The expected grammar structure is:
    /// ```text
    /// head = { relation_name ~ "(" ~ (head_arg ~ ("," ~ head_arg)*)? ~ ")" }
    /// ```
    ///
    /// # Parsing Process
    ///
    /// 1. **Extract relation name**: Takes the first inner rule as the relation identifier
    /// 2. **Parse arguments**: Processes remaining rules as head arguments
    /// 3. **Build structure**: Constructs the head with the extracted name and parsed arguments
    ///
    /// The parser handles:
    /// - **Nullary heads**: Relations with no arguments (`connected()`)
    /// - **Variable arguments**: Simple variable references (`person(X, Y)`)
    /// - **Arithmetic arguments**: Computed expressions (`result(X, Y + 1)`)
    /// - **Mixed arguments**: Combinations of variables and expressions
    ///
    /// # Argument Processing
    ///
    /// Each head argument is parsed by [`HeadArg::from_parsed_rule`], which:
    /// - Distinguishes between variables and arithmetic expressions
    /// - Optimizes simple variables for efficiency
    /// - Preserves complex expressions for computation
    ///
    /// # Examples
    ///
    /// For input text like:
    /// - `"flag()"` → `Head { name: "flag", head_arguments: [] }`
    /// - `"person(X, Y)"` → `Head { name: "person", head_arguments: [Var("X"), Var("Y")] }`
    /// - `"result(A, B + 1)"` → `Head { name: "result", head_arguments: [Var("A"), Arith(...)] }`
    ///
    /// # Grammar Rules
    ///
    /// The method expects:
    /// - First inner rule: relation name as a string
    /// - Subsequent rules: head argument rules processed by `HeadArg::from_parsed_rule`
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::ConstType;
    use crate::{ArithmeticOperator, Factor};

    // Helper functions for creating test arguments
    fn var_arg(name: &str) -> HeadArg {
        HeadArg::Var(name.to_string())
    }

    fn arith_arg(arith: Arithmetic) -> HeadArg {
        HeadArg::Arith(arith)
    }

    fn simple_var_arithmetic(name: &str) -> Arithmetic {
        Arithmetic::new(Factor::Var(name.to_string()), vec![])
    }

    fn simple_const_arithmetic(value: i32) -> Arithmetic {
        Arithmetic::new(Factor::Const(ConstType::Integer(value)), vec![])
    }

    fn complex_arithmetic() -> Arithmetic {
        // X + 5
        Arithmetic::new(
            Factor::Var("X".to_string()),
            vec![(
                ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(5)),
            )],
        )
    }

    #[test]
    fn test_head_arg_var() {
        let arg = var_arg("X");

        match &arg {
            HeadArg::Var(name) => assert_eq!(name, "X"),
            _ => panic!("Expected Var variant"),
        }

        let vars = arg.vars();
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0], "X");
        assert_eq!(arg.to_string(), "X");
    }

    #[test]
    fn test_head_arg_arithmetic() {
        let arith = complex_arithmetic();
        let arg = arith_arg(arith.clone());

        match &arg {
            HeadArg::Arith(a) => assert_eq!(a, &arith),
            _ => panic!("Expected Arith variant"),
        }

        let vars = arg.vars();
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0], "X");
        assert_eq!(arg.to_string(), "X + 5");
    }

    #[test]
    fn test_head_arg_aggregation_vars() {
        use crate::logic::AggregationOperator;

        let arith = simple_var_arithmetic("X");
        let aggregation = Aggregation::new(AggregationOperator::Count, arith);
        let arg = HeadArg::Aggregation(aggregation);
        let vars = arg.vars();
        assert_eq!(vars.len(), 1);
        assert_eq!(vars[0], "X");
    }

    #[test]
    fn test_head_arg_aggregation_display() {
        use crate::logic::AggregationOperator;

        let arith = simple_var_arithmetic("X");
        let aggregation = Aggregation::new(AggregationOperator::Count, arith);
        let arg = HeadArg::Aggregation(aggregation);
        assert_eq!(arg.to_string(), "count(X)");
    }

    #[test]
    fn test_head_arg_clone_hash() {
        let arg = var_arg("test");
        let cloned = arg.clone();
        assert_eq!(arg, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(var_arg("X"));
        set.insert(arith_arg(simple_var_arithmetic("Y")));
        set.insert(var_arg("X")); // Duplicate
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_head_creation() {
        let args = vec![var_arg("X"), var_arg("Y"), arith_arg(complex_arithmetic())];
        let head = Head::new("result".to_string(), args);

        assert_eq!(head.name(), "result");
        assert_eq!(head.arity(), 3);
        assert_eq!(head.head_arguments().len(), 3);
    }

    #[test]
    fn test_head_empty() {
        let head = Head::new("nullary".to_string(), vec![]);
        assert_eq!(head.name(), "nullary");
        assert_eq!(head.arity(), 0);
        assert!(head.head_arguments().is_empty());
    }

    #[test]
    fn test_head_accessors() {
        let args = vec![var_arg("A"), var_arg("B")];
        let head = Head::new("test".to_string(), args.clone());

        assert_eq!(head.name(), "test");
        assert_eq!(head.arity(), 2);

        let head_args = head.head_arguments();
        assert_eq!(head_args.len(), 2);
        assert_eq!(head_args[0], args[0]);
        assert_eq!(head_args[1], args[1]);
    }

    #[test]
    fn test_head_display() {
        // Nullary head
        let nullary = Head::new("flag".to_string(), vec![]);
        assert_eq!(nullary.to_string(), "flag()");

        // Unary head
        let unary = Head::new("student".to_string(), vec![var_arg("X")]);
        assert_eq!(unary.to_string(), "student(X)");

        // Binary head with variables
        let binary = Head::new("edge".to_string(), vec![var_arg("X"), var_arg("Y")]);
        assert_eq!(binary.to_string(), "edge(X, Y)");

        // Head with arithmetic
        let with_arith = Head::new(
            "computed".to_string(),
            vec![var_arg("X"), arith_arg(complex_arithmetic())],
        );
        assert_eq!(with_arith.to_string(), "computed(X, X + 5)");

        // Complex head
        let complex = Head::new(
            "complex".to_string(),
            vec![
                var_arg("A"),
                arith_arg(simple_const_arithmetic(42)),
                var_arg("B"),
                arith_arg(complex_arithmetic()),
            ],
        );
        assert_eq!(complex.to_string(), "complex(A, 42, B, X + 5)");
    }

    #[test]
    fn test_head_clone_hash() {
        let head = Head::new("test".to_string(), vec![var_arg("X")]);
        let cloned = head.clone();
        assert_eq!(head, cloned);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(head.clone());
        set.insert(cloned);
        assert_eq!(set.len(), 1); // Should be deduplicated
    }

    #[test]
    fn test_head_arg_vars_multiple() {
        // Test HeadArg with multiple variables in arithmetic
        let arith = Arithmetic::new(
            Factor::Var("X".to_string()),
            vec![
                (ArithmeticOperator::Plus, Factor::Var("Y".to_string())),
                (
                    ArithmeticOperator::Multiply,
                    Factor::Const(ConstType::Integer(2)),
                ),
            ],
        );
        let arg = arith_arg(arith);

        let vars = arg.vars();
        assert_eq!(vars.len(), 2);
        assert!(vars.contains(&&"X".to_string()));
        assert!(vars.contains(&&"Y".to_string()));
    }

    #[test]
    fn test_head_arg_constants_only() {
        // Test HeadArg with only constants
        let arith = Arithmetic::new(
            Factor::Const(ConstType::Integer(10)),
            vec![(
                ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(5)),
            )],
        );
        let arg = arith_arg(arith);

        let vars = arg.vars();
        assert!(vars.is_empty());
        assert_eq!(arg.to_string(), "10 + 5");
    }

    #[test]
    fn test_complex_head_example() {
        // Test a realistic rule head: derived(Person, Age + 1, Status)
        let args = vec![
            var_arg("Person"),
            arith_arg(Arithmetic::new(
                Factor::Var("Age".to_string()),
                vec![(
                    ArithmeticOperator::Plus,
                    Factor::Const(ConstType::Integer(1)),
                )],
            )),
            var_arg("Status"),
        ];
        let head = Head::new("derived".to_string(), args);

        assert_eq!(head.name(), "derived");
        assert_eq!(head.arity(), 3);
        assert_eq!(head.to_string(), "derived(Person, Age + 1, Status)");

        // Check that head arguments work correctly
        let head_args = head.head_arguments();
        assert_eq!(head_args[0].vars(), vec![&"Person".to_string()]);
        assert_eq!(head_args[1].vars(), vec![&"Age".to_string()]);
        assert_eq!(head_args[2].vars(), vec![&"Status".to_string()]);
    }

    #[test]
    fn test_head_arg_display_formats() {
        // Test various display formats for HeadArg
        assert_eq!(var_arg("SimpleVar").to_string(), "SimpleVar");
        assert_eq!(arith_arg(simple_const_arithmetic(100)).to_string(), "100");

        let complex_arith = Arithmetic::new(
            Factor::Var("X".to_string()),
            vec![
                (
                    ArithmeticOperator::Multiply,
                    Factor::Const(ConstType::Integer(2)),
                ),
                (ArithmeticOperator::Plus, Factor::Var("Y".to_string())),
            ],
        );
        assert_eq!(arith_arg(complex_arith).to_string(), "X * 2 + Y");
    }
}
