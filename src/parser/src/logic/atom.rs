//! Atom types for FlowLog.
//!
//! This module provides types for representing atoms in FlowLog rules.
//! Atoms are the fundamental building blocks of logic programming and consist of a
//! relation name followed by a list of arguments. They represent facts and queries
//! in the logical knowledge base.
//!
//! # Overview
//!
//! The module defines two main types:
//! - [`AtomArg`]: Arguments that can appear in atoms (variables, constants, placeholders)
//! - [`Atom`]: Complete atomic predicates with relation names and arguments
//!
//! # Atom Structure
//!
//! An atom follows the pattern: `relation_name(arg1, arg2, ..., argN)`
//!
//! Arguments can be:
//! - **Variables**: Identifiers that can be bound to values (e.g., `X`, `Person`)
//! - **Constants**: Literal values like integers or strings (e.g., `42`, `"Alice"`)
//! - **Placeholders**: Anonymous variables represented by `_`
//!
//! # Examples
//!
//! ```rust
//! use parser::logic::{Atom, AtomArg};
//! use parser::primitive::ConstType;
//!
//! // Create atom: person("Alice", 25, _)
//! let person_atom = Atom::new("person", vec![
//!     AtomArg::Const(ConstType::Text("Alice".to_string())),
//!     AtomArg::Const(ConstType::Integer(25)),
//!     AtomArg::Placeholder,
//! ]);
//! assert_eq!(person_atom.arity(), 3);
//! assert_eq!(person_atom.to_string(), "person(\"Alice\", 25, _)");
//!
//! // Create atom with variables: edge(X, Y)
//! let edge_atom = Atom::new("edge", vec![
//!     AtomArg::Var("X".to_string()),
//!     AtomArg::Var("Y".to_string()),
//! ]);
//! assert_eq!(edge_atom.arity(), 2);
//! assert_eq!(edge_atom.to_string(), "edge(X, Y)");
//!
//! // Create nullary atom (no arguments): connected()
//! let flag_atom = Atom::new("connected", vec![]);
//! assert_eq!(flag_atom.arity(), 0);
//! assert_eq!(flag_atom.to_string(), "connected()");
//! ```

use crate::primitive::ConstType;
use crate::{Lexeme, Rule};

use pest::iterators::Pair;
use std::fmt;

/// Represents an argument to an atom in rule expressions.
///
/// Atom arguments are the building blocks of atoms, providing the ability
/// to express variables, concrete values, and anonymous placeholders within logical
/// expressions. Each argument type serves a specific purpose in logic programming.
///
/// # Variants
///
/// - **`Var(String)`**: Variable arguments that can be bound to values during rule
///   evaluation. Variables enable pattern matching and data flow between predicates.
/// - **`Const(ConstType)`**: Constant arguments representing literal values that are
///   known at compile time. These include integers, strings, and other data types.
/// - **`Placeholder`**: Anonymous variables represented by `_` that match any value
///   but don't bind the matched value to a name. Useful for ignoring specific positions.
///
/// # Usage Patterns
///
/// Arguments are commonly used for:
/// - **Variables**: Establishing relationships and data flow (`X`, `Person`, `Count`)
/// - **Constants**: Specifying concrete values (`42`, `"Alice"`, `true`)
/// - **Placeholders**: Ignoring positions while maintaining arity (`_`)
///
/// # Examples
///
/// ```rust
/// use parser::logic::AtomArg;
/// use parser::primitive::ConstType;
///
/// // Variable argument - can be bound to any value
/// let var_arg = AtomArg::Var("X".to_string());
/// assert!(var_arg.is_var());
/// assert_eq!(var_arg.as_var(), &"X".to_string());
/// assert_eq!(var_arg.to_string(), "X");
///
/// // Constant integer argument - fixed value
/// let int_arg = AtomArg::Const(ConstType::Integer(42));
/// assert!(int_arg.is_const());
/// assert_eq!(int_arg.to_string(), "42");
///
/// // Constant string argument - fixed text value
/// let str_arg = AtomArg::Const(ConstType::Text("Alice".to_string()));
/// assert!(str_arg.is_const());
/// assert_eq!(str_arg.to_string(), "\"Alice\"");
///
/// // Placeholder argument - matches anything, doesn't bind
/// let placeholder = AtomArg::Placeholder;
/// assert!(placeholder.is_placeholder());
/// assert_eq!(placeholder.to_string(), "_");
/// ```
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum AtomArg {
    /// A variable argument
    Var(String),
    /// A constant argument
    Const(ConstType),
    /// A placeholder argument (_)
    Placeholder,
}

impl AtomArg {
    /// Checks if this argument is a variable.
    ///
    /// Returns `true` if this argument represents a variable that can be bound
    /// to values during rule evaluation, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AtomArg;
    /// use parser::primitive::ConstType;
    ///
    /// let var = AtomArg::Var("Person".to_string());
    /// let constant = AtomArg::Const(ConstType::Integer(42));
    /// let placeholder = AtomArg::Placeholder;
    ///
    /// assert!(var.is_var());
    /// assert!(!constant.is_var());
    /// assert!(!placeholder.is_var());
    /// ```
    #[must_use]
    pub fn is_var(&self) -> bool {
        matches!(self, Self::Var(_))
    }

    /// Checks if this argument is a constant.
    ///
    /// Returns `true` if this argument represents a literal constant value
    /// that is known at compile time, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AtomArg;
    /// use parser::primitive::ConstType;
    ///
    /// let var = AtomArg::Var("X".to_string());
    /// let int_const = AtomArg::Const(ConstType::Integer(100));
    /// let str_const = AtomArg::Const(ConstType::Text("hello".to_string()));
    /// let placeholder = AtomArg::Placeholder;
    ///
    /// assert!(!var.is_const());
    /// assert!(int_const.is_const());
    /// assert!(str_const.is_const());
    /// assert!(!placeholder.is_const());
    /// ```
    #[must_use]
    pub fn is_const(&self) -> bool {
        matches!(self, Self::Const(_))
    }

    /// Checks if this argument is a placeholder.
    ///
    /// Returns `true` if this argument is an anonymous variable (`_`) that
    /// matches any value without binding it to a name, `false` otherwise.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AtomArg;
    /// use parser::primitive::ConstType;
    ///
    /// let var = AtomArg::Var("Y".to_string());
    /// let constant = AtomArg::Const(ConstType::Integer(25));
    /// let placeholder = AtomArg::Placeholder;
    ///
    /// assert!(!var.is_placeholder());
    /// assert!(!constant.is_placeholder());
    /// assert!(placeholder.is_placeholder());
    /// ```
    #[must_use]
    pub fn is_placeholder(&self) -> bool {
        matches!(self, Self::Placeholder)
    }

    /// Extracts the variable name from this argument.
    ///
    /// Returns a reference to the variable name string. This method should only
    /// be called on arguments that are known to be variables, as it will panic
    /// for constants or placeholders.
    ///
    /// # Returns
    ///
    /// A reference to the variable name string.
    ///
    /// # Panics
    ///
    /// Panics if this argument is not a variable (i.e., if it's a constant or placeholder).
    /// Use [`is_var`](Self::is_var) to check the argument type before calling this method.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AtomArg;
    ///
    /// let var_arg = AtomArg::Var("PersonName".to_string());
    /// assert_eq!(var_arg.as_var(), &"PersonName".to_string());
    ///
    /// // Safe usage with type checking
    /// if var_arg.is_var() {
    ///     println!("Variable name: {}", var_arg.as_var());
    /// }
    /// ```
    ///
    /// ```should_panic
    /// use parser::logic::AtomArg;
    /// use parser::primitive::ConstType;
    ///
    /// let const_arg = AtomArg::Const(ConstType::Integer(42));
    /// let _ = const_arg.as_var(); // This will panic!
    /// ```
    #[must_use]
    pub fn as_var(&self) -> &String {
        match self {
            Self::Var(var) => var,
            _ => unreachable!("Expected variable but got: {self:?}"),
        }
    }
}

impl fmt::Display for AtomArg {
    /// Formats the atom argument for display.
    ///
    /// Each argument type is formatted according to its semantic meaning:
    /// - Variables are displayed as their name without any special formatting
    /// - Constants use their own `Display` implementation (strings include quotes)
    /// - Placeholders are displayed as the underscore symbol (`_`)
    ///
    /// This formatting is suitable for debugging, logging, and generating
    /// human-readable representations of FlowLog expressions.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::AtomArg;
    /// use parser::primitive::ConstType;
    ///
    /// let var = AtomArg::Var("PersonId".to_string());
    /// let int_const = AtomArg::Const(ConstType::Integer(12345));
    /// let str_const = AtomArg::Const(ConstType::Text("John".to_string()));
    /// let placeholder = AtomArg::Placeholder;
    ///
    /// assert_eq!(var.to_string(), "PersonId");
    /// assert_eq!(int_const.to_string(), "12345");
    /// assert_eq!(str_const.to_string(), "\"John\"");
    /// assert_eq!(placeholder.to_string(), "_");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Var(var) => write!(f, "{var}"),
            Self::Const(constant) => write!(f, "{constant}"),
            Self::Placeholder => write!(f, "_"),
        }
    }
}

impl Lexeme for AtomArg {
    /// Parses an atom argument from a pest parsing rule.
    ///
    /// This method constructs an `AtomArg` from parsed grammar tokens, handling
    /// the conversion from textual representation to the appropriate argument type.
    /// It expects the parsed rule to contain exactly one inner rule representing
    /// the specific argument variant.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed atom argument rule
    ///
    /// # Returns
    ///
    /// A new `AtomArg` instance representing the parsed argument.
    ///
    /// # Grammar Mapping
    ///
    /// The method maps grammar rules to argument types:
    /// - `Rule::variable` → `AtomArg::Var(String)` - Variable references like `X`, `Person`
    /// - `Rule::constant` → `AtomArg::Const(ConstType)` - Literal values like `42`, `"text"`
    /// - `Rule::placeholder` → `AtomArg::Placeholder` - Anonymous variables (`_`)
    ///
    /// # Panics
    ///
    /// Panics if:
    /// - The parsed rule contains no inner rules
    /// - The inner rule type is not one of the expected argument types
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let inner = parsed_rule
            .into_inner()
            .next()
            .expect("Expected inner rule for head_arg");

        match inner.as_rule() {
            Rule::variable => Self::Var(inner.as_str().to_string()),
            Rule::constant => Self::Const(ConstType::from_parsed_rule(inner)),
            Rule::placeholder => Self::Placeholder,
            _ => unreachable!("Invalid atom argument type: {:?}", inner.as_rule()),
        }
    }
}

/// Represents an atom in FlowLog rules.
///
/// Atoms are the fundamental building blocks of FlowLog rules, representing
/// relations with named arguments. They correspond to predicates in logic programming
/// and form the basis of both facts and queries in the knowledge base.
///
/// # Structure
///
/// An atom consists of:
/// - **Name**: A string identifier for the relation (e.g., `"person"`, `"edge"`, `"connected"`)
/// - **Arguments**: A vector of [`AtomArg`] values representing the relation's parameters
///
/// The structure follows the pattern: `relation_name(arg1, arg2, ..., argN)`
///
/// # Arity
///
/// The arity of an atom is the number of arguments it contains. Atoms can be:
/// - **Nullary** (arity 0): Relations with no arguments, like `connected()`
/// - **Unary** (arity 1): Relations with one argument, like `student(X)`
/// - **Binary** (arity 2): Relations with two arguments, like `parent(X, Y)`
/// - **N-ary** (arity N): Relations with N arguments
///
/// # Examples
///
/// ```rust
/// use parser::logic::{Atom, AtomArg};
/// use parser::primitive::ConstType;
///
/// // Nullary atom: connected()
/// let nullary = Atom::new("connected", vec![]);
/// assert_eq!(nullary.arity(), 0);
/// assert_eq!(nullary.to_string(), "connected()");
///
/// // Unary atom with variable: student(X)
/// let unary = Atom::new("student", vec![
///     AtomArg::Var("X".to_string())
/// ]);
/// assert_eq!(unary.arity(), 1);
/// assert_eq!(unary.to_string(), "student(X)");
///
/// // Binary atom with constants: parent("John", "Mary")
/// let binary = Atom::new("parent", vec![
///     AtomArg::Const(ConstType::Text("John".to_string())),
///     AtomArg::Const(ConstType::Text("Mary".to_string()))
/// ]);
/// assert_eq!(binary.arity(), 2);
/// assert_eq!(binary.to_string(), "parent(\"John\", \"Mary\")");
///
/// // Mixed arguments: person("Alice", 25, _)
/// let mixed = Atom::new("person", vec![
///     AtomArg::Const(ConstType::Text("Alice".to_string())),
///     AtomArg::Const(ConstType::Integer(25)),
///     AtomArg::Placeholder
/// ]);
/// assert_eq!(mixed.arity(), 3);
/// assert_eq!(mixed.to_string(), "person(\"Alice\", 25, _)");
///
/// // Dynamic construction
/// let mut dynamic = Atom::new("relation", vec![]);
/// dynamic.push_arg(AtomArg::Var("X".to_string()));
/// dynamic.push_arg(AtomArg::Const(ConstType::Integer(42)));
/// assert_eq!(dynamic.arity(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Atom {
    name: String,
    arguments: Vec<AtomArg>,
}

impl fmt::Display for Atom {
    /// Formats the atom for display using standard predicate notation.
    ///
    /// The format follows the standard logical notation: `relation_name(arg1, arg2, ..., argN)`
    /// where arguments are separated by commas and spaces. This produces output suitable
    /// for debugging, logging, query representation, and human-readable rule display.
    ///
    /// # Format Structure
    ///
    /// - **Relation name**: Displayed as-is without quotes or formatting
    /// - **Parentheses**: Always present, even for nullary relations
    /// - **Arguments**: Comma-separated with spaces, using each argument's `Display` implementation
    /// - **Empty arguments**: Results in just parentheses: `relation()`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// // Nullary relation
    /// let nullary = Atom::new("flag", vec![]);
    /// assert_eq!(nullary.to_string(), "flag()");
    ///
    /// // Unary relation with variable
    /// let unary = Atom::new("student", vec![
    ///     AtomArg::Var("Person".to_string())
    /// ]);
    /// assert_eq!(unary.to_string(), "student(Person)");
    ///
    /// // Binary relation with constants
    /// let binary = Atom::new("age", vec![
    ///     AtomArg::Const(ConstType::Text("Alice".to_string())),
    ///     AtomArg::Const(ConstType::Integer(30))
    /// ]);
    /// assert_eq!(binary.to_string(), "age(\"Alice\", 30)");
    ///
    /// // Complex relation with mixed argument types
    /// let complex = Atom::new("relationship", vec![
    ///     AtomArg::Var("X".to_string()),
    ///     AtomArg::Const(ConstType::Text("knows".to_string())),
    ///     AtomArg::Placeholder,
    ///     AtomArg::Const(ConstType::Integer(2023))
    /// ]);
    /// assert_eq!(complex.to_string(), "relationship(X, \"knows\", _, 2023)");
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.arguments
                .iter()
                .map(|arg| arg.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )
    }
}

impl Atom {
    /// Creates a new atom from a name and a list of arguments.
    ///
    /// This is the primary constructor for building atoms programmatically.
    /// The relation name should follow FlowLog naming conventions, and arguments
    /// can be any combination of variables, constants, and placeholders.
    ///
    /// # Arguments
    ///
    /// * `name` - The relation name as a string slice
    /// * `arguments` - A vector of [`AtomArg`] values representing the relation's parameters
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// // Create a simple fact: person("Alice")
    /// let person = Atom::new("person", vec![
    ///     AtomArg::Const(ConstType::Text("Alice".to_string()))
    /// ]);
    /// assert_eq!(person.name(), "person");
    /// assert_eq!(person.arity(), 1);
    ///
    /// // Create a relationship: edge(X, Y)
    /// let edge = Atom::new("edge", vec![
    ///     AtomArg::Var("X".to_string()),
    ///     AtomArg::Var("Y".to_string())
    /// ]);
    /// assert_eq!(edge.name(), "edge");
    /// assert_eq!(edge.arity(), 2);
    ///
    /// // Create a nullary atom: connected()
    /// let connected = Atom::new("connected", vec![]);
    /// assert_eq!(connected.name(), "connected");
    /// assert_eq!(connected.arity(), 0);
    /// ```
    #[must_use]
    pub fn new(name: &str, arguments: Vec<AtomArg>) -> Self {
        Self {
            name: name.to_string(),
            arguments,
        }
    }

    /// Adds an argument to this atom.
    ///
    /// This method allows for dynamic construction of atoms by appending arguments
    /// one at a time. It's useful when building atoms incrementally or when the
    /// number of arguments is determined at runtime.
    ///
    /// # Arguments
    ///
    /// * `arg` - The [`AtomArg`] to append to this atom's argument list
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// // Start with an empty atom
    /// let mut person = Atom::new("person", vec![]);
    /// assert_eq!(person.arity(), 0);
    ///
    /// // Add name argument
    /// person.push_arg(AtomArg::Const(ConstType::Text("Bob".to_string())));
    /// assert_eq!(person.arity(), 1);
    /// assert_eq!(person.to_string(), "person(\"Bob\")");
    ///
    /// // Add age argument
    /// person.push_arg(AtomArg::Const(ConstType::Integer(25)));
    /// assert_eq!(person.arity(), 2);
    /// assert_eq!(person.to_string(), "person(\"Bob\", 25)");
    ///
    /// // Add a placeholder
    /// person.push_arg(AtomArg::Placeholder);
    /// assert_eq!(person.arity(), 3);
    /// assert_eq!(person.to_string(), "person(\"Bob\", 25, _)");
    /// ```
    pub fn push_arg(&mut self, arg: AtomArg) {
        self.arguments.push(arg);
    }

    /// Returns the name of this atom.
    ///
    /// The name identifies the relation that this atom represents. It's used
    /// for matching atoms in rule bodies, determining which rules apply,
    /// and organizing facts in the knowledge base.
    ///
    /// # Returns
    ///
    /// A string slice containing the relation name.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// let student = Atom::new("student", vec![
    ///     AtomArg::Var("Person".to_string())
    /// ]);
    /// assert_eq!(student.name(), "student");
    ///
    /// let parent = Atom::new("parent", vec![
    ///     AtomArg::Var("X".to_string()),
    ///     AtomArg::Var("Y".to_string())
    /// ]);
    /// assert_eq!(parent.name(), "parent");
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the arguments of this atom.
    ///
    /// The arguments represent the parameters of the relation. Each argument
    /// can be a variable, constant, or placeholder, and together they define
    /// the structure and constraints of the atomic predicate.
    ///
    /// # Returns
    ///
    /// A reference to the vector of [`AtomArg`] values.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// let atom = Atom::new("knows", vec![
    ///     AtomArg::Var("Person1".to_string()),
    ///     AtomArg::Var("Person2".to_string()),
    ///     AtomArg::Const(ConstType::Integer(2023))
    /// ]);
    ///
    /// let args = atom.arguments();
    /// assert_eq!(args.len(), 3);
    /// assert!(args[0].is_var());
    /// assert!(args[1].is_var());
    /// assert!(args[2].is_const());
    ///
    /// // Access individual arguments
    /// if let AtomArg::Var(name) = &args[0] {
    ///     assert_eq!(name, "Person1");
    /// }
    /// ```
    #[must_use]
    pub fn arguments(&self) -> &Vec<AtomArg> {
        &self.arguments
    }

    /// Returns the arity (number of arguments) of this atom.
    ///
    /// The arity is a fundamental property that determines how many parameters
    /// the relation accepts. It's used for type checking, rule matching, and
    /// ensuring consistency in the knowledge base.
    ///
    /// # Returns
    ///
    /// The number of arguments as a `usize`.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::logic::{Atom, AtomArg};
    /// use parser::primitive::ConstType;
    ///
    /// // Nullary atom (0 arguments)
    /// let nullary = Atom::new("flag", vec![]);
    /// assert_eq!(nullary.arity(), 0);
    ///
    /// // Unary atom (1 argument)
    /// let unary = Atom::new("student", vec![
    ///     AtomArg::Var("X".to_string())
    /// ]);
    /// assert_eq!(unary.arity(), 1);
    ///
    /// // Binary atom (2 arguments)
    /// let binary = Atom::new("parent", vec![
    ///     AtomArg::Var("X".to_string()),
    ///     AtomArg::Var("Y".to_string())
    /// ]);
    /// assert_eq!(binary.arity(), 2);
    ///
    /// // Ternary atom (3 arguments)
    /// let ternary = Atom::new("grade", vec![
    ///     AtomArg::Var("Student".to_string()),
    ///     AtomArg::Var("Course".to_string()),
    ///     AtomArg::Var("Grade".to_string())
    /// ]);
    /// assert_eq!(ternary.arity(), 3);
    /// ```
    #[must_use]
    pub fn arity(&self) -> usize {
        self.arguments.len()
    }
}

impl Lexeme for Atom {
    /// Parses an atom from a pest parsing rule.
    ///
    /// This method constructs an `Atom` from parsed grammar tokens, extracting
    /// the relation name and parsing each argument. It handles the conversion
    /// from textual FlowLog syntax to the structured atom representation.
    ///
    /// # Arguments
    ///
    /// * `parsed_rule` - A pest `Pair` containing the parsed atom rule
    ///
    /// # Returns
    ///
    /// A new `Atom` instance representing the parsed atomic predicate.
    ///
    /// # Grammar Structure
    ///
    /// The expected grammar structure is:
    /// ```text
    /// atom = { relation_name ~ "(" ~ (atom_arg ~ ("," ~ atom_arg)*)? ~ ")" }
    /// ```
    ///
    /// # Parsing Process
    ///
    /// 1. **Extract relation name**: Takes the first inner rule as the relation identifier
    /// 2. **Parse arguments**: Iterates through remaining `atom_arg` rules
    /// 3. **Build structure**: Constructs the atom with the extracted name and parsed arguments
    ///
    /// The parser handles:
    /// - **Nullary atoms**: Relations with no arguments (`connected()`)
    /// - **N-ary atoms**: Relations with any number of arguments (`person("Alice", 25)`)
    /// - **Mixed argument types**: Variables, constants, and placeholders in any combination
    ///
    /// # Examples
    ///
    /// For input text like:
    /// - `"flag()"` → `Atom { name: "flag", arguments: [] }`
    /// - `"student(X)"` → `Atom { name: "student", arguments: [Var("X")] }`
    /// - `"person(\"Alice\", 25, _)"` → `Atom { name: "person", arguments: [Const(Text("Alice")), Const(Integer(25)), Placeholder] }`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();
        let name = inner.next().unwrap().as_str().to_string();
        let mut arguments = Vec::new();

        for pair in inner {
            if pair.as_rule() == Rule::atom_arg {
                arguments.push(AtomArg::from_parsed_rule(pair));
            }
        }

        Self { name, arguments }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Helper functions for creating test arguments
    fn var_arg(name: &str) -> AtomArg {
        AtomArg::Var(name.to_string())
    }

    fn int_arg(value: i32) -> AtomArg {
        AtomArg::Const(ConstType::Integer(value))
    }

    fn str_arg(value: &str) -> AtomArg {
        AtomArg::Const(ConstType::Text(value.to_string()))
    }

    fn placeholder_arg() -> AtomArg {
        AtomArg::Placeholder
    }

    #[test]
    fn test_atom_arg_variable() {
        let arg = var_arg("x");
        assert!(arg.is_var());
        assert!(!arg.is_const());
        assert!(!arg.is_placeholder());
        assert_eq!(arg.as_var(), &"x".to_string());
        assert_eq!(arg.to_string(), "x");
    }

    #[test]
    fn test_atom_arg_constant_integer() {
        let arg = int_arg(42);
        assert!(!arg.is_var());
        assert!(arg.is_const());
        assert!(!arg.is_placeholder());
        assert_eq!(arg.to_string(), "42");
    }

    #[test]
    fn test_atom_arg_constant_string() {
        let arg = str_arg("hello");
        assert!(!arg.is_var());
        assert!(arg.is_const());
        assert!(!arg.is_placeholder());
        assert_eq!(arg.to_string(), "\"hello\"");
    }

    #[test]
    fn test_atom_arg_placeholder() {
        let arg = placeholder_arg();
        assert!(!arg.is_var());
        assert!(!arg.is_const());
        assert!(arg.is_placeholder());
        assert_eq!(arg.to_string(), "_");
    }

    #[test]
    #[should_panic(expected = "Expected variable but got:")]
    fn test_atom_arg_as_var_panic_on_const() {
        let arg = int_arg(42);
        let _ = arg.as_var(); // Should panic
    }

    #[test]
    #[should_panic(expected = "Expected variable but got:")]
    fn test_atom_arg_as_var_panic_on_placeholder() {
        let arg = placeholder_arg();
        let _ = arg.as_var(); // Should panic
    }

    #[test]
    fn test_atom_creation() {
        let atom = Atom::new(
            "person",
            vec![str_arg("Alice"), int_arg(25), placeholder_arg()],
        );

        assert_eq!(atom.name(), "person");
        assert_eq!(atom.arity(), 3);
        assert_eq!(atom.arguments().len(), 3);
    }

    #[test]
    fn test_atom_empty() {
        let atom = Atom::new("nullary", vec![]);
        assert_eq!(atom.name(), "nullary");
        assert_eq!(atom.arity(), 0);
        assert!(atom.arguments().is_empty());
    }

    #[test]
    fn test_atom_push_arg() {
        let mut atom = Atom::new("relation", vec![var_arg("x")]);
        assert_eq!(atom.arity(), 1);

        atom.push_arg(int_arg(42));
        assert_eq!(atom.arity(), 2);

        atom.push_arg(placeholder_arg());
        assert_eq!(atom.arity(), 3);
    }

    #[test]
    fn test_atom_display() {
        // Nullary atom
        let nullary = Atom::new("flag", vec![]);
        assert_eq!(nullary.to_string(), "flag()");

        // Unary atom
        let unary = Atom::new("student", vec![str_arg("John")]);
        assert_eq!(unary.to_string(), "student(\"John\")");

        // Binary atom
        let binary = Atom::new("edge", vec![var_arg("X"), var_arg("Y")]);
        assert_eq!(binary.to_string(), "edge(X, Y)");

        // Mixed arguments
        let mixed = Atom::new(
            "person",
            vec![
                str_arg("Alice"),
                int_arg(30),
                placeholder_arg(),
                var_arg("Z"),
            ],
        );
        assert_eq!(mixed.to_string(), "person(\"Alice\", 30, _, Z)");
    }

    #[test]
    fn test_atom_complex_example() {
        // hasChild(parent, child, age, isAdult)
        let atom = Atom::new(
            "hasChild",
            vec![
                var_arg("Parent"),
                str_arg("Emma"),
                int_arg(16),
                placeholder_arg(),
            ],
        );

        assert_eq!(atom.name(), "hasChild");
        assert_eq!(atom.arity(), 4);
        assert_eq!(atom.to_string(), "hasChild(Parent, \"Emma\", 16, _)");

        // Check individual arguments
        let args = atom.arguments();
        assert!(args[0].is_var());
        assert!(args[1].is_const());
        assert!(args[2].is_const());
        assert!(args[3].is_placeholder());
    }
}
