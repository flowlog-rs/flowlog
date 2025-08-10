//! Relation declaration types for Datalog programs (Macaron engine).

use super::Attribute;
use crate::primitive::DataType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;
use std::str::FromStr;

/// Represents a relation declaration in a Datalog program.
///
/// A relation declaration defines the schema and metadata for a relation,
/// including its name, attributes (columns), and optional input/output paths.
/// Relations can be:
/// - **EDB relations**: Input data loaded from external sources (with input paths)
/// - **IDB relations**: Derived data computed by rules (with output paths)
///
/// # Relation Types
///
/// ## EDB Relations (Extensional Database)
/// These relations contain base facts, typically loaded from external files:
/// ```text
/// person(name: string, age: number) .input "data/people.csv"
/// ```
///
/// ## IDB Relations (Intensional Database)
/// These relations contain derived facts computed by rules:
/// ```text
/// adult(name: string) .output "results/adults.csv"
/// ```
///
/// # Examples
///
/// ```rust
/// use parser::declaration::{Attribute, Relation};
/// use parser::primitive::DataType;
///
/// // Create a simple relation: person(name: string, age: number)
/// let attributes = vec![
///     Attribute::new("name".to_string(), DataType::String),
///     Attribute::new("age".to_string(), DataType::Integer),
/// ];
/// let rel = Relation::new("person", attributes, None, None);
/// assert_eq!(rel.arity(), 2);
/// assert_eq!(rel.name(), "person");
/// ```
///
/// # Creating Relations with I/O Paths
///
/// ```rust
/// use parser::declaration::{Attribute, Relation};
/// use parser::primitive::DataType;
///
/// let attrs = vec![
///     Attribute::new("id".to_string(), DataType::Integer),
/// ];
///
/// // EDB relation with input path
/// let edb = Relation::new("facts", attrs.clone(), Some("input.csv"), None);
/// assert!(edb.input_path().is_some());
///
/// // IDB relation with output path  
/// let idb = Relation::new("results", attrs, None, Some("output.csv"));
/// assert!(idb.output_path().is_some());
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    name: String,
    attributes: Vec<Attribute>,
    input_path: Option<String>,
    output_path: Option<String>,
}

impl Relation {
    /// Creates a new relation declaration from string representations.
    ///
    /// # Arguments
    ///
    /// * `name` - The name of the relation
    /// * `attributes` - Vector of attributes defining the relation schema
    /// * `input_path` - Optional path for loading input data (for EDB relations)
    /// * `output_path` - Optional path for writing output data (for IDB relations)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::{Attribute, Relation};
    /// use parser::primitive::DataType;
    ///
    /// let attrs = vec![
    ///     Attribute::new("name".to_string(), DataType::String),
    /// ];
    ///
    /// // Basic relation without I/O
    /// let rel = Relation::new("person", attrs.clone(), None, None);
    ///
    /// // EDB relation with input
    /// let edb = Relation::new("data", attrs.clone(), Some("input.csv"), None);
    ///
    /// // IDB relation with output
    /// let idb = Relation::new("results", attrs, None, Some("output.csv"));
    /// ```
    #[must_use]
    pub fn new(
        name: &str,
        attributes: Vec<Attribute>,
        input_path: Option<&str>,
        output_path: Option<&str>,
    ) -> Self {
        Self {
            name: name.to_string(),
            attributes,
            input_path: input_path.map(str::to_string),
            output_path: output_path.map(str::to_string),
        }
    }

    /// Returns the name of this relation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Relation;
    ///
    /// let rel = Relation::new("users", vec![], None, None);
    /// assert_eq!(rel.name(), "users");
    /// ```
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the attributes of this relation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::{Attribute, Relation};
    /// use parser::primitive::DataType;
    ///
    /// let attrs = vec![
    ///     Attribute::new("id".to_string(), DataType::Integer),
    /// ];
    /// let rel = Relation::new("test", attrs.clone(), None, None);
    /// assert_eq!(rel.attributes().len(), 1);
    /// ```
    #[must_use]
    pub fn attributes(&self) -> &Vec<Attribute> {
        &self.attributes
    }

    /// Returns the arity (number of attributes) of this relation.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::{Attribute, Relation};
    /// use parser::primitive::DataType;
    ///
    /// let attrs = vec![
    ///     Attribute::new("name".to_string(), DataType::String),
    ///     Attribute::new("age".to_string(), DataType::Integer),
    /// ];
    /// let rel = Relation::new("person", attrs, None, None);
    /// assert_eq!(rel.arity(), 2);
    /// ```
    #[must_use]
    pub fn arity(&self) -> usize {
        self.attributes.len()
    }

    /// Returns the input path for this relation, if it exists.
    ///
    /// Input paths are typically used for EDB relations to specify
    /// where to load base facts from.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Relation;
    ///
    /// let rel_with_input = Relation::new("data", vec![], Some("input.csv"), None);
    /// assert_eq!(rel_with_input.input_path(), Some(&"input.csv".to_string()));
    ///
    /// let rel_without_input = Relation::new("data", vec![], None, None);
    /// assert_eq!(rel_without_input.input_path(), None);
    /// ```
    #[must_use]
    pub fn input_path(&self) -> Option<&String> {
        self.input_path.as_ref()
    }

    /// Returns the output path for this relation, if it exists.
    ///
    /// Output paths are typically used for IDB relations to specify
    /// where to write derived results.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::Relation;
    ///
    /// let rel_with_output = Relation::new("results", vec![], None, Some("output.csv"));
    /// assert_eq!(rel_with_output.output_path(), Some(&"output.csv".to_string()));
    ///
    /// let rel_without_output = Relation::new("results", vec![], None, None);
    /// assert_eq!(rel_without_output.output_path(), None);
    /// ```
    #[must_use]
    pub fn output_path(&self) -> Option<&String> {
        self.output_path.as_ref()
    }

    /// Checks if this relation is nullary (has no attributes).
    ///
    /// Nullary relations are relations with zero arity, which can be useful
    /// for representing flags or boolean conditions.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::{Attribute, Relation};
    /// use parser::primitive::DataType;
    ///
    /// let nullary = Relation::new("flag", vec![], None, None);
    /// assert!(nullary.is_nullary());
    ///
    /// let binary = Relation::new("pair", vec![
    ///     Attribute::new("x".to_string(), DataType::Integer),
    ///     Attribute::new("y".to_string(), DataType::Integer),
    /// ], None, None);
    /// assert!(!binary.is_nullary());
    /// ```
    #[must_use]
    pub fn is_nullary(&self) -> bool {
        self.attributes.is_empty()
    }
}

impl fmt::Display for Relation {
    /// Format the relation declaration in Macaron syntax.
    ///
    /// The format is: `name(attr1: type1, attr2: type2) [.input path] [.output path]`
    ///
    /// # Examples
    ///
    /// ```rust
    /// use parser::declaration::{Attribute, Relation};
    /// use parser::primitive::DataType;
    ///
    /// let attrs = vec![
    ///     Attribute::new("name".to_string(), DataType::String),
    ///     Attribute::new("age".to_string(), DataType::Integer),
    /// ];
    ///
    /// let rel = Relation::new("person", attrs, Some("people.csv"), None);
    /// let expected = "person(name: string, age: integer) .input people.csv";
    /// assert_eq!(rel.to_string(), expected);
    /// ```
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({})",
            self.name,
            self.attributes
                .iter()
                .map(|attr| attr.to_string())
                .collect::<Vec<String>>()
                .join(", ")
        )?;

        if let Some(path) = &self.input_path {
            write!(f, " .input {path}")?;
        }

        if let Some(path) = &self.output_path {
            write!(f, " .output {path}")?;
        }

        Ok(())
    }
}

impl Lexeme for Relation {
    /// Constructs a `Relation` from a parsed grammar rule.
    ///
    /// This function is called by the Macaron parser to convert a pest grammar rule
    /// representing a relation declaration into a fully-typed `Relation` object.
    /// It extracts the relation name, attributes, and optional input/output paths
    /// from the grammar tree, supporting both EDB and IDB relation forms.
    ///
    /// # Arguments
    /// * `parsed_rule` - A pest grammar rule representing a relation declaration.
    ///
    /// # Returns
    /// * `Relation` - A constructed relation object for use in the Datalog program.
    ///
    /// # Panics
    /// Panics if the grammar tree contains unexpected rules or invalid data types.
    ///
    /// # Example
    /// ```text
    /// person(name: string, age: number) .input "data/people.csv"
    /// adult(name: string) .output "results/adults.csv"
    /// ```
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let rule_type = parsed_rule.as_rule();
        let mut inner_rules = parsed_rule.into_inner();

        // Parse the relation name
        let name = inner_rules.next().unwrap().as_str();

        // Initialize empty attributes vector
        let mut attributes = Vec::new();
        let mut input_path = None;
        let mut output_path = None;

        // Process remaining rules
        for rule in inner_rules {
            match rule.as_rule() {
                Rule::attributes_decl => {
                    // Process attributes
                    attributes = rule
                        .into_inner()
                        .map(|attr| {
                            let mut attr = attr.into_inner();
                            let name = attr.next().unwrap().as_str();
                            let data_type = attr.next().unwrap().as_str();
                            Attribute::new(
                                name.to_string(),
                                DataType::from_str(data_type).expect("Invalid data type"),
                            )
                        })
                        .collect();
                }
                Rule::in_decl => {
                    input_path = Some(rule.into_inner().next().unwrap().as_str());
                }
                Rule::out_decl => {
                    output_path = Some(rule.into_inner().next().unwrap().as_str());
                }
                _ => unreachable!(
                    "Unexpected rule in relation declaration: {:?}",
                    rule.as_rule()
                ),
            }
        }

        // Create the appropriate relation based on rule type
        match rule_type {
            Rule::edb_relation_decl => Self::new(name, attributes, input_path, None),
            Rule::idb_relation_decl => Self::new(name, attributes, None, output_path),
            _ => Self::new(name, attributes, input_path, output_path),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_attributes() -> Vec<Attribute> {
        vec![
            Attribute::new("id".to_string(), DataType::Integer),
            Attribute::new("name".to_string(), DataType::String),
        ]
    }

    #[test]
    fn test_new_relation() {
        let attrs = create_test_attributes();
        let rel = Relation::new("test", attrs, None, None);

        assert_eq!(rel.name(), "test");
        assert_eq!(rel.arity(), 2);
        assert!(!rel.is_nullary());
        assert_eq!(rel.input_path(), None);
        assert_eq!(rel.output_path(), None);
    }

    #[test]
    fn test_relation_with_input_path() {
        let attrs = create_test_attributes();
        let rel = Relation::new("edb_rel", attrs, Some("input.csv"), None);

        assert_eq!(rel.input_path(), Some(&"input.csv".to_string()));
        assert_eq!(rel.output_path(), None);
    }

    #[test]
    fn test_relation_with_output_path() {
        let attrs = create_test_attributes();
        let rel = Relation::new("idb_rel", attrs, None, Some("output.csv"));

        assert_eq!(rel.input_path(), None);
        assert_eq!(rel.output_path(), Some(&"output.csv".to_string()));
    }

    #[test]
    fn test_nullary_relation() {
        let rel = Relation::new("flag", vec![], None, None);

        assert!(rel.is_nullary());
        assert_eq!(rel.arity(), 0);
        assert!(rel.attributes().is_empty());
    }

    #[test]
    fn test_relation_display() {
        let attrs = vec![
            Attribute::new("x".to_string(), DataType::Integer),
            Attribute::new("y".to_string(), DataType::String),
        ];

        // Basic relation
        let basic = Relation::new("test", attrs.clone(), None, None);
        assert_eq!(basic.to_string(), "test(x: integer, y: string)");

        // With input path
        let with_input = Relation::new("edb", attrs.clone(), Some("data.csv"), None);
        assert_eq!(
            with_input.to_string(),
            "edb(x: integer, y: string) .input data.csv"
        );

        // With output path
        let with_output = Relation::new("idb", attrs.clone(), None, Some("result.csv"));
        assert_eq!(
            with_output.to_string(),
            "idb(x: integer, y: string) .output result.csv"
        );

        // Nullary relation
        let nullary = Relation::new("flag", vec![], None, None);
        assert_eq!(nullary.to_string(), "flag()");
    }

    #[test]
    fn test_relation_equality() {
        let attrs1 = create_test_attributes();
        let attrs2 = create_test_attributes();

        let rel1 = Relation::new("test", attrs1, None, None);
        let rel2 = Relation::new("test", attrs2, None, None);
        let rel3 = Relation::new("different", create_test_attributes(), None, None);

        assert_eq!(rel1, rel2);
        assert_ne!(rel1, rel3);
    }

    #[test]
    fn test_relation_clone() {
        let attrs = create_test_attributes();
        let original = Relation::new("test", attrs, Some("input.csv"), Some("output.csv"));
        let cloned = original.clone();

        assert_eq!(original, cloned);
        assert_eq!(original.name(), cloned.name());
        assert_eq!(original.arity(), cloned.arity());
        assert_eq!(original.input_path(), cloned.input_path());
        assert_eq!(original.output_path(), cloned.output_path());
    }
}
