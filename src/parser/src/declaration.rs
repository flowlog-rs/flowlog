//! Relation and attribute declarations for FlowLog.
//!
//! This module defines the structures for declaring relations and their attributes
//! in FlowLog programs. Relations can be:
//! - EDB (Extensional Database) relations: Input data loaded from files
//! - IDB (Intensional Database) relations: Derived data computed by rules

use pest::iterators::Pair;
use std::fmt;
use std::str::FromStr;

use super::{primitive::DataType, Lexeme, Rule};

/// Represents an attribute declaration in a relation.
///
/// An attribute consists of a name and a data type, defining the schema
/// for a column in a relation. Attributes are used to specify the structure
/// of both EDB and IDB relations.
///
/// # Examples
///
/// ```rust
/// use parser::declaration::Attribute;
/// use parser::primitive::DataType;
///
/// // Create an attribute for a person's name
/// let name_attr = Attribute::new("name".to_string(), DataType::String);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Attribute {
    name: String,
    data_type: DataType,
}

impl Attribute {
    /// Creates a new attribute from a name and data type.
    #[must_use]
    pub fn new(name: String, data_type: DataType) -> Self {
        Self { name, data_type }
    }

    /// Returns the name of this attribute.
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the data type of this attribute.
    #[must_use]
    pub fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

impl fmt::Display for Attribute {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.name, self.data_type)
    }
}

/// Represents a relation declaration in a FlowLog program.
///
/// A relation declaration defines the schema and metadata for a relation,
/// including its name, attributes (columns), and optional input/output paths.
/// Relations can be:
/// - **EDB relations**: Input data loaded from external sources
/// - **IDB relations**: Derived data computed by rules
///
/// # Examples
///
/// ```rust
/// use parser::declaration::{Attribute, RelationDecl};
/// use parser::primitive::DataType;
///
/// // Create a simple relation: person(name: string, age: int)
/// let attributes = vec![
///     Attribute::new("name".to_string(), DataType::String),
///     Attribute::new("age".to_string(), DataType::Integer),
/// ];
/// let rel = RelationDecl::new("person", attributes, None, None);
/// assert_eq!(rel.arity(), 2);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationDecl {
    name: String,
    attributes: Vec<Attribute>,
    input_path: Option<String>,
    output_path: Option<String>,
}

impl RelationDecl {
    /// Creates a new relation declaration from string representations.
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
    #[must_use]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the attributes of this relation.
    #[must_use]
    pub fn attributes(&self) -> &Vec<Attribute> {
        &self.attributes
    }

    /// Returns the arity (number of attributes) of this relation.
    #[must_use]
    pub fn arity(&self) -> usize {
        self.attributes.len()
    }

    /// Returns the input path for this relation, if it exists.
    #[must_use]
    pub fn input_path(&self) -> Option<&String> {
        self.input_path.as_ref()
    }

    /// Returns the output path for this relation, if it exists.
    #[must_use]
    pub fn output_path(&self) -> Option<&String> {
        self.output_path.as_ref()
    }

    /// Checks if this relation is nullary (has no attributes).
    #[must_use]
    pub fn is_nullary(&self) -> bool {
        self.attributes.is_empty()
    }
}

impl fmt::Display for RelationDecl {
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

impl Lexeme for RelationDecl {
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
