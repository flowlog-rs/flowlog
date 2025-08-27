//! Relation declaration types for Macaron Datalog programs.

use super::Attribute;
use crate::primitive::DataType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;
use std::str::FromStr;

/// A relation schema with an optional input (EDB) and/or output (IDB) path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    name: String,
    attributes: Vec<Attribute>,
    input_path: Option<String>,
    output_path: Option<String>,
}

impl Relation {
    /// Create a new relation.
    #[must_use]
    #[inline]
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

    /// Relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Attributes of the relation.
    #[must_use]
    #[inline]
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    /// Number of attributes.
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.attributes.len()
    }

    /// Optional EDB input path.
    #[must_use]
    #[inline]
    pub fn input_path(&self) -> Option<&String> {
        self.input_path.as_ref()
    }

    /// Optional IDB output path.
    #[must_use]
    #[inline]
    pub fn output_path(&self) -> Option<&String> {
        self.output_path.as_ref()
    }

    /// `true` if no attributes.
    #[must_use]
    #[inline]
    pub fn is_nullary(&self) -> bool {
        self.attributes.is_empty()
    }
}

impl fmt::Display for Relation {
    /// Formats as `name(a: ty, b: ty) [.input path] [.output path]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}(", self.name)?;
        for (i, attr) in self.attributes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{attr}")?;
        }
        write!(f, ")")?;

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
    /// Build a `Relation` from a parsed grammar rule.
    ///
    /// # Panics
    /// Panics if the grammar tree is malformed or contains unknown datatypes.
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let rule_type = parsed_rule.as_rule();
        let mut inner = parsed_rule.into_inner();

        // name
        let name = inner
            .next()
            .expect("Parser error: relation missing name")
            .as_str();

        let mut attributes: Vec<Attribute> = Vec::new();
        let mut input_path: Option<String> = None;
        let mut output_path: Option<String> = None;

        for rule in inner {
            match rule.as_rule() {
                Rule::attributes_decl => {
                    attributes = rule
                        .into_inner()
                        .map(|attr| {
                            let mut parts = attr.into_inner();
                            let aname = parts
                                .next()
                                .expect("Parser error: attribute missing name")
                                .as_str();
                            let dts = parts
                                .next()
                                .expect("Parser error: attribute missing datatype")
                                .as_str();
                            let dt = DataType::from_str(dts).unwrap_or_else(|e| {
                                panic!(
                                    "Parser error: invalid datatype `{dts}` for attribute `{aname}`: {e}"
                                )
                            });
                            Attribute::new(aname.to_string(), dt)
                        })
                        .collect();
                }
                Rule::in_decl => {
                    // inner is likely a quoted string; store unquoted path
                    let raw = rule
                        .into_inner()
                        .next()
                        .expect("Parser error: .input missing path")
                        .as_str();
                    input_path = Some(raw.trim_matches('"').to_string());
                }
                Rule::out_decl => {
                    let raw = rule
                        .into_inner()
                        .next()
                        .expect("Parser error: .output missing path")
                        .as_str();
                    output_path = Some(raw.trim_matches('"').to_string());
                }
                _ => {
                    unreachable!(
                        "Parser error: unexpected rule in relation declaration: {:?}",
                        rule.as_rule()
                    )
                }
            }
        }

        match rule_type {
            Rule::edb_relation_decl => Self::new(name, attributes, input_path.as_deref(), None),
            Rule::idb_relation_decl => Self::new(name, attributes, None, output_path.as_deref()),
            _ => unreachable!(
                "Parser error: unexpected relation declaration rule: {:?}",
                rule_type
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::primitive::DataType::{Integer, String as TString};

    fn attrs() -> Vec<Attribute> {
        vec![
            Attribute::new("id".into(), Integer),
            Attribute::new("name".into(), TString),
        ]
    }

    #[test]
    fn new_and_accessors() {
        let rel = Relation::new("users", attrs(), None, None);
        assert_eq!(rel.name(), "users");
        assert_eq!(rel.arity(), 2);
        assert!(!rel.is_nullary());
        assert!(rel.input_path().is_none());
        assert!(rel.output_path().is_none());
        assert_eq!(rel.attributes()[0].name(), "id");
    }

    #[test]
    fn display_variants() {
        let a = vec![
            Attribute::new("x".into(), Integer),
            Attribute::new("y".into(), TString),
        ];

        let basic = Relation::new("r", a.clone(), None, None);
        assert_eq!(basic.to_string(), "r(x: integer, y: string)");

        let with_in = Relation::new("r", a.clone(), Some("data.csv"), None);
        assert_eq!(
            with_in.to_string(),
            "r(x: integer, y: string) .input data.csv"
        );

        let with_out = Relation::new("r", a, None, Some("out.csv"));
        assert_eq!(
            with_out.to_string(),
            "r(x: integer, y: string) .output out.csv"
        );

        let nullary = Relation::new("flag", vec![], None, None);
        assert_eq!(nullary.to_string(), "flag()");
    }

    #[test]
    fn equality_semantics() {
        let r1 = Relation::new("t", attrs(), None, None);
        let r2 = Relation::new("t", attrs(), None, None);
        let r3 = Relation::new("u", attrs(), None, None);
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }

    #[test]
    fn with_paths() {
        let r = Relation::new("e", attrs(), Some("in.csv"), Some("out.csv"));
        assert_eq!(r.input_path(), Some(&"in.csv".to_string()));
        assert_eq!(r.output_path(), Some(&"out.csv".to_string()));
    }
}
