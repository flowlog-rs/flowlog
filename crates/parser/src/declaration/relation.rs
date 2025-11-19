//! Relation declaration types for Macaron Datalog programs.

use super::Attribute;
use crate::primitive::DataType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use common::compute_fp;

/// A relation schema with input/output annotations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    /// Relation name.
    name: String,

    /// Relation fingerprint.
    fingerprint: u64,

    /// Attributes of the relation.
    attributes: Vec<Attribute>,

    /// Input parameters (e.g., filename="file.csv", IO="file")
    input_params: Option<HashMap<String, String>>,

    /// Whether to output detailed results
    output: bool,

    /// Whether to print results size (e.g. row count)
    printsize: bool,
}

impl Relation {
    /// Create a new relation.
    #[must_use]
    #[inline]
    pub fn new(name: &str, attributes: Vec<Attribute>) -> Self {
        Self {
            name: name.to_ascii_lowercase(),
            fingerprint: compute_fp(&name),
            attributes,
            input_params: None,
            output: false,
            printsize: false,
        }
    }

    /// Relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Relation fingerprint.
    #[must_use]
    #[inline]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Attributes of the relation.
    #[must_use]
    #[inline]
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    /// Data types of the relation.
    #[must_use]
    #[inline]
    pub fn data_type(&self) -> &DataType {
        self.attributes[0].data_type()
    }

    /// Input parameters if this is an EDB relation.
    #[must_use]
    #[inline]
    pub fn input_params(&self) -> Option<&HashMap<String, String>> {
        self.input_params.as_ref()
    }

    /// Whether to print size for this relation.
    #[must_use]
    #[inline]
    pub fn printsize(&self) -> bool {
        self.printsize
    }

    /// Whether to output results for this relation.
    #[must_use]
    #[inline]
    pub fn output(&self) -> bool {
        self.output
    }

    /// Check if this is an EDB relation (has input parameters).
    #[must_use]
    #[inline]
    pub fn is_edb(&self) -> bool {
        self.input_params.is_some()
    }

    /// Check if this is an output/printsize relation.
    /// Notice not every IDB is an output/printsize relation.
    #[must_use]
    #[inline]
    pub fn is_output_printsize(&self) -> bool {
        self.output || self.printsize
    }

    /// Set input parameters for this relation.
    pub fn set_input_params(&mut self, params: HashMap<String, String>) {
        self.input_params = Some(params);
    }

    /// Mark relation for output.
    pub fn set_output(&mut self, output: bool) {
        self.output = output;
    }

    /// Set printsize flag.
    pub fn set_printsize(&mut self, printsize: bool) {
        self.printsize = printsize;
    }

    /// Number of attributes.
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.attributes.len()
    }

    /// `true` if no attributes.
    #[must_use]
    #[inline]
    pub fn is_nullary(&self) -> bool {
        self.attributes.is_empty()
    }
}

impl fmt::Display for Relation {
    /// Formats as `.decl name(a: ty, b: ty)` with optional input/output annotations on the same line.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".decl {}(", self.name)?;
        for (i, attr) in self.attributes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{attr}")?;
        }
        write!(f, ")")?;

        // Add input directive on the same line if present
        if let Some(params) = &self.input_params {
            write!(f, " .input(")?;
            let mut param_strs: Vec<String> = params
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect();
            param_strs.sort(); // Ensure consistent order
            write!(f, "{})", param_strs.join(", "))?;
        }

        // Add output directive on the same line if present
        if self.output {
            write!(f, " .output")?;
        }

        // Add printsize directive on the same line if present
        if self.printsize {
            write!(f, " .printsize")?;
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
        let mut inner = parsed_rule.into_inner();

        // name
        let name = inner
            .next()
            .expect("Parser error: relation missing name")
            .as_str();

        let mut attributes: Vec<Attribute> = Vec::new();

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
                _ => {
                    unreachable!(
                        "Parser error: unexpected rule in relation declaration: {:?}",
                        rule.as_rule()
                    )
                }
            }
        }

        // Create relation with parsed attributes; output directive handled separately.
        Self::new(name, attributes)
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
        let rel = Relation::new("Users", attrs());
        assert_eq!(rel.name(), "users");
        assert_eq!(rel.arity(), 2);
        assert!(!rel.is_nullary());
        assert_eq!(rel.attributes()[0].name(), "id");
        assert!(!rel.is_edb());
        assert!(!rel.output());
        assert!(!rel.printsize());
    }

    #[test]
    fn display_variants() {
        let a = vec![
            Attribute::new("x".into(), Integer),
            Attribute::new("y".into(), TString),
        ];

        let basic = Relation::new("r", a.clone());
        assert_eq!(basic.to_string(), ".decl r(x: number, y: string)");

        let nullary = Relation::new("flag", vec![]);
        assert_eq!(nullary.to_string(), ".decl flag()");

        // Test with input params
        let mut with_input = Relation::new("input_rel", a.clone());
        let mut params = HashMap::new();
        params.insert("filename".to_string(), "data.csv".to_string());
        params.insert("IO".to_string(), "file".to_string());
        with_input.set_input_params(params);
        assert_eq!(
            with_input.to_string(),
            ".decl input_rel(x: number, y: string) .input(IO=\"file\", filename=\"data.csv\")"
        );

        // Test with output
        let mut with_output = Relation::new("output_rel", a.clone());
        with_output.set_output(true);
        assert_eq!(
            with_output.to_string(),
            ".decl output_rel(x: number, y: string) .output"
        );

        // Test with printsize
        let mut with_printsize = Relation::new("print_rel", a.clone());
        with_printsize.set_printsize(true);
        assert_eq!(
            with_printsize.to_string(),
            ".decl print_rel(x: number, y: string) .printsize"
        );

        // Test with all annotations
        let mut full_rel = Relation::new("full_rel", a);
        let mut params = HashMap::new();
        params.insert("filename".to_string(), "input.csv".to_string());
        full_rel.set_input_params(params);
        full_rel.set_output(true);
        full_rel.set_printsize(true);
        assert_eq!(
            full_rel.to_string(),
            ".decl full_rel(x: number, y: string) .input(filename=\"input.csv\") .output .printsize"
        );
    }

    #[test]
    fn equality_semantics() {
        let r1 = Relation::new("t", attrs());
        let r2 = Relation::new("t", attrs());
        let r3 = Relation::new("u", attrs());
        assert_eq!(r1, r2);
        assert_ne!(r1, r3);
    }
}
