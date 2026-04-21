//! Relation declaration types for FlowLog Datalog programs.

use super::Attribute;
use crate::parser::error::{grammar_bug, ParseError};
use crate::parser::primitive::DataType;
use crate::parser::{span_of, Lexeme, Rule};
use pest::iterators::Pair;
use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use crate::common::compute_fp;
use crate::common::source::{FileId, Ignored, Span};

/// A relation schema with input/output annotations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Relation {
    /// Canonical (lowercased) relation name.
    name: String,

    /// Original surface-syntax name.
    raw_name: String,

    /// Relation fingerprint.
    fingerprint: u64,

    /// Attributes of the relation.
    attributes: Vec<Attribute>,

    /// Input parameters (e.g., filename="file.csv", IO="file")
    input_params: Option<HashMap<String, String>>,

    /// Whether to output detailed results
    output: bool,

    /// Output parameters (e.g., delimiter="|")
    output_params: Option<HashMap<String, String>>,

    /// Whether to print results size (e.g. row count)
    printsize: bool,

    /// Span of the `.decl` declaration.
    span: Ignored<Span>,
}

impl Relation {
    /// Create a new relation.
    #[must_use]
    #[inline]
    pub fn new(name: &str, attributes: Vec<Attribute>) -> Self {
        let raw_name = name.to_string();
        let name = name.to_lowercase();
        let fingerprint = compute_fp(&name);
        Self {
            name,
            raw_name,
            fingerprint,
            attributes,
            input_params: None,
            output: false,
            output_params: None,
            printsize: false,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location of this `.decl` declaration.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span.0
    }

    /// Canonical (lowercased) relation name.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface-syntax name.
    #[must_use]
    #[inline]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
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

    /// Data types of the relation, one per attribute.
    #[must_use]
    #[inline]
    pub fn data_type(&self) -> Vec<DataType> {
        self.attributes.iter().map(|a| *a.data_type()).collect()
    }

    /// Get the input filename.
    /// Defaults to `<relation_name>.csv` when no filename parameter is set.
    #[must_use]
    #[inline]
    pub fn input_file_name(&self) -> String {
        self.input_params
            .as_ref()
            .and_then(|params| params.get("filename").cloned())
            .unwrap_or_else(|| format!("{}.csv", self.name()))
    }

    /// Get the input delimiter for a file-backed relation.
    #[must_use]
    #[inline]
    pub fn input_delimiter(&self) -> &str {
        self.input_params
            .as_ref()
            .and_then(|m| m.get("delimiter").map(String::as_str))
            .unwrap_or(",")
    }

    /// Whether to skip the first (header) line when reading this file-backed relation.
    #[must_use]
    #[inline]
    pub fn input_has_header(&self) -> bool {
        self.input_params
            .as_ref()
            .and_then(|m| m.get("header").map(String::as_str))
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
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

    /// Check whether this relation has a `.input` directive.
    #[must_use]
    #[inline]
    pub fn has_input(&self) -> bool {
        self.input_params.is_some()
    }

    /// Check whether this relation is file-backed (`IO="file"`).
    /// Returns false for `IO="command"` (interactive-only).
    #[must_use]
    #[inline]
    pub fn is_file_backed(&self) -> bool {
        self.input_params
            .as_ref()
            .and_then(|m| m.get("IO"))
            .is_some_and(|io| io.eq_ignore_ascii_case("file"))
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

    /// Set output parameters for this relation.
    pub fn set_output_params(&mut self, params: HashMap<String, String>) {
        self.output_params = Some(params);
    }

    /// Get the output delimiter. Defaults to `","`.
    #[must_use]
    #[inline]
    pub fn output_delimiter(&self) -> &str {
        self.output_params
            .as_ref()
            .and_then(|m| m.get("delimiter").map(String::as_str))
            .unwrap_or(",")
    }

    /// Get the output row limit, if specified.
    #[must_use]
    pub fn output_limit(&self) -> Option<usize> {
        let limit = self
            .output_params
            .as_ref()
            .and_then(|m| m.get("limit"))
            .map(|v| {
                v.parse::<usize>().unwrap_or_else(|_| {
                    panic!(
                        "Parser error: invalid limit '{}' for relation '{}', expected a non-negative integer",
                        v, self.name
                    )
                })
            });
        if limit.is_some() {
            assert!(
                self.output_params
                    .as_ref()
                    .and_then(|m| m.get("order_by"))
                    .is_some(),
                "Parser error: limit requires order_by for relation '{}'",
                self.name
            );
        }
        limit
    }

    /// Get the output ordering specification, if specified.
    #[must_use]
    pub fn output_order_by(&self) -> Option<Vec<(usize, DataType, bool)>> {
        self.output_params
            .as_ref()
            .and_then(|m| m.get("order_by"))
            .map(|spec| {
                spec.split(',')
                    .map(|part| {
                        let tokens: Vec<&str> = part.split_whitespace().collect();
                        assert!(
                            !tokens.is_empty(),
                            "Parser error: empty order_by clause in relation '{}'",
                            self.name
                        );
                        let attr_name = tokens[0].to_lowercase();
                        assert!(
                            tokens.len() <= 2,
                            "Parser error: unexpected extra tokens in order_by clause '{}' for relation '{}'",
                            part.trim(), self.name
                        );
                        let ascending = match tokens.get(1) {
                            Some(d) if d.eq_ignore_ascii_case("desc") => false,
                            Some(d) if d.eq_ignore_ascii_case("asc") => true,
                            Some(d) => panic!(
                                "Parser error: invalid order_by direction '{}' in relation '{}', expected ASC or DESC",
                                d, self.name
                            ),
                            None => true,
                        };
                        let (idx, attr) = self
                            .attributes
                            .iter()
                            .enumerate()
                            .find(|(_, a)| a.name() == attr_name)
                            .unwrap_or_else(|| {
                                panic!(
                                    "Parser error: order_by attribute '{}' not found in relation '{}'",
                                    attr_name, self.name
                                )
                            });
                        (idx, *attr.data_type(), ascending)
                    })
                    .collect()
            })
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
    fn from_parsed_rule(parsed_rule: Pair<Rule>, file: FileId) -> Result<Self, ParseError> {
        let span = span_of(&parsed_rule, file);
        let mut inner = parsed_rule.into_inner();

        // name
        let name = inner
            .next()
            .ok_or_else(|| grammar_bug("relation missing name"))?
            .as_str();

        let mut attributes: Vec<Attribute> = Vec::new();

        for rule in inner {
            match rule.as_rule() {
                Rule::attributes_decl => {
                    // Per-attribute span lookup keyed on the canonicalized
                    // (lowercased) name, so case-collisions can cite the
                    // earlier occurrence just like exact duplicates.
                    let mut seen: HashMap<String, Span> = HashMap::new();
                    for attr in rule.into_inner() {
                        let attr_span = span_of(&attr, file);
                        let mut parts = attr.into_inner();
                        let aname = parts
                            .next()
                            .ok_or_else(|| grammar_bug("attribute missing name"))?
                            .as_str();
                        let dts = parts
                            .next()
                            .ok_or_else(|| grammar_bug("attribute missing datatype"))?
                            .as_str();
                        let dt = DataType::from_str(dts).map_err(|e| {
                            grammar_bug(format!(
                                "invalid datatype `{dts}` for attribute `{aname}`: {e}"
                            ))
                        })?;
                        let canonical = aname.to_lowercase();
                        if let Some(prior) = seen.get(&canonical) {
                            return Err(ParseError::DuplicateAttribute {
                                span: attr_span,
                                prior: *prior,
                                relation: name.to_string(),
                                name: aname.to_string(),
                            });
                        }
                        seen.insert(canonical, attr_span);
                        attributes.push(Attribute::new(aname.to_string(), dt));
                    }
                }
                other => {
                    return Err(grammar_bug(format!(
                        "unexpected rule in relation declaration: {other:?}"
                    )));
                }
            }
        }

        let raw_name = name.to_string();
        let lname = name.to_lowercase();
        let fingerprint = compute_fp(&lname);
        Ok(Self {
            name: lname,
            raw_name,
            fingerprint,
            attributes,
            input_params: None,
            output: false,
            output_params: None,
            printsize: false,
            span: Ignored(span),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::primitive::DataType::{Int32, String};

    fn attrs() -> Vec<Attribute> {
        vec![
            Attribute::new("id".into(), Int32),
            Attribute::new("name".into(), String),
        ]
    }

    #[test]
    fn output_limit_some() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "42".to_string());
        params.insert("order_by".to_string(), "id".to_string());
        rel.set_output_params(params);
        assert_eq!(rel.output_limit(), Some(42));
    }

    #[test]
    #[should_panic(expected = "limit requires order_by")]
    fn output_limit_without_order_by() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "42".to_string());
        rel.set_output_params(params);
        let _ = rel.output_limit();
    }

    #[test]
    #[should_panic(expected = "invalid limit")]
    fn output_limit_bad_value() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "abc".to_string());
        rel.set_output_params(params);
        let _ = rel.output_limit();
    }

    #[test]
    fn output_order_by_single_asc_default() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "id".to_string());
        rel.set_output_params(params);
        let spec = rel.output_order_by().unwrap();
        assert_eq!(spec, vec![(0, Int32, true)]);
    }

    #[test]
    fn output_order_by_multi_mixed() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "name DESC, id ASC".to_string());
        rel.set_output_params(params);
        let spec = rel.output_order_by().unwrap();
        assert_eq!(spec, vec![(1, String, false), (0, Int32, true)]);
    }

    #[test]
    #[should_panic(expected = "not found in relation")]
    fn output_order_by_unknown_attr() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "nonexistent".to_string());
        rel.set_output_params(params);
        let _ = rel.output_order_by();
    }
}
