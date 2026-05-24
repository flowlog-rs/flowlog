//! Relation declaration types for FlowLog Datalog programs.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use pest::iterators::Pair;

use super::Attribute;
use crate::common::{FileId, Ignored, Span, compute_fp};
use crate::parser::error::{ParseError, grammar_bug};
use crate::parser::primitive::DataType;
use crate::parser::{Lexeme, Rule, span_of};

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

    /// Validated `output(limit=…)` value, populated by [`set_output_params`].
    output_limit_value: Option<usize>,

    /// Validated `output(order_by=…)` spec (attribute index, type, ascending),
    /// populated by [`set_output_params`].
    output_order_by_spec: Option<Vec<(usize, DataType, bool)>>,

    /// Whether to print results size (e.g. row count)
    printsize: bool,

    /// Span of the `.decl` declaration.
    span: Ignored<Span>,
}

/// Decode the standard Datalog delimiter escape sequences (`\t`, `\n`, `\r`,
/// `\\`, `\0`). Unknown `\x` sequences pass through as the two literal bytes,
/// matching Soufflé's behavior. A trailing lone backslash is preserved.
fn unescape_delimiter(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b'\\' && i + 1 < bytes.len() {
            let next = bytes[i + 1];
            match next {
                b't' => out.push(b'\t'),
                b'n' => out.push(b'\n'),
                b'r' => out.push(b'\r'),
                b'\\' => out.push(b'\\'),
                b'0' => out.push(0),
                _ => {
                    out.push(b'\\');
                    out.push(next);
                }
            }
            i += 2;
        } else {
            out.push(b);
            i += 1;
        }
    }
    // All decoded bytes are either ASCII or copied verbatim from a valid UTF-8
    // input (the only multi-byte path skips the backslash branch), so the
    // result is always valid UTF-8.
    String::from_utf8(out).expect("unescape preserves UTF-8 validity")
}

impl Relation {
    /// Build a fresh relation. Tests only; production code goes through the parser.
    #[cfg(test)]
    #[must_use]
    #[inline]
    pub(crate) fn new(name: &str, attributes: Vec<Attribute>) -> Self {
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
            output_limit_value: None,
            output_order_by_spec: None,
            printsize: false,
            span: Ignored(Span::DUMMY),
        }
    }

    /// Source location of this `.decl` declaration.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
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
    pub(crate) fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Relation fingerprint.
    #[must_use]
    #[inline]
    pub(crate) fn fingerprint(&self) -> u64 {
        self.fingerprint
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
        self.input_param("filename")
            .map_or_else(|| format!("{}.csv", self.name()), str::to_owned)
    }

    /// Get the input delimiter for a file-backed relation.
    ///
    /// Interprets `\t`, `\n`, `\r`, `\\`, and `\0` as the corresponding byte;
    /// other `\x` sequences pass through unchanged (matching Soufflé).
    #[must_use]
    #[inline]
    pub fn input_delimiter(&self) -> String {
        unescape_delimiter(self.input_param("delimiter").unwrap_or(","))
    }

    /// Whether to skip the first (header) line when reading this file-backed relation.
    #[must_use]
    #[inline]
    pub fn input_has_header(&self) -> bool {
        self.input_param("header")
            .is_some_and(|v| v.eq_ignore_ascii_case("true"))
    }

    /// Whether to print size for this relation.
    #[must_use]
    #[inline]
    pub(crate) fn printsize(&self) -> bool {
        self.printsize
    }

    /// Whether to output results for this relation.
    #[must_use]
    #[inline]
    pub(crate) fn output(&self) -> bool {
        self.output
    }

    /// Check whether this relation has a `.input` directive.
    #[must_use]
    #[inline]
    pub(crate) fn has_input(&self) -> bool {
        self.input_params.is_some()
    }

    /// Check whether this relation is file-backed (`IO="file"`).
    /// Returns false for `IO="command"` (interactive-only).
    #[must_use]
    #[inline]
    pub fn is_file_backed(&self) -> bool {
        self.input_param("IO")
            .is_some_and(|io| io.eq_ignore_ascii_case("file"))
    }

    /// Check if this is an output/printsize relation.
    /// Notice not every IDB is an output/printsize relation.
    #[must_use]
    #[inline]
    pub(crate) fn is_output_printsize(&self) -> bool {
        self.output || self.printsize
    }

    /// Look up an entry in the `.input` parameter map.
    fn input_param(&self, key: &str) -> Option<&str> {
        self.input_params
            .as_ref()
            .and_then(|m| m.get(key))
            .map(String::as_str)
    }

    /// Look up an entry in the `.output` parameter map.
    fn output_param(&self, key: &str) -> Option<&str> {
        self.output_params
            .as_ref()
            .and_then(|m| m.get(key))
            .map(String::as_str)
    }

    /// Set input parameters for this relation.
    pub(crate) fn set_input_params(&mut self, params: HashMap<String, String>) {
        self.input_params = Some(params);
    }

    /// Mark relation for output.
    pub(crate) fn set_output(&mut self, output: bool) {
        self.output = output;
    }

    /// Set output parameters for this relation.
    ///
    /// Validates the `limit` and `order_by` entries up-front, returning a
    /// [`ParseError::Internal`] if either is malformed (bad `limit` value,
    /// `limit` without `order_by`, unknown attribute, etc.). On success, the
    /// validated values are cached on the relation so the codegen-facing
    /// accessors are infallible.
    pub(crate) fn set_output_params(
        &mut self,
        params: HashMap<String, String>,
    ) -> Result<(), ParseError> {
        // Parse `order_by` first so `limit` validation can refer to it.
        let order_by_spec = if let Some(spec) = params.get("order_by") {
            let mut parsed: Vec<(usize, DataType, bool)> = Vec::new();
            for part in spec.split(',') {
                let tokens: Vec<&str> = part.split_whitespace().collect();
                if tokens.is_empty() {
                    return Err(grammar_bug(format!(
                        "empty order_by clause for relation `{}`",
                        self.name
                    )));
                }
                if tokens.len() > 2 {
                    return Err(grammar_bug(format!(
                        "unexpected extra tokens in order_by clause `{}` for relation `{}`",
                        part.trim(),
                        self.name
                    )));
                }
                let attr_name = tokens[0].to_lowercase();
                let ascending = match tokens.get(1) {
                    Some(d) if d.eq_ignore_ascii_case("asc") => true,
                    Some(d) if d.eq_ignore_ascii_case("desc") => false,
                    Some(d) => {
                        return Err(grammar_bug(format!(
                            "invalid order_by direction `{d}` for relation `{}`, expected ASC or DESC",
                            self.name
                        )));
                    }
                    None => true,
                };
                let (idx, attr) = self
                    .attributes
                    .iter()
                    .enumerate()
                    .find(|(_, a)| a.name() == attr_name)
                    .ok_or_else(|| {
                        grammar_bug(format!(
                            "order_by attribute `{attr_name}` not found in relation `{}`",
                            self.name
                        ))
                    })?;
                parsed.push((idx, *attr.data_type(), ascending));
            }
            Some(parsed)
        } else {
            None
        };

        // Parse `limit`; require `order_by` whenever `limit` is set.
        let limit_value = if let Some(raw) = params.get("limit") {
            let limit = raw.parse::<usize>().map_err(|_| {
                grammar_bug(format!(
                    "invalid limit `{raw}` for relation `{}`, expected a non-negative integer",
                    self.name
                ))
            })?;
            if order_by_spec.is_none() {
                return Err(grammar_bug(format!(
                    "limit requires order_by for relation `{}`",
                    self.name
                )));
            }
            Some(limit)
        } else {
            None
        };

        self.output_params = Some(params);
        self.output_limit_value = limit_value;
        self.output_order_by_spec = order_by_spec;
        Ok(())
    }

    /// Get the output delimiter. Defaults to `","`.
    ///
    /// Interprets `\t`, `\n`, `\r`, `\\`, and `\0` as the corresponding byte;
    /// other `\x` sequences pass through unchanged (matching Soufflé).
    #[must_use]
    #[inline]
    pub fn output_delimiter(&self) -> String {
        unescape_delimiter(self.output_param("delimiter").unwrap_or(","))
    }

    /// Get the output row limit, if specified. Validated at parse time by
    /// [`Relation::set_output_params`].
    #[must_use]
    #[inline]
    pub(crate) fn output_limit(&self) -> Option<usize> {
        self.output_limit_value
    }

    /// Get the output ordering specification, if specified. Validated at
    /// parse time by [`Relation::set_output_params`].
    #[must_use]
    #[inline]
    pub(crate) fn output_order_by(&self) -> Option<Vec<(usize, DataType, bool)>> {
        self.output_order_by_spec.clone()
    }

    /// Set printsize flag.
    pub(crate) fn set_printsize(&mut self, printsize: bool) {
        self.printsize = printsize;
    }

    /// Number of attributes.
    #[must_use]
    #[inline]
    pub fn arity(&self) -> usize {
        self.attributes.len()
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
            output_limit_value: None,
            output_order_by_spec: None,
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
        rel.set_output_params(params).unwrap();
        assert_eq!(rel.output_limit(), Some(42));
    }

    #[test]
    fn output_limit_without_order_by() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "42".to_string());
        let err = rel.set_output_params(params).unwrap_err();
        assert!(matches!(err, ParseError::Internal(_)));
        assert!(err.to_string().contains("limit requires order_by"));
    }

    #[test]
    fn output_limit_bad_value() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("limit".to_string(), "abc".to_string());
        let err = rel.set_output_params(params).unwrap_err();
        assert!(matches!(err, ParseError::Internal(_)));
        assert!(err.to_string().contains("invalid limit"));
    }

    #[test]
    fn output_order_by_single_asc_default() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "id".to_string());
        rel.set_output_params(params).unwrap();
        let spec = rel.output_order_by().unwrap();
        assert_eq!(spec, vec![(0, Int32, true)]);
    }

    #[test]
    fn output_order_by_multi_mixed() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "name DESC, id ASC".to_string());
        rel.set_output_params(params).unwrap();
        let spec = rel.output_order_by().unwrap();
        assert_eq!(spec, vec![(1, String, false), (0, Int32, true)]);
    }

    #[test]
    fn unescape_delimiter_basic_escapes() {
        assert_eq!(unescape_delimiter("\\t"), "\t");
        assert_eq!(unescape_delimiter("\\n"), "\n");
        assert_eq!(unescape_delimiter("\\r"), "\r");
        assert_eq!(unescape_delimiter("\\\\"), "\\");
        assert_eq!(unescape_delimiter("\\0"), "\0");
    }

    #[test]
    fn unescape_delimiter_literal_passthrough() {
        assert_eq!(unescape_delimiter(","), ",");
        assert_eq!(unescape_delimiter("|"), "|");
        assert_eq!(unescape_delimiter(""), "");
    }

    #[test]
    fn unescape_delimiter_unknown_passthrough() {
        // Soufflé leaves unknown \x sequences as literal — `\x` becomes `\x`.
        assert_eq!(unescape_delimiter("\\x"), "\\x");
        // Trailing lone backslash is preserved verbatim.
        assert_eq!(unescape_delimiter("\\"), "\\");
    }

    #[test]
    fn input_delimiter_decodes_tab() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("delimiter".to_string(), "\\t".to_string());
        rel.set_input_params(params);
        assert_eq!(rel.input_delimiter(), "\t");
        assert_eq!(rel.input_delimiter().as_bytes(), b"\t");
    }

    #[test]
    fn output_delimiter_decodes_tab() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("delimiter".to_string(), "\\t".to_string());
        rel.set_output_params(params).unwrap();
        assert_eq!(rel.output_delimiter(), "\t");
    }

    #[test]
    fn delimiter_defaults_to_comma() {
        let rel = Relation::new("r", attrs());
        assert_eq!(rel.input_delimiter(), ",");
        assert_eq!(rel.output_delimiter(), ",");
    }

    #[test]
    fn output_order_by_unknown_attr() {
        let mut rel = Relation::new("r", attrs());
        let mut params = HashMap::new();
        params.insert("order_by".to_string(), "nonexistent".to_string());
        let err = rel.set_output_params(params).unwrap_err();
        assert!(matches!(err, ParseError::Internal(_)));
        assert!(err.to_string().contains("not found"));
    }
}
