//! Relation declaration types for FlowLog Datalog programs.

use std::collections::HashMap;
use std::fmt;

use educe::Educe;
use flowlog_common::compute_fp;
use flowlog_error::Span;

use super::Attribute;
use super::InputSource;
use super::OutputSink;
use crate::Node;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::types::DataType;
use crate::types::TypeId;
use crate::types::TypeRegistry;

/// A relation schema with input/output annotations.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub struct Relation {
    /// Canonical (lowercased) relation name.
    name: String,

    /// Original surface-syntax name (case-preserved). For relations
    /// inlined out of a `.comp`, this keeps the dotted form (`c.R`)
    /// even after `set_name` rewrites `name` to the Rust-safe
    /// `\u{b7}` form, used by I/O sinks for Souffle-style filenames.
    raw_name: String,

    /// Relation fingerprint.
    fingerprint: u64,

    /// Attributes of the relation.
    attributes: Vec<Attribute>,

    /// The relation's `.input` directive, resolved, or `None` when it has
    /// none: absence is what keeps a rule-derived relation from being read
    /// off disk.
    input: Option<InputSource>,

    /// The relation's `.output` directive, resolved, or `None` when FlowLog
    /// does not write it.
    output: Option<OutputSink>,

    /// Whether to print results size (e.g. row count)
    printsize: bool,

    /// Span of the `.decl` declaration.
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl Relation {
    /// Like `Lexeme::from_parsed_rule` but threads the type registry
    /// through so each attribute's surface type name can be resolved.
    pub(crate) fn from_parsed_rule_with_registry(
        node: Node,
        registry: &TypeRegistry,
    ) -> Result<Self, ParseError> {
        debug_assert_eq!(node.rule(), Rule::declaration);
        let span = node.span();
        let mut children = node.children();

        let name_node = children.next_any("name")?;
        let name = name_node.text();

        let mut attributes: Vec<Attribute> = Vec::new();

        for child in children {
            match child.rule() {
                Rule::attributes_decl => {
                    let mut seen: HashMap<String, Span> = HashMap::new();
                    for attr in child.children() {
                        let attr_span = attr.span();
                        let mut parts = attr.children();
                        let aname_node = parts.next_any("name")?;
                        let aname = aname_node.text();
                        let type_ref = parts.next_any("type_ref")?;
                        let type_ref_span = type_ref.span();
                        let type_name = type_ref.text().trim().to_string();
                        let type_id = registry.lookup(&type_name).ok_or_else(|| {
                            ParseError::UnknownAttributeType {
                                span: type_ref_span,
                                name: type_name.clone(),
                            }
                        })?;
                        let primitive = registry.root_primitive(type_id);

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
                        attributes.push(Attribute::with_type(
                            aname.to_string(),
                            primitive,
                            type_id,
                        ));
                    }
                }
                Rule::overridable_kw => {
                    return Err(ParseError::OverridableOutsideComp {
                        span: child.span(),
                        name: name.to_string(),
                    });
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
            input: None,
            output: None,
            printsize: false,
            span,
        })
    }

    /// Build a fresh relation. Tests only; production code goes through the parser.
    #[cfg(test)]
    #[must_use]
    #[inline]
    pub fn new(name: &str, attributes: Vec<Attribute>) -> Self {
        Self::from_components(name, attributes, Span::DUMMY)
    }

    /// Build a relation from a pre-resolved name and attribute list.
    /// Callers must supply attributes whose `TypeId` is already bound
    /// to the program's `TypeRegistry`.
    #[must_use]
    pub(crate) fn from_components(name: &str, attributes: Vec<Attribute>, span: Span) -> Self {
        let raw_name = name.to_string();
        let name = name.to_lowercase();
        let fingerprint = compute_fp(&name);
        Self {
            name,
            raw_name,
            fingerprint,
            attributes,
            input: None,
            output: None,
            printsize: false,
            span,
        }
    }

    /// Source location of this `.decl` declaration.
    #[must_use]
    #[inline]
    pub fn span(&self) -> Span {
        self.span
    }

    /// Canonical (lowercased) relation name: the relation's **internal**
    /// identity.
    ///
    /// Every internal use routes through this form: fingerprints, map
    /// keys, generated idents, and the lib-mode API surface
    /// (`insert_<name>`, result fields). Unique per program; for inlined
    /// component relations the dots are rewritten to `\u{b7}` (see
    /// `set_name`). Never show this to the user when
    /// [`Self::raw_name`] is available.
    #[must_use]
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Original surface-syntax name (case and dots preserved): the
    /// relation's **user-facing** spelling.
    ///
    /// Anything a human reads uses this form: profiler labels,
    /// diagnostics, and Souffle-compatible I/O filenames (`Edge.facts` /
    /// `c.holds.csv`) rather than the lowercased canonical form returned
    /// by [`Self::name`]. Canonicalization is deterministic
    /// (lowercase, `.` to `\u{b7}`), so distinct relations always have
    /// distinct raw names too.
    #[must_use]
    #[inline]
    pub fn raw_name(&self) -> &str {
        &self.raw_name
    }

    /// Rename in-place. Refreshes the cached fingerprint and the
    /// canonical lower-case `name`, but **leaves `raw_name` alone** so
    /// the original surface form is preserved for I/O sinks and
    /// diagnostics.
    ///
    /// Used by the post-inliner pass that rewrites dotted instance
    /// names (`c.holds`) to the Rust-ident-safe middle-dot form
    /// (`c\u{b7}holds`); the dataflow refers to relations by `name`, but
    /// file paths still need the original `c.holds` (Souffle writes
    /// `c.holds.csv`).
    pub(crate) fn set_name(&mut self, name: String) {
        self.name = name.to_lowercase();
        self.fingerprint = compute_fp(&self.name);
    }

    /// Relation fingerprint.
    #[must_use]
    #[inline]
    pub fn fingerprint(&self) -> u64 {
        self.fingerprint
    }

    /// Data types of the relation, one per attribute.
    #[must_use]
    #[inline]
    pub fn data_type(&self) -> Vec<DataType> {
        self.attributes
            .iter()
            .map(|a| a.data_type().clone())
            .collect()
    }

    /// The relation's declared attributes (column name + type), in order.
    /// Use this to inspect column *names*; [`Self::data_type`] gives just the
    /// types.
    #[must_use]
    #[inline]
    pub fn attributes(&self) -> &[Attribute] {
        &self.attributes
    }

    /// Per-attribute declared `TypeId`s. Used by the typechecker;
    /// downstream stages use [`Self::data_type`].
    #[must_use]
    pub(crate) fn attribute_declared_ids(&self) -> Vec<TypeId> {
        self.attributes.iter().map(|a| a.declared_id()).collect()
    }

    /// This relation's `.input` directive, or `None` when it has none.
    #[must_use]
    #[inline]
    pub fn input(&self) -> Option<&InputSource> {
        self.input.as_ref()
    }

    /// Returns `true` if this relation's row count is reported
    /// (`.printsize`).
    #[must_use]
    #[inline]
    pub fn printsize(&self) -> bool {
        self.printsize
    }

    /// Returns `true` if this relation has an `.output` directive, whatever
    /// storage it names.
    #[must_use]
    #[inline]
    pub fn output(&self) -> bool {
        self.output.is_some()
    }

    /// Returns `true` if this relation has a `.input` directive, whatever
    /// storage it names.
    #[must_use]
    #[inline]
    pub fn has_input(&self) -> bool {
        self.input.is_some()
    }

    /// Returns `true` if anything of this relation is reported at all,
    /// rows or count; an IDB no directive names derives silently.
    #[must_use]
    #[inline]
    pub fn is_output_printsize(&self) -> bool {
        self.output.is_some() || self.printsize
    }

    /// Adopt a `.input` directive's parameters, resolving them against this
    /// relation's name for the filename default.
    ///
    /// `span` labels the directive, which is where a parameter this relation
    /// cannot be read with is reported.
    pub(crate) fn set_input(
        &mut self,
        params: &HashMap<String, String>,
        span: Span,
    ) -> Result<(), ParseError> {
        let source = InputSource::from_params(params, &self.raw_name, span)?;
        self.input = Some(source);
        Ok(())
    }

    /// Adopt a `.output` directive's parameters, resolving them against this
    /// relation's name and attributes.
    ///
    /// `span` labels the directive, which is where a parameter this relation
    /// cannot be written with is reported.
    pub(crate) fn set_output(
        &mut self,
        params: &HashMap<String, String>,
        span: Span,
    ) -> Result<(), ParseError> {
        let sink = OutputSink::from_params(params, &self.raw_name, &self.attributes, span)?;
        self.output = Some(sink);
        Ok(())
    }

    /// This relation's `.output` directive, or `None` when FlowLog does not
    /// write it.
    #[must_use]
    #[inline]
    pub fn output_sink(&self) -> Option<&OutputSink> {
        self.output.as_ref()
    }

    /// Whether this `.output` relation takes the parallel file-drain path
    /// (binary file sink, arity > 0, no `ORDER BY`). Nullary, `ORDER BY`/`LIMIT`,
    /// and stderr stay on the sequential drain. Defined here so the drain router
    /// and the `itoa` feature marking (in two crates) share one predicate.
    #[must_use]
    pub fn uses_parallel_file_drain(&self, output_to_stdout: bool) -> bool {
        !output_to_stdout
            && self.arity() > 0
            && self.output.as_ref().is_some_and(|s| s.order_by().is_none())
    }

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
    /// One line, `.decl name(a: ty, b: ty)`, followed by each directive the
    /// relation carries with its resolved parameters spelled out.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, ".decl {}(", self.name)?;
        for (i, attr) in self.attributes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{attr}")?;
        }
        write!(f, ")")?;

        if let Some(input) = &self.input {
            write!(f, " .input({input})")?;
        }
        if let Some(output) = &self.output {
            write!(f, " .output({output})")?;
        }
        if self.printsize {
            write!(f, " .printsize")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use flowlog_error::FileId;

    use super::*;
    use crate::assert_err;
    use crate::test_util::parse_pair;
    use crate::types::DataType::Int32;
    use crate::types::DataType::String;
    use crate::types::TypeRegistry;

    fn attrs() -> Vec<Attribute> {
        let reg = TypeRegistry::new();
        vec![
            Attribute::with_type("id".into(), Int32, reg.primitive_id(Int32).unwrap()),
            Attribute::with_type("name".into(), String, reg.primitive_id(String).unwrap()),
        ]
    }

    /// Adopt a `.output` directive with the given parameters. Which values
    /// each parameter accepts is [`OutputSink`]'s own contract.
    fn with_output<const N: usize>(name: &str, pairs: [(&str, &str); N]) -> Relation {
        let mut rel = Relation::new(name, attrs());
        let params = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        rel.set_output(&params, Span::DUMMY)
            .expect("parameters resolve");
        rel
    }

    /// A `.output` with no parameters is still a directive, so the relation
    /// counts as written.
    #[test]
    fn output_is_true_for_a_directive_without_parameters() {
        assert!(with_output("r", []).output());
    }

    /// A relation with no `.output` is not written, which is what keeps a
    /// bare `.decl` out of the sink list.
    #[test]
    fn output_is_false_without_an_output_directive() {
        assert!(!Relation::new("r", attrs()).output());
    }

    /// Adopt a `.input` directive with the given parameters. Which values
    /// each parameter accepts is [`InputSource`]'s own contract.
    fn with_input<const N: usize>(name: &str, pairs: [(&str, &str); N]) -> Relation {
        let mut rel = Relation::new(name, attrs());
        let params = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        rel.set_input(&params, Span::DUMMY)
            .expect("parameters resolve");
        rel
    }

    /// The relation hands over the source its directive resolved to, rather
    /// than answering for it: what each parameter means is
    /// [`InputSource`]'s contract, tested there.
    #[test]
    fn input_hands_over_the_resolved_source() {
        assert_eq!(
            with_input("Edge", [("delimiter", ",")]).input(),
            Some(&InputSource::File {
                filename: "Edge.facts".to_string(),
                delim: b',',
                has_header: false,
            })
        );
    }

    /// The same for the sink side.
    #[test]
    fn output_sink_hands_over_the_resolved_sink() {
        assert_eq!(
            with_output("Edge", [("delimiter", "|")]).output_sink(),
            Some(&OutputSink::File {
                filename: "Edge.csv".to_string(),
                delim: b'|',
                order_by: None,
                limit: None,
            })
        );
    }

    /// A relation with NO `.input` directive at all holds no source; only
    /// the presence of the directive makes one. Pins the `input == None`
    /// arm against a regression that would mistakenly try to open
    /// `<RawName>.facts` for purely-IDB relations.
    #[test]
    fn a_relation_without_directives_holds_neither() {
        let rel = Relation::new("r", attrs());
        assert_eq!(rel.input(), None);
        assert_eq!(rel.output_sink(), None);
        assert!(!rel.has_input());
        assert!(!rel.output());
    }

    /// A directive with no parameters is still a directive, which is what
    /// separates `.input Edge` from a relation only rules produce.
    #[test]
    fn a_directive_without_parameters_still_counts_as_present() {
        assert!(with_input("r", []).has_input());
        assert!(with_output("r", []).output());
    }

    /// The parallel drain needs a file to write and no ordering to apply, so
    /// an `ORDER BY`, a nullary relation, and stderr each rule it out.
    #[test]
    fn the_parallel_file_drain_needs_a_file_and_no_ordering() {
        assert!(with_output("r", []).uses_parallel_file_drain(false));
        assert!(!with_output("r", []).uses_parallel_file_drain(true));
        assert!(!with_output("r", [("order_by", "id")]).uses_parallel_file_drain(false));

        let mut nullary = Relation::new("r", vec![]);
        nullary
            .set_output(&HashMap::new(), Span::DUMMY)
            .expect("no parameters");
        assert!(!nullary.uses_parallel_file_drain(false));
    }

    /// `set_name` updates `name` (canonical, Rust-ident-safe) but
    /// MUST NOT touch `raw_name`: the inliner relies on the original
    /// surface form (incl. literal dots like `c.R`) surviving for
    /// I/O sinks. A regression that re-introduces `self.raw_name = name`
    /// would silently rename Souffle output files from `c.R.csv` to
    /// `c\u{b7}R.csv`.
    #[test]
    fn set_name_preserves_raw_name() {
        let mut rel = Relation::new("c.R", attrs());
        assert_eq!(rel.raw_name(), "c.R");
        rel.set_name("c\u{00B7}R".to_string());
        assert_eq!(rel.name(), "c\u{00B7}r");
        assert_eq!(rel.raw_name(), "c.R");
    }

    /// Parse `.decl` source through a fresh registry. Returns the `Result`
    /// so error-path tests can pin the variant.
    fn parse_decl(src: &str) -> Result<Relation, ParseError> {
        Relation::from_parsed_rule_with_registry(
            Node::new(parse_pair(Rule::declaration, src), FileId::new(0)),
            &TypeRegistry::new(),
        )
    }

    /// A well-formed `.decl` lowercases the relation name, keeps the surface
    /// spelling in `raw_name`, and resolves each attribute's declared type
    /// through the registry (`number` to `Int32`, `symbol` to `String`).
    #[test]
    fn decl_lowercases_name_and_resolves_attribute_types() {
        let rel = parse_decl(".decl Edge(src: number, dst: symbol)").expect("decl parses");
        assert_eq!(rel.name(), "edge");
        assert_eq!(rel.raw_name(), "Edge");
        assert_eq!(rel.data_type(), vec![Int32, String]);
    }

    #[test]
    fn decl_with_unknown_attribute_type_is_rejected() {
        assert_err!(
            parse_decl(".decl R(x: NoSuchType)"),
            ParseError::UnknownAttributeType { .. }
        );
    }

    #[test]
    fn decl_with_duplicate_attribute_name_is_rejected() {
        // `X` collides with `x`; attribute names are case-insensitive.
        assert_err!(
            parse_decl(".decl R(x: number, X: number)"),
            ParseError::DuplicateAttribute { .. }
        );
    }

    /// `overridable` is a comp-only keyword; on a top-level `.decl` it reaches
    /// this producer as a stray `overridable_kw` and is rejected. (Inside a
    /// `.comp` body decls take the `RawRelation` path, which keeps the flag.)
    #[test]
    fn decl_with_overridable_outside_comp_is_rejected() {
        assert_err!(
            parse_decl(".decl Foo(x: number) overridable"),
            ParseError::OverridableOutsideComp { .. }
        );
    }
}
