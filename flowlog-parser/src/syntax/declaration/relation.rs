//! Relation declaration types for FlowLog Datalog programs.

use std::collections::HashMap;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;
use flowlog_common::compute_fp;

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

    /// The resolved `.input` specification.
    input: Option<InputSource>,

    /// The resolved `.output` specification.
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

    /// This relation's `.output` directive, or `None` when FlowLog does not
    /// write it.
    #[must_use]
    #[inline]
    pub fn output_sink(&self) -> Option<&OutputSink> {
        self.output.as_ref()
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
    pub fn has_output(&self) -> bool {
        self.output.is_some()
    }

    /// Returns `true` if this relation has an `.input` directive, whatever
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
        self.input = Some(InputSource::from_params(params, &self.raw_name, span)?);
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

        // Every parameter is rendered resolved, so what a relation prints is
        // what it will read and write, not what the user happened to write.
        if let Some(source) = &self.input {
            write!(f, " .input({source})")?;
        }

        if let Some(sink) = &self.output {
            write!(f, " .output({sink})")?;
        }

        // Add printsize directive on the same line if present
        if self.printsize {
            write!(f, " .printsize")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::test_util::parse_pair;
    use crate::types::DataType::Int32;
    use crate::types::DataType::String as Str;
    use crate::types::TypeRegistry;

    fn attrs() -> Vec<Attribute> {
        let reg = TypeRegistry::new();
        vec![
            Attribute::with_type("id".into(), Int32, reg.primitive_id(Int32).unwrap()),
            Attribute::with_type("name".into(), Str, reg.primitive_id(Str).unwrap()),
        ]
    }

    /// Directive parameters as the parser hands them over.
    fn params<const N: usize>(pairs: [(&str, &str); N]) -> HashMap<String, String> {
        pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // What each resolved parameter *means* is pinned on `InputSource` and
    // `OutputSink` themselves; these cover only what a relation adds:
    // adopting a directive, and supplying the name and attributes it
    // resolves against.

    #[test]
    fn a_relation_with_no_directive_is_neither_read_nor_written() {
        let rel = Relation::new("r", attrs());
        assert!(rel.input().is_none());
        assert!(rel.output_sink().is_none());
        assert!(!rel.has_input());
        assert!(!rel.has_output());
    }

    /// The `.input` filename default is the relation's case-preserved
    /// surface name, matching Souffle, not its canonical lowercase one.
    #[test]
    fn an_adopted_input_defaults_its_filename_to_the_raw_name() {
        let mut rel = Relation::new("Edge", attrs());
        rel.set_input(&HashMap::new(), Span::DUMMY).unwrap();
        assert_eq!(rel.input().unwrap().filename(), Some("Edge.facts"));
        assert!(rel.has_input());
    }

    #[test]
    fn an_adopted_output_defaults_its_filename_to_the_raw_name() {
        let mut rel = Relation::new("Path", attrs());
        rel.set_output(&HashMap::new(), Span::DUMMY).unwrap();
        assert_eq!(rel.output_sink().unwrap().filename(), "Path.csv");
        assert!(rel.has_output());
    }

    /// An `order_by` names a column, so only the relation can resolve it:
    /// the directive it comes from has no attributes.
    #[test]
    fn an_adopted_output_resolves_order_by_against_the_relations_attributes() {
        let mut rel = Relation::new("r", attrs());
        rel.set_output(&params([("order_by", "name DESC")]), Span::DUMMY)
            .unwrap();
        assert_eq!(
            rel.output_sink().unwrap().order_by(),
            Some([(1, Str, false)].as_slice())
        );
    }

    /// A parameter the relation cannot be read or written with leaves it
    /// with no directive at all, rather than a half-resolved one.
    #[test]
    fn a_refused_input_leaves_the_relation_unread() {
        let mut rel = Relation::new("r", attrs());
        assert_err!(
            rel.set_input(&params([("delimiter", "::")]), Span::DUMMY),
            ParseError::InvalidDelimiter { .. }
        );
        assert!(!rel.has_input());
    }

    #[test]
    fn a_refused_output_leaves_the_relation_unwritten() {
        let mut rel = Relation::new("r", attrs());
        assert_err!(
            rel.set_output(&params([("order_by", "nonexistent")]), Span::DUMMY),
            ParseError::InvalidOrderBy { .. }
        );
        assert!(!rel.has_output());
    }

    /// Rows are formatted across cores only when nothing constrains their
    /// order: an `ORDER BY`, a nullary relation, and stderr each keep the
    /// sequential drain.
    #[rstest]
    //     order_by       arity  to_stdout  parallel
    #[case(None, 2, false, true)]
    #[case(Some("id"), 2, false, false)]
    #[case(None, 0, false, false)]
    #[case(None, 2, true, false)]
    fn only_an_unordered_multi_column_file_sink_drains_in_parallel(
        #[case] order_by: Option<&str>,
        #[case] arity: usize,
        #[case] output_to_stdout: bool,
        #[case] parallel: bool,
    ) {
        let mut rel = Relation::new("r", attrs().into_iter().take(arity).collect());
        let p = order_by.map_or_else(HashMap::new, |spec| params([("order_by", spec)]));
        rel.set_output(&p, Span::DUMMY).unwrap();
        assert_eq!(rel.uses_parallel_file_drain(output_to_stdout), parallel);
    }

    /// A relation with no `.output` drains nothing, so it never takes the
    /// parallel path whatever its shape.
    #[test]
    fn a_relation_with_no_output_never_drains_in_parallel() {
        assert!(!Relation::new("r", attrs()).uses_parallel_file_drain(false));
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

    /// A rendered relation spells out both directives resolved, so it shows
    /// what will actually be read and written.
    #[test]
    fn display_renders_both_directives_resolved() {
        let mut rel = Relation::new("Edge", attrs());
        rel.set_input(&params([("delimiter", ",")]), Span::DUMMY)
            .unwrap();
        rel.set_output(&params([("filename", "out.tsv")]), Span::DUMMY)
            .unwrap();
        assert_eq!(
            rel.to_string(),
            ".decl edge(id: int32, name: string) \
             .input(IO=\"file\", filename=\"Edge.facts\", delimiter=\",\", header=\"false\") \
             .output(IO=\"file\", filename=\"out.tsv\", delimiter=\"\\t\")"
        );
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
        assert_eq!(rel.data_type(), vec![Int32, Str]);
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
