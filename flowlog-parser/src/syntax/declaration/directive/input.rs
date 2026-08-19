//! `.input R(...)`: where a relation's facts come from.
//!
//! [`InputDirective`] names the target relation and carries its parameters;
//! [`InputSource`] is what those mean once the adopting relation fills in the
//! defaults and the rest is refused.

use std::collections::HashMap;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::param::parse_delimiter;
use super::param::parse_io_directive;
use crate::Lexeme;
use crate::Node;
use crate::error::ParseError;

// =============================================================================
// InputDirective
// =============================================================================

/// `.input R(...)`: an EDB source plus parameters (IO type, file path, ...).
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct InputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl InputDirective {
    /// Creates a directive from already-parsed parts, for a directive
    /// synthesized rather than parsed from a pest node (e.g. a comp-internal
    /// directive deferred to the enclosing scope).
    pub(crate) fn new(
        relation_name: String,
        parameters: HashMap<String, String>,
        span: Span,
    ) -> Self {
        Self {
            relation_name,
            parameters,
            span,
        }
    }

    /// Canonical (lowercased) target relation name.
    #[must_use]
    pub(crate) fn relation_name(&self) -> &str {
        &self.relation_name
    }

    /// Parsed I/O parameters, keyed by name (`IO`, `filename`, ...).
    #[must_use]
    pub(crate) fn parameters(&self) -> &HashMap<String, String> {
        &self.parameters
    }

    /// Span of the directive's target relation-name token.
    #[must_use]
    #[inline]
    pub(crate) fn span(&self) -> Span {
        self.span
    }
}

impl Lexeme for InputDirective {
    /// Keeps the parameters as written, because the filename default is named
    /// for the target relation, which a directive only names.
    fn from_parsed_rule(node: Node) -> Result<Self, ParseError> {
        let (raw_name, parameters, span) = parse_io_directive(node)?;
        Ok(Self {
            relation_name: raw_name.to_lowercase(),
            parameters,
            span,
        })
    }
}

// =============================================================================
// InputSource
// =============================================================================

/// Where one relation's `.input` directive reads its facts, every parameter
/// resolved.
///
/// Only a directive that is present produces one, so a relation with no
/// `.input` holds no `InputSource` rather than a defaulted one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSource {
    /// `IO="file"`, or no `IO=` at all.
    File {
        filename: String,
        delim: u8,
        has_header: bool,
    },

    /// `IO="command"`: nothing is read at startup; every fact arrives as a
    /// `put` tuple, which `delim` splits into cells.
    Command { delim: u8 },

    /// `IO="sqlite"`: the database's table named for the relation.
    Sqlite { filename: String },
}

impl InputSource {
    /// Resolves one `.input` directive's parameters, filling in each default
    /// and refusing what no reader could act on.
    ///
    /// `raw_name` supplies the filename default, case-preserved to match
    /// Souffle; `span` labels the directive.
    pub(crate) fn from_params(
        params: &HashMap<String, String>,
        raw_name: &str,
        span: Span,
    ) -> Result<Self, ParseError> {
        // Checked even where it goes unused, so a value no reader could split
        // on is refused whichever storage the directive names.
        let delim = parse_delimiter(params, span)?;
        let filename = |ext: &str| {
            params
                .get("filename")
                .cloned()
                .unwrap_or_else(|| format!("{raw_name}.{ext}"))
        };
        let file = || Self::File {
            filename: filename("facts"),
            delim,
            has_header: params
                .get("header")
                .is_some_and(|v| v.eq_ignore_ascii_case("true")),
        };

        // An absent `IO=` is a text file, matching Souffle, so `.input Edge`
        // and `.input Edge(filename="...")` name the same storage.
        match params.get("IO") {
            None => Ok(file()),
            Some(io) if io.eq_ignore_ascii_case("file") => Ok(file()),
            Some(io) if io.eq_ignore_ascii_case("command") => Ok(Self::Command { delim }),
            Some(io) if io.eq_ignore_ascii_case("sqlite") => Ok(Self::Sqlite {
                filename: filename("sqlite"),
            }),
            Some(io) => Err(ParseError::UnknownInputIo {
                span,
                io: io.clone(),
            }),
        }
    }

    /// The file this source reads, `None` for `IO="command"`, which waits
    /// for `put` tuples instead of reading one.
    #[must_use]
    #[inline]
    pub fn filename(&self) -> Option<&str> {
        match self {
            Self::File { filename, .. } | Self::Sqlite { filename } => Some(filename),
            Self::Command { .. } => None,
        }
    }

    /// The byte cells are split on, `None` for a source whose columns arrive
    /// already typed.
    #[must_use]
    #[inline]
    pub fn delim(&self) -> Option<u8> {
        match self {
            Self::File { delim, .. } | Self::Command { delim } => Some(*delim),
            Self::Sqlite { .. } => None,
        }
    }

    /// Returns `true` if the first line names columns instead of holding a
    /// row. Only a text file has a header line, so every other source
    /// reports `false`.
    #[must_use]
    #[inline]
    pub fn has_header(&self) -> bool {
        matches!(
            self,
            Self::File {
                has_header: true,
                ..
            }
        )
    }

    /// Returns `true` if this source reads a file on disk at startup, which
    /// a database does as much as a text file: only `IO="command"` waits for
    /// `put` tuples instead.
    #[must_use]
    #[inline]
    pub fn is_file_backed(&self) -> bool {
        matches!(self, Self::File { .. } | Self::Sqlite { .. })
    }
}

impl fmt::Display for InputSource {
    /// The directive's parameters as source syntax, every default spelled
    /// out, so a rendered relation shows what will actually be read.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::File {
                filename,
                delim,
                has_header,
            } => write!(
                f,
                "IO=\"file\", filename=\"{filename}\", delimiter=\"{}\", \
                 header=\"{has_header}\"",
                delim.escape_ascii()
            ),
            Self::Command { delim } => {
                write!(f, "IO=\"command\", delimiter=\"{}\"", delim.escape_ascii())
            }
            Self::Sqlite { filename } => write!(f, "IO=\"sqlite\", filename=\"{filename}\""),
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::FileId;
    use rstest::rstest;

    use super::*;
    use crate::Rule;
    use crate::assert_err;
    use crate::test_util::parse_pair;

    // --- InputDirective ---

    fn input(src: &str) -> InputDirective {
        InputDirective::from_parsed_rule(Node::new(
            parse_pair(Rule::input_directive, src),
            FileId::new(0),
        ))
        .expect("input_directive parses")
    }

    /// The target is keyed by its canonical name, as every other reference
    /// to a relation is.
    #[test]
    fn input_lowercases_the_target_relation_name() {
        assert_eq!(input(".input Edge").relation_name(), "edge");
    }

    /// Parameter values arrive string-literal decoded, quotes stripped, so
    /// nothing downstream decodes them a second time.
    #[test]
    fn input_parameters_arrive_decoded() {
        let d = input(r#".input Edge(IO="file", filename="edge.csv", delimiter="\t")"#);
        assert_eq!(d.parameters()["IO"], "file");
        assert_eq!(d.parameters()["filename"], "edge.csv");
        assert_eq!(d.parameters()["delimiter"], "\t");
    }

    /// A bare `.input Edge` with no `(...)` yields no parameters, leaving the
    /// adopting relation to supply every default.
    #[test]
    fn input_without_params_is_empty() {
        assert!(input(".input Edge").parameters().is_empty());
    }

    // --- InputSource ---

    /// Directive parameters as the parser hands them over: already
    /// string-literal decoded, so a `\t` written in the program is a real
    /// tab here.
    fn resolve<const N: usize>(pairs: [(&str, &str); N]) -> Result<InputSource, ParseError> {
        let params: HashMap<String, String> = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        InputSource::from_params(&params, "Edge", Span::DUMMY)
    }

    fn text_file(filename: &str, delim: u8, has_header: bool) -> InputSource {
        InputSource::File {
            filename: filename.to_string(),
            delim,
            has_header,
        }
    }

    /// A bare `.input Edge` is a text file named for the relation, read with
    /// the default delimiter, which is what a directive with no parameters
    /// at all relies on.
    #[test]
    fn absent_io_resolves_to_a_tab_delimited_file_named_for_the_relation() {
        assert_eq!(
            resolve([]).expect("resolved"),
            text_file("Edge.facts", b'\t', false)
        );
    }

    /// Every storage, in the case the user happened to write it. A database
    /// hands over typed columns, so it resolves without a delimiter at all.
    #[rstest]
    #[case("file", text_file("Edge.facts", b'\t', false))]
    #[case("FILE", text_file("Edge.facts", b'\t', false))]
    #[case("command", InputSource::Command { delim: b'\t' })]
    #[case("Command", InputSource::Command { delim: b'\t' })]
    #[case("sqlite", InputSource::Sqlite { filename: "Edge.sqlite".to_string() })]
    #[case("SQLite", InputSource::Sqlite { filename: "Edge.sqlite".to_string() })]
    fn io_names_each_storage_case_insensitively(#[case] io: &str, #[case] expected: InputSource) {
        assert_eq!(resolve([("IO", io)]).expect("resolved"), expected);
    }

    /// A storage no reader implements is refused here rather than leaving
    /// the relation with no startup facts and no complaint.
    #[test]
    fn an_unknown_io_is_rejected() {
        assert_err!(
            resolve([("IO", "parquet")]),
            ParseError::UnknownInputIo { .. }
        );
    }

    /// A database reads a file on disk as much as a text file does, so both
    /// are file-backed; only the storage that waits for `put` tuples is not.
    #[rstest]
    #[case("file", true)]
    #[case("sqlite", true)]
    #[case("command", false)]
    fn only_the_put_fed_storage_is_not_file_backed(#[case] io: &str, #[case] expected: bool) {
        assert_eq!(
            resolve([("IO", io)]).expect("resolved").is_file_backed(),
            expected
        );
    }

    /// A `filename` replaces the default on each storage that reads one; the
    /// `put`-fed storage names no file to replace.
    #[rstest]
    #[case("file", Some("rows.tsv"))]
    #[case("sqlite", Some("rows.tsv"))]
    #[case("command", None)]
    fn a_filename_parameter_replaces_the_default(#[case] io: &str, #[case] expected: Option<&str>) {
        let source = resolve([("IO", io), ("filename", "rows.tsv")]).expect("resolved");
        assert_eq!(source.filename(), expected);
    }

    /// The extension of the default filename is the storage's own, so two
    /// storages of one relation do not collide on disk.
    #[rstest]
    #[case("file", Some("Edge.facts"))]
    #[case("sqlite", Some("Edge.sqlite"))]
    #[case("command", None)]
    fn a_default_filename_is_named_for_the_relation_and_the_storage(
        #[case] io: &str,
        #[case] expected: Option<&str>,
    ) {
        assert_eq!(
            resolve([("IO", io)]).expect("resolved").filename(),
            expected
        );
    }

    /// A database hands over typed columns, so it splits nothing.
    #[rstest]
    #[case("file", Some(b','))]
    #[case("command", Some(b','))]
    #[case("sqlite", None)]
    fn only_a_text_storage_carries_a_delimiter(#[case] io: &str, #[case] expected: Option<u8>) {
        let source = resolve([("IO", io), ("delimiter", ",")]).expect("resolved");
        assert_eq!(source.delim(), expected);
    }

    /// `header="true"` is the only spelling that skips a line, case aside;
    /// anything else leaves the first line a row.
    #[rstest]
    #[case("true", true)]
    #[case("TRUE", true)]
    #[case("false", false)]
    #[case("yes", false)]
    fn a_header_is_skipped_only_when_the_parameter_says_true(
        #[case] value: &str,
        #[case] expected: bool,
    ) {
        assert_eq!(
            resolve([("header", value)]).expect("resolved").has_header(),
            expected
        );
    }

    /// A storage that reads no file has no header line to skip either.
    #[test]
    fn a_put_fed_source_has_no_header() {
        assert!(
            !resolve([("IO", "command"), ("header", "true")])
                .expect("resolved")
                .has_header()
        );
    }

    /// The rendering spells out every default, so what a relation prints is
    /// what it will read, not what the user happened to write.
    #[rstest]
    #[case(InputSource::Command { delim: b'\t' }, "IO=\"command\", delimiter=\"\\t\"")]
    #[case(
        text_file("Edge.facts", b',', false),
        "IO=\"file\", filename=\"Edge.facts\", delimiter=\",\", header=\"false\""
    )]
    #[case(
        InputSource::Sqlite { filename: "Edge.sqlite".to_string() },
        "IO=\"sqlite\", filename=\"Edge.sqlite\""
    )]
    fn display_spells_out_every_resolved_parameter(
        #[case] source: InputSource,
        #[case] expected: &str,
    ) {
        assert_eq!(source.to_string(), expected);
    }
}
