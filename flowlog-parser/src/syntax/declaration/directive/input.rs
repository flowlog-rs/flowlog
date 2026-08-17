//! `.input R(...)`: where a relation's facts come from.
//!
//! [`InputDirective`] names the target relation and carries its parameters;
//! [`InputSource`] is what those mean once the adopting relation fills in the
//! defaults and the rest is refused.

use std::collections::HashMap;
use std::fmt;

use educe::Educe;
use flowlog_error::Span;

use super::params::parse_delimiter;
use super::params::parse_io_params;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
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
    /// Creates a directive from already-parsed parts, for one synthesized
    /// rather than read from a pest node.
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
        let mut children = node.children();
        let name_node = children.next_any("relation name")?;
        let span = name_node.span();
        let parameters = match children.take_if(Rule::io_params) {
            Some(node) => parse_io_params(node)?,
            None => HashMap::new(),
        };
        Ok(Self {
            relation_name: name_node.text().to_lowercase(),
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
///
/// A `delim` is one ASCII byte, never a line terminator, and belongs to the
/// variants that split text into cells; a database hands over typed columns
/// and has none.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InputSource {
    /// `IO="file"`, or no `IO=` at all.
    File {
        filename: String,
        delim: u8,
        /// Whether the first line names columns instead of holding a row.
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
    /// Souffle.
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

    /// The file this source reads, `None` for the one that reads none.
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
    /// row.
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
    use flowlog_error::FileId;
    use rstest::rstest;

    use super::*;
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

    /// Directive parameters as `parse_io_params` hands them over: already
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

    /// A database reads a file on disk as much as a text file does, so both
    /// are file-backed; only the storage that waits for `put` tuples is not.
    #[rstest]
    #[case("file", true)]
    #[case("sqlite", true)]
    #[case("command", false)]
    fn every_storage_but_command_is_file_backed(#[case] io: &str, #[case] expected: bool) {
        assert_eq!(
            resolve([("IO", io)]).expect("resolved").is_file_backed(),
            expected
        );
    }

    /// `IO=` is a closed set, so a value outside it is refused rather than
    /// leaving the relation with no reader and no diagnostic. `"csv"` names
    /// the format rather than the storage, which is the confusion the
    /// diagnostic's note answers.
    #[rstest]
    #[case("csv")]
    #[case("filesystem")]
    fn an_unknown_io_is_rejected(#[case] io: &str) {
        assert_err!(resolve([("IO", io)]), ParseError::UnknownInputIo { .. });
    }

    /// A `filename` replaces the default on each storage that reads one.
    #[rstest]
    #[case("file", "Pair.tsv", text_file("Pair.tsv", b'\t', false))]
    #[case("sqlite", "db.sqlite", InputSource::Sqlite { filename: "db.sqlite".to_string() })]
    fn a_filename_parameter_replaces_the_default(
        #[case] io: &str,
        #[case] name: &str,
        #[case] expected: InputSource,
    ) {
        assert_eq!(
            resolve([("IO", io), ("filename", name)]).expect("resolved"),
            expected
        );
    }

    /// A header is declared, never guessed.
    #[rstest]
    #[case(None, false)]
    #[case(Some("true"), true)]
    #[case(Some("TRUE"), true)]
    #[case(Some("false"), false)]
    fn a_header_is_skipped_only_when_declared_true(
        #[case] header: Option<&str>,
        #[case] expected: bool,
    ) {
        let source = match header {
            Some(v) => resolve([("header", v)]),
            None => resolve([]),
        }
        .expect("resolved");
        assert_eq!(source, text_file("Edge.facts", b'\t', expected));
    }

    /// A `put` tuple is split on the delimiter too, so the storage that
    /// reads no file still carries one.
    #[rstest]
    #[case("file", text_file("Edge.facts", b',', false))]
    #[case("command", InputSource::Command { delim: b',' })]
    fn a_one_character_delimiter_resolves_to_its_byte(
        #[case] io: &str,
        #[case] expected: InputSource,
    ) {
        assert_eq!(
            resolve([("IO", io), ("delimiter", ",")]).expect("resolved"),
            expected
        );
    }

    /// A delimiter is checked even where the storage has no use for one, so
    /// an unusable value cannot hide behind `IO="sqlite"`.
    #[test]
    fn an_unusable_delimiter_is_rejected_even_for_a_database() {
        assert_err!(
            resolve([("IO", "sqlite"), ("delimiter", "||")]),
            ParseError::InvalidDelimiter { .. }
        );
    }

    /// Anything but one byte names no byte to split on: an empty value, two
    /// characters (`\t` written with a literal backslash), and a multi-byte
    /// character.
    #[rstest]
    #[case("")]
    #[case("||")]
    #[case("\\t")]
    #[case("\u{2192}")]
    fn a_delimiter_that_is_not_one_byte_is_rejected(#[case] delim: &str) {
        assert_err!(
            resolve([("delimiter", delim)]),
            ParseError::InvalidDelimiter { .. }
        );
    }

    /// A line terminator is refused even though it is one byte: the reader
    /// consumes it to end the line, so no cell could be split on it.
    #[rstest]
    #[case("\n")]
    #[case("\r")]
    fn a_line_terminator_is_rejected_as_a_delimiter(#[case] delim: &str) {
        assert_err!(
            resolve([("delimiter", delim)]),
            ParseError::InvalidDelimiter { .. }
        );
    }
}
