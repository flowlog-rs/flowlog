//! `.output R(...)`: where a relation's rows are written.
//!
//! [`OutputDirective`] names the target relation; [`OutputSink`] is what its
//! parameters mean once defaults are filled in and the rest is refused.
//! Unlike an `.input`, a directive cannot resolve itself: `order_by` names
//! one of the target's attributes, so only the relation can turn it into a
//! column index.

use std::collections::HashMap;
use std::fmt;

use educe::Educe;
use flowlog_common::Span;

use super::param::parse_delimiter;
use super::param::parse_io_directive;
use crate::Lexeme;
use crate::Node;
use crate::declaration::Attribute;
use crate::error::ParseError;
use crate::types::DataType;

// =============================================================================
// OutputDirective
// =============================================================================

/// `.output R(...)`: which relation to write, with optional parameters.
#[derive(Debug, Clone, Educe)]
#[educe(PartialEq, Eq)]
pub(crate) struct OutputDirective {
    relation_name: String,
    parameters: HashMap<String, String>,
    #[educe(PartialEq(ignore))]
    span: Span,
}

impl OutputDirective {
    /// Creates a directive from already-parsed parts. See
    /// [`InputDirective::new`](super::InputDirective::new).
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

    /// Parsed output parameters, keyed by name.
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

impl Lexeme for OutputDirective {
    /// Keeps the parameters as written, because resolving them needs the
    /// target's attributes, which a directive does not have.
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
// OutputSink
// =============================================================================

/// One column an `.output` sorts by: its position, its type, and whether it
/// ascends.
pub type OrderKey = (usize, DataType, bool);

/// Where one relation's `.output` directive writes its rows, every parameter
/// resolved.
///
/// Only a directive that is present produces one, so a relation FlowLog does
/// not write holds no `OutputSink`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OutputSink {
    /// `IO="file"`, or no `IO=` at all.
    File {
        filename: String,
        delim: u8,
        order_by: Option<Vec<OrderKey>>,
        limit: Option<usize>,
    },

    /// `IO="sqlite"`: a table in the database, named for the relation.
    Sqlite {
        filename: String,
        order_by: Option<Vec<OrderKey>>,
        limit: Option<usize>,
    },
}

impl OutputSink {
    /// Resolves one `.output` directive's parameters against the relation it
    /// targets, filling in each default and refusing what no sink could act
    /// on.
    ///
    /// `raw_name` supplies the filename default and names the relation in
    /// diagnostics; `attributes` are what an `order_by` column resolves
    /// against. `span` labels the directive.
    pub(crate) fn from_params(
        params: &HashMap<String, String>,
        raw_name: &str,
        attributes: &[Attribute],
        span: Span,
    ) -> Result<Self, ParseError> {
        // Checked even where the sink has no use for them, so a parameter no
        // sink could act on is refused whichever storage the directive names.
        let delim = parse_delimiter(params, span)?;
        // Resolved before `limit`, which is only legal alongside one.
        let order_by = params
            .get("order_by")
            .map(|spec| resolve_order_by(spec, raw_name, attributes, span))
            .transpose()?;
        let limit = params
            .get("limit")
            .map(|raw| resolve_limit(raw, raw_name, order_by.is_some(), span))
            .transpose()?;
        let filename = |ext: &str| {
            params
                .get("filename")
                .cloned()
                .unwrap_or_else(|| format!("{raw_name}.{ext}"))
        };
        let file = || Self::File {
            filename: filename("csv"),
            delim,
            order_by: order_by.clone(),
            limit,
        };

        // `IO="stdout"` is absent on purpose: the compiler's `-D -` already
        // chooses that, and two ways to say it would disagree.
        match params.get("IO") {
            None => Ok(file()),
            Some(io) if io.eq_ignore_ascii_case("file") => Ok(file()),
            Some(io) if io.eq_ignore_ascii_case("sqlite") => Ok(Self::Sqlite {
                filename: filename("sqlite"),
                order_by,
                limit,
            }),
            Some(io) => Err(ParseError::UnknownOutputIo {
                span,
                io: io.clone(),
            }),
        }
    }

    /// The file this sink writes. Every sink names one.
    #[must_use]
    #[inline]
    pub fn filename(&self) -> &str {
        match self {
            Self::File { filename, .. } | Self::Sqlite { filename, .. } => filename,
        }
    }

    /// The byte written between columns, `None` for a sink that writes typed
    /// columns rather than text.
    #[must_use]
    #[inline]
    pub fn delim(&self) -> Option<u8> {
        match self {
            Self::File { delim, .. } => Some(*delim),
            Self::Sqlite { .. } => None,
        }
    }

    /// The columns rows are sorted by, in the order given.
    #[must_use]
    #[inline]
    pub fn order_by(&self) -> Option<&[OrderKey]> {
        match self {
            Self::File { order_by, .. } | Self::Sqlite { order_by, .. } => order_by.as_deref(),
        }
    }

    /// How many rows are written, `None` for all of them.
    #[must_use]
    #[inline]
    pub fn limit(&self) -> Option<usize> {
        match self {
            Self::File { limit, .. } | Self::Sqlite { limit, .. } => *limit,
        }
    }
}

impl fmt::Display for OutputSink {
    /// The directive's parameters spelled the way one is written, every
    /// default filled in, so a rendered relation shows what will actually
    /// be written. Not re-parseable: an `order_by` column renders as its
    /// resolved position, because the key no longer carries the name.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (order_by, limit) = match self {
            Self::File {
                filename,
                delim,
                order_by,
                limit,
            } => {
                write!(
                    f,
                    "IO=\"file\", filename=\"{filename}\", delimiter=\"{}\"",
                    delim.escape_ascii()
                )?;
                (order_by, limit)
            }
            Self::Sqlite {
                filename,
                order_by,
                limit,
            } => {
                write!(f, "IO=\"sqlite\", filename=\"{filename}\"")?;
                (order_by, limit)
            }
        };
        if let Some(keys) = order_by {
            write!(f, ", order_by=\"")?;
            for (i, (column, _, ascending)) in keys.iter().enumerate() {
                let direction = if *ascending { "ASC" } else { "DESC" };
                let comma = if i > 0 { ", " } else { "" };
                write!(f, "{comma}{column} {direction}")?;
            }
            write!(f, "\"")?;
        }
        if let Some(limit) = limit {
            write!(f, ", limit=\"{limit}\"")?;
        }
        Ok(())
    }
}

/// The `order_by` parameter as one key per column, in the order written.
///
/// A column is named, not numbered, so the name must match one of
/// `attributes`; a direction is `ASC` or `DESC`, ascending when omitted.
fn resolve_order_by(
    spec: &str,
    raw_name: &str,
    attributes: &[Attribute],
    span: Span,
) -> Result<Vec<OrderKey>, ParseError> {
    let invalid = |reason: String| ParseError::InvalidOrderBy {
        span,
        relation: raw_name.to_string(),
        reason,
    };
    let mut keys = Vec::new();
    for part in spec.split(',') {
        let tokens: Vec<&str> = part.split_whitespace().collect();
        let (name, direction) = match tokens.as_slice() {
            [name] => (*name, None),
            [name, direction] => (*name, Some(*direction)),
            [] => return Err(invalid("a column is missing".to_string())),
            _ => {
                return Err(invalid(format!(
                    "`{}` names more than a column and a direction",
                    part.trim()
                )));
            }
        };
        let ascending = match direction {
            None => true,
            Some(d) if d.eq_ignore_ascii_case("asc") => true,
            Some(d) if d.eq_ignore_ascii_case("desc") => false,
            Some(d) => return Err(invalid(format!("`{d}` is not ASC or DESC"))),
        };
        let lower = name.to_lowercase();
        let (column, attribute) = attributes
            .iter()
            .enumerate()
            .find(|(_, a)| a.name() == lower)
            .ok_or_else(|| invalid(format!("`{name}` is not one of its attributes")))?;
        keys.push((column, attribute.data_type().clone(), ascending));
    }
    Ok(keys)
}

/// The `limit` parameter as a row count.
///
/// A count without an `order_by` is refused: which rows survive would depend
/// on the order they happened to be derived in.
fn resolve_limit(
    raw: &str,
    raw_name: &str,
    has_order_by: bool,
    span: Span,
) -> Result<usize, ParseError> {
    let limit = raw.parse::<usize>().map_err(|_| ParseError::InvalidLimit {
        span,
        relation: raw_name.to_string(),
        value: raw.to_string(),
    })?;
    if !has_order_by {
        return Err(ParseError::LimitWithoutOrderBy {
            span,
            relation: raw_name.to_string(),
        });
    }
    Ok(limit)
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::types::DataType::Int32;
    use crate::types::DataType::String as Str;
    use crate::types::TypeRegistry;

    /// A two-column relation to resolve `order_by` against.
    fn attrs() -> Vec<Attribute> {
        let reg = TypeRegistry::new();
        vec![
            Attribute::with_type("id".into(), Int32, reg.primitive_id(Int32).unwrap()),
            Attribute::with_type("name".into(), Str, reg.primitive_id(Str).unwrap()),
        ]
    }

    /// Directive parameters as the parser hands them over, resolved
    /// against a relation named `R` with those two columns.
    fn resolve<const N: usize>(pairs: [(&str, &str); N]) -> Result<OutputSink, ParseError> {
        let params: HashMap<String, String> = pairs
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        OutputSink::from_params(&params, "R", &attrs(), Span::DUMMY)
    }

    /// The default file sink for a relation named `R`, with the given
    /// delimiter, ordering, and row cap.
    fn csv(delim: u8, order_by: Option<Vec<OrderKey>>, limit: Option<usize>) -> OutputSink {
        OutputSink::File {
            filename: "R.csv".to_string(),
            delim,
            order_by,
            limit,
        }
    }

    // --- Defaults ---

    /// A bare `.output R` writes a tab-delimited file named for the relation,
    /// sorted and truncated by nothing.
    #[test]
    fn no_parameters_resolves_every_default() {
        assert_eq!(resolve([]).expect("resolved"), csv(b'\t', None, None));
    }

    /// A `filename` replaces the default on each storage that writes one.
    #[rstest]
    #[case("file", "rows.tsv", OutputSink::File {
        filename: "rows.tsv".to_string(), delim: b'\t', order_by: None, limit: None })]
    #[case("sqlite", "db.sqlite", OutputSink::Sqlite {
        filename: "db.sqlite".to_string(), order_by: None, limit: None })]
    fn a_filename_parameter_replaces_the_default(
        #[case] io: &str,
        #[case] name: &str,
        #[case] expected: OutputSink,
    ) {
        assert_eq!(
            resolve([("IO", io), ("filename", name)]).expect("resolved"),
            expected
        );
    }

    /// The extension of the default filename is the storage's own.
    #[rstest]
    #[case("file", "R.csv")]
    #[case("sqlite", "R.sqlite")]
    fn a_default_filename_is_named_for_the_relation_and_the_storage(
        #[case] io: &str,
        #[case] expected: &str,
    ) {
        assert_eq!(
            resolve([("IO", io)]).expect("resolved").filename(),
            expected
        );
    }

    // --- IO ---

    /// A database writes typed columns, so it delimits nothing.
    #[rstest]
    #[case("file", Some(b'|'))]
    #[case("sqlite", None)]
    fn only_a_text_sink_carries_a_delimiter(#[case] io: &str, #[case] expected: Option<u8>) {
        let sink = resolve([("IO", io), ("delimiter", "|")]).expect("resolved");
        assert_eq!(sink.delim(), expected);
    }

    /// A sink FlowLog does not write is refused here rather than writing the
    /// rows to a file under another name.
    #[test]
    fn an_unknown_io_is_rejected() {
        assert_err!(
            resolve([("IO", "parquet")]),
            ParseError::UnknownOutputIo { .. }
        );
    }

    // --- order_by ---

    /// A column is named, and resolves to its position and declared type so
    /// the sink can sort without the schema.
    #[test]
    fn order_by_resolves_a_column_name_to_its_position_and_type() {
        assert_eq!(
            resolve([("order_by", "name")])
                .expect("resolved")
                .order_by(),
            Some([(1, Str, true)].as_slice())
        );
    }

    /// Keys hold the order they were written, which is the order rows sort
    /// by; a direction is optional and ascends when omitted.
    #[test]
    fn order_by_keeps_the_written_order_and_defaults_to_ascending() {
        assert_eq!(
            resolve([("order_by", "name DESC, id")])
                .expect("resolved")
                .order_by(),
            Some([(1, Str, false), (0, Int32, true)].as_slice())
        );
    }

    /// Directions and column names are matched case-insensitively, as the
    /// rest of the language matches relation and attribute names.
    #[rstest]
    #[case("ID asc")]
    #[case("Id ASC")]
    #[case("id Asc")]
    fn order_by_matches_a_column_and_a_direction_case_insensitively(#[case] spec: &str) {
        assert_eq!(
            resolve([("order_by", spec)]).expect("resolved").order_by(),
            Some([(0, Int32, true)].as_slice())
        );
    }

    /// Every way an `order_by` can name no sortable column: an unknown one,
    /// an empty clause, a direction that is neither, and a clause carrying
    /// more than a column and a direction.
    #[rstest]
    #[case("nonexistent")]
    #[case("")]
    #[case("id sideways")]
    #[case("id ASC DESC")]
    fn order_by_rejects_what_names_no_sortable_column(#[case] spec: &str) {
        assert_err!(
            resolve([("order_by", spec)]),
            ParseError::InvalidOrderBy { .. }
        );
    }

    // --- limit ---

    /// A row cap needs an ordering, so the rows that survive are the same on
    /// every run.
    #[test]
    fn limit_alongside_an_order_by_resolves_to_a_row_count() {
        assert_eq!(
            resolve([("limit", "42"), ("order_by", "id")])
                .expect("resolved")
                .limit(),
            Some(42)
        );
    }

    #[test]
    fn limit_without_an_order_by_is_rejected() {
        assert_err!(
            resolve([("limit", "42")]),
            ParseError::LimitWithoutOrderBy { .. }
        );
    }

    /// A cap that is not a row count is refused before the ordering is even
    /// considered, so the message names the value the user wrote.
    #[rstest]
    #[case("abc")]
    #[case("-1")]
    #[case("")]
    fn limit_rejects_what_is_not_a_row_count(#[case] value: &str) {
        assert_err!(
            resolve([("limit", value), ("order_by", "id")]),
            ParseError::InvalidLimit { .. }
        );
    }

    // --- Display ---

    /// The rendering spells out every default and appends only the optional
    /// parameters that are set, so what a relation prints is what will be
    /// written.
    #[rstest]
    #[case(
        csv(b'\t', None, None),
        "IO=\"file\", filename=\"R.csv\", delimiter=\"\\t\""
    )]
    #[case(
        csv(b',', Some(vec![(1, Str, false), (0, Int32, true)]), Some(5)),
        "IO=\"file\", filename=\"R.csv\", delimiter=\",\", order_by=\"1 DESC, 0 ASC\", limit=\"5\""
    )]
    #[case(
        OutputSink::Sqlite { filename: "R.sqlite".to_string(), order_by: None, limit: None },
        "IO=\"sqlite\", filename=\"R.sqlite\""
    )]
    fn display_spells_out_every_resolved_parameter(
        #[case] sink: OutputSink,
        #[case] expected: &str,
    ) {
        assert_eq!(sink.to_string(), expected);
    }
}
