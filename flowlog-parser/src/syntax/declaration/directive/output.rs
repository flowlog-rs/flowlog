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
use flowlog_error::Span;

use super::params::parse_delimiter;
use super::params::parse_io_params;
use crate::Lexeme;
use crate::Node;
use crate::Rule;
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
        /// The columns rows are sorted by, in the order given.
        order_by: Option<Vec<OrderKey>>,
        /// How many rows are written, `None` for all of them.
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

    /// Directive parameters as `parse_io_params` hands them over, resolved
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
    #[case("sqlite", "db.sqlite", OutputSink::Sqlite { filename: "db.sqlite".to_string(), order_by: None, limit: None })]
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

    // --- IO ---

    /// Each storage FlowLog names, in the case the user happened to write it.
    /// A database writes typed columns in no particular order, so it resolves
    /// without a delimiter or an ordering at all.
    #[rstest]
    #[case("file", csv(b'\t', None, None))]
    #[case("FILE", csv(b'\t', None, None))]
    #[case("sqlite", OutputSink::Sqlite { filename: "R.sqlite".to_string(), order_by: None, limit: None })]
    #[case("SQLite", OutputSink::Sqlite { filename: "R.sqlite".to_string(), order_by: None, limit: None })]
    fn io_names_each_storage_case_insensitively(#[case] io: &str, #[case] expected: OutputSink) {
        assert_eq!(resolve([("IO", io)]).expect("resolved"), expected);
    }

    /// `IO="stdout"` is not a sink here even though Souffle prints for it: the
    /// compiler's `-D -` already chooses that, and an unknown value is refused
    /// rather than written as a file under another name.
    #[rstest]
    #[case("stdout")]
    #[case("bogus")]
    fn an_io_that_names_no_sink_is_rejected(#[case] io: &str) {
        assert_err!(resolve([("IO", io)]), ParseError::UnknownOutputIo { .. });
    }

    // --- delimiter ---

    /// A delimiter is one byte, exactly as an `.input`'s is, so a file
    /// FlowLog writes reads back through FlowLog's own reader.
    #[test]
    fn a_one_character_delimiter_resolves_to_its_byte() {
        assert_eq!(
            resolve([("delimiter", "|")]).expect("resolved"),
            csv(b'|', None, None)
        );
    }

    /// A separator no reader could split on is refused on the way out too:
    /// FlowLog would otherwise write a file it cannot read back.
    #[rstest]
    #[case("")]
    #[case("||")]
    #[case("\n")]
    fn a_delimiter_that_is_not_one_byte_is_rejected(#[case] delim: &str) {
        assert_err!(
            resolve([("delimiter", delim)]),
            ParseError::InvalidDelimiter { .. }
        );
    }

    /// Checked even where the sink has no use for it, so an unusable value
    /// cannot hide behind `IO="sqlite"`.
    #[test]
    fn an_unusable_delimiter_is_rejected_even_for_a_database() {
        assert_err!(
            resolve([("IO", "sqlite"), ("delimiter", "||")]),
            ParseError::InvalidDelimiter { .. }
        );
    }

    // --- order_by ---

    /// A column is named, and ascends unless told otherwise.
    #[test]
    fn a_single_order_by_column_ascends_by_default() {
        assert_eq!(
            resolve([("order_by", "id")]).expect("resolved"),
            csv(b'\t', Some(vec![(0, Int32, true)]), None)
        );
    }

    /// Columns keep the order they were written, each with its own direction.
    #[test]
    fn order_by_keeps_the_order_written_with_each_direction() {
        assert_eq!(
            resolve([("order_by", "name DESC, id ASC")]).expect("resolved"),
            csv(b'\t', Some(vec![(1, Str, false), (0, Int32, true)]), None)
        );
    }

    /// A direction is matched however the user cased it.
    #[rstest]
    #[case("id asc", true)]
    #[case("id ASC", true)]
    #[case("id desc", false)]
    #[case("id DeSc", false)]
    fn an_order_by_direction_is_case_insensitive(#[case] spec: &str, #[case] ascending: bool) {
        assert_eq!(
            resolve([("order_by", spec)]).expect("resolved"),
            csv(b'\t', Some(vec![(0, Int32, ascending)]), None)
        );
    }

    /// Every clause a sink could not sort by: a column the relation does not
    /// have, an empty entry, a direction that is neither, and more tokens
    /// than a column and a direction.
    #[rstest]
    #[case("nonexistent")]
    #[case("id,")]
    #[case("id sideways")]
    #[case("id ASC DESC")]
    fn an_order_by_a_sink_cannot_sort_by_is_rejected(#[case] spec: &str) {
        assert_err!(
            resolve([("order_by", spec)]),
            ParseError::InvalidOrderBy { .. }
        );
    }

    /// An ordering and a row cap choose which rows are written at all, so a
    /// database sink keeps both even though its table holds no order itself.
    #[test]
    fn a_database_sink_keeps_its_ordering_and_row_cap() {
        assert_eq!(
            resolve([("IO", "sqlite"), ("order_by", "id"), ("limit", "3")]).expect("resolved"),
            OutputSink::Sqlite {
                filename: "R.sqlite".to_string(),
                order_by: Some(vec![(0, Int32, true)]),
                limit: Some(3),
            }
        );
    }

    // --- limit ---

    /// A count truncates the sorted rows.
    #[test]
    fn a_limit_alongside_an_order_by_is_a_row_count() {
        assert_eq!(
            resolve([("order_by", "id"), ("limit", "42")]).expect("resolved"),
            csv(b'\t', Some(vec![(0, Int32, true)]), Some(42))
        );
    }

    /// Which rows survive would otherwise depend on the order they happened
    /// to be derived in.
    #[test]
    fn a_limit_without_an_order_by_is_rejected() {
        assert_err!(
            resolve([("limit", "42")]),
            ParseError::LimitWithoutOrderBy { .. }
        );
    }

    /// A count that is not one is refused, order_by present or not.
    #[rstest]
    #[case("abc")]
    #[case("-1")]
    #[case("")]
    fn a_limit_that_is_not_a_row_count_is_rejected(#[case] limit: &str) {
        assert_err!(
            resolve([("order_by", "id"), ("limit", limit)]),
            ParseError::InvalidLimit { .. }
        );
    }

    // --- Display ---

    /// Rendered as the parameters that produce it, so a rendered relation
    /// shows what will be written rather than what was typed.
    #[test]
    fn display_spells_out_every_resolved_parameter() {
        assert_eq!(
            resolve([]).expect("resolved").to_string(),
            r#"IO="file", filename="R.csv", delimiter="\t""#
        );
        assert_eq!(
            resolve([("order_by", "name DESC"), ("limit", "5")])
                .expect("resolved")
                .to_string(),
            r#"IO="file", filename="R.csv", delimiter="\t", order_by="1 DESC", limit="5""#
        );
        assert_eq!(
            resolve([("IO", "sqlite"), ("order_by", "id"), ("limit", "3")])
                .expect("resolved")
                .to_string(),
            r#"IO="sqlite", filename="R.sqlite", order_by="0 ASC", limit="3""#
        );
    }
}
