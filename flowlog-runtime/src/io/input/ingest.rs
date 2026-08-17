//! One handler per relation, and the loop beneath it.
//!
//! [`Ingest`] holds the relation's [`RelationSpec`] and its [`Session`],
//! with one entry point per kind of source and the three lifecycle steps a
//! driver needs. A relation supplies two accessors; everything else is a
//! default.
//!
//! Handlers are reached as named fields of a generated container, never
//! boxed or looked up, so every call is direct. The one place a relation is
//! named at run time, a transaction op in incremental mode, resolves
//! through a generated match rather than an erased trait.

use std::path::Path;

use differential_dataflow::Data;
use differential_dataflow::difference::Semigroup;
use timely::progress::Timestamp;

use crate::error::RuntimeError;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::Line;
use crate::io::input::reader::Reader;
use crate::io::input::reader::line::LineReader;
use crate::io::input::reader::text::TextReader;
use crate::io::input::reader::vec::VecReader;
use crate::io::input::session::Session;
use crate::io::spec::InputSpec;
use crate::io::spec::RelationSpec;

// =============================================================================
// The pump
// =============================================================================

/// Read one source's share of tuples into `apply`.
///
/// `R` names the reader and the spec names its source; a worker with no
/// share returns `Ok` having applied nothing. A row the source refuses is
/// reported to `on_skip` and the drive continues; a cursor error stops
/// it, because a source that failed to produce a row makes no
/// forward-progress promise.
pub(crate) fn ingest<'src, R, T, F, A>(
    spec: &InputSpec<'src, R::Source>,
    mut apply: A,
    mut on_skip: F,
) -> Result<(), RuntimeError>
where
    R: Reader<'src, T>,
    A: FnMut(T),
    F: FnMut(&RuntimeError),
{
    let Some(mut reader) = R::open(spec)? else {
        return Ok(());
    };
    loop {
        let Some(row) = reader.next()? else {
            return Ok(());
        };
        match row {
            Ok(tuple) => apply(tuple),
            Err(e) => on_skip(&e),
        }
    }
}

// =============================================================================
// Ingest
// =============================================================================

/// A relation's input: what it was declared as, where its rows go, and one
/// entry point per place rows come from.
///
/// A relation supplies four types and two accessors. Everything below them
/// is a default written once here, so a relation that reads a file emits no
/// code for the entry points it never calls.
pub trait Ingest: Sized {
    /// The dataflow timestamp the execution mode fixes.
    type Ts: Timestamp + Clone;
    /// The update weight the execution mode fixes.
    type Diff: Semigroup + Copy + 'static;
    /// The slot tuple this relation's rows decode into, as the dataflow
    /// holds it: interned strings, wrapped floats.
    type Tuple: for<'l> Decode<Line<'l>> + Decode<Self::Rows> + Data;
    /// The host-facing tuple [`load_vec`](Self::load_vec) accepts, as a
    /// host program builds it: plain `String`, bare floats.
    type Rows;

    /// The constants this relation was declared with.
    fn spec(&self) -> &'static RelationSpec;

    /// The relation's handle on the dataflow.
    fn session(&mut self) -> &mut Session<Self::Ts, Self::Tuple, Self::Diff>;

    /// Apply the `.fact` rows written in the program itself.
    ///
    /// The one origin that needs no reader: the parser already typed these,
    /// so they arrive as slot values rather than as text to decode. Worker
    /// `index` decides who applies them. Defaults to none, which is what
    /// most relations declare.
    fn inline_facts(&mut self, index: usize) {
        let _ = index;
    }

    /// Read this worker's share of a file.
    ///
    /// A row the relation refuses is reported and skipped. An error that
    /// would leave the relation silently short is returned instead, because
    /// a partly-read input is indistinguishable from a smaller one.
    ///
    /// A nullary relation has no columns for a file to hold, so it reports
    /// and reads nothing. That is not an `Err`: nothing was applied, and a
    /// mistyped command should not end an interactive session.
    fn load_file(
        &mut self,
        path: &Path,
        diff: Self::Diff,
        peers: usize,
        index: usize,
    ) -> Result<(), RuntimeError> {
        let rel = self.spec();
        let name = rel.name;
        if rel.arity == 0 {
            if index == 0 {
                eprintln!(
                    "[relation][{name}] nullary relations take no file; \
                     use `put {name} True|False`"
                );
            }
            return Ok(());
        }
        let spec = InputSpec {
            rel,
            source: path,
            peers,
            index,
        };
        let session = self.session();
        ingest::<TextReader<Self::Tuple>, Self::Tuple, _, _>(
            &spec,
            |t| session.update(t, diff),
            |e| eprintln!("[relation][{name}] {e} reading {}", path.display()),
        )
    }

    /// Apply one delimited line, on whichever worker owns it.
    ///
    /// Every worker is handed the same line and the reader decides who owns
    /// it, so exactly one applies it. A one-row source has nothing to skip
    /// past, so a refusal is the whole call failing: reported, with nothing
    /// applied anywhere.
    fn load_line(&mut self, line: &str, diff: Self::Diff, peers: usize, index: usize) {
        let rel = self.spec();
        let name = rel.name;
        let spec = InputSpec {
            rel,
            source: line,
            peers,
            index,
        };
        let session = self.session();
        let result = ingest::<LineReader<'_, Self::Tuple>, Self::Tuple, _, _>(
            &spec,
            |t| session.update(t, diff),
            |e| eprintln!("[relation][{name}] {e} in put"),
        );
        if let Err(e) = result {
            eprintln!("[relation][{name}] {e} in put");
        }
    }

    /// Apply this worker's share of rows a host program supplied.
    ///
    /// The compiler fixes a host row's types, so today's conversions never
    /// refuse one. Host data is not trusted beyond that: a refusal reports
    /// and skips exactly as a file row's would.
    fn load_vec(&mut self, rows: &[Self::Rows], diff: Self::Diff, peers: usize, index: usize) {
        let rel = self.spec();
        let name = rel.name;
        let spec = InputSpec {
            rel,
            source: rows,
            peers,
            index,
        };
        let session = self.session();
        if let Err(e) = ingest::<VecReader<'_, Self::Rows, Self::Tuple>, Self::Tuple, _, _>(
            &spec,
            |t| session.update(t, diff),
            |e| eprintln!("[relation][{name}] {e} in a host-supplied row"),
        ) {
            eprintln!("[relation][{name}] {e} in host-supplied rows");
        }
    }

    /// Move the relation's session to epoch `t`.
    ///
    /// The three lifecycle steps are here rather than on the session alone
    /// so a handler is a complete input: a caller that holds one never
    /// needs to reach past it.
    fn advance_to(&mut self, t: Self::Ts) {
        self.session().advance_to(t);
    }

    /// Push buffered updates into the dataflow.
    fn flush(&mut self) {
        self.session().flush();
    }

    /// Close the session, so the dataflow can drain to fixpoint.
    fn close(&mut self) {
        self.session().close();
    }
}

/// Define a relation's handler: the struct around its [`Session`].
///
/// The caller supplies `impl Ingest`, which is where the relation's
/// constants, types, and any `.fact` rows live.
///
/// ```ignore
/// flowlog_runtime::relation!(RelEdge, Ts, Diff, (i32, Spur));
///
/// impl flowlog_runtime::io::Ingest for RelEdge {
///     type Ts = Ts;
///     type Diff = Diff;
///     type Tuple = (i32, Spur);   // interned, as the dataflow holds it
///     type Rows = (i32, String);  // plain, as a host program builds it
///     fn spec(&self) -> &'static RelationSpec { &EDGE_SPEC }
///     fn session(&mut self) -> &mut Session<Ts, Self::Tuple, Diff> {
///         &mut self.session
///     }
/// }
/// ```
#[macro_export]
macro_rules! relation {
    ($name:ident, $ts:ty, $diff:ty, $tuple:ty) => {
        pub(crate) struct $name {
            session: $crate::io::Session<$ts, $tuple, $diff>,
        }

        impl $name {
            pub fn new(
                h: $crate::differential_dataflow::input::InputSession<$ts, $tuple, $diff>,
            ) -> Self {
                Self {
                    session: $crate::io::Session::new(h),
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::Write as _;
    use std::path::PathBuf;

    use differential_dataflow::input::InputSession;
    use lasso::Spur;
    use ordered_float::OrderedFloat;

    use super::*;
    use crate::io::ShardKey;
    use crate::io::spec::Format;

    const fn text_rel(uses_ord: bool, shard: ShardKey) -> RelationSpec {
        RelationSpec {
            name: "R",
            arity: 2,
            delim: b',',
            format: Format::Text {
                delim: b',',
                has_header: false,
            },
            shard,
            uses_ord,
        }
    }

    static R_TEXT: RelationSpec = text_rel(false, ShardKey::Str);
    static R_TEXT_ORD: RelationSpec = text_rel(true, ShardKey::Str);

    fn text_file(dir: &Path, content: &str) -> PathBuf {
        let path = dir.join("in.csv");
        File::create(&path)
            .expect("create")
            .write_all(content.as_bytes())
            .expect("write");
        path
    }

    fn text_spec<'a>(path: &'a Path, uses_ord: bool, peers: usize, index: usize) -> InputSpec<'a> {
        InputSpec {
            rel: if uses_ord { &R_TEXT_ORD } else { &R_TEXT },
            source: path,
            peers,
            index,
        }
    }

    /// Decode every row of the spec's share as `(String, i64)`.
    fn read(spec: &InputSpec<'_>) -> (Vec<(String, i64)>, Vec<String>) {
        let mut rows = Vec::new();
        let mut skipped = Vec::new();
        ingest::<TextReader<_>, (String, i64), _, _>(
            spec,
            |t| rows.push(t),
            |e| skipped.push(e.to_string()),
        )
        .expect("cursor");
        (rows, skipped)
    }

    /// Rows decode to tuples in file order.
    #[test]
    fn text_rows_decode_in_order() {
        let dir = tempfile::tempdir().expect("dir");
        let path = text_file(dir.path(), "a,1\nb,2\nc,3\n");
        let (rows, skipped) = read(&text_spec(&path, false, 1, 0));
        assert_eq!(
            rows,
            vec![("a".into(), 1), ("b".into(), 2), ("c".into(), 3)]
        );
        assert!(skipped.is_empty());
    }

    /// A row that does not decode is reported to on_skip and the rows
    /// after it still arrive.
    #[test]
    fn row_that_does_not_decode_skips_and_continues() {
        let dir = tempfile::tempdir().expect("dir");
        let path = text_file(dir.path(), "a,1\nb,x\nc,3\n");
        let (rows, skipped) = read(&text_spec(&path, false, 1, 0));
        assert_eq!(rows, vec![("a".into(), 1), ("c".into(), 3)]);
        assert_eq!(skipped.len(), 1, "{skipped:?}");
        assert!(skipped[0].contains("column 1"), "{skipped:?}");
    }

    /// A line that is not UTF-8 is a corrupt file, not a bad cell: the
    /// cursor stops the load with the error.
    #[test]
    fn invalid_utf8_line_stops_the_load() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("in.csv");
        File::create(&path)
            .expect("create")
            .write_all(b"a,1\n\xFF\xFE,2\n")
            .expect("write");
        let mut rows: Vec<(String, i64)> = Vec::new();
        let err = ingest::<TextReader<_>, (String, i64), _, _>(
            &text_spec(&path, false, 1, 0),
            |t| rows.push(t),
            |_| {},
        );
        assert!(err.is_err());
        assert_eq!(rows.len(), 1, "the row before the corruption arrived");
    }

    /// Under ord, a text scan interns while reading, so only worker 0
    /// reads: the rest get nothing and worker 0 gets every row.
    #[test]
    fn text_under_ord_collapses_to_worker_zero() {
        let dir = tempfile::tempdir().expect("dir");
        let path = text_file(dir.path(), "a,1\nb,2\nc,3\nd,4\n");
        let (w0, _) = read(&text_spec(&path, true, 4, 0));
        assert_eq!(w0.len(), 4, "worker 0 reads the whole file");
        for index in 1..4 {
            let (rows, _) = read(&text_spec(&path, true, 4, index));
            assert!(rows.is_empty(), "worker {index} must read nothing");
        }
    }

    /// Without ord, the workers together decode every row exactly once.
    #[test]
    fn text_without_ord_shares_the_file() {
        let dir = tempfile::tempdir().expect("dir");
        let content: String = (0..300).map(|i| format!("r{i},{i}\n")).collect();
        let path = text_file(dir.path(), &content);

        let mut seen = Vec::new();
        for index in 0..4 {
            let (rows, _) = read(&text_spec(&path, false, 4, index));
            seen.extend(rows.into_iter().map(|(s, _)| s));
        }
        seen.sort();
        let mut expected: Vec<String> = (0..300).map(|i| format!("r{i}")).collect();
        expected.sort();
        assert_eq!(seen, expected);
    }

    /// A missing text file keeps its legacy contract: reported to stderr,
    /// relation loads as empty, no error.
    #[test]
    fn missing_text_file_loads_as_empty() {
        let dir = tempfile::tempdir().expect("dir");
        let path = dir.path().join("absent.csv");
        let (rows, skipped) = read(&text_spec(&path, false, 1, 0));
        assert!(rows.is_empty());
        assert!(skipped.is_empty());
    }

    /// Apply one put line as `(Spur, i64)` if this worker owns it.
    fn put_line(rel: &RelationSpec, line: &str, peers: usize, index: usize) -> Vec<(Spur, i64)> {
        let spec = InputSpec {
            rel,
            source: line,
            peers,
            index,
        };
        let mut rows = Vec::new();
        ingest::<LineReader<'_, _>, (Spur, i64), _, _>(&spec, |t| rows.push(t), |_| {})
            .expect("cursor");
        rows
    }

    /// One put line has exactly one owner across the workers.
    #[test]
    fn put_has_exactly_one_owner() {
        let rel = text_rel(false, ShardKey::Str);
        let owners: usize = (0..4).map(|i| put_line(&rel, "alpha,1", 4, i).len()).sum();
        assert_eq!(owners, 1);
    }

    /// Under ord, worker 0 owns every put: one worker interning in command
    /// order is deterministic where racing workers are not.
    #[test]
    fn put_under_ord_is_owned_by_worker_zero() {
        let rel = text_rel(true, ShardKey::Str);
        for index in 0..4 {
            let rows = put_line(&rel, "alpha,1", 4, index);
            assert_eq!(rows.len(), usize::from(index == 0), "worker {index}");
        }
    }

    /// Cells of a put line are trimmed like file cells.
    #[test]
    fn put_cells_are_trimmed() {
        let rel = text_rel(false, ShardKey::Str);
        let rows = put_line(&rel, "  alpha , 1 ", 1, 0);
        assert_eq!(rows, vec![(crate::intern::intern("alpha"), 1)]);
    }

    // --- The generated faces, through the macro ---

    type Ts = u32;
    type Diff = i32;

    static MIXED: RelationSpec = RelationSpec {
        name: "Mixed",
        arity: 4,
        delim: b',',
        format: Format::Text {
            delim: b',',
            has_header: false,
        },
        shard: ShardKey::Int,
        uses_ord: false,
    };

    crate::relation!(RelMixed, Ts, Diff, (i32, Spur, bool, OrderedFloat<f64>));

    impl Ingest for RelMixed {
        type Ts = Ts;
        type Diff = Diff;
        type Tuple = (i32, Spur, bool, OrderedFloat<f64>);
        type Rows = (i32, String, bool, f64);

        fn spec(&self) -> &'static RelationSpec {
            &MIXED
        }

        fn session(&mut self) -> &mut Session<Ts, Self::Tuple, Diff> {
            &mut self.session
        }
    }

    /// `load_rows` is the typed source's entry point: each worker decodes
    /// its share into its session without error.
    #[test]
    fn load_vec_shares_and_decodes_host_tuples() {
        let rows: Vec<(i32, String, bool, f64)> =
            (0..7).map(|i| (i, format!("h{i}"), true, 0.5)).collect();
        for index in 0..3 {
            RelMixed::new(InputSession::new()).load_vec(&rows, 1, 3, index);
        }
    }

    /// A handler carries its own lifecycle, so a driver holding one never
    /// reaches past it to the session.
    #[test]
    fn a_handler_drives_its_own_lifecycle() {
        let mut rel = RelMixed::new(InputSession::new());
        rel.load_line("1,a,true,2.5", 1, 1, 0);
        rel.advance_to(1);
        rel.flush();
        rel.close();
    }

    /// A relation declaring no `.fact` rows inherits an inline step that
    /// does nothing, so codegen overrides it only where there are rows.
    #[test]
    fn inline_facts_default_to_none() {
        RelMixed::new(InputSession::new()).inline_facts(0);
    }
}
