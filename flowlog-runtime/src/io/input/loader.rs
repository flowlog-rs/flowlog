//! Loading input sources into a relation's dataflow session.
//!
//! [`Loader`] binds a [`Relation`] to its session and worker partition.
//! Readers supply decoded rows; the loader applies weights and error policy.

use std::fs::File;
use std::ops::Neg;
use std::path::Path;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::input::InputSession;
use timely::progress::Timestamp;

use crate::error::RuntimeError;
use crate::io::Relation;
use crate::io::input::decode::Decode;
use crate::io::input::decode::text::TextRow;
use crate::io::input::reader::Reader;
use crate::io::input::reader::file::FileReader;
use crate::io::input::reader::host::HostReader;
use crate::io::input::reader::ingest;
use crate::io::input::reader::put::PutReader;

// =============================================================================
// Loader
// =============================================================================

/// Loads one worker's share of a relation at the session's current timestamp.
///
/// Every worker receives the same sources and uses fixed worker coordinates.
/// `T` and `D` select the timestamp and update-weight types. Batch execution
/// closes the loader after loading; incremental execution advances and
/// flushes the session between epochs.
///
/// # Panics
///
/// Operations on a closed loader panic, except repeated [`close`](Self::close).
pub struct Loader<R: Relation, T: Timestamp, D: Semigroup + 'static> {
    session: Option<InputSession<T, R::Tuple, D>>,
    /// Coordinates used to divide input; `None` disables reading on this
    /// worker.
    partition: Option<(usize, usize)>,
}

impl<R: Relation, T: Timestamp, D: Semigroup + 'static> Loader<R, T, D> {
    /// Binds the session to worker `index` among `peers` workers.
    ///
    /// `peers` must be nonzero and `index < peers`. When the program uses
    /// `ord`, worker 0 reads every source as a lone peer and the others
    /// read nothing, so string interning order is independent of worker
    /// count. Pass the same program-wide `uses_ord` to every loader.
    pub fn new(
        session: InputSession<T, R::Tuple, D>,
        peers: usize,
        index: usize,
        uses_ord: bool,
    ) -> Result<Self, RuntimeError> {
        if peers == 0 || index >= peers {
            return Err(RuntimeError::InvalidWorker { peers, index });
        }
        let partition = match (uses_ord, index) {
            (false, _) => Some((peers, index)),
            (true, 0) => Some((1, 0)),
            (true, _) => None,
        };
        Ok(Self {
            session: Some(session),
            partition,
        })
    }

    /// Loads this worker's share of the file at `path`, applying each row
    /// with weight `diff`.
    ///
    /// `delimiter` must be ASCII. Cells are trimmed; delimiters and line
    /// endings cannot be quoted or escaped. `has_header` skips the first
    /// line of this file, not the first line of every worker's share.
    ///
    /// Rejected rows are reported to stderr and skipped. File-open failures
    /// are reported and load as empty. Metadata and read failures are
    /// returned without rolling back updates already applied.
    pub fn load_file(
        &mut self,
        path: &Path,
        delimiter: u8,
        has_header: bool,
        diff: D,
    ) -> Result<(), RuntimeError>
    where
        R::Tuple: for<'l> Decode<TextRow<'l>>,
    {
        let partition = self.partition;
        let session = self.session();
        let Some((peers, index)) = partition else {
            return Ok(());
        };
        validate_delimiter(delimiter)?;
        let file = match File::open(path) {
            Ok(file) => file,
            Err(error) => {
                let name = R::NAME;
                eprintln!(
                    "[relation][{name}] cannot open {}: {error}; loading as empty",
                    path.display()
                );
                return Ok(());
            }
        };
        let reader = FileReader::open(file, R::ARITY == 0, delimiter, has_header, peers, index)?;
        ingest(
            reader,
            |tuple| session.update(tuple, diff.clone()),
            |error| Self::report_skip(&error, Some(path)),
        )
    }

    /// Applies one `put` on its owning worker with weight `diff`.
    ///
    /// `ordinal` is the operation's index in the transaction. Every worker
    /// must receive the same text and ordinal so exactly one applies it.
    ///
    /// A decoding error is returned with no update applied. Text follows the
    /// delimiter rules of [`load_file`](Self::load_file).
    pub fn load_put(
        &mut self,
        text: &str,
        ordinal: usize,
        delimiter: u8,
        diff: D,
    ) -> Result<(), RuntimeError>
    where
        R::Tuple: for<'l> Decode<TextRow<'l>>,
    {
        let partition = self.partition;
        let session = self.session();
        let Some((peers, index)) = partition else {
            return Ok(());
        };
        validate_delimiter(delimiter)?;
        let Some(mut reader) = PutReader::open(text, ordinal, delimiter, peers, index) else {
            return Ok(());
        };
        if let Some(tuple) = reader.next()? {
            session.update(tuple?, diff);
        }
        Ok(())
    }

    /// Applies a nullary `put` with weight `diff` for true or `-diff` for
    /// false.
    ///
    /// The text is decoded as a standalone boolean. Ownership and error
    /// handling follow [`load_put`](Self::load_put).
    pub fn load_flag(
        &mut self,
        text: &str,
        ordinal: usize,
        delimiter: u8,
        diff: D,
    ) -> Result<(), RuntimeError>
    where
        R: Relation<Tuple = ()>,
        D: Neg<Output = D>,
    {
        let partition = self.partition;
        let session = self.session();
        let Some((peers, index)) = partition else {
            return Ok(());
        };
        validate_delimiter(delimiter)?;
        let Some(mut reader) = PutReader::open(text, ordinal, delimiter, peers, index) else {
            return Ok(());
        };
        if let Some(holds) = Reader::<bool>::next(&mut reader)? {
            session.update((), if holds? { diff } else { -diff });
        }
        Ok(())
    }

    /// Loads this worker's share of typed host rows with weight `diff`.
    ///
    /// Field types select the conversion: integers and booleans keep their
    /// values, strings stay owned or are interned, and floats become
    /// ordered floats. Tuple-valued fields are converted recursively.
    /// Unsupported source and destination pairs fail to compile.
    pub fn load_rows<U>(&mut self, rows: &[U], diff: D) -> Result<(), RuntimeError>
    where
        R::Tuple: Decode<U>,
    {
        let partition = self.partition;
        let session = self.session();
        let Some((peers, index)) = partition else {
            return Ok(());
        };
        let Some(reader) = HostReader::open(rows, peers, index) else {
            return Ok(());
        };
        ingest(
            reader,
            |tuple| session.update(tuple, diff.clone()),
            |error| Self::report_skip(&error, None),
        )
    }

    /// Applies the relation's inline facts on worker zero with weight `one`.
    ///
    /// Each call applies the facts again; this method does not track whether
    /// they have already been loaded.
    pub fn inline_facts(&mut self, one: D) {
        let partition = self.partition;
        let session = self.session();
        if !matches!(partition, Some((_, 0))) {
            return;
        }
        for tuple in R::facts() {
            session.update(tuple, one.clone());
        }
    }

    /// Advances the timestamp for subsequent updates.
    ///
    /// Call [`flush`](Self::flush) before waiting for dataflow progress.
    ///
    /// # Panics
    ///
    /// Panics if the current timestamp is not less than or equal to `t`.
    pub fn advance_to(&mut self, t: T) {
        self.session().advance_to(t);
    }

    /// Sends buffered updates and publishes the session's timestamp.
    pub fn flush(&mut self) {
        self.session().flush();
    }

    /// Flushes pending updates and releases the input handle.
    ///
    /// Repeated calls have no effect. This does not wait for dataflow
    /// completion.
    pub fn close(&mut self) {
        if let Some(session) = self.session.take() {
            session.close();
        }
    }

    /// # Panics
    ///
    /// Panics if the session is already closed.
    fn session(&mut self) -> &mut InputSession<T, R::Tuple, D> {
        self.session
            .as_mut()
            .unwrap_or_else(|| panic!("relation `{}`: loaded after close", R::NAME))
    }

    /// Reports a rejected row with its relation and optional file path.
    fn report_skip(error: &RuntimeError, path: Option<&Path>) {
        let name = R::NAME;
        match path {
            Some(path) => eprintln!(
                "[relation][{name}] {error} in {}; row skipped",
                path.display()
            ),
            None => eprintln!("[relation][{name}] {error}; row skipped"),
        }
    }
}

/// Rejects delimiter bytes that could split a UTF-8 character.
fn validate_delimiter(delimiter: u8) -> Result<(), RuntimeError> {
    if !delimiter.is_ascii() {
        return Err(RuntimeError::InvalidDelimiter { delimiter });
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::rc::Rc;

    use lasso::Spur;
    use ordered_float::OrderedFloat;
    use rstest::rstest;

    use super::*;
    use crate::error::Position;
    use crate::intern::intern;
    use crate::io::input::decode::typed::DecodeField;

    type Ts = u32;
    type Diff = i32;

    struct Numbers;

    impl Relation for Numbers {
        const NAME: &'static str = "Numbers";
        const ARITY: usize = 1;
        type Tuple = (i32,);
    }

    struct Mixed;

    impl Relation for Mixed {
        const NAME: &'static str = "Mixed";
        const ARITY: usize = 4;
        type Tuple = (i32, Spur, bool, OrderedFloat<f64>);
    }

    struct Flagged;

    impl Relation for Flagged {
        const NAME: &'static str = "Flagged";
        const ARITY: usize = 0;
        type Tuple = ();
    }

    struct WithFacts;

    impl Relation for WithFacts {
        const NAME: &'static str = "WithFacts";
        const ARITY: usize = 2;
        type Tuple = (i32, Spur);

        fn facts() -> impl IntoIterator<Item = Self::Tuple> {
            [(1, intern("a")), (2, intern("b"))]
        }
    }

    /// A slot that cannot decode text at all, only host rows: no
    /// `Decode<TextRow>` impl exists for it anywhere.
    #[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Debug, Hash)]
    struct HostOnlySlot {
        a: i32,
        b: Spur,
    }

    impl Decode<(i32, String)> for HostOnlySlot {
        fn decode(record: &(i32, String)) -> Result<Self, RuntimeError> {
            Ok(Self {
                a: i32::decode_field(&record.0),
                b: Spur::decode_field(&record.1),
            })
        }
    }

    struct HostOnly;

    impl Relation for HostOnly {
        const NAME: &'static str = "HostOnly";
        const ARITY: usize = 2;
        type Tuple = HostOnlySlot;
    }

    /// Inspect one loader's updates in a local dataflow. Its input
    /// partition can model any worker without starting the other workers.
    fn deliveries<R>(
        peers: usize,
        index: usize,
        uses_ord: bool,
        drive: impl FnOnce(&mut Loader<R, Ts, Diff>) + Send + Sync + 'static,
    ) -> Vec<(R::Tuple, Diff)>
    where
        R: Relation + 'static,
        R::Tuple: Send,
    {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut session: InputSession<Ts, R::Tuple, Diff> = InputSession::new();
            let probe = worker.dataflow::<Ts, _, _>(|scope| {
                let seen = Rc::clone(&seen);
                // Differential's `probe` hands the collection back beside
                // the handle; only the handle may leave the scope.
                let (probe, _) = session
                    .to_collection(scope)
                    .inspect(move |(tuple, _, diff)| {
                        seen.borrow_mut().push((tuple.clone(), *diff));
                    })
                    .probe();
                probe
            });
            let mut loader =
                Loader::<R, Ts, Diff>::new(session, peers, index, uses_ord).expect("valid worker");
            drive(&mut loader);
            loader.advance_to(1);
            loader.flush();
            worker.step_while(|| probe.less_than(&1));
            loader.close();
            let mut out = seen.take();
            out.sort();
            out
        })
    }

    #[test]
    fn load_rows_decodes_and_applies_with_the_given_weight() {
        let got = deliveries::<Mixed>(1, 0, false, |loader| {
            let rows = vec![
                (1, "a".to_string(), true, 0.5),
                (2, "b".to_string(), false, 1.5),
            ];
            loader.load_rows(&rows, 3).expect("host rows");
        });
        assert_eq!(
            got,
            vec![
                ((1, intern("a"), true, OrderedFloat(0.5)), 3),
                ((2, intern("b"), false, OrderedFloat(1.5)), 3),
            ]
        );
    }

    #[rstest]
    #[case(0, false, vec![((1,), 2), ((3,), -3)])]
    #[case(1, false, vec![((2,), 2), ((4,), -3)])]
    #[case(0, true, vec![((1,), 2), ((2,), 2), ((3,), -3), ((4,), -3)])]
    #[case(1, true, vec![])]
    fn load_rows_partitions_each_batch_with_its_own_weight(
        #[case] index: usize,
        #[case] uses_ord: bool,
        #[case] expected: Vec<((i32,), i32)>,
    ) {
        let got = deliveries::<Numbers>(2, index, uses_ord, |loader| {
            loader.load_rows(&[(1,), (2,)], 2).expect("insert batch");
            loader.load_rows(&[(3,), (4,)], -3).expect("retract batch");
        });
        assert_eq!(got, expected);
    }

    #[rstest]
    #[case(0, &[])]
    #[case(1, &[])]
    #[case(0, &[(7,)])]
    fn an_empty_host_partition_applies_nothing(
        #[case] index: usize,
        #[case] rows: &'static [(i32,)],
    ) {
        let got = deliveries::<Numbers>(2, index, false, move |loader| {
            loader.load_rows(rows, -1).expect("host rows");
        });
        assert!(got.is_empty());
    }

    #[test]
    fn load_file_applies_rows_and_skips_a_refused_one() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, b"1,a,true,0.5\n2,b,maybe,0.5\n3,c,false,1.5\n").expect("write");
        let got = deliveries::<Mixed>(1, 0, false, move |loader| {
            loader.load_file(&path, b',', false, 1).expect("file");
        });
        assert_eq!(
            got,
            vec![
                ((1, intern("a"), true, OrderedFloat(0.5)), 1),
                ((3, intern("c"), false, OrderedFloat(1.5)), 1),
            ]
        );
    }

    #[test]
    fn a_file_open_failure_loads_as_empty() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("missing.csv");
        let updates = deliveries::<Numbers>(1, 0, false, move |loader| {
            loader
                .load_file(&path, b',', false, 1)
                .expect("empty input");
        });
        assert!(updates.is_empty());
    }

    #[rstest]
    #[case(0x80)]
    #[case(0xA9)]
    #[case(0xFF)]
    fn invalid_delimiters_are_rejected_before_opening_a_file(#[case] delimiter: u8) {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("missing.csv");
        let mut loader =
            Loader::<Numbers, Ts, Diff>::new(InputSession::new(), 1, 0, false).expect("loader");
        let error = loader
            .load_file(&path, delimiter, false, 1)
            .expect_err("invalid delimiter");
        assert!(matches!(
            error,
            RuntimeError::InvalidDelimiter { delimiter: actual } if actual == delimiter
        ));
    }

    #[test]
    fn a_file_read_failure_is_returned_without_rolling_back_prior_updates() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, b"1\n\xFF\n").expect("write");
        let updates = deliveries::<Numbers>(1, 0, false, move |loader| {
            let error = loader
                .load_file(&path, b',', false, 1)
                .expect_err("invalid UTF-8");
            assert!(matches!(
                error,
                RuntimeError::NotUtf8 {
                    at: Position::Line(2)
                }
            ));
        });
        assert_eq!(updates, vec![((1,), 1)]);
    }

    #[test]
    fn load_file_uses_the_relations_nullary_arity() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, b"\n").expect("write");
        let got = deliveries::<Flagged>(1, 0, false, move |loader| {
            loader.load_file(&path, b',', false, 1).expect("file");
        });
        assert_eq!(got, vec![((), 1)]);
    }

    #[test]
    fn file_format_is_chosen_per_load() {
        let dir = tempfile::tempdir().expect("temp dir");
        let csv = dir.path().join("rows.csv");
        let tsv = dir.path().join("rows.tsv");
        fs::write(&csv, b"1,a,true,0.5\n").expect("write csv");
        fs::write(&tsv, b"9\theader\ttrue\t9.5\n2\tb\tfalse\t1.5\n").expect("write tsv");

        let got = deliveries::<Mixed>(1, 0, false, move |loader| {
            loader.load_file(&csv, b',', false, 1).expect("csv");
            loader.load_file(&tsv, b'\t', true, 2).expect("tsv");
        });
        assert_eq!(
            got,
            vec![
                ((1, intern("a"), true, OrderedFloat(0.5)), 1),
                ((2, intern("b"), false, OrderedFloat(1.5)), 2),
            ]
        );
    }

    #[rstest]
    #[case(0, false, vec![1, 2])]
    #[case(1, false, vec![3, 4])]
    #[case(0, true, vec![1, 2, 3, 4])]
    #[case(1, true, vec![])]
    fn loads_use_the_worker_settings_from_construction(
        #[case] index: usize,
        #[case] uses_ord: bool,
        #[case] expected: Vec<i32>,
    ) {
        let got = deliveries::<WithFacts>(2, index, uses_ord, |loader| {
            let rows = [
                (1, "a".to_string()),
                (2, "b".to_string()),
                (3, "c".to_string()),
                (4, "d".to_string()),
            ];
            loader.load_rows(&rows, 1).expect("host rows");
        });
        assert_eq!(
            got.into_iter().map(|((n, _), _)| n).collect::<Vec<_>>(),
            expected
        );
    }

    #[test]
    fn ord_loads_the_whole_file_on_worker_zero() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, b"1,a\n2,b\n").expect("write");
        let got = deliveries::<WithFacts>(4, 0, true, move |loader| {
            loader.load_file(&path, b',', false, 1).expect("file");
        });
        assert_eq!(got, vec![((1, intern("a")), 1), ((2, intern("b")), 1)]);
    }

    #[test]
    fn ord_skips_other_workers_before_opening_a_reader() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("rows.csv");
        fs::write(&path, b"\xFF\n\xFF\n").expect("write");
        let got = deliveries::<WithFacts>(2, 1, true, move |loader| {
            // An invalid delimiter fails at open, even for an empty byte range.
            loader
                .load_file(&path, 0xA9, false, 1)
                .expect("no reader opened");
            loader
                .load_put("invalid", 1, 0xA9, 1)
                .expect("no reader opened");
            loader.inline_facts(1);
        });
        assert!(got.is_empty());
    }

    #[test]
    fn ord_applies_every_put_on_worker_zero() {
        let got = deliveries::<WithFacts>(4, 0, true, |loader| {
            loader.load_put("1\ta", 3, b'\t', 1).expect("put");
        });
        assert_eq!(got, vec![((1, intern("a")), 1)]);
    }

    #[rstest]
    #[case(0, 0, false)]
    #[case(0, 0, true)]
    #[case(2, 2, false)]
    #[case(2, 3, true)]
    fn invalid_worker_coordinates_are_rejected(
        #[case] peers: usize,
        #[case] index: usize,
        #[case] uses_ord: bool,
    ) {
        let result = Loader::<Mixed, Ts, Diff>::new(InputSession::new(), peers, index, uses_ord);
        assert!(matches!(result,
            Err(RuntimeError::InvalidWorker { peers: actual_peers, index: actual_index })
                if actual_peers == peers && actual_index == index
        ));
    }

    #[test]
    fn load_put_applies_the_owned_tuple() {
        let got = deliveries::<Mixed>(1, 0, false, |loader| {
            loader.load_put("7,z,true,2.5", 0, b',', -1).expect("put");
        });
        assert_eq!(got, vec![((7, intern("z"), true, OrderedFloat(2.5)), -1)]);
    }

    #[test]
    fn a_refused_put_is_the_calls_error() {
        let got = deliveries::<Mixed>(1, 0, false, |loader| {
            let err = loader
                .load_put("x,z,true,2.5", 0, b',', 1)
                .expect_err("x is not i32");
            assert!(
                matches!(
                    err,
                    RuntimeError::Malformed {
                        at: Position::Put,
                        column: 0,
                        ..
                    }
                ),
                "got: {err}"
            );
        });
        assert!(got.is_empty());
    }

    #[test]
    fn a_non_owner_does_not_decode_a_put() {
        let updates = deliveries::<Numbers>(2, 0, false, |loader| {
            loader
                .load_put("invalid", 1, b',', 1)
                .expect("not this worker's put");
        });
        assert!(updates.is_empty());
    }

    #[rstest]
    #[case(0, false)]
    #[case(1, false)]
    #[case(0, true)]
    #[case(1, true)]
    fn text_puts_validate_delimiters_before_selecting_the_owner(
        #[case] index: usize,
        #[case] flag: bool,
    ) {
        let mut loader =
            Loader::<Flagged, Ts, Diff>::new(InputSession::new(), 2, index, false).expect("loader");
        let result = if flag {
            loader.load_flag("True", 0, 0xA9, 1)
        } else {
            loader.load_put("True", 0, 0xA9, 1)
        };
        assert!(matches!(
            result,
            Err(RuntimeError::InvalidDelimiter { delimiter: 0xA9 })
        ));
    }

    #[test]
    fn load_flag_asserts_on_true_and_retracts_on_false() {
        let got = deliveries::<Flagged>(1, 0, false, |loader| {
            for (ordinal, text) in ["True", " false "].into_iter().enumerate() {
                loader.load_flag(text, ordinal, b',', 1).expect("flag");
            }
        });
        assert_eq!(got, vec![((), -1), ((), 1)]);
    }

    #[test]
    fn load_flag_refuses_any_other_spelling() {
        let got = deliveries::<Flagged>(1, 0, false, |loader| {
            let err = loader
                .load_flag("maybe", 0, b',', 1)
                .expect_err("maybe is not a flag");
            assert!(
                matches!(
                    &err,
                    RuntimeError::Malformed { at: Position::Put, value, expected: "`True` or `False`", .. }
                        if value == "maybe"
                ),
                "got: {err}"
            );
        });
        assert!(got.is_empty());
    }

    #[test]
    fn load_flag_on_a_non_owner_is_a_no_op() {
        let got = deliveries::<Flagged>(2, 0, false, |loader| {
            loader
                .load_flag("maybe", 1, b',', 1)
                .expect("not this worker's put");
        });
        assert!(got.is_empty());
    }

    #[test]
    fn ord_excluded_workers_skip_flag_validation() {
        let updates = deliveries::<Flagged>(2, 1, true, |loader| {
            loader
                .load_flag("invalid", 1, 0xA9, 1)
                .expect("excluded worker");
        });
        assert!(updates.is_empty());
    }

    #[test]
    fn facts_apply_on_worker_zero_only() {
        let on_zero = deliveries::<WithFacts>(2, 0, false, |loader| loader.inline_facts(2));
        assert_eq!(on_zero, vec![((1, intern("a")), 2), ((2, intern("b")), 2)]);

        let on_one = deliveries::<WithFacts>(2, 1, false, |loader| loader.inline_facts(2));
        assert!(on_one.is_empty());
    }

    #[test]
    fn a_slot_that_cannot_decode_text_still_loads_host_rows() {
        let got = deliveries::<HostOnly>(1, 0, false, |loader| {
            let rows = vec![(7, "a".to_string()), (8, "b".to_string())];
            loader.load_rows(&rows, 1).expect("host rows");
        });
        assert_eq!(
            got,
            vec![
                (
                    HostOnlySlot {
                        a: 7,
                        b: intern("a")
                    },
                    1
                ),
                (
                    HostOnlySlot {
                        a: 8,
                        b: intern("b")
                    },
                    1
                ),
            ]
        );
    }

    #[test]
    fn batch_close_flushes_pending_updates_and_releases_the_input() {
        timely::execute_directly(|worker| {
            let updates = Rc::new(RefCell::new(Vec::new()));
            let mut session = InputSession::<Ts, (i32,), Diff>::new();
            let probe = worker.dataflow::<Ts, _, _>(|scope| {
                let updates = Rc::clone(&updates);
                let (probe, _) = session
                    .to_collection(scope)
                    .inspect(move |update| updates.borrow_mut().push(*update))
                    .probe();
                probe
            });
            let mut loader =
                Loader::<Numbers, Ts, Diff>::new(session, 1, 0, false).expect("loader");
            loader.load_rows(&[(7,)], 3).expect("rows");
            loader.close();
            worker.step_while(|| !probe.done());
            assert_eq!(*updates.borrow(), vec![((7,), 0, 3)]);
        });
    }

    #[test]
    fn incremental_updates_use_the_current_epoch_and_publish_on_flush() {
        timely::execute_directly(|worker| {
            let updates = Rc::new(RefCell::new(Vec::new()));
            let mut session = InputSession::<Ts, (i32,), Diff>::new();
            let probe = worker.dataflow::<Ts, _, _>(|scope| {
                let updates = Rc::clone(&updates);
                let (probe, _) = session
                    .to_collection(scope)
                    .inspect(move |update| updates.borrow_mut().push(*update))
                    .probe();
                probe
            });
            let mut loader =
                Loader::<Numbers, Ts, Diff>::new(session, 1, 0, false).expect("loader");
            loader.load_rows(&[(7,)], 1).expect("initial rows");
            loader.advance_to(1);
            loader.flush();
            worker.step_while(|| probe.less_than(&1));
            assert_eq!(*updates.borrow(), vec![((7,), 0, 1)]);

            loader.load_rows(&[(7,)], -1).expect("retraction");
            loader.load_rows(&[(8,)], 1).expect("insertion");
            loader.advance_to(2);
            loader.flush();
            worker.step_while(|| probe.less_than(&2));
            assert_eq!(
                *updates.borrow(),
                vec![((7,), 0, 1), ((7,), 1, -1), ((8,), 1, 1)]
            );
            loader.close();
        });
    }

    #[test]
    fn close_is_idempotent() {
        let mut loader =
            Loader::<Mixed, Ts, Diff>::new(InputSession::new(), 1, 0, false).expect("valid worker");
        loader.close();
        loader.close();
    }

    #[rstest]
    #[case(0, false)]
    #[case(1, true)]
    #[should_panic(expected = "relation `Mixed`: loaded after close")]
    fn a_closed_loader_refuses_to_load_even_when_excluded(
        #[case] index: usize,
        #[case] uses_ord: bool,
    ) {
        let mut loader = Loader::<Mixed, Ts, Diff>::new(InputSession::new(), 2, index, uses_ord)
            .expect("valid worker");
        loader.close();
        loader.inline_facts(1);
    }
}
