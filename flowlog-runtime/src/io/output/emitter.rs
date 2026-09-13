//! Shared output collection and consumption for one relation.
//!
//! [`Emitter`] owns published batches and independent size reports.
//! [`Worker`] buffers inspected updates locally and publishes at an engine
//! boundary. Writers consume the same batches as typed library results.

use std::cell::RefCell;
use std::convert::Infallible;
use std::fmt;
use std::fmt::Debug;
use std::io;
use std::path::Path;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use crate::io::Relation;
use crate::io::output::sort;
use crate::io::output::writer::Writer;
use crate::io::output::writer::file::FileWriter;
use crate::io::output::writer::host::HostResult;
use crate::io::output::writer::host::HostWriter;
use crate::io::output::writer::stdout::StdoutWriter;

type Updates<D, T> = Vec<(D, T, i32)>;

// =============================================================================
// Emitter
// =============================================================================

/// Collects and emits one relation independently of engine scheduling.
///
/// Clones share published updates and counts. Every worker must publish after
/// its dataflow has completed the requested time. The engine must then exclude
/// publication until consumption finishes; this type does not advance epochs
/// or wait on probes or barriers.
///
/// # Panics
///
/// Operations on shared state panic if a previous operation poisoned its lock.
pub struct Emitter<R: Relation, T> {
    shared: Arc<Mutex<Shared<R::Tuple, T>>>,
}

impl<R: Relation, T: Default> Emitter<R, T> {
    /// Starts with no rows and a zero count at the default timestamp.
    pub fn new() -> Self {
        Self {
            shared: Arc::new(Mutex::new(Shared {
                published: Vec::new(),
                spare: Vec::new(),
                size: (T::default(), 0),
            })),
        }
    }
}

impl<R: Relation, T> Emitter<R, T> {
    /// Creates a worker-local producer. Clone it for an inspection closure.
    pub fn worker(&self) -> Worker<R, T> {
        Worker {
            emitter: self.clone(),
            local: Rc::new(RefCell::new(Vec::new())),
        }
    }

    /// Stores the latest consolidated count and its timestamp independently
    /// of retained rows. An epoch with no count event leaves this value intact.
    pub fn record_size(&self, time: &T, size: i32)
    where
        T: Clone,
    {
        self.shared().size = (time.clone(), size);
    }

    /// Returns the latest batch count, clamping a negative count to zero.
    pub fn batch_size(&self) -> usize {
        self.shared().size.1.max(0) as usize
    }

    /// Returns the latest incremental count without folding it into a snapshot.
    pub fn delta_size(&self) -> i32 {
        self.shared().size.1
    }

    /// Emits the latest count on stdout without consuming or retaining rows.
    pub fn emit_size(&self) -> io::Result<()>
    where
        T: Debug,
    {
        let shared = self.shared();
        StdoutWriter::write_size(R::NAME, &shared.size.0, shared.size.1)
    }

    /// Consumes published updates into the inferred host result.
    ///
    /// Batch vectors follow declared ordering and limits. `INCREMENTAL`
    /// vectors hold `(row, weight)` pairs in publication order, bypassing
    /// ordering and limits. Owned fields move into the returned vector.
    /// Nullary results are a `bool` presence flag in batch mode and an `i32`
    /// net weight in incremental mode; neither uses ordering or limits.
    pub fn emit_host<const INCREMENTAL: bool, S: HostResult>(&self) -> S
    where
        HostWriter<S>: Writer<R, T, INCREMENTAL, Output = S, Error = Infallible>,
    {
        let mut shared = self.shared();
        let ordered = !INCREMENTAL && R::ARITY > 0;
        let capacity = if R::ARITY == 0 {
            0
        } else {
            let rows = shared.published.iter().map(Vec::len).sum();
            if ordered && R::ORDERED {
                R::LIMIT.map_or(rows, |limit| limit.min(rows))
            } else {
                rows
            }
        };
        Self::emit::<_, INCREMENTAL>(
            &mut shared,
            ordered,
            HostWriter::new(S::with_capacity(capacity)),
        )
        .unwrap_or_else(|never| match never {})
    }

    /// Writes published rows to stdout in declared output order, retaining
    /// timestamps and signed weights. A sink error discards remaining rows.
    pub fn emit_stdout(&self) -> io::Result<()>
    where
        StdoutWriter: Writer<R, T, Output = (), Error = io::Error>,
    {
        Self::emit::<_, false>(&mut self.shared(), true, StdoutWriter::new(R::NAME))
    }

    /// Creates or truncates a file and consumes published rows into it,
    /// separating columns with [`Relation::OUTPUT_DELIMITER`].
    ///
    /// Incremental rows include signed weights. Unordered, non-nullary output
    /// formats bounded waves in parallel. A write or flush error may leave a
    /// partial file; a create error leaves the published rows untouched.
    pub fn emit_file<const INCREMENTAL: bool>(&self, path: &Path) -> io::Result<()>
    where
        FileWriter: Writer<R, T, INCREMENTAL, Output = (), Error = io::Error>,
    {
        let writer = FileWriter::create(path, R::OUTPUT_DELIMITER)?;
        Self::emit::<_, INCREMENTAL>(&mut self.shared(), true, writer)
    }

    fn shared(&self) -> MutexGuard<'_, Shared<R::Tuple, T>> {
        self.shared.lock().expect("output buffer poisoned")
    }

    fn emit<W, const INCREMENTAL: bool>(
        shared: &mut Shared<R::Tuple, T>,
        ordered: bool,
        mut writer: W,
    ) -> Result<W::Output, W::Error>
    where
        W: Writer<R, T, INCREMENTAL>,
    {
        if ordered && R::ORDERED {
            Self::drain(shared, |row| writer.write_row(row))?;
        } else {
            let result = writer.write_batch(&mut shared.published);
            shared.recycle();
            result?;
        }
        writer.finish()
    }

    #[inline]
    fn drain<E>(
        shared: &mut Shared<R::Tuple, T>,
        mut sink: impl FnMut((R::Tuple, T, i32)) -> Result<(), E>,
    ) -> Result<(), E> {
        let result = (|| {
            if R::ORDERED {
                let compare =
                    |a: &(R::Tuple, T, i32), b: &(R::Tuple, T, i32)| R::compare(&a.0, &b.0);
                if let Some(limit) = R::LIMIT {
                    if limit == 0 {
                        return Ok(());
                    }
                    // Keep top-k's consuming flatten: retaining empty worker
                    // allocations beside its full merged vector doubles the
                    // tuple storage. Other drains recycle worker buffers.
                    let rows = shared.published.drain(..).flatten().collect();
                    for row in sort::topk(rows, limit, compare) {
                        sink(row)?;
                    }
                } else {
                    for rows in &mut shared.published {
                        rows.sort_by(compare);
                    }
                    sort::drain_merge(&mut shared.published, compare, sink)?;
                }
            } else {
                for rows in &mut shared.published {
                    rows.drain(..).try_for_each(&mut sink)?;
                }
            }
            Ok(())
        })();
        shared.recycle();
        result
    }
}

impl<R: Relation, T: Default> Default for Emitter<R, T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<R: Relation, T> Clone for Emitter<R, T> {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl<R: Relation, T> Debug for Emitter<R, T> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("Emitter")
            .field("relation", &R::NAME)
            .finish_non_exhaustive()
    }
}

// =============================================================================
// Worker
// =============================================================================

/// Buffers one worker's updates locally; clones share that same local buffer.
///
/// # Panics
///
/// Publishing or reclaiming storage panics if shared state is poisoned.
pub struct Worker<R: Relation, T> {
    emitter: Emitter<R, T>,
    local: Rc<RefCell<Updates<R::Tuple, T>>>,
}

impl<R: Relation, T> Worker<R, T> {
    /// Copies an inspected update into local storage. Shared storage is
    /// accessed only when local storage has no capacity to reuse.
    #[inline]
    pub fn record(&self, row: &R::Tuple, time: &T, diff: i32)
    where
        T: Clone,
    {
        let mut local = self.local.borrow_mut();
        if local.capacity() == 0
            && let Some(spare) = self.emitter.shared().spare.pop()
        {
            *local = spare;
        }
        local.push((row.clone(), time.clone(), diff));
    }

    /// Publishes this batch without copying rows. Repeated calls publish
    /// empty batches until more updates arrive.
    pub fn publish(&self) {
        self.emitter.shared().published.push(self.local.take());
    }
}

impl<R: Relation, T> Clone for Worker<R, T> {
    fn clone(&self) -> Self {
        Self {
            emitter: self.emitter.clone(),
            local: Rc::clone(&self.local),
        }
    }
}

impl<R: Relation, T> Debug for Worker<R, T> {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("Worker")
            .field("relation", &R::NAME)
            .finish_non_exhaustive()
    }
}

// =============================================================================
// Shared
// =============================================================================

#[derive(Debug)]
struct Shared<D, T> {
    published: Vec<Updates<D, T>>,
    spare: Vec<Updates<D, T>>,
    size: (T, i32),
}

impl<D, T> Shared<D, T> {
    fn recycle(&mut self) {
        for mut rows in self.published.drain(..) {
            rows.clear();
            if rows.capacity() > 0 {
                self.spare.push(rows);
            }
        }
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cmp::Ordering;
    use std::fs;

    use rstest::rstest;

    use super::*;

    struct Numbers<const ORDERED: bool = false, const LIMIT: usize = { usize::MAX }>;

    impl<const ORDERED: bool, const LIMIT: usize> Relation for Numbers<ORDERED, LIMIT> {
        const NAME: &'static str = "Numbers";
        const ARITY: usize = 1;
        const ORDERED: bool = ORDERED;
        const LIMIT: Option<usize> = if LIMIT == usize::MAX {
            None
        } else {
            Some(LIMIT)
        };
        type Tuple = (i32,);
    }

    struct Flag;

    impl Relation for Flag {
        const NAME: &'static str = "Flag";
        const ARITY: usize = 0;
        type Tuple = ();
    }

    #[test]
    fn updates_are_visible_only_after_publication_and_consumed_once() {
        let emitter = Emitter::<Numbers, u32>::new();
        let worker = emitter.worker();
        let inspector = worker.clone();
        inspector.record(&(7,), &4, -2);
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), []);
        worker.publish();
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), [((7,), -2)]);
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), []);
        worker.publish();
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), []);
    }

    /// Host results omit timestamps and stdout cannot accept a test sink.
    #[test]
    fn worker_publication_preserves_timestamps_weights_and_row_order() {
        let emitter = Emitter::<Numbers, u32>::new();
        for (value, time, weight) in [(9, 3, -2), (2, 4, 0), (7, 4, 3)] {
            let other = emitter.clone();
            std::thread::spawn(move || {
                let worker = other.worker();
                worker.record(&(value,), &time, weight);
                worker.publish();
            })
            .join()
            .unwrap();
        }
        let mut rows = Vec::new();
        Emitter::<Numbers, u32>::drain(&mut emitter.shared(), |row| {
            rows.push(row);
            Ok::<_, Infallible>(())
        })
        .expect("drain rows");
        assert_eq!(rows, [((9,), 3, -2), ((2,), 4, 0), ((7,), 4, 3)]);
    }

    #[test]
    fn ordered_snapshots_merge_worker_partitions() {
        let emitter = Emitter::<Numbers<true>, u32>::new();
        for rows in [[9, 1, 5], [4, 7, 2]] {
            let worker = emitter.worker();
            for row in rows {
                worker.record(&(row,), &0, 1);
            }
            worker.publish();
        }
        assert_eq!(
            emitter.emit_host::<false, Vec<_>>(),
            [(1,), (2,), (4,), (5,), (7,), (9,)]
        );
    }

    #[test]
    fn equal_order_keys_keep_publication_and_partition_order() {
        struct Keyed;
        impl Relation for Keyed {
            const NAME: &'static str = "Keyed";
            const ARITY: usize = 2;
            const ORDERED: bool = true;
            type Tuple = (i32, i32);
            fn compare(a: &Self::Tuple, b: &Self::Tuple) -> Ordering {
                a.0.cmp(&b.0)
            }
        }
        let emitter = Emitter::<Keyed, u32>::new();
        for rows in [[(2, 10), (1, 11), (1, 12)], [(1, 20), (2, 21), (1, 22)]] {
            let worker = emitter.worker();
            for row in rows {
                worker.record(&row, &0, 1);
            }
            worker.publish();
        }
        assert_eq!(
            emitter.emit_host::<false, Vec<_>>(),
            [(1, 11), (1, 12), (1, 20), (1, 22), (2, 10), (2, 21)]
        );
    }

    #[test]
    fn limit_does_not_change_the_independent_count() {
        let emitter = Emitter::<Numbers<true, 2>, u32>::new();
        let worker = emitter.worker();
        for value in [5, 1, 3] {
            worker.record(&(value,), &0, 1);
        }
        worker.publish();
        emitter.record_size(&0, 3);
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), [(1,), (3,)]);
        assert_eq!(emitter.batch_size(), 3);
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), []);
    }

    /// Output values cannot reveal whether worker storage was retained.
    #[test]
    fn zero_limit_discards_rows_and_reuses_worker_storage() {
        let emitter = Emitter::<Numbers<true, 0>, u32>::new();
        let worker = emitter.worker();
        worker.record(&(8,), &0, 1);
        let storage = (
            worker.local.borrow().as_ptr(),
            worker.local.borrow().capacity(),
        );
        worker.publish();
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), []);
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), []);
        worker.record(&(9,), &1, -1);
        assert_eq!(
            (
                worker.local.borrow().as_ptr(),
                worker.local.borrow().capacity()
            ),
            storage,
        );
        worker.publish();
        assert_eq!(emitter.emit_host::<true, Vec<_>>(), [((9,), -1)]);
    }

    #[test]
    fn incremental_library_deltas_bypass_ordering_and_limit() {
        let emitter = Emitter::<Numbers<true, 1>, u32>::new();
        let worker = emitter.worker();
        for (row, diff) in [(9, -1), (2, 3), (9, 1)] {
            worker.record(&(row,), &6, diff);
        }
        worker.publish();
        assert_eq!(
            emitter.emit_host::<true, Vec<_>>(),
            [((9,), -1), ((2,), 3), ((9,), 1)]
        );
    }

    /// Output values cannot reveal whether worker storage was retained.
    #[rstest]
    #[case(false)]
    #[case(true)]
    fn completed_drains_reuse_worker_allocations(#[case] ordered: bool) {
        fn check<const ORDERED: bool>() {
            let emitter = Emitter::<Numbers<ORDERED>, u32>::new();
            let worker = emitter.worker();
            for row in 0..64 {
                worker.record(&(row,), &0, 1);
            }
            let pointer = worker.local.borrow().as_ptr();
            let capacity = worker.local.borrow().capacity();
            worker.publish();
            assert_eq!(emitter.emit_host::<false, Vec<_>>().len(), 64);
            worker.record(&(65,), &1, -1);
            assert_eq!(worker.local.borrow().as_ptr(), pointer);
            assert_eq!(worker.local.borrow().capacity(), capacity);
            worker.publish();
            assert_eq!(emitter.emit_host::<true, Vec<_>>(), [((65,), -1)]);
        }
        if ordered {
            check::<true>();
        } else {
            check::<false>();
        }
    }

    /// Inspect's required clone precedes the measured ownership transfer.
    /// The collected string's allocation is only accessible through storage.
    #[test]
    fn owned_strings_move_from_collected_rows_into_library_results() {
        struct Text;
        impl Relation for Text {
            const NAME: &'static str = "Text";
            const ARITY: usize = 1;
            type Tuple = (String,);
        }
        let emitter = Emitter::<Text, u32>::new();
        let worker = emitter.worker();
        worker.record(&("owned".to_owned(),), &0, 1);
        let pointer = worker.local.borrow()[0].0.0.as_ptr();
        worker.publish();
        let result = emitter.emit_host::<false, Vec<_>>();
        assert_eq!(result[0].0, "owned");
        assert_eq!(result[0].0.as_ptr(), pointer);
    }

    /// The scalar result API cannot expose row buffer allocations.
    #[test]
    fn count_only_epochs_do_not_allocate_row_buffers_or_reset_missing_events() {
        let emitter = Emitter::<Numbers, u32>::new();
        assert_eq!(emitter.batch_size(), 0);
        emitter.record_size(&3, -4);
        assert_eq!(emitter.batch_size(), 0);
        assert_eq!(emitter.delta_size(), -4);
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), []);
        assert_eq!(emitter.delta_size(), -4);
        let shared = emitter.shared();
        assert_eq!(shared.size, (3, -4));
        assert_eq!(shared.published.capacity(), 0);
        assert_eq!(shared.spare.capacity(), 0);
    }

    #[test]
    fn nullary_snapshots_and_deltas_use_presence_and_net_weight() {
        let emitter = Emitter::<Flag, u32>::new();
        let worker = emitter.worker();
        assert!(!emitter.emit_host::<false, bool>());
        worker.record(&(), &0, -2);
        worker.publish();
        assert!(emitter.emit_host::<false, bool>());
        assert!(!emitter.emit_host::<false, bool>());
        worker.record(&(), &1, -3);
        worker.record(&(), &1, 1);
        worker.publish();
        assert_eq!(emitter.emit_host::<true, i32>(), -2);
        assert_eq!(emitter.emit_host::<true, i32>(), 0);
    }

    #[test]
    fn nullary_host_results_bypass_ordering_and_limit() {
        #[derive(Debug)]
        struct LimitedFlag;

        impl Relation for LimitedFlag {
            const NAME: &'static str = "LimitedFlag";
            const ARITY: usize = 0;
            const ORDERED: bool = true;
            const LIMIT: Option<usize> = Some(0);
            type Tuple = ();
        }

        let emitter = Emitter::<LimitedFlag, u32>::new();
        let worker = emitter.worker();
        worker.record(&(), &0, 1);
        worker.publish();
        let present: bool = emitter.emit_host::<false, _>();
        assert!(present);

        worker.record(&(), &1, -2);
        worker.record(&(), &1, 1);
        worker.publish();
        let delta: i32 = emitter.emit_host::<true, _>();
        assert_eq!(delta, -1);
    }

    #[rstest]
    #[case(false, "9\t-2\n1\t+3\n")]
    #[case(true, "1\t+3\n9\t-2\n")]
    fn file_output_uses_shared_rows_and_declared_order(
        #[case] ordered: bool,
        #[case] expected: &str,
    ) {
        fn write<const ORDERED: bool>(path: &Path) {
            let emitter = Emitter::<Numbers<ORDERED>, u32>::new();
            let worker = emitter.worker();
            worker.record(&(9,), &4, -2);
            worker.record(&(1,), &4, 3);
            worker.publish();
            emitter.emit_file::<true>(path).unwrap();
            assert_eq!(emitter.emit_host::<true, Vec<_>>(), []);
            worker.record(&(2,), &5, 1);
            worker.publish();
            assert_eq!(emitter.emit_host::<true, Vec<_>>(), [((2,), 1)]);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.tsv");
        if ordered {
            write::<true>(&path);
        } else {
            write::<false>(&path);
        }
        assert_eq!(fs::read_to_string(path).unwrap(), expected);
    }

    #[test]
    fn file_creation_errors_preserve_published_rows() {
        let dir = tempfile::tempdir().unwrap();
        let emitter = Emitter::<Numbers, u32>::new();
        let worker = emitter.worker();
        worker.record(&(8,), &0, 1);
        worker.publish();
        let error = emitter.emit_file::<false>(&dir.path().join("missing/out"));
        assert_eq!(error.unwrap_err().kind(), io::ErrorKind::NotFound);
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), [(8,)]);
    }

    #[test]
    fn file_output_uses_its_relations_output_delimiter() {
        struct Pair;
        impl Relation for Pair {
            const NAME: &'static str = "Pair";
            const ARITY: usize = 2;
            const INPUT_DELIMITER: u8 = b',';
            const OUTPUT_DELIMITER: u8 = b'|';
            type Tuple = (i32, i32);
        }
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("out.txt");
        let emitter = Emitter::<Pair, u32>::new();
        let worker = emitter.worker();
        worker.record(&(7, 8), &0, -2);
        worker.publish();
        emitter.emit_file::<true>(&path).unwrap();
        assert_eq!(fs::read_to_string(path).unwrap(), "7|8|-2\n");
    }

    #[test]
    fn typed_output_does_not_use_text_delimiters() {
        struct Typed;
        impl Relation for Typed {
            const NAME: &'static str = "Typed";
            const ARITY: usize = 1;
            const INPUT_DELIMITER: u8 = 0xFF;
            const OUTPUT_DELIMITER: u8 = 0xFF;
            type Tuple = (String,);
        }
        let emitter = Emitter::<Typed, u32>::new();
        let worker = emitter.worker();
        worker.record(&("a,b|c".to_owned(),), &0, 1);
        worker.publish();
        assert_eq!(
            emitter.emit_host::<false, Vec<_>>(),
            [("a,b|c".to_owned(),)]
        );
        worker.record(&("a,b|c".to_owned(),), &1, -1);
        worker.publish();
        assert_eq!(
            emitter.emit_host::<true, Vec<_>>(),
            [(("a,b|c".to_owned(),), -1)]
        );
    }

    /// The public sinks cannot inject a failure at a chosen row boundary.
    #[rstest]
    #[case(Emitter::<Numbers, u32>::new())]
    #[case(Emitter::<Numbers<true>, u32>::new())]
    #[case(Emitter::<Numbers<true, 1>, u32>::new())]
    fn failed_delivery_discards_rows_without_finishing<const ORDERED: bool, const LIMIT: usize>(
        #[case] emitter: Emitter<Numbers<ORDERED, LIMIT>, u32>,
    ) {
        struct Failing;

        impl<R: Relation, T> Writer<R, T> for Failing {
            type Output = ();
            type Error = &'static str;

            fn write_row(&mut self, _: (R::Tuple, T, i32)) -> Result<(), Self::Error> {
                Err("sink failure")
            }

            fn write_batch(
                &mut self,
                _: &mut [Vec<(R::Tuple, T, i32)>],
            ) -> Result<(), Self::Error> {
                Err("sink failure")
            }

            fn finish(self) -> Result<(), Self::Error> {
                panic!("failed delivery must not finish");
            }
        }

        let worker = emitter.worker();
        worker.record(&(8,), &0, 1);
        worker.record(&(9,), &0, 1);
        worker.publish();
        let result = Emitter::<Numbers<ORDERED, LIMIT>, u32>::emit::<_, false>(
            &mut emitter.shared(),
            true,
            Failing,
        );
        assert_eq!(result, Err("sink failure"));
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), []);
        worker.record(&(7,), &1, 1);
        worker.publish();
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), [(7,)]);
    }

    /// Injecting a completion failure requires an internal writer.
    #[rstest]
    #[case(Emitter::<Numbers, u32>::new())]
    #[case(Emitter::<Numbers<true>, u32>::new())]
    #[case(Emitter::<Numbers<true, 1>, u32>::new())]
    fn failed_completion_does_not_replay_rows<const ORDERED: bool, const LIMIT: usize>(
        #[case] emitter: Emitter<Numbers<ORDERED, LIMIT>, u32>,
    ) {
        struct Failing;

        impl<R: Relation, T> Writer<R, T> for Failing {
            type Output = ();
            type Error = &'static str;

            fn write_row(&mut self, _: (R::Tuple, T, i32)) -> Result<(), Self::Error> {
                Ok(())
            }

            fn write_batch(
                &mut self,
                _: &mut [Vec<(R::Tuple, T, i32)>],
            ) -> Result<(), Self::Error> {
                Ok(())
            }

            fn finish(self) -> Result<(), Self::Error> {
                Err("completion failure")
            }
        }

        let worker = emitter.worker();
        worker.record(&(8,), &0, 1);
        worker.record(&(9,), &0, 1);
        worker.publish();
        let result = Emitter::<Numbers<ORDERED, LIMIT>, u32>::emit::<_, false>(
            &mut emitter.shared(),
            true,
            Failing,
        );
        assert_eq!(result, Err("completion failure"));
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), []);
        worker.record(&(7,), &1, 1);
        worker.publish();
        assert_eq!(emitter.emit_host::<false, Vec<_>>(), [(7,)]);
    }
}
