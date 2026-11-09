//! Lookups: a row probes an arrangement and is answered once the
//! arrangement's frontier lets it. [`Version`] defines which updates a
//! row sees and when it is ready; [`Reader`] holds the trace history that
//! pending and future rows need. [`flowlog_lookup`] answers unarranged
//! rows with the values they match, and
//! [`lookup_absent`](absent::lookup_absent) keeps an arrangement's rows
//! whose key a static arrangement lacks.

mod absent;
mod found;

use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use differential_dataflow::trace::cursor::CursorList;
use differential_dataflow::trace::cursor::cursor_list;
use timely::progress::Antichain;
use timely::progress::Timestamp;
use timely::progress::frontier::AntichainRef;

pub(crate) use self::absent::lookup_absent;
pub use self::found::flowlog_lookup;
use crate::time::TotalClock;

// =============================================================================
// Version
// =============================================================================

/// Which updates of the arrangement a row sees: [`New`] or [`Old`], a
/// token [`flowlog_lookup`] takes so that the choice is made at compile
/// time.
///
/// A join of two changing collections is the sum of exactly one lookup
/// per side, one reading the other side as of the row's time and the
/// other as of just before it, so that every pair of updates is matched
/// by exactly one of them: the lookup driven by the later update, the
/// first among equals. The pairing needs every two times ordered, so
/// [`Old`] runs only on a clock that orders its times totally; [`New`]
/// runs on any clock.
pub trait Version<T: Timestamp>: Copy + sealed::Version + 'static {
    /// Returns `true` if a row at `row` sees an update at `update`. A row
    /// that sees an update sees every update at or before that one too.
    #[must_use]
    fn sees(self, update: &T, row: &T) -> bool;

    /// Returns a time at or before `row` to which the arrangement may
    /// compact while a row at `row` is pending: compacting there moves no
    /// update across what the row sees. The time never falls as `row`
    /// rises.
    #[must_use]
    fn compaction(self, row: &T) -> T;

    /// Returns `true` if a row at `row` sees no update still to come, the
    /// arrangement's frontier being `frontier`.
    #[must_use]
    fn ready(self, frontier: AntichainRef<'_, T>, row: &T) -> bool {
        // An update still to come is at or after a time of the frontier,
        // so a row that sees the update sees that time too.
        !frontier.iter().any(|time| self.sees(time, row))
    }
}

/// Updates at times up to and including the row's.
#[derive(Debug, Clone, Copy)]
pub struct New;

impl<T: Timestamp> Version<T> for New {
    fn sees(self, update: &T, row: &T) -> bool {
        update.less_equal(row)
    }

    fn compaction(self, row: &T) -> T {
        // Compaction keeps how an update compares with every time at or
        // after the bound, the row's included.
        row.clone()
    }
}

/// Updates at times strictly before the row's.
///
/// A mutable loop's clock orders its times only partly, so `Old` does not
/// run there:
///
/// ```compile_fail,E0277
/// use differential_dataflow::VecCollection;
/// use flowlog_runtime::diff;
/// use flowlog_runtime::operators::Old;
/// use flowlog_runtime::operators::flowlog_arrange;
/// use flowlog_runtime::operators::flowlog_lookup;
/// use flowlog_runtime::time::EpochLoop;
///
/// fn lookup<'scope>(
///     rows: VecCollection<'scope, EpochLoop, (u64, u64), diff::Mutable>,
///     values: VecCollection<'scope, EpochLoop, (u64, u64), diff::Mutable>,
/// ) {
///     let values = flowlog_arrange(values, "Values");
///     flowlog_lookup(rows, values, "Lookup", Old, |_, _, _| None::<u64>);
/// }
/// ```
#[derive(Debug, Clone, Copy)]
pub struct Old;

impl<T: TotalClock> Version<T> for Old {
    fn sees(self, update: &T, row: &T) -> bool {
        update.less_than(row)
    }

    fn compaction(self, row: &T) -> T {
        // On a total order an update is before the row exactly when it is
        // at or before the time just before it, which compaction there
        // keeps. A row at the minimum sees nothing.
        row.previous().unwrap_or_else(T::minimum)
    }
}

mod sealed {
    pub trait Version {}

    impl Version for super::New {}
    impl Version for super::Old {}
}

// =============================================================================
// Reader
// =============================================================================

/// The arrangement's trace, read whole and compacted only as far as the
/// lookup allows.
#[derive(Debug)]
struct Reader<Tr> {
    /// `None` once released, when no row is pending or still to come.
    trace: Option<Tr>,
}

impl<Tr: TraceReader<Batch: Navigable>> Reader<Tr> {
    #[must_use]
    fn new(mut trace: Tr) -> Self {
        // Only logical compaction decides answers; merging batches
        // physically never changes one.
        trace.set_physical_compaction(Antichain::new().borrow());
        Self { trace: Some(trace) }
    }

    /// Returns a cursor over every batch of the trace, with the batches it
    /// reads. Step its values rather than seek them: differential 0.26's
    /// `CursorList::seek_val` forwards to every batch, including one whose
    /// keys are exhausted, which indexes past that batch's offsets. See
    /// `src/trace/cursor/cursor_list.rs` at
    /// <https://github.com/TimelyDataflow/differential-dataflow/tree/e7c2470>.
    ///
    /// # Panics
    ///
    /// Panics if the trace is released or cannot supply its complete history.
    #[must_use]
    fn cursor(&mut self) -> (CursorList<BatchCursor<Tr>>, Vec<Tr::Batch>) {
        let trace = self
            .trace
            .as_mut()
            .expect("the trace is held while rows are pending");
        let batches = trace
            .batches_through(Antichain::new().borrow())
            .expect("a trace yields every batch through the empty frontier");
        cursor_list(batches)
    }

    /// Lets the trace compact as far as `version` allows rows at `times`,
    /// the times of every row pending or still to come, and releases it
    /// once there are none.
    ///
    /// Each time must be at or after one passed before, so that the bound,
    /// which rises with the row, never falls.
    fn advance<'time, V: Version<Tr::Time>>(
        &mut self,
        version: V,
        times: impl Iterator<Item = &'time Tr::Time>,
    ) where
        Tr::Time: 'time,
    {
        let bound: Antichain<_> = times.map(|time| version.compaction(time)).collect();
        if bound.is_empty() {
            self.trace = None;
        } else if let Some(trace) = self.trace.as_mut() {
            trace.set_logical_compaction(bound.borrow());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;

    use differential_dataflow::lattice::Lattice;
    use rstest::rstest;

    use super::*;
    use crate::time::EpochLoop;
    use crate::time::OnceLoop;

    // Cases: version, update, row, visible.
    #[rstest]
    #[case(New, OnceLoop::new((), 2), OnceLoop::new((), 2), true)]
    #[case(New, OnceLoop::new((), 1), OnceLoop::new((), 2), true)]
    #[case(New, OnceLoop::new((), 3), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 2), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 1), OnceLoop::new((), 2), true)]
    #[case(Old, OnceLoop::new((), 3), OnceLoop::new((), 2), false)]
    #[case(Old, OnceLoop::new((), 0), OnceLoop::new((), 0), false)]
    #[case(New, (), (), true)]
    #[case(New, EpochLoop::new(0, 1), EpochLoop::new(1, 0), false)]
    #[case(New, EpochLoop::new(1, 0), EpochLoop::new(0, 1), false)]
    fn a_version_sees_only_visible_updates<T: Timestamp, V: Version<T>>(
        #[case] version: V,
        #[case] update: T,
        #[case] row: T,
        #[case] expected: bool,
    ) {
        assert_eq!(version.sees(&update, &row), expected);
    }

    // Cases: version, frontier, row, ready.
    #[rstest]
    #[case(New, vec![1_u32], 2, false)]
    #[case(Old, vec![1_u32], 2, false)]
    #[case(New, vec![2_u32], 2, false)]
    #[case(Old, vec![2_u32], 2, true)]
    #[case(New, vec![3_u32], 2, true)]
    #[case(Old, vec![3_u32], 2, true)]
    #[case(New, vec![], 2_u32, true)]
    #[case(Old, vec![], 2_u32, true)]
    #[case(New, vec![EpochLoop::new(1, 0)], EpochLoop::new(0, 5), true)]
    #[case(New, vec![EpochLoop::new(0, 3), EpochLoop::new(1, 0)], EpochLoop::new(0, 5), false)]
    fn a_row_is_ready_once_no_future_update_is_visible<T: Timestamp, V: Version<T>>(
        #[case] version: V,
        #[case] frontier: Vec<T>,
        #[case] row: T,
        #[case] expected: bool,
    ) {
        assert_eq!(version.ready(AntichainRef::new(&frontier), &row), expected);
    }

    /// Every time of a small grid of each clock, as `(epoch, iteration)`
    /// pairs for the clocks that have both.
    #[must_use]
    fn grid<T>(make: impl Fn(u16, u16) -> T) -> Vec<T> {
        (0..4)
            .flat_map(|e| (0..4).map(move |i| (e, i)))
            .map(|(e, i)| make(e, i))
            .collect()
    }

    /// Compacting an update to a pending row's bound, as differential does
    /// by joining it with the bound, keeps what the row sees of it, on
    /// every clock; and a later row's bound is no earlier.
    // Cases: version, clock samples.
    #[rstest]
    #[case(New, grid(|_, _| ()))]
    #[case(New, grid(|epoch, iteration| u32::from(epoch) * 4 + u32::from(iteration)))]
    #[case(Old, grid(|epoch, iteration| u32::from(epoch) * 4 + u32::from(iteration)))]
    #[case(New, grid(|_, iteration| OnceLoop::new((), iteration)))]
    #[case(Old, grid(|_, iteration| OnceLoop::new((), iteration)))]
    #[case(New, grid(|epoch, iteration| (u32::from(epoch), iteration)))]
    #[case(Old, grid(|epoch, iteration| (u32::from(epoch), iteration)))]
    #[case(New, grid(|epoch, iteration| EpochLoop::new(u32::from(epoch), iteration)))]
    fn the_compaction_bound_keeps_answers_and_rises_with_the_row<
        T: Timestamp + Lattice,
        V: Version<T> + Debug,
    >(
        #[case] version: V,
        #[case] times: Vec<T>,
    ) {
        for row in &times {
            let bound = version.compaction(row);
            assert!(
                bound.less_equal(row),
                "{version:?} bound {bound:?} after {row:?}"
            );
            for update in &times {
                assert_eq!(
                    version.sees(&update.join(&bound), row),
                    version.sees(update, row),
                    "{version:?} of {update:?} from {row:?}",
                );
            }
            let later_times = times.iter().filter(|later| row.less_equal(later));
            for later in later_times {
                assert!(
                    bound.less_equal(&version.compaction(later)),
                    "{version:?} bound falls from {row:?} to {later:?}",
                );
            }
        }
    }
}
