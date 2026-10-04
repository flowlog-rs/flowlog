//! Timestamps, at two levels.
//!
//! The engine's outer time is program-wide: [`Once`] for an engine that
//! runs once, when every input is static, and [`Epoch`] for one whose
//! epochs advance. A loop's time refines it with an [`Iteration`], ordered
//! by the loop's mutability:
//!
//! | engine | static or append loop | mutable loop |
//! |---|---|---|
//! | [`Once`] | [`OnceLoop`] | none |
//! | [`Epoch`] | [`LexLoop`] | [`EpochLoop`] |
//!
//! [`EpochLoop`] orders `(epoch, iteration)` pairs only when both
//! coordinates agree, and every total-order operator (the presence dedup,
//! the presence reduce, `flowlog_reduce_leave`) needs every pair of times
//! comparable. [`LexLoop`] orders the same pairs lexicographically, so
//! those operators apply inside a presence loop. [`OnceLoop`] is total
//! already: its outer time has one value.

use differential_dataflow::lattice::Lattice;
use serde::Deserialize;
use serde::Serialize;
use timely::order::PartialOrder;
use timely::order::Product;
use timely::order::TotalOrder;
use timely::progress::PathSummary;
use timely::progress::Timestamp;
use timely::progress::timestamp::Refines;

// =============================================================================
// Engine and loop times
// =============================================================================

/// The outer time of an engine that runs once.
pub type Once = ();

/// The outer time of an engine whose epochs advance, one per transaction.
pub type Epoch = u32;

/// A loop's iteration counter.
pub type Iteration = u16;

/// The time of a static loop in an engine that runs once.
pub type OnceLoop = Product<Once, Iteration>;

/// The time of a mutable loop, ordered only when both coordinates agree.
pub type EpochLoop = Product<Epoch, Iteration>;

// =============================================================================
// LexLoop
// =============================================================================

/// The time of a static or append loop in an engine whose epochs advance:
/// an `(epoch, iteration)` pair ordered epoch first, then iteration.
///
/// Iteration `i` of epoch `e` accumulates every update of every earlier
/// epoch, whatever its iteration, so each epoch's fixpoint resumes from the
/// last one. That is correct only for a loop whose collections never lose a
/// row. A loop over collections with deletions must use [`EpochLoop`]
/// instead, where a later epoch recomputes each iteration.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct LexLoop {
    epoch: Epoch,
    iteration: Iteration,
}

impl LexLoop {
    /// The feedback edge of a loop, as a summary: one iteration, same epoch.
    pub const NEXT_ITERATION: Self = Self {
        epoch: 0,
        iteration: 1,
    };
}

impl PartialOrder for LexLoop {
    #[inline]
    fn less_equal(&self, other: &Self) -> bool {
        self <= other
    }
}

impl TotalOrder for LexLoop {}

/// A `LexLoop` also summarizes a path, as timely's integer times summarize
/// their own: a path advances a time by the summary's epoch and iteration.
impl Timestamp for LexLoop {
    type Summary = Self;

    fn minimum() -> Self {
        Self::default()
    }
}

impl PathSummary<LexLoop> for LexLoop {
    fn results_in(&self, time: &LexLoop) -> Option<LexLoop> {
        Some(Self {
            epoch: time.epoch.checked_add(self.epoch)?,
            iteration: time.iteration.checked_add(self.iteration)?,
        })
    }

    fn followed_by(&self, other: &Self) -> Option<Self> {
        self.results_in(other)
    }
}

impl Refines<Epoch> for LexLoop {
    fn to_inner(epoch: Epoch) -> Self {
        Self {
            epoch,
            iteration: 0,
        }
    }

    fn to_outer(self) -> Epoch {
        self.epoch
    }

    fn summarize(path: Self) -> Epoch {
        path.epoch
    }
}

impl Lattice for LexLoop {
    #[inline]
    fn join(&self, other: &Self) -> Self {
        *self.max(other)
    }

    #[inline]
    fn meet(&self, other: &Self) -> Self {
        *self.min(other)
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    fn at(epoch: Epoch, iteration: Iteration) -> LexLoop {
        LexLoop { epoch, iteration }
    }

    /// Times compare epoch first, so a later epoch follows every iteration
    /// of an earlier one, the pair `Product` leaves incomparable.
    #[rstest]
    #[case(at(0, 5), at(1, 1), true)]
    #[case(at(1, 1), at(0, 5), false)]
    #[case(at(1, 1), at(1, 2), true)]
    #[case(at(1, 2), at(1, 1), false)]
    #[case(at(1, 1), at(1, 1), true)]
    fn times_compare_epoch_first(
        #[case] earlier: LexLoop,
        #[case] later: LexLoop,
        #[case] expected: bool,
    ) {
        assert_eq!(earlier.less_equal(&later), expected);
    }

    /// A summary advances each coordinate by its own, and a path whose
    /// iteration would overflow leads nowhere.
    #[rstest]
    #[case(LexLoop::NEXT_ITERATION, at(3, 4), Some(at(3, 5)))]
    #[case(at(1, 0), at(3, 4), Some(at(4, 4)))]
    #[case(LexLoop::NEXT_ITERATION, at(3, Iteration::MAX), None)]
    fn a_summary_advances_each_coordinate(
        #[case] summary: LexLoop,
        #[case] time: LexLoop,
        #[case] expected: Option<LexLoop>,
    ) {
        assert_eq!(summary.results_in(&time), expected);
    }

    /// Two summaries followed one after the other advance a time as their
    /// sum does.
    #[test]
    fn followed_summaries_add() {
        let twice = LexLoop::NEXT_ITERATION
            .followed_by(&LexLoop::NEXT_ITERATION)
            .expect("no overflow");
        assert_eq!(twice.results_in(&at(2, 0)), Some(at(2, 2)));
    }

    /// Entering the loop starts at iteration 0 of the epoch, and leaving it
    /// drops the iteration.
    #[test]
    fn entering_and_leaving_round_trips_the_epoch() {
        assert_eq!(<LexLoop as Refines<Epoch>>::to_inner(7), at(7, 0));
        assert_eq!(<LexLoop as Refines<Epoch>>::to_outer(at(7, 3)), 7);
    }

    /// Outside the loop, a path summary keeps only its epoch: the feedback
    /// edge advances no epoch.
    #[test]
    fn the_feedback_edge_summarizes_to_no_epoch() {
        assert_eq!(
            <LexLoop as Refines<Epoch>>::summarize(LexLoop::NEXT_ITERATION),
            0
        );
        assert_eq!(<LexLoop as Refines<Epoch>>::summarize(at(2, 9)), 2);
    }
}
