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
//! the presence reduce, `flowlog_lattice_leave`) needs every pair of times
//! comparable. [`LexLoop`] orders the same pairs lexicographically, so
//! those operators apply inside a presence loop. [`OnceLoop`] is total
//! already: its outer time has one value.

use timely::order::Product;

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

/// The time of a static or append loop in an engine whose epochs advance:
/// an `(epoch, iteration)` pair ordered epoch first, then iteration, as
/// timely orders every tuple. Its path summary is a pair too, so a loop's
/// feedback edge is `(0, 1)`: one iteration, same epoch.
///
/// Iteration `i` of epoch `e` accumulates every update of every earlier
/// epoch, whatever its iteration, so each epoch's fixpoint resumes from the
/// last one. That is correct only for a loop whose collections never lose a
/// row. A loop over collections with deletions must use [`EpochLoop`]
/// instead, where a later epoch recomputes each iteration.
pub type LexLoop = (Epoch, Iteration);

#[cfg(test)]
mod tests {
    use differential_dataflow::lattice::Lattice;
    use rstest::rstest;
    use timely::order::PartialOrder;
    use timely::progress::PathSummary;
    use timely::progress::Timestamp;
    use timely::progress::timestamp::Refines;

    use super::*;

    /// Times compare epoch first, so a later epoch follows every iteration
    /// of an earlier one, the pair `Product` leaves incomparable.
    #[rstest]
    #[case((0, 5), (1, 1), true)]
    #[case((1, 1), (0, 5), false)]
    #[case((1, 1), (1, 2), true)]
    #[case((1, 2), (1, 1), false)]
    #[case((1, 1), (1, 1), true)]
    fn times_compare_epoch_first(
        #[case] earlier: LexLoop,
        #[case] later: LexLoop,
        #[case] expected: bool,
    ) {
        assert_eq!(earlier.less_equal(&later), expected);
    }

    /// Two times join to the later one and meet at the earlier one. `Product`
    /// would join `(0, 5)` and `(1, 1)` to `(1, 5)` instead.
    // Cases: time, other, join, meet.
    #[rstest]
    #[case((0, 5), (1, 1), (1, 1), (0, 5))]
    #[case((1, 2), (1, 1), (1, 2), (1, 1))]
    fn join_and_meet_follow_the_order(
        #[case] time: LexLoop,
        #[case] other: LexLoop,
        #[case] join: LexLoop,
        #[case] meet: LexLoop,
    ) {
        assert_eq!(time.join(&other), join);
        assert_eq!(other.join(&time), join);
        assert_eq!(time.meet(&other), meet);
        assert_eq!(other.meet(&time), meet);
    }

    /// A summary advances each coordinate by its own, and a path whose
    /// iteration would overflow leads nowhere.
    #[rstest]
    #[case((0, 1), (3, 4), Some((3, 5)))]
    #[case((1, 0), (3, 4), Some((4, 4)))]
    #[case((0, 1), (3, Iteration::MAX), None)]
    fn a_summary_advances_each_coordinate(
        #[case] summary: <LexLoop as Timestamp>::Summary,
        #[case] time: LexLoop,
        #[case] expected: Option<LexLoop>,
    ) {
        assert_eq!(summary.results_in(&time), expected);
    }

    /// Two summaries followed one after the other advance a time as their
    /// sum does.
    #[test]
    fn followed_summaries_add() {
        let next_iteration: <LexLoop as Timestamp>::Summary = (0, 1);
        let twice = PathSummary::<LexLoop>::followed_by(&next_iteration, &next_iteration)
            .expect("no overflow");
        let start: LexLoop = (2, 0);
        assert_eq!(twice.results_in(&start), Some((2, 2)));
    }

    /// Entering the loop starts at iteration 0 of the epoch, and leaving it
    /// drops the iteration.
    #[test]
    fn entering_and_leaving_round_trips_the_epoch() {
        assert_eq!(<LexLoop as Refines<Epoch>>::to_inner(7), (7, 0));
        assert_eq!(<LexLoop as Refines<Epoch>>::to_outer((7, 3)), 7);
    }

    /// Outside the loop, a path summary keeps only its epoch: the feedback
    /// edge advances no epoch.
    #[test]
    fn the_feedback_edge_summarizes_to_no_epoch() {
        assert_eq!(<LexLoop as Refines<Epoch>>::summarize((0, 1)), 0);
        assert_eq!(<LexLoop as Refines<Epoch>>::summarize((2, 9)), 2);
    }
}
