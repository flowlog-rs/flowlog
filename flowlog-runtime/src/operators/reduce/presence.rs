//! `diff::Static` building blocks: contributions accumulated as semiring
//! weights, which can only add answers.
//!
//! - [`fold`] reads an aggregate's arranged matches into weighted keys,
//!   each distinct match once; [`lift`] maps a lattice's rows to them.
//! - [`settle`] turns the weights into answers as they first appear or
//!   supersede the one before.
//! - [`lower`] maps the settled weights back to rows.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::ThresholdTotal;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::TotalOrder;
use timely::progress::Timestamp;

use super::semiring::Semiring;
use crate::diff;
use crate::diff::Presence;
use crate::operators::map::flowlog_map;

/// Folds arranged `matches` into their keys' weights: a distinct match
/// contributes once, at the first time it appears, and a key's
/// contributions at one time arrive as one weight.
pub(in crate::operators) fn fold<'scope, Tr, K, V, S>(
    matches: Arranged<'scope, Tr>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
) -> VecCollection<'scope, Tr::Time, K, S>
where
    Tr: TraceReader<Time: TotalOrder, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>:
        Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = Tr::Time, Diff: Presence>,
    K: Data,
    V: Data,
    S: Semiring,
{
    // Reading the batches in place, rather than back as rows to dedup,
    // spares an exchange and a sort of every match, and a second trace
    // of them. The matches' own trace is the history a match is looked up
    // in, as in `first_occurrences`.
    let Arranged { stream, mut trace } = matches;
    stream
        .unary(Pipeline, name, move |_, _| {
            // One key's contributions, reused across keys.
            let mut contributions: Vec<(Tr::Time, S)> = Vec::new();
            move |input, output| {
                input.for_each(|capability, spans| {
                    let mut session = output.session(&capability);
                    for span in spans.drain(..) {
                        if let Some(batch) = &span.inner {
                            let history = trace
                                .batches_through(span.lower().borrow())
                                .expect("physical compaction stops at the processed boundary");
                            // A cursor per batch, as the append `SetRewrite`
                            // probes them: `CursorList::seek_val` forwards to
                            // every batch, including one whose keys ran out,
                            // and indexes past that batch's offsets.
                            let mut prior: Vec<_> = history
                                .iter()
                                .map(|batch| (batch.cursor(), batch))
                                .collect();
                            let mut current = batch.cursor();
                            while let Some(key) = current.get_key(batch) {
                                for (cursor, storage) in &mut prior {
                                    cursor.seek_key(storage, key);
                                }
                                while let Some(value) = current.get_val(batch) {
                                    // A presence never cancels: a match the
                                    // history holds has contributed already.
                                    let seen = prior.iter_mut().any(|(cursor, storage)| {
                                        cursor.get_key(storage) == Some(key) && {
                                            cursor.seek_val(storage, value);
                                            cursor.get_val(storage) == Some(value)
                                        }
                                    });
                                    if !seen {
                                        let first = first_time(&mut current, batch);
                                        let weight = contribute(value);
                                        // Matches mostly share a time, so a
                                        // run of them adds up in place.
                                        match contributions.last_mut() {
                                            Some((time, sum)) if *time == first => {
                                                sum.plus_equals(&weight);
                                            }
                                            _ => contributions.push((first, weight)),
                                        }
                                    }
                                    current.step_val(batch);
                                }
                                // Runs and the stable sort both follow the
                                // cursor, so a float sum adds up the same way
                                // on every run.
                                consolidate(&mut contributions);
                                for (time, weight) in contributions.drain(..) {
                                    session.give((key.clone(), time, weight));
                                }
                                current.step_key(batch);
                            }
                        }
                        trace.set_logical_compaction(span.upper().borrow());
                        trace.set_physical_compaction(span.upper().borrow());
                    }
                });
            }
        })
        .as_collection()
}

/// The least time of the value `cursor` is at, which must have one.
fn first_time<C: Cursor>(cursor: &mut C, storage: &C::Storage) -> C::Time {
    let mut first: Option<C::Time> = None;
    cursor.map_times(storage, |time, _| {
        let time = C::owned_time(time);
        if first.as_ref().is_none_or(|first| time < *first) {
            first = Some(time);
        }
    });
    first.expect("an arranged value has an update")
}

/// Converts each row to a group key weighted by its contribution.
pub(in crate::operators) fn lift<'scope, T, D, K, V, S>(
    rows: VecCollection<'scope, T, D, diff::Static>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
    mut split: impl FnMut(D) -> (K, V) + 'static,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp,
    D: Data,
    K: Data,
    S: Semiring,
{
    flowlog_map(rows, name, move |row, time, _| {
        let (key, value) = split(row);
        std::iter::once((key, time, contribute(&value)))
    })
}

/// Settles weighted keys into answers: each group's accumulated weight,
/// emitted when it first appears and whenever it supersedes the answer
/// before.
pub(in crate::operators) fn settle<'scope, T, K, S>(
    weights: VecCollection<'scope, T, K, S>,
) -> VecCollection<'scope, T, K, S>
where
    T: Timestamp + TotalOrder + Lattice,
    K: ExchangeData + Hashable,
    S: Semiring + ExchangeData,
{
    // Unchanged weights carry no new answer. Suppressing them avoids
    // restating a contribution downstream at later timestamps.
    weights.threshold_semigroup(|_key, new, current| match current {
        Some(current) => new.supersedes(current).then(|| new.clone()),
        None => (!new.is_zero()).then(|| new.clone()),
    })
}

/// Converts settled weights back into output rows with `diff::Static`.
pub(in crate::operators) fn lower<'scope, T, K, S, O>(
    collection: VecCollection<'scope, T, K, S>,
    name: &str,
    mut merge: impl FnMut(K, S::Value) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Static>
where
    T: Timestamp,
    K: Data,
    S: Semiring,
    O: Data,
{
    flowlog_map(collection, name, move |key, time, accumulated| {
        std::iter::once((merge(key, accumulated.finish()), time, diff::Static))
    })
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::operators::reduce::semiring::SumAccumulator;

    /// One match, its group key and summed value, at an epoch.
    type Update = ((u8, i64), u32);

    /// The sums `fold` answers for `rounds` of matches: each round is
    /// arranged as one span before the next arrives.
    fn fold_sums(rounds: Vec<Vec<Update>>) -> Vec<(u8, u32, i64)> {
        timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, matches) = scope.new_collection::<(u8, i64), diff::Static>();
                let seen = Rc::clone(&seen);
                fold(matches.arrange_by_key(), "Fold", |value: &i64| {
                    SumAccumulator::lift(*value)
                })
                .inspect(move |&(key, time, sum)| {
                    seen.borrow_mut().push((key, time, sum.finish()));
                })
                .probe_with(&probe);
                input
            });
            for round in rounds {
                let upper = round.iter().map(|&(_, time)| time + 1).max();
                let upper = upper.expect("a round has a match");
                for (row, time) in round {
                    input.update_at(row, time, diff::Static);
                }
                input.advance_to(upper);
                input.flush();
                worker.step_while(|| probe.less_than(&upper));
            }
            input.close();
            while worker.step() {}
            let mut sums = seen.take();
            sums.sort();
            sums
        })
    }

    /// A key's matches at one time arrive as one weight, adjacent in the
    /// key's values or not.
    #[test]
    fn a_keys_matches_at_one_time_fold_into_one_weight() {
        let rounds = vec![vec![
            ((1, 10), 1),
            ((1, 20), 0),
            ((1, 30), 1),
            ((2, 5), 0),
            ((2, 6), 0),
        ]];
        assert_eq!(fold_sums(rounds), vec![(1, 0, 20), (1, 1, 40), (2, 0, 11)]);
    }

    /// A distinct match contributes once, at the first time it appears,
    /// whether it appears again in a later span or within its own.
    // Cases: rounds, sums.
    #[rstest]
    #[case::again_in_a_later_span(
        vec![vec![((1, 10), 0)], vec![((1, 10), 1), ((1, 20), 1)]],
        vec![(1, 0, 10), (1, 1, 20)],
    )]
    #[case::around_an_earlier_match(
        vec![vec![((1, 20), 0)], vec![((1, 10), 1), ((1, 20), 1), ((1, 30), 1)]],
        vec![(1, 0, 20), (1, 1, 40)],
    )]
    #[case::twice_in_one_span(
        vec![vec![((1, 10), 1), ((1, 10), 0), ((1, 20), 1)]],
        vec![(1, 0, 10), (1, 1, 20)],
    )]
    #[case::past_a_span_without_its_key(
        vec![
            vec![((1, 10), 0)],
            vec![((2, 20), 1)],
            vec![((2, 20), 2), ((2, 30), 2)],
        ],
        vec![(1, 0, 10), (2, 1, 20), (2, 2, 30)],
    )]
    fn a_distinct_match_contributes_once_at_its_first_time(
        #[case] rounds: Vec<Vec<Update>>,
        #[case] expected: Vec<(u8, u32, i64)>,
    ) {
        assert_eq!(fold_sums(rounds), expected);
    }
}
