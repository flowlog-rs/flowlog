//! The fold that turns an aggregate's arranged matches into weights. A
//! match adds its contribution to its key's weight when it appears and
//! subtracts it when it disappears, so a key's weights add up to its
//! group's reduction. It handles presence and signed matches alike.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::VecCollection;
use differential_dataflow::consolidation::consolidate;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::trace::BatchCursor;
use differential_dataflow::trace::Cursor;
use differential_dataflow::trace::Navigable;
use differential_dataflow::trace::TraceReader;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::order::TotalOrder;

use super::semiring::Semiring;
use crate::diff;
use crate::diff::Weight;

/// Folds arranged `matches` into their keys' weights. A match is present
/// while its accumulated count is above zero, however large the count.
/// It emits its contribution when it becomes present and the negated
/// contribution when it stops being present. A presence count never
/// decreases, so a presence match contributes once, at the first time
/// it appears. Within each batch, a key's contributions at one time are
/// emitted as one weight.
///
/// Signed matches can disappear, so they need an `S` that is
/// [`Semiring::INVERTIBLE`].
///
/// # Panics
///
/// During execution, panics if the trace cannot supply history at the
/// processed batch boundary, or if a match disappears and `S` has no
/// negation.
#[must_use]
pub(in crate::operators) fn fold<'scope, Tr, K, V, S>(
    matches: Arranged<'scope, Tr>,
    name: &str,
    contribute: impl Fn(&V) -> S + 'static,
) -> VecCollection<'scope, Tr::Time, K, S>
where
    Tr: TraceReader<Time: TotalOrder, Batch: Navigable> + 'static,
    for<'a> BatchCursor<Tr>:
        Cursor<Key<'a> = &'a K, Val<'a> = &'a V, Time = Tr::Time, Diff: Weight>,
    K: Data,
    V: Data,
    S: Semiring,
{
    // Reuse the matches' trace instead of arranging rows again for dedup.
    // This saves an exchange, a sort, and a second trace, at the cost of
    // probing the retained history batches for each match.
    let Arranged { stream, mut trace } = matches;
    stream
        .unary(Pipeline, name, move |_, _| {
            let mut changes: Vec<(Tr::Time, diff::Mutable)> = Vec::new();
            let mut contributions: Vec<(Tr::Time, S)> = Vec::new();
            move |input, output| {
                input.for_each(|capability, spans| {
                    let mut session = output.session(&capability);
                    for span in spans.drain(..) {
                        if let Some(batch) = &span.inner {
                            let history = trace
                                .batches_through(span.lower().borrow())
                                .expect("physical compaction stops at the processed boundary");
                            // Differential 0.26's CursorList::seek_val also
                            // seeks exhausted batches and can index past
                            // their offsets. Separate cursors avoid that
                            // panic but require probing each batch here.
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
                                    let mut count = 0;
                                    for (cursor, storage) in &mut prior {
                                        if cursor.get_key(storage) == Some(key) {
                                            cursor.seek_val(storage, value);
                                            if cursor.get_val(storage) == Some(value) {
                                                count += total(cursor, storage);
                                            }
                                        }
                                    }
                                    read_changes(&mut current, batch, &mut changes);
                                    let ordered = changes.drain(..);
                                    for (time, change) in ordered {
                                        let present = count > 0;
                                        count += change;
                                        let weight = match (present, count > 0) {
                                            (false, true) => contribute(value),
                                            (true, false) => contribute(value).negate(),
                                            (false, false) | (true, true) => continue,
                                        };
                                        // Matches mostly share a time, so a
                                        // run of them adds up in place.
                                        match contributions.last_mut() {
                                            Some((last, sum)) if *last == time => {
                                                sum.plus_equals(&weight);
                                            }
                                            Some(_) | None => contributions.push((time, weight)),
                                        }
                                    }
                                    current.step_val(batch);
                                }
                                // Preserve value order within each batch;
                                // later trace merges can still regroup float
                                // sums and change their rounding.
                                consolidate(&mut contributions);
                                let weights = contributions.drain(..);
                                for (time, weight) in weights {
                                    session.give((key.clone(), time, weight));
                                }
                                current.step_key(batch);
                            }
                        }
                        // Keep history readable until this span is processed;
                        // the next span only needs it through this upper bound.
                        trace.set_logical_compaction(span.upper().borrow());
                        trace.set_physical_compaction(span.upper().borrow());
                    }
                });
            }
        })
        .as_collection()
}

/// Returns the count of the cursor's current value, added up over all
/// of its updates.
#[must_use]
fn total<C: Cursor<Diff: Weight>>(cursor: &mut C, storage: &C::Storage) -> diff::Mutable {
    let mut count = 0;
    cursor.map_times(storage, |_, weight| count += C::owned_diff(weight).count());
    count
}

/// Appends the updates of the cursor's current value to `changes` as
/// counts, then sorts `changes` by time with one entry per time.
fn read_changes<C>(
    cursor: &mut C,
    storage: &C::Storage,
    changes: &mut Vec<(C::Time, diff::Mutable)>,
) where
    C: Cursor<Diff: Weight, Time: TotalOrder>,
{
    cursor.map_times(storage, |time, weight| {
        changes.push((C::owned_time(time), C::owned_diff(weight).count()));
    });
    consolidate(changes);
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::ExchangeData;
    use differential_dataflow::input::Input;
    use rstest::rstest;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::operators::reduce::semiring::Counted;
    use crate::operators::reduce::semiring::SumAccumulator;

    #[derive(Debug)]
    struct Update {
        key: u8,
        value: i64,
        time: u32,
    }

    /// Runs `fold` over `rounds` of weighted updates to the matches and
    /// returns the weights it emits. Each round is arranged as its own
    /// span before the next one is sent.
    fn fold_rounds<R, S>(
        rounds: Vec<Vec<(Update, R)>>,
        contribute: fn(&i64) -> S,
    ) -> Vec<(u8, u32, S)>
    where
        R: Weight + Sync,
        S: Semiring + ExchangeData,
    {
        timely::execute_directly(move |worker| {
            // The 'static inspect closure and the test both need the results.
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, matches) = scope.new_collection::<(u8, i64), R>();
                let seen = Rc::clone(&seen);
                fold(matches.arrange_by_key(), "Fold", contribute)
                    .inspect(move |(key, time, weight)| {
                        seen.borrow_mut().push((*key, *time, weight.clone()));
                    })
                    .probe_with(&probe);
                input
            });
            for round in rounds {
                let upper = round.iter().map(|(update, _)| update.time + 1).max();
                let upper = upper.expect("a round has a match");
                for (Update { key, value, time }, weight) in round {
                    input.update_at((key, value), time, weight);
                }
                input.advance_to(upper);
                input.flush();
                worker.step_while(|| probe.less_than(&upper));
            }
            input.close();
            while worker.step() {}
            let mut weights = seen.take();
            weights.sort();
            weights
        })
    }

    /// Runs `fold` over `rounds` of presence matches and returns the
    /// sums it emits.
    fn fold_sums(rounds: Vec<Vec<Update>>) -> Vec<(u8, u32, i64)> {
        let rounds = rounds
            .into_iter()
            .map(|round| {
                round
                    .into_iter()
                    .map(|update| (update, diff::Static))
                    .collect()
            })
            .collect();
        fold_rounds(rounds, |value| SumAccumulator::lift(*value))
            .into_iter()
            .map(|(key, time, sum)| (key, time, sum.finish()))
            .collect()
    }

    /// The weight of `count` matches that add up to `sum`.
    fn counted(count: i64, sum: i64) -> Counted<SumAccumulator<i64>> {
        Counted::new(count, SumAccumulator::lift(sum))
    }

    /// Equal times must combine even when separated in value order.
    #[test]
    fn a_keys_matches_at_one_time_fold_into_one_weight() {
        let rounds = vec![vec![
            Update {
                key: 1,
                value: 10,
                time: 1,
            },
            Update {
                key: 1,
                value: 20,
                time: 0,
            },
            Update {
                key: 1,
                value: 30,
                time: 1,
            },
            Update {
                key: 2,
                value: 5,
                time: 0,
            },
            Update {
                key: 2,
                value: 6,
                time: 0,
            },
        ]];
        assert_eq!(fold_sums(rounds), vec![(1, 0, 20), (1, 1, 40), (2, 0, 11)]);
    }

    // Cases: rounds, sums.
    #[rstest]
    #[case::no_matches(vec![], vec![])]
    #[case::again_in_a_later_span(
        vec![
            vec![Update { key: 1, value: 10, time: 0 }],
            vec![
                Update { key: 1, value: 10, time: 1 },
                Update { key: 1, value: 20, time: 1 },
            ],
        ],
        vec![(1, 0, 10), (1, 1, 20)],
    )]
    #[case::around_an_earlier_match(
        vec![
            vec![Update { key: 1, value: 20, time: 0 }],
            vec![
                Update { key: 1, value: 10, time: 1 },
                Update { key: 1, value: 20, time: 1 },
                Update { key: 1, value: 30, time: 1 },
            ],
        ],
        vec![(1, 0, 20), (1, 1, 40)],
    )]
    #[case::twice_in_one_span(
        vec![vec![
            Update { key: 1, value: 10, time: 1 },
            Update { key: 1, value: 10, time: 0 },
            Update { key: 1, value: 20, time: 1 },
        ]],
        vec![(1, 0, 10), (1, 1, 20)],
    )]
    #[case::past_a_span_without_its_key(
        vec![
            vec![Update { key: 1, value: 10, time: 0 }],
            vec![Update { key: 2, value: 20, time: 1 }],
            vec![
                Update { key: 2, value: 20, time: 2 },
                Update { key: 2, value: 30, time: 2 },
            ],
        ],
        vec![(1, 0, 10), (2, 1, 20), (2, 2, 30)],
    )]
    fn a_distinct_match_contributes_once_at_its_first_time(
        #[case] rounds: Vec<Vec<Update>>,
        #[case] expected: Vec<(u8, u32, i64)>,
    ) {
        assert_eq!(fold_sums(rounds), expected);
    }

    /// A signed match contributes once while its count is positive,
    /// whatever the count, and its contribution is removed when the count
    /// returns to zero. How the changes split across spans does not
    /// matter.
    // Cases: rounds of signed changes, counted weights.
    #[rstest]
    #[case::a_repeated_match_adds_nothing(
        vec![
            vec![(Update { key: 1, value: 10, time: 0 }, 2)],
            vec![(Update { key: 1, value: 10, time: 1 }, 1)],
        ],
        vec![(1, 0, counted(1, 10))],
    )]
    #[case::a_partial_delete_keeps_the_match(
        vec![
            vec![(Update { key: 1, value: 10, time: 0 }, 2)],
            vec![(Update { key: 1, value: 10, time: 1 }, -1)],
        ],
        vec![(1, 0, counted(1, 10))],
    )]
    #[case::a_match_leaves_at_count_zero(
        vec![
            vec![
                (Update { key: 1, value: 10, time: 0 }, 1),
                (Update { key: 1, value: 20, time: 0 }, 1),
            ],
            vec![(Update { key: 1, value: 10, time: 1 }, -1)],
        ],
        vec![(1, 0, counted(2, 30)), (1, 1, counted(-1, -10))],
    )]
    #[case::a_deleted_match_returns(
        vec![
            vec![(Update { key: 1, value: 10, time: 0 }, 1)],
            vec![(Update { key: 1, value: 10, time: 1 }, -1)],
            vec![(Update { key: 1, value: 10, time: 2 }, 1)],
        ],
        vec![(1, 0, counted(1, 10)), (1, 1, counted(-1, -10)), (1, 2, counted(1, 10))],
    )]
    #[case::in_and_out_in_one_span(
        vec![vec![
            (Update { key: 1, value: 10, time: 1 }, -1),
            (Update { key: 1, value: 10, time: 0 }, 1),
        ]],
        vec![(1, 0, counted(1, 10)), (1, 1, counted(-1, -10))],
    )]
    #[case::a_swap_at_one_time(
        vec![
            vec![(Update { key: 1, value: 10, time: 0 }, 1)],
            vec![
                (Update { key: 1, value: 10, time: 1 }, -1),
                (Update { key: 1, value: 20, time: 1 }, 1),
            ],
        ],
        vec![(1, 0, counted(1, 10)), (1, 1, counted(0, 10))],
    )]
    #[case::a_count_spread_over_history(
        vec![
            vec![(Update { key: 1, value: 10, time: 0 }, 1)],
            vec![(Update { key: 1, value: 10, time: 1 }, 1)],
            vec![(Update { key: 1, value: 10, time: 2 }, -1)],
            vec![(Update { key: 2, value: 5, time: 3 }, 1)],
            vec![(Update { key: 1, value: 10, time: 4 }, -1)],
        ],
        vec![(1, 0, counted(1, 10)), (1, 4, counted(-1, -10)), (2, 3, counted(1, 5))],
    )]
    fn a_signed_match_contributes_while_present(
        #[case] rounds: Vec<Vec<(Update, diff::Mutable)>>,
        #[case] expected: Vec<(u8, u32, Counted<SumAccumulator<i64>>)>,
    ) {
        let contribute = |value: &i64| counted(1, *value);
        assert_eq!(fold_rounds(rounds, contribute), expected);
    }
}
