//! A loop's recursive collection, [`flowlog_variable`], fed back by
//! [`Variable::set`]: differential's `Variable`, except that a row still
//! changing in the last iteration a loop can count is reported, not lost.
//!
//! A loop counts its iterations up to [`LAST_ITERATION`], so a row that
//! changes in it has no next iteration to reach. Differential's variable
//! drops such a row without a word: a recursion that never reaches its
//! fixpoint, such as `Nat(n + 1) :- Nat(n)`, would end with a truncated
//! result and a clean exit. This one records the run's [`Failure`] as well,
//! which the drivers check once the epoch drains, so a truncated result is
//! never reported. Differential's variable also lets rows reach the
//! counter's top, which in an incremental engine shuts a loop down while
//! later epochs are still to come: timely then panics.

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::ConnectLoop;
use timely::dataflow::operators::Feedback;
use timely::dataflow::operators::feedback::Handle;
use timely::dataflow::operators::generic::Operator;

use crate::Failure;
use crate::time::LAST_ITERATION;
use crate::time::LoopClock;

// =============================================================================
// Variable
// =============================================================================

/// The open feedback edge of a recursive relation, which [`Variable::set`]
/// closes with the relation's rows.
pub struct Variable<'scope, T: LoopClock, D: 'static, R: 'static> {
    feedback: Handle<'scope, T, Vec<(D, T, R)>>,
    relation: &'static str,
    failure: Failure,
}

/// Creates the recursive collection of `relation` in the loop `scope`:
/// empty until its [`Variable`] is set, and then the rows it is set to, one
/// iteration later. A row that cannot advance is recorded in `failure`.
pub fn flowlog_variable<'scope, T, D, R>(
    scope: Scope<'scope, T>,
    relation: &'static str,
    failure: &Failure,
) -> (Variable<'scope, T, D, R>, VecCollection<'scope, T, D, R>)
where
    T: LoopClock,
    D: Clone + 'static,
    R: Clone + 'static,
{
    let (feedback, rows) = scope.feedback(T::next_iteration());
    let variable = Variable {
        feedback,
        relation,
        failure: failure.clone(),
    };
    (variable, rows.as_collection())
}

impl<'scope, T, D, R> Variable<'scope, T, D, R>
where
    T: LoopClock,
    D: Clone + 'static,
    R: Clone + 'static,
{
    /// Feeds `next` back as the relation's rows, one iteration later.
    ///
    /// A single pass advances each row's time in place, as differential's
    /// `results_in` does, so the loop has the operators differential's
    /// would. A row moves on to its time's [`LoopClock::successor`], so no
    /// time in the loop passes [`LAST_ITERATION`]. A row in the last
    /// iteration has none and is dropped, but its worker records the
    /// failure first, once. It records before it releases the row's time,
    /// so every worker sees the failure once that time drains.
    ///
    /// The feedback edge advances each message's stamp, not its rows, so a
    /// stamp in the last iteration would pass it too. A message stamped
    /// there goes out stamped with its remaining rows' own times instead.
    pub fn set(self, next: VecCollection<'scope, T, D, R>) {
        let Self {
            feedback,
            relation,
            failure,
        } = self;
        let mut reported = false;
        next.inner
            .unary(Pipeline, "ResultsIn", move |_, _| {
                move |input, output| {
                    input.for_each(|capability, rows| {
                        // Only a lexicographic clock lets a message stamped
                        // in the last iteration carry rows that can move
                        // on: those of a later epoch.
                        let restamp = capability
                            .stamp()
                            .iter()
                            .any(|time| time.successor().is_none());
                        let mut stamp = CapabilitySet::new();
                        rows.retain_mut(|(_, time, _)| match time.successor() {
                            Some(later) => {
                                if restamp {
                                    stamp.insert(capability.delayed(time, 0));
                                }
                                *time = later;
                                true
                            }
                            None => {
                                if !reported {
                                    reported = true;
                                    failure.record(unconverged(relation));
                                }
                                false
                            }
                        });
                        if restamp {
                            output.session(&stamp).give_container(rows);
                        } else {
                            output.session(&capability).give_container(rows);
                        }
                    });
                }
            })
            .connect_loop(feedback);
    }
}

/// The failure of a recursion through `relation` that still changed in the
/// last iteration.
fn unconverged(relation: &str) -> String {
    format!(
        "recursion through `{relation}` did not reach a fixpoint: it still changed \
         in iteration {LAST_ITERATION}, the last a loop can count"
    )
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use differential_dataflow::input::Input;
    use timely::Config;
    use timely::dataflow::operators::Inspect;
    use timely::dataflow::operators::core::ToStream;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;

    use super::*;
    use crate::diff;
    use crate::operators::flowlog_arrange;
    use crate::operators::flowlog_dedup;
    use crate::operators::flowlog_join;
    use crate::operators::flowlog_map;
    use crate::time::Epoch;
    use crate::time::Iteration;
    use crate::time::LexLoop;
    use crate::time::OnceLoop;

    /// Runs `Nat(n + 1) :- Nat(n), n < 2` from `Nat(0)`, set at iteration
    /// `first` rather than 0 so that a test reaches the last iteration in
    /// a few. Returns each fed-back row with its iteration, and the failure.
    fn count_to_two(first: Iteration) -> (Vec<(u32, Iteration)>, Failure) {
        let failure = Failure::new();
        let fed = Arc::new(Mutex::new(Vec::new()));
        let (shared, sink) = (failure.clone(), Arc::clone(&fed));
        timely::execute(Config::thread(), move |worker| {
            let sink = Arc::clone(&sink);
            worker.dataflow::<(), _, _>(|scope| {
                let zero: VecCollection<'_, (), u32, diff::Static> =
                    Some((0_u32, (), diff::Static))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                scope.scoped::<OnceLoop, _, _>("Iterative", |inner| {
                    let (variable, nat) = flowlog_variable(inner, "Nat", &shared);
                    let seed = flowlog_map(zero.enter(inner), "Late", move |n, _, weight| {
                        [(n, OnceLoop::new((), first), weight)]
                    });
                    let succ = flowlog_map(nat, "Succ", |n: u32, time, weight| {
                        (n < 2).then_some((n + 1, time, weight))
                    });
                    let next = seed.concat(succ);
                    next.clone()
                        .inspect(move |(n, time, _)| sink.lock().unwrap().push((*n, time.inner)));
                    variable.set(next);
                });
            });
        })
        .expect("timely starts")
        .join()
        .into_iter()
        .for_each(|result| result.expect("worker completes"));
        let mut fed = fed.lock().unwrap().clone();
        fed.sort_unstable();
        (fed, failure)
    }

    /// A loop whose rows stop changing before the last iteration records
    /// nothing.
    #[test]
    fn a_loop_settling_before_the_last_iteration_records_nothing() {
        let last = LAST_ITERATION;
        let (fed, failure) = count_to_two(last - 3);
        assert_eq!(fed, [(0, last - 3), (1, last - 2), (2, last - 1)]);
        assert!(!failure.any());
    }

    /// A row that changes in the last iteration has no next one to reach,
    /// so its relation is recorded.
    #[test]
    fn a_row_changing_in_the_last_iteration_records_its_relation() {
        let last = LAST_ITERATION;
        let (fed, failure) = count_to_two(last - 2);
        assert_eq!(fed, [(0, last - 2), (1, last - 1), (2, last)]);
        assert_eq!(
            failure.messages(),
            [
                "recursion through `Nat` did not reach a fixpoint: it still changed in \
                 iteration 65534, the last a loop can count"
            ]
        );
    }

    /// A loop that runs out of iterations in one epoch still lets the next
    /// epoch through, so the run drains and reports the failure. Built as
    /// the planner builds `Nat(n + 1) :- Nat(n), Limit(m), n < m` from a
    /// late `Nat(0)`: a row in the top iteration would hide the next epoch
    /// from the arrangement of `Nat`, which would shut down early and make
    /// timely panic once that epoch reached it.
    #[test]
    fn a_loop_out_of_iterations_still_lets_the_next_epoch_through() {
        let failure = Failure::new();
        let shared = failure.clone();
        timely::execute(Config::thread(), move |worker| {
            let probe = ProbeHandle::new();
            let mut limits = worker.dataflow::<Epoch, _, _>(|scope| {
                let zero: VecCollection<'_, Epoch, (u32,), diff::Append> =
                    Some(((0_u32,), 0, diff::Append))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                let (limits, limit) = scope.new_collection::<(u32,), diff::Append>();
                let limit = flowlog_map(limit, "Key: Limit", |m, time, weight| {
                    [(((), m), time, weight)]
                });
                let limit = flowlog_arrange(limit, "Arrange: Limit");
                scope
                    .scoped::<LexLoop, _, _>("Iterative", |inner| {
                        let limit = limit.enter(inner);
                        let (variable, nat) =
                            flowlog_variable::<_, (u32,), diff::Append>(inner, "Nat", &shared);
                        let seed = flowlog_map(
                            zero.enter(inner),
                            "Late",
                            |n, (epoch, _): LexLoop, weight| {
                                [(n, (epoch, LAST_ITERATION - 2), weight)]
                            },
                        );
                        let nat = flowlog_map(nat, "Key: Nat", |n: (u32,), time, weight| {
                            [(((), n), time, weight)]
                        });
                        let nat = flowlog_arrange(nat, "Arrange: Nat");
                        let succ = flowlog_join(nat, limit, "Join", |_, n, m| {
                            (n.0 < m.0).then_some((n.0 + 1,))
                        });
                        let next = flowlog_dedup(seed.concat(succ));
                        variable.set(next.clone());
                        next.leave(scope)
                    })
                    .probe_with(&probe);
                limits
            });
            limits.update((u32::MAX,), diff::Append);
            for epoch in 1..=2 {
                limits.advance_to(epoch);
                limits.flush();
                worker.step_while(|| probe.less_than(&epoch));
            }
        })
        .expect("timely starts")
        .join()
        .into_iter()
        .for_each(|result| result.expect("worker completes"));
        assert_eq!(failure.messages(), [unconverged("Nat")]);
    }

    /// A message stamped in the last iteration of one epoch may carry a
    /// row of the next. It goes around under that row's time, not the
    /// counter's top, where it would hide the next epoch as such a row
    /// would.
    #[test]
    fn a_message_stamped_in_the_last_iteration_goes_around_under_its_rows_times() {
        let failure = Failure::new();
        let seen = Arc::new(Mutex::new(Vec::new()));
        let (shared, sink) = (failure.clone(), Arc::clone(&seen));
        timely::execute(Config::thread(), move |worker| {
            let sink = Arc::clone(&sink);
            worker.dataflow::<Epoch, _, _>(|scope| {
                let zero: VecCollection<'_, Epoch, (u32,), diff::Append> =
                    Some(((0_u32,), 0, diff::Append))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                scope.scoped::<LexLoop, _, _>("Iterative", |inner| {
                    let (variable, nat) =
                        flowlog_variable::<_, (u32,), diff::Append>(inner, "Nat", &shared);
                    let held = zero
                        .enter(inner)
                        .inner
                        .unary(Pipeline, "Hold", |_, _| {
                            |input, output| {
                                input.for_each(|capability, rows| {
                                    let last = capability.delayed(&(0, LAST_ITERATION), 0);
                                    let mut next_epoch: Vec<_> = rows
                                        .drain(..)
                                        .map(|(n, _, weight)| (n, (1, 0), weight))
                                        .collect();
                                    output.session(&last).give_container(&mut next_epoch);
                                });
                            }
                        })
                        .as_collection();
                    variable.set(held);
                    nat.inner.inspect_core(move |event| {
                        if let Ok((stamp, rows)) = event {
                            let stamp: Vec<LexLoop> = stamp.iter().cloned().collect();
                            let times: Vec<LexLoop> = rows.iter().map(|row| row.1).collect();
                            sink.lock().unwrap().push((stamp, times));
                        }
                    });
                });
            });
        })
        .expect("timely starts")
        .join()
        .into_iter()
        .for_each(|result| result.expect("worker completes"));
        assert_eq!(*seen.lock().unwrap(), [(vec![(1, 1)], vec![(1, 1)])]);
        assert!(!failure.any());
    }
}
