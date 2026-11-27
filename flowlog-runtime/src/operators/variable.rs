//! A loop's recursive collection, [`Variable`]: differential's, except that
//! a row still changing in the last iteration a loop can count fails the
//! run instead of vanishing.
//!
//! A loop counts its iterations in an [`Iteration`], so a row that changes
//! in the counter's last value has no next iteration to reach.
//! Differential's variable drops such a row without a word: a recursion
//! that needs more iterations, such as `Nat(n + 1) :- Nat(n), n < 70000`,
//! would end with a truncated result and a clean exit. Its times also
//! reach the counter's top, where a [`LexLoop`]'s feedback edge stops
//! keeping their order: timely can then shut the loop down while a later
//! epoch still has work for it, and panic. This variable stops one short
//! of the top, at [`LAST_ITERATION`], and records a [`Failure`] for a row
//! it cannot advance before that row's time drains.
//!
//! [`Iteration`]: crate::time::Iteration
//! [`LexLoop`]: crate::time::LexLoop

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use timely::dataflow::Scope;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::ConnectLoop;
use timely::dataflow::operators::Feedback;
use timely::dataflow::operators::InputCapability;
use timely::dataflow::operators::feedback::Handle;
use timely::dataflow::operators::generic::Operator;

use crate::Failure;
use crate::time::LAST_ITERATION;
use crate::time::LoopClock;

// =============================================================================
// Variable
// =============================================================================

/// The recursive collection of a relation in a loop: empty until
/// [`Variable::set`] feeds it the relation's rows, one iteration later.
pub struct Variable<'scope, T: LoopClock, D: 'static, R: 'static> {
    feedback: Handle<'scope, T, Vec<(D, T, R)>>,
    relation: &'static str,
    failure: Failure,
}

impl<'scope, T, D, R> Variable<'scope, T, D, R>
where
    T: LoopClock,
    D: Clone + 'static,
    R: Clone + 'static,
{
    /// Creates the recursive collection of `relation` in the loop `scope`,
    /// with its variable. A row the variable cannot advance is recorded in
    /// `failure`.
    pub fn new(
        scope: Scope<'scope, T>,
        relation: &'static str,
        failure: &Failure,
    ) -> (Self, VecCollection<'scope, T, D, R>) {
        let (feedback, rows) = scope.feedback(T::next_iteration());
        let variable = Self {
            feedback,
            relation,
            failure: failure.clone(),
        };
        (variable, rows.as_collection())
    }

    /// Feeds `next` back as the relation's rows, one iteration later.
    ///
    /// A single pass advances each row's time in place, as differential's
    /// `results_in` does, so the loop has the operators differential's
    /// would. A row moves on to its time's [`LoopClock::successor`], so no
    /// time in the loop passes [`LAST_ITERATION`]. A row in the last
    /// iteration has none and is dropped, but its worker records the
    /// failure first, once. It records before it releases the row's time,
    /// which still reaches every probe through the feedback edge, so every
    /// worker sees the failure once that time drains.
    ///
    /// The feedback edge advances a message's stamp by one iteration too,
    /// so a stamp in the last iteration would come back at
    /// `Iteration::MAX`. A message stamped there goes out stamped with its
    /// remaining rows' own times instead, each of which has a successor.
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
                        let stamp = restamp(&capability, rows);
                        if advance(rows) && !reported {
                            reported = true;
                            failure.record(unconverged(relation));
                        }
                        match stamp {
                            Some(stamp) => output.session(&stamp).give_container(rows),
                            None => output.session(&capability).give_container(rows),
                        }
                    });
                }
            })
            .connect_loop(feedback);
    }
}

/// The stamp to feed `rows` back under instead of `capability`'s, if that
/// has an element in the last iteration: the times of the rows that
/// advance. Such a stamp can still carry rows that move on: those of a
/// later epoch in a lexicographic loop, or those above another element of
/// a product stamp.
fn restamp<D, T: LoopClock, R>(
    capability: &InputCapability<T>,
    rows: &[(D, T, R)],
) -> Option<CapabilitySet<T>> {
    let last = capability
        .stamp()
        .iter()
        .any(|time| time.successor().is_none());
    last.then(|| {
        rows.iter()
            .filter(|(_, time, _)| time.successor().is_some())
            .map(|(_, time, _)| capability.delayed(time, 0))
            .collect()
    })
}

/// Moves each of `rows` on to its time's successor, and drops a row in the
/// last iteration. Returns whether it dropped any.
fn advance<D, T: LoopClock, R>(rows: &mut Vec<(D, T, R)>) -> bool {
    let len = rows.len();
    rows.retain_mut(|(_, time, _)| match time.successor() {
        Some(later) => {
            *time = later;
            true
        }
        None => false,
    });
    rows.len() < len
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
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use differential_dataflow::lattice::Lattice;
    use rstest::rstest;
    use timely::dataflow::operators::Inspect;
    use timely::dataflow::operators::core::ToStream;
    use timely::dataflow::operators::probe::Handle as ProbeHandle;
    use timely::progress::timestamp::Refines;

    use super::*;
    use crate::diff;
    use crate::operators::arrange;
    use crate::operators::dedup;
    use crate::operators::join;
    use crate::operators::map;
    use crate::time::Epoch;
    use crate::time::EpochLoop;
    use crate::time::Iteration;
    use crate::time::LexLoop;
    use crate::time::OnceLoop;

    /// Runs `Nat(n + 1) :- Nat(n), n < 2` from `Nat(0)`, seeded at
    /// iteration `first` rather than 0 so that the loop reaches the last
    /// iteration in a few. Returns each row of the loop's result with its
    /// iteration, and the failure.
    fn count_to_two(first: Iteration) -> (Vec<(u32, Iteration)>, Failure) {
        let failure = Failure::new();
        let shared = failure.clone();
        let mut fed = timely::execute_directly(move |worker| {
            let fed = Rc::new(RefCell::new(Vec::new()));
            let sink = Rc::clone(&fed);
            worker.dataflow::<(), _, _>(|scope| {
                let zero: VecCollection<'_, (), u32, diff::Static> =
                    Some((0_u32, (), diff::Static))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                scope.scoped::<OnceLoop, _, _>("Iterative", |inner| {
                    let (variable, nat) = Variable::new(inner, "Nat", &shared);
                    let seed = map(zero.enter(inner), "Late", move |n, _, weight| {
                        [(n, OnceLoop::new((), first), weight)]
                    });
                    let succ = map(nat, "Succ", |n: u32, time, weight| {
                        (n < 2).then_some((n + 1, time, weight))
                    });
                    let next = seed.concat(succ);
                    next.clone()
                        .inspect(move |(n, time, _)| sink.borrow_mut().push((*n, time.inner)));
                    variable.set(next);
                });
            });
            while worker.step() {}
            fed.take()
        });
        fed.sort_unstable();
        (fed, failure)
    }

    /// A loop whose rows stop changing before the last iteration records
    /// nothing, and one whose rows still change in it records its relation.
    // Cases: first iteration, rows by iteration, failures.
    #[rstest]
    #[case::settling_in_time(
        LAST_ITERATION - 3,
        vec![(0, LAST_ITERATION - 3), (1, LAST_ITERATION - 2), (2, LAST_ITERATION - 1)],
        vec![],
    )]
    #[case::changing_in_the_last_iteration(
        LAST_ITERATION - 2,
        vec![(0, LAST_ITERATION - 2), (1, LAST_ITERATION - 1), (2, LAST_ITERATION)],
        vec![
            "recursion through `Nat` did not reach a fixpoint: it still changed in \
             iteration 65534, the last a loop can count"
                .to_owned(),
        ],
    )]
    fn a_loop_fails_only_if_its_last_iteration_changes(
        #[case] first: Iteration,
        #[case] rows: Vec<(u32, Iteration)>,
        #[case] failures: Vec<String>,
    ) {
        let (fed, failure) = count_to_two(first);
        assert_eq!(fed, rows);
        assert_eq!(failure.messages(), failures);
    }

    /// A loop that runs out of iterations in one epoch still lets the next
    /// epoch through, so the run drains and reports the failure. Built as
    /// the planner builds `Nat(n + 1) :- Nat(n), Limit(m), n < m` from a
    /// late `Nat(0)`: a time at the counter's top would hide the next epoch
    /// from the arrangement of `Nat`, which would shut down early and make
    /// timely panic once that epoch reached it.
    #[test]
    fn a_loop_out_of_iterations_still_lets_the_next_epoch_through() {
        let failure = Failure::new();
        let shared = failure.clone();
        timely::execute_directly(move |worker| {
            let probe = ProbeHandle::new();
            let mut limits = worker.dataflow::<Epoch, _, _>(|scope| {
                let zero: VecCollection<'_, Epoch, (u32,), diff::Append> =
                    Some(((0_u32,), 0, diff::Append))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                let (limits, limit) = scope.new_collection::<(u32,), diff::Append>();
                let limit = map(limit, "Key: Limit", |m, time, weight| {
                    [(((), m), time, weight)]
                });
                let limit = arrange(limit, "Arrange: Limit");
                scope
                    .scoped::<LexLoop, _, _>("Iterative", |inner| {
                        let limit = limit.enter(inner);
                        let (variable, nat) =
                            Variable::<_, (u32,), diff::Append>::new(inner, "Nat", &shared);
                        let seed = map(
                            zero.enter(inner),
                            "Late",
                            |n, (epoch, _): LexLoop, weight| {
                                [(n, (epoch, LAST_ITERATION - 2), weight)]
                            },
                        );
                        let nat = map(nat, "Key: Nat", |n: (u32,), time, weight| {
                            [(((), n), time, weight)]
                        });
                        let nat = arrange(nat, "Arrange: Nat");
                        let succ = join(nat, limit, "Join", |_, n, m| {
                            (n.0 < m.0).then_some((n.0 + 1,))
                        });
                        let next = dedup(seed.concat(succ));
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
        });
        assert_eq!(failure.messages(), [unconverged("Nat")]);
    }

    /// A message, as its stamp and its rows' times.
    type Message<T> = (Vec<T>, Vec<T>);

    /// Sends one message into a variable, stamped `stamp` and holding a row
    /// at each of `times`. Returns each message the variable feeds back, and
    /// the failure.
    fn feed_back<T>(stamp: Vec<T>, times: Vec<T>) -> (Vec<Message<T>>, Failure)
    where
        T: LoopClock + Lattice + Refines<Epoch> + Sync,
    {
        let failure = Failure::new();
        let shared = failure.clone();
        let seen = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let sink = Rc::clone(&seen);
            worker.dataflow::<Epoch, _, _>(|scope| {
                let zero: VecCollection<'_, Epoch, u32, diff::Append> =
                    Some((0_u32, 0, diff::Append))
                        .into_iter()
                        .to_stream(scope)
                        .as_collection();
                scope.scoped::<T, _, _>("Iterative", |inner| {
                    let (variable, fed) =
                        Variable::<_, u32, diff::Append>::new(inner, "Nat", &shared);
                    let held = zero
                        .enter(inner)
                        .inner
                        .unary(Pipeline, "Hold", move |_, _| {
                            move |input, output| {
                                input.for_each(|capability, _| {
                                    let capabilities: CapabilitySet<T> = stamp
                                        .iter()
                                        .map(|time| capability.delayed(time, 0))
                                        .collect();
                                    let mut rows: Vec<_> = times
                                        .iter()
                                        .map(|time| (0, time.clone(), diff::Append))
                                        .collect();
                                    output.session(&capabilities).give_container(&mut rows);
                                });
                            }
                        })
                        .as_collection();
                    variable.set(held);
                    fed.inner.inspect_core(move |event| {
                        if let Ok((stamp, rows)) = event {
                            let stamp = stamp.iter().cloned().collect();
                            let times = rows.iter().map(|(_, time, _)| time.clone()).collect();
                            sink.borrow_mut().push((stamp, times));
                        }
                    });
                });
            });
            while worker.step() {}
            seen.take()
        });
        (seen, failure)
    }

    /// A variable feeds a message back under its stamp, one iteration on,
    /// until the stamp reaches the last iteration: from there the message
    /// goes around under its remaining rows' times, never the counter's
    /// top.
    // Cases: stamp, row times, fed back as (stamp, row times), failed.
    #[rstest]
    #[case::before_the_last_iteration(
        vec![(0, 3)],
        vec![(0, 3), (1, 0)],
        vec![(vec![(0, 4)], vec![(0, 4), (1, 1)])],
        false,
    )]
    #[case::carrying_a_later_epoch(
        vec![(0, LAST_ITERATION)],
        vec![(1, 0)],
        vec![(vec![(1, 1)], vec![(1, 1)])],
        false,
    )]
    #[case::carrying_the_last_iteration(
        vec![(0, LAST_ITERATION)],
        vec![(0, LAST_ITERATION), (1, 0)],
        vec![(vec![(1, 1)], vec![(1, 1)])],
        true,
    )]
    fn a_lexicographic_message_never_reaches_the_top(
        #[case] stamp: Vec<LexLoop>,
        #[case] times: Vec<LexLoop>,
        #[case] fed_back: Vec<Message<LexLoop>>,
        #[case] failed: bool,
    ) {
        let (seen, failure) = feed_back(stamp, times);
        assert_eq!(seen, fed_back);
        assert_eq!(failure.any(), failed);
    }

    /// A product stamp with one element in the last iteration goes around
    /// under the times of the rows above its other elements.
    #[test]
    fn a_product_message_goes_around_under_its_advancing_rows() {
        let last = EpochLoop::new(0, LAST_ITERATION);
        let later = EpochLoop::new(1, 3);
        let (seen, failure) = feed_back(vec![last, later], vec![last, later]);
        let after = EpochLoop::new(1, 4);
        assert_eq!(seen, [(vec![after], vec![after])]);
        assert_eq!(failure.messages(), [unconverged("Nat")]);
    }
}
