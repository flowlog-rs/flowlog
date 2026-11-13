//! Named row-at-a-time transformations for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Multiply;
use timely::container::DrainContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Timestamp;

use crate::diff::Weight;

/// Creates a new collection by applying `logic` to each update and
/// accumulating the results, under the name FlowLog gives the step.
///
/// `logic` sees the whole update, not just its record, so unlike a
/// collection's `flat_map` it rewrites the row, the timestamp and the
/// weight together.
pub fn flowlog_map<'scope, T, D, D2, R, R2, I, L>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, T, D2, R2>
where
    T: Timestamp,
    D: 'static,
    D2: Clone + 'static,
    R: 'static,
    R2: Clone + 'static,
    I: IntoIterator<Item = (D2, T, R2)>,
    L: FnMut(D, T, R) -> I + 'static,
{
    collection
        .inner
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each_stamp(|capability, data| {
                    output.session(&capability).give_iterator(
                        data.flat_map(DrainContainer::drain)
                            .flat_map(|(data, time, diff)| logic(data, time, diff)),
                    );
                });
            }
        })
        .as_collection()
}

/// Creates a new collection containing those input updates satisfying
/// `logic`, under the name FlowLog gives the step.
///
/// `logic` sees the whole update, so it can decide on the timestamp and
/// the weight as well as the row. It only ever decides: an update is kept
/// as it stands or dropped.
pub fn flowlog_filter<'scope, T, D, R, L>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, T, D, R>
where
    T: Timestamp,
    D: 'static,
    R: 'static,
    L: FnMut(&D, &T, &R) -> bool + 'static,
{
    collection
        .inner
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each_stamp(|capability, data| {
                    output.session(&capability).give_iterator(
                        data.flat_map(DrainContainer::drain)
                            .filter(|(row, time, diff)| logic(row, time, diff)),
                    );
                });
            }
        })
        .as_collection()
}

/// Creates a new collection by applying `logic` to each update, under the
/// name FlowLog gives the step.
///
/// Although the name suggests in-place mutation, this does not change the
/// source collection, but rather re-uses the underlying allocations. It is
/// semantically a [`flowlog_map`] whose types all stay the same, since a
/// rewrite lands back in the slot it came from, but can be more efficient.
pub fn flowlog_map_in_place<'scope, T, D, R, L>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, T, D, R>
where
    T: Timestamp,
    D: 'static,
    R: 'static,
    L: FnMut(&mut D, &mut T, &mut R) + 'static,
{
    collection
        .inner
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each_stamp(|capability, data| {
                    let mut session = output.session(&capability);
                    for chunk in data {
                        for (row, time, diff) in chunk.iter_mut() {
                            logic(row, time, diff);
                        }
                        session.give_container(chunk);
                    }
                });
            }
        })
        .as_collection()
}

/// Reweighs a collection to the weight `R2`, at or above its own in the
/// order [`diff`](crate::diff) gives the weights, so it can join a union
/// whose parts must share a weight. Rows and times pass through
/// unchanged, and each update keeps its count.
///
/// A presence update counts one, so a row announced more than once counts
/// more than once at a signed weight, and the output is a set only after
/// a dedup. A weight below the source's cannot hold the source's changes,
/// such as a signed retraction, so lifting down does not compile:
///
/// ```compile_fail,E0271
/// use differential_dataflow::VecCollection;
/// use flowlog_runtime::diff;
/// use flowlog_runtime::operators::flowlog_lift;
///
/// fn lower(rows: VecCollection<'_, u32, u64, diff::Mutable>) {
///     flowlog_lift::<diff::Static, _, _, _>(rows, "Lower");
/// }
/// ```
pub fn flowlog_lift<'scope, R2, T, D, R1>(
    collection: VecCollection<'scope, T, D, R1>,
    name: &str,
) -> VecCollection<'scope, T, D, R2>
where
    T: Timestamp,
    D: Clone + 'static,
    R1: Weight + Multiply<R2, Output = R2>,
    R2: Weight,
{
    flowlog_map(collection, name, |row, time, diff| {
        std::iter::once((row, time, diff.multiply(&R2::one())))
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;

    use super::*;
    use crate::diff;

    /// A lift keeps each update's count: a presence row counts one, and a
    /// signed row keeps its own count, a retraction included.
    #[test]
    fn a_lift_keeps_each_update_count() {
        let mut actual = timely::execute_directly(|worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let (mut fixed, mut counted) = worker.dataflow::<u32, _, _>(|scope| {
                let (fixed, present) = scope.new_collection::<char, diff::Static>();
                let (counted, signed) = scope.new_collection::<char, diff::Mutable>();
                let seen = Rc::clone(&seen);
                flowlog_lift::<diff::Mutable, _, _, _>(present, "Lift")
                    .concat(flowlog_lift(signed, "Lift"))
                    .inspect(move |update| seen.borrow_mut().push(*update));
                (fixed, counted)
            });
            fixed.update('a', diff::Static);
            fixed.close();
            counted.update('b', 3);
            counted.advance_to(1);
            counted.update('b', -2);
            counted.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        assert_eq!(actual, vec![('a', 0, 1), ('b', 0, 3), ('b', 1, -2)]);
    }
}
