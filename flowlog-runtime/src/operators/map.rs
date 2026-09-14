//! Named row-at-a-time transformations for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use timely::container::DrainContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Timestamp;

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
                input.for_each_time(|time, data| {
                    output.session(&time).give_iterator(
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
                input.for_each_time(|time, data| {
                    output.session(&time).give_iterator(
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
                input.for_each_time(|time, data| {
                    let mut session = output.session(&time);
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
