//! Named row-at-a-time transformations for generated FlowLog rules.

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use timely::container::DrainContainer;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::Operator;
use timely::progress::Timestamp;

/// Applies `logic` to each input record and records `name` on the operator.
///
/// Every record produced by `logic` inherits its input timestamp and weight.
pub fn flowlog_flat_map<'scope, T, D, R, I, L>(
    collection: VecCollection<'scope, T, D, R>,
    name: &str,
    mut logic: L,
) -> VecCollection<'scope, T, I::Item, R>
where
    T: Timestamp + Clone,
    D: Clone + 'static,
    R: Clone + 'static,
    I: IntoIterator<Item: Clone + 'static>,
    L: FnMut(D) -> I + 'static,
{
    collection
        .inner
        .unary(Pipeline, name, move |_, _| {
            move |input, output| {
                input.for_each_time(|time, data| {
                    output.session(&time).give_iterator(
                        data.flat_map(DrainContainer::drain)
                            .flat_map(|(data, time, diff)| {
                                logic(data)
                                    .into_iter()
                                    .map(move |output| (output, time.clone(), diff.clone()))
                            }),
                    );
                });
            }
        })
        .as_collection()
}
