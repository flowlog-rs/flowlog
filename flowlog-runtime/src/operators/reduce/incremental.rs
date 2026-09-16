//! Aggregation with insertions and retractions, including empty groups.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use timely::dataflow::operators::core::ToStream;
use timely::progress::Timestamp;

use super::ReduceStrategy;
use super::semiring::Semiring;

impl<T: Timestamp + Lattice> ReduceStrategy<T> for i32 {
    fn reduce<'scope, D, K, V, S, O>(
        collection: VecCollection<'scope, T, D, Self>,
        name: &str,
        empty_group: Option<(K, S::Value)>,
        contribute: impl Fn(&V) -> S + 'static,
        split: impl FnMut(D) -> (K, V) + 'static,
        mut merge: impl FnMut(K, S::Value) -> O + 'static,
    ) -> VecCollection<'scope, T, O, Self>
    where
        D: Data,
        K: ExchangeData + Hashable,
        V: ExchangeData,
        S: Semiring + ExchangeData,
        O: Data,
    {
        // Reduce does not call its logic for an empty input group. Keep one
        // permanent default output instead, and let the same reduce cancel
        // it while real input exists. This preserves the input layout and
        // avoids a second arrangement/reduce, at the cost of one correction
        // entry in the reduced state and a concat on the result stream.
        let scope = collection.inner.scope();
        let default_result = empty_group.as_ref().map(|(key, empty)| {
            // Minimum time makes the default visible at every later logical
            // time. The iterator is consumed once on worker 0; a recursive
            // iteration does not construct a new source or insert it again.
            (scope.index() == 0)
                .then(|| (merge(key.clone(), *empty), T::minimum(), 1_i32))
                .into_iter()
                .to_stream(scope)
                .as_collection()
        });
        let reduced = collection
            .map(split)
            .arrange_by_key()
            .reduce_abelian::<_, ValBuilder<K, S::Value, T, i32>, ValSpine<K, S::Value, T, i32>, _, _>(
                name,
                move |key, input, output| {
                    // This callback describes the desired current output.
                    // reduce_abelian emits its difference from the old output.
                    let mut accumulated = S::zero();
                    for (value, _) in input {
                        accumulated.plus_equals(&contribute(value));
                    }
                    output.push((accumulated.finish(), 1));

                    // For default z, the permanent source contributes (z, +1).
                    // The desired reduced state and their combined result are:
                    //
                    //   Input group   Reduced state          Combined result
                    //   empty         {}                     {(z, +1)}
                    //   answer v      {(v, +1), (z, -1)}      {(v, +1)}
                    //   answer z      {(z, +1), (z, -1)}      {(z, +1)}
                    //
                    // For 5 -> 6, the unchanged (z, -1) cancels inside reduce;
                    // only (5, -1) and (6, +1) are emitted. When input empties,
                    // reduce retracts its entire old state without calling
                    // this closure, restoring z. A real result equal to z
                    // also leaves exactly one copy of z in the combined set.
                    if let Some((empty_key, empty)) = &empty_group
                        && key == empty_key
                    {
                        output.push((*empty, -1));
                    }
                },
                |buffer, key, updates| {
                    buffer.clear();
                    buffer.extend(updates.drain(..).map(|(v, t, r)| ((key.clone(), v), t, r)));
                },
            )
            .as_collection(move |key, value| merge(key.clone(), *value));
        match default_result {
            Some(default_result) => reduced.concat(default_result),
            None => reduced,
        }
    }
}
