//! `diff::Append` aggregation: a group only grows, but its answer still
//! changes, so the answers are signed and each retracts the one before.

use differential_dataflow::AsCollection;
use differential_dataflow::Data;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::difference::Monoid;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::implementations::ValBuilder;
use differential_dataflow::trace::implementations::ValSpine;
use timely::dataflow::operators::core::ToStream;
use timely::progress::Timestamp;

use super::Aggregation;
use super::semiring::Scalar;
use super::semiring::Semiring;
use crate::diff;

/// Groups an append collection by key and reduces each group under
/// `aggregation`, answering in `diff::Mutable`: each new answer retracts
/// the one before, and the empty result, when `empty_key` names a group
/// and the aggregation defines one, is retracted by the first answer.
///
/// `split`, `merge` and `empty_key` follow the contracts of
/// [`flowlog_reduce`](super::flowlog_reduce).
pub fn flowlog_reduce_append<'scope, A, T, D, K, V, C, O>(
    collection: VecCollection<'scope, T, D, diff::Append>,
    name: &str,
    _aggregation: A,
    empty_key: Option<K>,
    split: impl FnMut(D) -> (K, V) + 'static,
    mut merge: impl FnMut(K, C) -> O + 'static,
) -> VecCollection<'scope, T, O, diff::Mutable>
where
    A: Aggregation<V, C>,
    A::Semiring: ExchangeData,
    C: Scalar,
    T: Timestamp + Lattice,
    D: Data,
    K: ExchangeData + Hashable,
    V: ExchangeData,
    O: Data,
{
    // TODO: give append its own accumulation. Until then this mirrors the
    // `diff::Mutable` strategy in `reduce/mutable.rs` line for line, and a
    // change to one must land in both.
    let empty_group = empty_key.zip(A::EMPTY_RESULT);
    let scope = collection.inner.scope();
    let default_result = empty_group.as_ref().map(|(key, empty)| {
        (scope.index() == 0)
            .then(|| (merge(key.clone(), *empty), T::minimum(), 1))
            .into_iter()
            .to_stream(scope)
            .as_collection()
    });
    let reduced = collection
        .map(split)
        .arrange_by_key()
        .reduce_abelian::<_, ValBuilder<K, C, T, diff::Mutable>, ValSpine<K, C, T, diff::Mutable>, _, _>(
            name,
            move |key, input, output| {
                let mut accumulated = A::Semiring::zero();
                for (value, _) in input {
                    accumulated.plus_equals(&A::contribute(value));
                }
                output.push((accumulated.finish(), 1));
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

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::input::Input;
    use timely::dataflow::operators::probe::Handle;

    use super::*;
    use crate::operators::reduce::Count;
    use crate::operators::reduce::Min;

    /// Runs an append reduce of `aggregation` over `epochs`, each epoch's
    /// rows inserted at its index, and returns the signed answers
    /// consolidated.
    fn append_epochs<A>(aggregation: A, epochs: Vec<Vec<i64>>) -> Vec<(i64, u32, diff::Mutable)>
    where
        A: Aggregation<i64, i64> + Send + Sync,
        A::Semiring: ExchangeData,
    {
        let mut actual = timely::execute_directly(move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let probe = Handle::new();
            let mut input = worker.dataflow::<u32, _, _>(|scope| {
                let (input, rows) = scope.new_collection::<i64, diff::Append>();
                let seen = Rc::clone(&seen);
                flowlog_reduce_append(
                    rows,
                    "Reduce",
                    aggregation,
                    Some(()),
                    |value| ((), value),
                    |(), value| value,
                )
                .inspect(move |update| seen.borrow_mut().push(*update))
                .probe_with(&probe);
                input
            });
            for (epoch, values) in epochs.into_iter().enumerate() {
                for value in values {
                    input.update(value, diff::Append);
                }
                let next = epoch as u32 + 1;
                input.advance_to(next);
                input.flush();
                worker.step_while(|| probe.less_than(&next));
            }
            input.close();
            while worker.step() {}
            seen.take()
        });
        consolidate_updates(&mut actual);
        actual
    }

    /// An append count answers in signed changes: the empty group's zero
    /// is retracted by the first answer, and each later answer retracts
    /// the one before, so the answers always form a one-row set.
    #[test]
    fn append_count_retracts_each_superseded_answer() {
        assert_eq!(
            append_epochs(Count, vec![vec![], vec![7], vec![], vec![8, 9]]),
            vec![(0, 0, 1), (0, 1, -1), (1, 1, 1), (1, 3, -1), (3, 3, 1)]
        );
    }

    /// An append minimum changes only when a smaller value arrives, so a
    /// larger later value emits nothing.
    #[test]
    fn append_min_changes_only_when_the_bound_tightens() {
        assert_eq!(
            append_epochs(Min, vec![vec![10], vec![20], vec![5]]),
            vec![(5, 2, 1), (10, 0, 1), (10, 2, -1)]
        );
    }

    /// An append group with no rows and no defined empty result emits
    /// nothing.
    #[test]
    fn append_min_of_an_empty_group_emits_nothing() {
        assert_eq!(append_epochs(Min, vec![vec![]]), vec![]);
    }
}
