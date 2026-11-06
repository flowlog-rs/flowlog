//! The unit, [`flowlog_unit`]: the collection of one empty row that a rule
//! body without a positive atom or an aggregate reads in their place.

use differential_dataflow::AsCollection;
use differential_dataflow::VecCollection;
use timely::dataflow::Scope;
use timely::dataflow::operators::core::ToStream;
use timely::progress::Timestamp;

use crate::diff;

/// One empty row at the minimum time, inserted once on worker 0 and never
/// retracted.
pub fn flowlog_unit<'scope, T: Timestamp>(
    scope: Scope<'scope, T>,
) -> VecCollection<'scope, T, (), diff::Static> {
    (scope.index() == 0)
        .then(|| ((), T::minimum(), diff::Static))
        .into_iter()
        .to_stream(scope)
        .as_collection()
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::Mutex;

    use timely::Config;

    use super::*;

    /// However many workers run, the unit is one update: a presence weight
    /// would hide a second copy from a consolidated view, so the raw
    /// updates are compared.
    #[test]
    fn the_unit_is_one_row_across_workers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let workers = timely::execute(Config::process(3), move |worker| {
            let sink = Arc::clone(&sink);
            worker.dataflow::<u64, _, _>(|scope| {
                flowlog_unit(scope).inspect(move |update| sink.lock().unwrap().push(*update));
            });
        })
        .expect("timely starts");
        for result in workers.join() {
            result.expect("worker completes");
        }

        assert_eq!(*seen.lock().unwrap(), [((), 0, diff::Static)]);
    }
}
