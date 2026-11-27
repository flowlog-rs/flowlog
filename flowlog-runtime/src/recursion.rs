//! How many iterations a loop may run, and what a loop that needs more
//! reports: Materialize's `ERROR AT RECURSION LIMIT` and `RETURN AT
//! RECURSION LIMIT`, set for a whole run.
//!
//! A loop runs one iteration after another until its rows stop changing.
//! Under a [`RecursionLimit`] of `N`, it runs iterations `0` to `N - 1`,
//! the first of which applies the base rules, so it returns what `N`
//! iterations derive. A loop still changing in its last iteration cannot
//! go on: its [`Variable`](crate::operators::Variable) drops the changes it
//! cannot feed back, and reports the relation to the dataflow's [`Loops`],
//! which fail the run by default, or warn.
//!
//! The report is conservative. A loop that changed in its last iteration
//! may have reached its fixpoint all the same, as only the next iteration
//! would tell, so its results may be complete.

use std::collections::BTreeSet;
use std::num::NonZero;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

use crate::Failure;
use crate::time::Iteration;
use crate::time::LAST_ITERATION;

// =============================================================================
// Recursion limit
// =============================================================================

/// How many iterations a loop may run, and what happens to a loop still
/// changing in the last of them: an error, or a warning beside the results
/// of the iterations it ran. [`Self::MAX`] by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RecursionLimit {
    iterations: NonZero<Iteration>,
    at_limit: AtLimit,
}

/// What a loop still changing at its limit does.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AtLimit {
    Error,
    Return,
}

impl RecursionLimit {
    /// Every iteration a loop can count, and an error for a loop that needs
    /// more.
    pub const MAX: Self = Self::error_at(NonZero::<Iteration>::MAX);

    /// At most `iterations`, and an error for a loop that needs more.
    #[must_use]
    pub const fn error_at(iterations: NonZero<Iteration>) -> Self {
        Self {
            iterations,
            at_limit: AtLimit::Error,
        }
    }

    /// At most `iterations`, and a warning for a loop that needs more, whose
    /// results are what those iterations derived.
    #[must_use]
    pub const fn return_at(iterations: NonZero<Iteration>) -> Self {
        Self {
            iterations,
            at_limit: AtLimit::Return,
        }
    }

    /// The last iteration a loop runs, counting from 0: at most
    /// [`LAST_ITERATION`], so that no loop time reaches `Iteration::MAX`.
    pub(crate) const fn last_iteration(self) -> Iteration {
        self.iterations.get() - 1
    }
}

impl Default for RecursionLimit {
    fn default() -> Self {
        Self::MAX
    }
}

// The default lets a loop count every iteration it can.
const _: () = assert!(RecursionLimit::MAX.last_iteration() == LAST_ITERATION);

// =============================================================================
// Loops
// =============================================================================

/// The loops of a run at its [`RecursionLimit`]: where a loop still changing
/// in its last iteration reports its relation, as a [`Failure`] or as a
/// warning.
///
/// Clone one into every worker: like a [`Failure`], it reaches one
/// process's workers, through memory. A loop reports before its last
/// iteration drains, so a driver reads [`Self::warnings`] once a run or a
/// commit has.
#[derive(Clone, Debug)]
pub struct Loops {
    limit: RecursionLimit,
    failure: Failure,
    /// The relations whose loops returned at the limit, each once.
    cut: Arc<Mutex<BTreeSet<&'static str>>>,
}

impl Loops {
    /// The loops of a run at `limit`, whose errors go to `failure`.
    pub fn new(limit: RecursionLimit, failure: &Failure) -> Self {
        Self {
            limit,
            failure: failure.clone(),
            cut: Arc::default(),
        }
    }

    /// A warning for each relation whose loop has returned at the limit, in
    /// order of relation.
    ///
    /// A relation stays listed for the rest of the run: a static or an append
    /// loop never derives the rows it missed, so every later result lacks
    /// them too. A mutable loop recomputes each epoch, and stays listed even
    /// once an epoch needs fewer iterations.
    pub fn warnings(&self) -> Vec<String> {
        let iterations = self.limit.iterations;
        self.cut()
            .iter()
            .map(|relation| stopped(relation, iterations))
            .collect()
    }

    /// The last iteration each loop runs.
    pub(crate) fn last_iteration(&self) -> Iteration {
        self.limit.last_iteration()
    }

    /// Reports that the loop through `relation` still changed in its last
    /// iteration.
    pub(crate) fn reached_limit(&self, relation: &'static str) {
        let iterations = self.limit.iterations;
        match self.limit.at_limit {
            AtLimit::Error => self.failure.record(unconverged(relation, iterations)),
            AtLimit::Return => {
                self.cut().insert(relation);
            }
        }
    }

    fn cut(&self) -> MutexGuard<'_, BTreeSet<&'static str>> {
        self.cut.lock().expect("loops poisoned")
    }
}

/// The error of a loop through `relation` still changing after `iterations`.
fn unconverged(relation: &str, iterations: NonZero<Iteration>) -> String {
    format!(
        "recursion through `{relation}` did not reach a fixpoint within the recursion \
         limit of {}",
        count(iterations)
    )
}

/// The warning of a loop through `relation` stopped after `iterations`.
fn stopped(relation: &str, iterations: NonZero<Iteration>) -> String {
    format!(
        "recursion through `{relation}` stopped at the recursion limit of {}, before \
         reaching a fixpoint: its results may be incomplete",
        count(iterations)
    )
}

/// `iterations`, in words.
fn count(iterations: NonZero<Iteration>) -> String {
    match iterations.get() {
        1 => "1 iteration".to_owned(),
        n => format!("{n} iterations"),
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn iterations(n: Iteration) -> NonZero<Iteration> {
        NonZero::new(n).expect("a positive limit")
    }

    /// An error fails the run, each relation once, and leaves no warning.
    #[test]
    fn an_error_fails_the_run() {
        let failure = Failure::new();
        let loops = Loops::new(RecursionLimit::default(), &failure);
        loops.reached_limit("Nat");
        loops.reached_limit("Nat");
        assert_eq!(
            failure.messages(),
            [
                "recursion through `Nat` did not reach a fixpoint within the recursion limit \
              of 65535 iterations"
            ]
        );
        assert!(loops.warnings().is_empty());
    }

    /// A return leaves the run going, and warns of each relation once, in
    /// order, whichever clone reported it, every time the warnings are read.
    #[test]
    fn a_return_warns() {
        let failure = Failure::new();
        let loops = Loops::new(RecursionLimit::return_at(iterations(3)), &failure);
        let worker = loops.clone();
        worker.reached_limit("Path");
        worker.reached_limit("Nat");
        loops.reached_limit("Path");
        let warnings = [
            "recursion through `Nat` stopped at the recursion limit of 3 iterations, before \
             reaching a fixpoint: its results may be incomplete",
            "recursion through `Path` stopped at the recursion limit of 3 iterations, before \
             reaching a fixpoint: its results may be incomplete",
        ];
        assert_eq!(loops.warnings(), warnings);
        assert_eq!(loops.warnings(), warnings);
        assert!(!failure.any());
    }

    #[test]
    fn a_limit_of_one_iteration_says_so() {
        let loops = Loops::new(RecursionLimit::return_at(iterations(1)), &Failure::new());
        loops.reached_limit("Nat");
        assert_eq!(
            loops.warnings(),
            [
                "recursion through `Nat` stopped at the recursion limit of 1 iteration, before \
              reaching a fixpoint: its results may be incomplete"
            ]
        );
    }
}
