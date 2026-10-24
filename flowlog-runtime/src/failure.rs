//! What stopped a process's workers, shared by every driver: for the
//! workers to stop on together, and for `main` to report once.
//!
//! A failure happens on one worker: a startup load that read bad input, or
//! a transaction's update that was refused or read bad input. The other
//! workers learn of it here, at a point past which every worker has
//! recorded its own: a batch worker after its dataflow drained, which
//! needs every worker's inputs closed; a shell worker after the preload
//! epoch drained, or at the barrier after a transaction's updates. A
//! worker that panicked is recorded by `main` once it has joined them.
//!
//! Workers share it through memory, so it reaches one process's workers,
//! which is every worker FlowLog runs today. Should a run ever span timely
//! processes, the news would have to travel through a dataflow instead;
//! the interface here would stay, with the transport behind it replaced.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::MutexGuard;

// =============================================================================
// Failure
// =============================================================================

/// What this process's workers have stopped on, each message once: a load
/// or an update that failed, recorded by the worker it failed on, or a
/// worker that panicked, recorded by `main`.
///
/// Clone one into every worker; `main` reads it after the workers return.
#[derive(Clone, Debug, Default)]
pub struct Failure(Arc<Mutex<BTreeSet<String>>>);

impl Failure {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records a failure.
    pub fn record(&self, message: String) {
        self.lock().insert(message);
    }

    /// Returns `true` if any failure has been recorded.
    pub fn any(&self) -> bool {
        !self.lock().is_empty()
    }

    /// The messages so far, each once, in order.
    pub fn messages(&self) -> Vec<String> {
        self.lock().iter().cloned().collect()
    }

    fn lock(&self) -> MutexGuard<'_, BTreeSet<String>> {
        self.0.lock().expect("failure poisoned")
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_record_has_no_failure() {
        let failure = Failure::new();
        assert!(!failure.any());
        assert!(failure.messages().is_empty());
    }

    #[test]
    fn a_clone_sees_what_another_recorded() {
        let failure = Failure::new();
        let worker = failure.clone();
        worker.record("relation `Edge` from put: tuple column 0: \"x\" is not i32".to_owned());
        assert!(failure.any());
        assert_eq!(
            failure.messages(),
            vec!["relation `Edge` from put: tuple column 0: \"x\" is not i32"]
        );
    }

    /// Every worker fails alike on a missing file; the message is reported
    /// once, and distinct messages in order.
    #[test]
    fn messages_are_distinct_and_ordered() {
        let failure = Failure::new();
        failure.record("b".to_owned());
        failure.record("a".to_owned());
        failure.record("b".to_owned());
        assert_eq!(failure.messages(), vec!["a", "b"]);
    }
}
