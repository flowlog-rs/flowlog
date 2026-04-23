//! Transaction state shared by every incremental driver.
//!
//! Both the binary-mode REPL (`flowlog-compiler`) and the library-mode
//! engine (`flowlog-build`, incremental codegen) use the same
//! epoch-broadcast protocol: a driver writes a [`TxnState`] into
//! `Arc<RwLock<_>>`, workers rendezvous on a [`std::sync::Barrier`] to
//! read the snapshot, apply its `pending` ops, then rendezvous again to
//! publish outputs. The only thing that differs between modes is who
//! plays the driver — stdin for the binary, the host thread for the
//! library.

use std::path::PathBuf;

/// Update multiplicity applied to a tuple. `+1` inserts, `-1` retracts;
/// larger magnitudes scale the count in ring-valued semirings.
pub type Diff = i32;

/// A single tuple-level update queued inside a transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnOp {
    /// Apply `diff` copies of `tuple` (serialized form) to `rel`.
    Put {
        rel: String,
        tuple: String,
        diff: Diff,
    },
    /// Apply `diff` copies of every row in `path` to `rel`.
    File {
        rel: String,
        path: PathBuf,
        diff: Diff,
    },
}

/// What workers should do when they observe a new published [`TxnState`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum TxnAction {
    /// No action (idle / cleared).
    #[default]
    None,
    /// Execute `pending`, then advance/flush once.
    Commit,
    /// Quit all workers.
    Quit,
}

/// Shared transaction snapshot. The driver mutates this behind an
/// `Arc<RwLock<_>>`; workers clone the inner value each epoch.
#[derive(Clone, Debug, Default)]
pub struct TxnState {
    /// Broadcast indicator: incremented on each publish so workers can
    /// detect "new txn".
    pub epoch: u32,
    /// Broadcast indicator: what the workers should do for this epoch.
    pub action: TxnAction,
    /// Updates queued for the next commit.
    pub pending: Vec<TxnOp>,
}

impl TxnState {
    /// Clear the pending queue — used by drivers when starting or
    /// aborting a transaction.
    pub fn clear_pending(&mut self) {
        self.pending.clear();
    }

    /// Append one op to the pending queue.
    pub fn enqueue(&mut self, op: TxnOp) {
        self.pending.push(op);
    }

    /// Snapshot the current state as a Commit broadcast at `next_epoch`.
    /// Clones `pending` so the driver can keep its queue for rollback.
    pub fn as_commit_snapshot(&self, next_epoch: u32) -> TxnState {
        TxnState {
            epoch: next_epoch,
            action: TxnAction::Commit,
            pending: self.pending.clone(),
        }
    }

    /// Freestanding Quit snapshot — no carried pending ops.
    pub fn as_quit_snapshot(next_epoch: u32) -> TxnState {
        TxnState {
            epoch: next_epoch,
            action: TxnAction::Quit,
            pending: Vec::new(),
        }
    }
}
