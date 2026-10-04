//! Transaction state shared by every incremental driver.
//!
//! Both the binary-mode REPL (`flowlog-compiler`) and the library-mode
//! engine (`flowlog-build`) use the same
//! epoch-broadcast protocol: a driver writes a [`TxnState`] into
//! `Arc<RwLock<_>>`, workers rendezvous on a [`std::sync::Barrier`] to
//! read the snapshot, apply its `pending` ops, then rendezvous again to
//! publish outputs. The only thing that differs between modes is who
//! plays the driver: stdin for the binary, the host thread for the
//! library.

use std::path::PathBuf;

/// A single update queued inside a transaction: the rows it names are
/// inserted into `rel` or deleted from it. A relation is a set, so a
/// command never counts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TxnOp {
    Insert { rel: String, rows: Rows },
    Delete { rel: String, rows: Rows },
}

impl TxnOp {
    /// The relation the update names, as the command spelled it.
    #[must_use]
    pub fn rel(&self) -> &str {
        match self {
            Self::Insert { rel, .. } | Self::Delete { rel, .. } => rel,
        }
    }
}

/// The rows a command names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Rows {
    /// One tuple in serialized form; empty for a nullary relation's fact.
    Tuple(String),
    /// Every row of a file.
    File(PathBuf),
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
    /// Clear the pending queue, used by drivers when starting or
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Both verbs name their relation as the command spelled it.
    #[test]
    fn an_update_names_its_relation_under_either_verb() {
        let rows = Rows::Tuple("1,2".to_string());
        let insert = TxnOp::Insert {
            rel: "Edge".to_string(),
            rows: rows.clone(),
        };
        let delete = TxnOp::Delete {
            rel: "Edge".to_string(),
            rows,
        };
        assert_eq!(insert.rel(), "Edge");
        assert_eq!(delete.rel(), "Edge");
    }
}
