//! The transaction layer: a run's committed transactions, discovered
//! from the metrics directory. For batch mode, there is only one
//! transaction, `t0`; for streaming mode, one transaction per commit,
//! starting at `t0` (the first commit, including facts staged at time
//! zero), then `t1`, `t2`, and so on.
//!
//! - [`discover`]: the metrics directory -> one [`Transaction`] of log
//!   text per commit

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;

use crate::ProfilerError;

/// One worker's log pair as raw text: the operator table, and its channels
/// sibling when present. `None` channels leave that worker time-only.
pub(crate) struct WorkerLog {
    pub(crate) operators: String,
    pub(crate) channels: Option<String>,
}

/// One transaction's per-worker log text.
pub(crate) struct Transaction {
    pub(crate) label: String,
    pub(crate) workers: Vec<WorkerLog>,
}

/// Discover the metrics directory into one [`Transaction`] per committed
/// transaction, grouped by the `_t{N}_` marker in each operator log's name.
/// An empty or log-free directory yields an empty vec; only an unreadable
/// directory or operator log is an error.
pub(crate) fn discover(dir: &Path) -> Result<Vec<Transaction>, ProfilerError> {
    let io_err = |path: &Path| {
        let path = path.to_path_buf();
        move |source| ProfilerError::Io { path, source }
    };
    let mut files: Vec<PathBuf> = fs::read_dir(dir)
        .map_err(io_err(dir))?
        .collect::<io::Result<Vec<_>>>()
        .map_err(io_err(dir))?
        .into_iter()
        .map(|e| e.path())
        .filter(|p| {
            p.is_file()
                && p.extension().is_some_and(|ext| ext == "log")
                && p.file_name()
                    .is_some_and(|n| n.to_string_lossy().starts_with("operators_worker"))
        })
        .collect();
    files.sort();

    let mut groups: BTreeMap<u64, Vec<&PathBuf>> = BTreeMap::new();
    for f in &files {
        let name = f.file_name().unwrap_or_default().to_string_lossy();
        groups.entry(txn_index(&name)).or_default().push(f);
    }

    let mut out = Vec::with_capacity(groups.len());
    for (ts, group) in groups {
        let mut workers = Vec::with_capacity(group.len());
        for ops_path in group {
            // Only the callee knows which worker log failed; name it.
            let operators = fs::read_to_string(ops_path).map_err(io_err(ops_path))?;
            // A missing or unreadable channels sibling is not an error; that
            // worker just contributes time without flow.
            let channels = fs::read_to_string(sibling_channels_path(ops_path)).ok();
            workers.push(WorkerLog {
                operators,
                channels,
            });
        }
        out.push(Transaction {
            label: format!("t{ts}"),
            workers,
        });
    }
    Ok(out)
}

/// Transaction index from a log filename: the first `_t<digits>_` marker, as
/// the writer formats it; `0` when absent.
fn txn_index(filename: &str) -> u64 {
    let mut rest = filename;
    while let Some(pos) = rest.find("_t") {
        let tail = &rest[pos + 2..];
        let digits = tail.len() - tail.trim_start_matches(|c: char| c.is_ascii_digit()).len();
        if digits > 0
            && tail[digits..].starts_with('_')
            && let Ok(ts) = tail[..digits].parse()
        {
            return ts;
        }
        rest = tail;
    }
    0
}

/// `operators_worker_t3_1.log` -> `channels_worker_t3_1.log` in place.
fn sibling_channels_path(ops_path: &Path) -> PathBuf {
    let name = ops_path.file_name().unwrap_or_default().to_string_lossy();
    ops_path.with_file_name(name.replacen("operators_worker", "channels_worker", 1))
}

#[cfg(test)]
mod tests {
    use super::*;

    const OPS: &str = "[0, 1]   2   1.000   Input\n";

    fn write_dir(files: &[(&str, &str)]) -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, content) in files {
            fs::write(dir.path().join(name), content).unwrap();
        }
        dir
    }

    #[test]
    fn empty_dir_yields_no_transactions() {
        let dir = tempfile::tempdir().unwrap();
        assert!(discover(dir.path()).unwrap().is_empty());
    }

    #[test]
    fn operator_logs_group_into_one_transaction_each() {
        let dir = write_dir(&[
            ("operators_worker_t0_0.log", OPS),
            ("operators_worker_t3_0.log", OPS),
        ]);
        let labels: Vec<String> = discover(dir.path())
            .unwrap()
            .iter()
            .map(|t| t.label.clone())
            .collect();
        assert_eq!(labels, ["t0", "t3"]);
    }

    #[test]
    fn present_channels_sibling_is_read_missing_is_none() {
        let dir = write_dir(&[
            ("operators_worker_t0_0.log", OPS),
            ("channels_worker_t0_0.log", "[0] 1 0 2 0 0 5 5\n"),
            ("operators_worker_t0_1.log", OPS),
        ]);
        let txns = discover(dir.path()).unwrap();
        assert!(txns[0].workers[0].channels.is_some());
        assert!(txns[0].workers[1].channels.is_none());
    }

    #[test]
    fn missing_directory_is_an_io_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("does-not-exist");
        assert!(matches!(discover(&missing), Err(ProfilerError::Io { .. })));
    }

    #[test]
    fn txn_index_reads_first_t_marker() {
        assert_eq!(txn_index("operators_worker_t7_0.log"), 7);
        assert_eq!(txn_index("operators_worker_t0_12.log"), 0);
    }

    #[test]
    fn txn_index_defaults_to_zero_without_marker() {
        assert_eq!(txn_index("operators_worker.log"), 0);
    }
}
