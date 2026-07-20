//! The output of the whole metrics module: one [`Snapshot`] of measured
//! facts per committed transaction, keyed to the plan it was read
//! against.
//!
//! - [`read`]: the crate entry and the only way in, per transaction,
//!   measure each worker, fold the workers per operator, bind each
//!   transformation, and pack the result

use std::collections::BTreeMap;
use std::path::Path;

use crate::Addr;
use crate::PlanGraph;
use crate::ProfilerError;
use crate::metrics::cardinality::Cardinality;
use crate::metrics::channel;
use crate::metrics::edge;
use crate::metrics::edge::Edge;
use crate::metrics::node;
use crate::metrics::node::NodeMetrics;
use crate::metrics::operator;
use crate::metrics::operator::Operator;
use crate::metrics::transaction::Transaction;
use crate::metrics::transaction::WorkerLog;
use crate::metrics::transaction::discover;

/// One committed transaction's measured metrics (`t0` for batch mode),
/// keyed to the plan by node id. A consumer joins it against the
/// [`crate::PlanGraph`] and derives its own view.
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// Transaction id (`t0` for batch mode); the only field not in the plan.
    pub label: String,
    /// Per-worker file count; sets the scale of the time
    /// [`Stats`](crate::metrics::Stats).
    pub num_workers: usize,
    /// Operator rows in the log, mapped and unmapped alike; the unmapped
    /// ones are scope shells the plan does not model.
    pub operators_in_log: usize,
    /// Measured metrics per plan node id.
    pub nodes: BTreeMap<usize, NodeMetrics>,
}

/// Read one run's metrics against the plan it was compiled from: one
/// [`Snapshot`] per committed transaction. Errors on an unreadable metrics
/// directory, or on a plan-predicted operator address absent from the log
/// (address prediction drifted, or the metrics are from a different run).
pub fn read(plan: &PlanGraph, dir: &Path) -> Result<Vec<Snapshot>, ProfilerError> {
    discover(dir)?
        .into_iter()
        .map(|txn| snapshot(plan, txn))
        .collect()
}

/// Bind one transaction to the plan: measure each worker, fold the
/// workers into per-operator metrics, then attribute the result to plan
/// nodes.
fn snapshot(plan: &PlanGraph, txn: Transaction) -> Result<Snapshot, ProfilerError> {
    // One entry per worker, in worker order: the layers below zip these
    // by index.
    let mut ops = Vec::with_capacity(txn.workers.len());
    let mut flows = Vec::with_capacity(txn.workers.len());
    let mut edges = Vec::with_capacity(txn.workers.len());
    for w in &txn.workers {
        let (o, f, e) = measure(w);
        ops.push(o);
        flows.push(f);
        edges.push(e);
    }
    let operators = operator::aggregate(&ops, &flows);

    let mut nodes = BTreeMap::new();
    for n in plan.nodes() {
        nodes.insert(n.id, node::node_metrics(n, &operators, &edges)?);
    }
    Ok(Snapshot {
        label: txn.label,
        num_workers: txn.workers.len(),
        operators_in_log: operators.len(),
        nodes,
    })
}

/// Measure one worker: parse both tables and resolve the channel volumes
/// into per-operator flow. A worker without a channels table contributes
/// time only: flow all-unmeasured, no edges.
fn measure(
    log: &WorkerLog,
) -> (
    BTreeMap<Addr, Operator>,
    BTreeMap<Addr, Cardinality>,
    Vec<Edge>,
) {
    let ops = operator::operators(&log.operators);
    let names: BTreeMap<Addr, String> = ops
        .iter()
        .map(|(a, r)| (a.clone(), r.op_name.clone()))
        .collect();
    let rows = log
        .channels
        .as_deref()
        .map(channel::channels)
        .unwrap_or_default();
    let (flow, edges) = edge::resolve(&rows, &names);
    (ops, flow, edges)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Addr;

    /// The committed reach fixtures reduce to their known operator values
    /// (flow as an integer total across the two workers), guarding fixture
    /// drift and a reader-format change in one shot.
    #[test]
    fn committed_fixtures_reduce_to_known_values() {
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../flowlog-visualizer/examples/metrics"
        );
        let raw = discover(Path::new(dir)).unwrap();
        assert_eq!(raw.len(), 1);
        assert_eq!(raw[0].workers.len(), 2);
        let mut ops = Vec::new();
        let mut flows = Vec::new();
        for w in &raw[0].workers {
            let (o, f, _) = measure(w);
            ops.push(o);
            flows.push(f);
        }
        let operators = operator::aggregate(&ops, &flows);
        let arrange = &operators[&Addr(vec![0, 13])];
        assert_eq!(arrange.op_name, "ArrangeByKey");
        assert_eq!(arrange.flow.tup_in, Some(19951));
        assert_eq!(arrange.flow.tup_out, Some(19951));
    }

    /// End to end: the committed reach plan + metrics bind to one snapshot
    /// with the expected shape.
    #[test]
    fn read_binds_the_reach_fixture() {
        let plan: PlanGraph = serde_json::from_str(include_str!(
            "../../../flowlog-visualizer/examples/ops.json"
        ))
        .expect("fixture ops.json deserializes");
        let dir = concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../flowlog-visualizer/examples/metrics"
        );
        let snaps = read(&plan, Path::new(dir)).expect("read succeeds");
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].label, "t0");
        assert_eq!(snaps[0].num_workers, 2);
        assert!(!snaps[0].nodes.is_empty());
    }

    /// A plan node mapped to an address the log never recorded is a hard
    /// error: address prediction drifted, or the metrics are another run's.
    #[test]
    fn predicted_address_absent_from_log_is_a_mismatch() {
        let plan: PlanGraph = serde_json::from_str(
            r#"{"rules":[],"nodes":[
                {"id":0,"name":"a","block":"input","parents":[],"fingerprint":null,"operators":[[0,99]]}]}"#,
        )
        .unwrap();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("operators_worker_t0_0.log"),
            "[0, 1]   2   1.000   Input\n",
        )
        .unwrap();
        assert!(matches!(
            read(&plan, dir.path()),
            Err(ProfilerError::MetricsMismatch(_))
        ));
    }
}
