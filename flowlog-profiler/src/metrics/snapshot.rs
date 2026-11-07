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
use crate::metrics::operator::Stats;
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
    /// The heap outside every operator, from the operator table's `[]`
    /// row: every byte allocated. This and the two below are `None` when
    /// unmeasured.
    pub outside_alloc_bytes: Option<Stats>,
    /// Of that heap, the bytes not yet freed when the tables were written.
    pub outside_live_bytes: Option<Stats>,
    /// Of that heap, the largest the live bytes have been.
    pub outside_peak_bytes: Option<Stats>,
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
    let mut operators = operator::aggregate(&ops, &flows);
    // The `[]` row is not an operator; keep it out of the operator count.
    let outside = operators.remove(&Addr(vec![]));

    let mut nodes = BTreeMap::new();
    for n in plan.nodes() {
        nodes.insert(n.id, node::node_metrics(n, &operators, &ops, &edges)?);
    }
    Ok(Snapshot {
        label: txn.label,
        num_workers: txn.workers.len(),
        operators_in_log: operators.len(),
        outside_alloc_bytes: outside.as_ref().and_then(|o| o.alloc_bytes.clone()),
        outside_live_bytes: outside.as_ref().and_then(|o| o.live_bytes.clone()),
        outside_peak_bytes: outside.and_then(|o| o.peak_bytes),
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

    /// The plan and two-worker metrics of one profiled run of
    /// `example/graph_analysis/reach.dl` over flowlog-bench's `G5K-0.001`
    /// arcs from source `1`, committed under `testdata/reach/`. The logs are
    /// written into a fresh directory, since [`discover`] scans a directory
    /// rather than taking file contents.
    const REACH_PLAN: &str = include_str!("testdata/reach/ops.json");
    const REACH_LOGS: [(&str, &str); 4] = [
        (
            "operators_worker_t0_0.log",
            include_str!("testdata/reach/operators_worker_t0_0.log"),
        ),
        (
            "operators_worker_t0_1.log",
            include_str!("testdata/reach/operators_worker_t0_1.log"),
        ),
        (
            "channels_worker_t0_0.log",
            include_str!("testdata/reach/channels_worker_t0_0.log"),
        ),
        (
            "channels_worker_t0_1.log",
            include_str!("testdata/reach/channels_worker_t0_1.log"),
        ),
    ];

    fn reach_metrics_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().unwrap();
        for (name, text) in REACH_LOGS {
            std::fs::write(dir.path().join(name), text).unwrap();
        }
        dir
    }

    /// The reach run's logs reduce to their known operator values (flow as
    /// an integer total across the two workers), pinning the reader format
    /// end to end.
    #[test]
    fn reach_logs_reduce_to_known_values() {
        let dir = reach_metrics_dir();
        let raw = discover(dir.path()).unwrap();
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
        assert_eq!(
            arrange.op_name,
            "Arrange: arc [0x7b72930965185fe2], key:(1.0), value:(1.1)"
        );
        // Every arc enters the arrangement: 11887 + 13159 received.
        assert_eq!(arrange.flow.tup_in, Some(25046));
        assert_eq!(arrange.flow.tup_out, Some(25046));
        // Worker peaks 219960 and 237328.
        let peak = arrange
            .peak_bytes
            .as_ref()
            .expect("the run measured memory");
        assert_eq!(
            (peak.mean, peak.min, peak.max),
            (228644.0, 219960.0, 237328.0)
        );
    }

    /// End to end: the reach plan and metrics bind to one snapshot with the
    /// expected shape.
    #[test]
    fn read_binds_the_reach_run() {
        let plan: PlanGraph = serde_json::from_str(REACH_PLAN).expect("ops.json deserializes");
        let dir = reach_metrics_dir();
        let snaps = read(&plan, dir.path()).expect("read succeeds");
        assert_eq!(snaps.len(), 1);
        assert_eq!(snaps[0].label, "t0");
        assert_eq!(snaps[0].num_workers, 2);
        assert!(!snaps[0].nodes.is_empty());
        // The `[]` row is reported as the outside heap, not as one of the
        // 33 operators each worker logged. Worker peaks 251572 and 245422.
        assert_eq!(snaps[0].operators_in_log, 33);
        let outside = snaps[0]
            .outside_peak_bytes
            .as_ref()
            .expect("the run measured memory");
        assert_eq!(
            (outside.mean, outside.min, outside.max),
            (248497.0, 245422.0, 251572.0)
        );
    }

    #[test]
    fn reach_node_heap_spread_uses_per_worker_totals() {
        let plan: PlanGraph = serde_json::from_str(REACH_PLAN).unwrap();
        let dir = reach_metrics_dir();
        let snaps = read(&plan, dir.path()).unwrap();
        let source = &snaps[0].nodes[&1];
        let allocated = source.alloc_bytes.as_ref().unwrap();
        let live = source.live_bytes.as_ref().unwrap();
        let peak = source.peak_bytes.as_ref().unwrap();

        // Source's dedup operators total 59636 allocated and 50528 peak
        // bytes on worker 0; worker 1 reports zero for each operator.
        assert_eq!(
            (
                allocated.mean,
                allocated.var.sqrt(),
                allocated.min,
                allocated.max
            ),
            (29818.0, 29818.0, 0.0, 59636.0)
        );
        assert_eq!(
            (live.mean, live.var, live.min, live.max),
            (0.0, 0.0, 0.0, 0.0)
        );
        assert_eq!(
            (peak.mean, peak.var.sqrt(), peak.min, peak.max),
            (25264.0, 25264.0, 0.0, 50528.0)
        );
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
            "[0, 1]   2   1.000   n/a   n/a   n/a   Input\n",
        )
        .unwrap();
        assert!(matches!(
            read(&plan, dir.path()),
            Err(ProfilerError::MetricsMismatch(_))
        ));
    }
}
