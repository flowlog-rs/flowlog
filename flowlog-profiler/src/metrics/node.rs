//! The transformation layer: operator metrics bound to the plan, one
//! [`NodeMetrics`] per plan node.
//!
//! A transformation is one logical operation of the compiled plan,
//! recorded as a [`Node`] and occupying a small run of timely operators
//! at runtime. Its time is the sum of those members' time; its flow is
//! not the sum of theirs: an edge between two members is internal
//! traffic, and a fanned-out exit port ships the same tuples to every
//! consumer, so flow is measured at the group boundary instead: what
//! crosses in, what crosses out, totaled across workers.
//!
//! - [`node_metrics`]: bind the aggregated operators to one plan node

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use crate::Addr;
use crate::ProfilerError;
use crate::metrics::cardinality::Cardinality;
use crate::metrics::edge::Edge;
use crate::metrics::operator::OperatorMetrics;
use crate::metrics::operator::Stats;
use crate::plan::Node;

// =============================================================================
// Plan binding
// =============================================================================

/// Measured metrics for one plan node.
#[derive(Debug, Clone)]
pub struct NodeMetrics {
    /// This node's operators, address-sorted.
    pub operators: Vec<OperatorMetrics>,
    /// Time summed over the node's operators (per-worker distribution).
    pub activations: Stats,
    pub active_ms: Stats,
    /// Tuples crossing the node's group boundary (a fully-internal group
    /// or an unobservable edge stays unmeasured).
    pub flow: Cardinality,
}

/// One plan node's metrics: time summed over its member operators, flow
/// measured at the group boundary across all workers. Errors when a
/// member address is absent from the aggregated log.
pub(crate) fn node_metrics(
    node: &Node,
    operators: &BTreeMap<Addr, OperatorMetrics>,
    worker_edges: &[Vec<Edge>],
) -> Result<NodeMetrics, ProfilerError> {
    let members: BTreeSet<Addr> = node.operators.iter().cloned().collect();
    let mut ops = Vec::new();
    let mut activations = Stats::default();
    let mut active_ms = Stats::default();
    for addr in &members {
        let op = operators.get(addr).ok_or_else(|| {
            ProfilerError::MetricsMismatch(format!(
                "node '{}' is mapped to addr {addr}, absent from the metrics log",
                node.id
            ))
        })?;
        activations = &activations + &op.activations;
        active_ms = &active_ms + &op.active_ms;
        ops.push(op.clone());
    }
    let flow = boundary_flow(worker_edges, &members);
    Ok(NodeMetrics {
        operators: ops,
        activations,
        active_ms,
        flow,
    })
}

// =============================================================================
// Boundary flow
// =============================================================================

/// A group's total boundary flow across workers: the tuples entering and
/// leaving it. A direction is `None` when no worker measured it, never zero.
fn boundary_flow(worker_edges: &[Vec<Edge>], members: &BTreeSet<Addr>) -> Cardinality {
    Cardinality::sum(
        worker_edges
            .iter()
            .map(|edges| worker_boundary(edges, members)),
    )
}

/// One worker's boundary flow for a group: the tuples entering from outside
/// and leaving it, a fanned-out output port counted once. A direction is
/// `None` when the group has no such edge, or a crossing edge is
/// unobservable.
fn worker_boundary(edges: &[Edge], members: &BTreeSet<Addr>) -> Cardinality {
    let inside = |a: &Option<Addr>| a.as_ref().is_some_and(|a| members.contains(a));

    // Crossing in-edges each carry distinct data: receives sum.
    let mut ins: Vec<Option<i64>> = Vec::new();
    // A fanned-out port ships the same tuples on every edge: max within a
    // port here, then sum across ports below.
    let mut out_ports: BTreeMap<(&Addr, u32), Option<i64>> = BTreeMap::new();
    for e in edges {
        if inside(&e.tgt) && !inside(&e.src) {
            ins.push(e.recvd);
        }
        if let Some(src) = &e.src
            && members.contains(src)
            && !inside(&e.tgt)
        {
            let slot = out_ports.entry((src, e.src_port)).or_insert(Some(0));
            *slot = slot.zip(e.sent).map(|(a, b)| a.max(b));
        }
    }
    Cardinality {
        tup_in: strict_sum(ins),
        tup_out: strict_sum(out_ports.into_values()),
    }
}

/// Sum of measured values where one unmeasured value poisons the total (a
/// partial sum would read as complete) and no values at all stays
/// unmeasured.
fn strict_sum(values: impl IntoIterator<Item = Option<i64>>) -> Option<i64> {
    let values: Vec<Option<i64>> = values.into_iter().collect();
    (!values.is_empty())
        .then(|| values.into_iter().sum::<Option<i64>>())
        .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::channel::test_support::addr;
    use crate::metrics::channel::test_support::chan;
    use crate::metrics::channel::test_support::names;
    use crate::metrics::edge::resolve;

    fn card(tup_in: Option<i64>, tup_out: Option<i64>) -> Cardinality {
        Cardinality { tup_in, tup_out }
    }

    /// The outer half (arrange to scope op) and its bridged inner
    /// continuation are the same output port; boundary flow over the
    /// arrange's group must count it once.
    #[test]
    fn entering_a_scope_does_not_double_an_arranges_boundary_output() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 2], "FlatMap"),
            (&[0, 9], "Iterative"),
            (&[0, 9, 1], "FlatMap"),
        ]);
        let (_, edges) = resolve(
            &[
                chan(&[0], (2, 0), (1, 0), false, 1000, 1000),
                chan(&[0], (1, 0), (9, 3), true, 5, 5),
                chan(&[0, 9], (0, 3), (1, 0), true, 5, 5),
            ],
            &ns,
        );
        let members: BTreeSet<Addr> = [addr(&[0, 1])].into();
        assert_eq!(
            boundary_flow(&[edges], &members),
            card(Some(1000), Some(1000))
        );
    }

    /// For a FlatMap-to-Arrange chain owned by one node, the internal edge
    /// must not contribute; the entry and the (fanned-out) arranged exits
    /// must.
    #[test]
    fn boundary_of_a_pipeline_counts_only_crossing_edges() {
        let ns = names(&[
            (&[0, 1], "FlatMap"),
            (&[0, 2], "ArrangeByKey"),
            (&[0, 3], "Join"),
            (&[0, 4], "Join"),
            (&[0, 5], "Input"),
        ]);
        let (_, edges) = resolve(
            &[
                chan(&[0], (5, 0), (1, 0), false, 80, 80),
                chan(&[0], (1, 0), (2, 0), false, 60, 60),
                chan(&[0], (2, 0), (3, 0), true, 2, 2),
                chan(&[0], (2, 0), (4, 0), true, 2, 2),
            ],
            &ns,
        );
        let members: BTreeSet<Addr> = [addr(&[0, 1]), addr(&[0, 2])].into();
        assert_eq!(boundary_flow(&[edges], &members), card(Some(80), Some(60)));
    }

    #[test]
    fn boundary_is_unknown_when_a_crossing_edge_is() {
        let ns = names(&[(&[0, 1], "Reduce"), (&[0, 2], "AsCollection")]);
        let (_, edges) = resolve(&[chan(&[0], (1, 0), (2, 0), true, 3, 3)], &ns);
        let members: BTreeSet<Addr> = [addr(&[0, 2])].into();
        assert_eq!(boundary_flow(&[edges], &members), card(None, None));
    }

    #[test]
    fn boundary_is_none_for_a_direction_with_no_crossing_edge() {
        let ns = names(&[(&[0, 1], "FlatMap"), (&[0, 5], "Input")]);
        let (_, edges) = resolve(&[chan(&[0], (5, 0), (1, 0), false, 80, 80)], &ns);
        let members: BTreeSet<Addr> = [addr(&[0, 5])].into();
        assert_eq!(
            boundary_flow(std::slice::from_ref(&edges), &members),
            card(None, Some(80))
        );
        assert_eq!(boundary_flow(&[edges], &BTreeSet::new()), card(None, None));
    }

    /// The cross-worker total sums each worker's crossing volume; a direction
    /// absent on every worker stays `None`.
    #[test]
    fn boundary_flow_sums_across_workers() {
        let ns = names(&[(&[0, 1], "FlatMap"), (&[0, 5], "Input")]);
        let (_, w0) = resolve(&[chan(&[0], (5, 0), (1, 0), false, 80, 80)], &ns);
        let (_, w1) = resolve(&[chan(&[0], (5, 0), (1, 0), false, 50, 50)], &ns);
        let members: BTreeSet<Addr> = [addr(&[0, 1])].into();
        assert_eq!(boundary_flow(&[w0, w1], &members), card(Some(130), None));
    }
}
