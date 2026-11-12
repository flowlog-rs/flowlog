//! Work queue for propagating arrangement group boundaries.
//!
//! Local refinement reports changed arrangements; only their incident edges
//! need to run again. Queue membership prevents duplicate pending work.

use std::collections::VecDeque;

use super::super::arrangement_graph::ArrangementGraph;
use super::refinement::refine_edge;
use super::stratum_graph::StratumGraph;

/// Refines groups until no edge can split a group further.
/// Groups only split, so each arrangement can change at most once per new
/// boundary. Fixed arrangements retain their original physical layouts.
pub(super) fn contract(graph: &ArrangementGraph<'_>, subgraph: &mut StratumGraph<'_, '_>) {
    let mut queue: VecDeque<usize> = (0..subgraph.edges.len()).collect();
    let mut queued = vec![true; subgraph.edges.len()];
    while let Some(edge_index) = queue.pop_front() {
        // A split can invalidate this same edge, so allow it to requeue itself.
        queued[edge_index] = false;
        let changed = refine_edge(graph, &subgraph.edges[edge_index], &mut subgraph.nodes);
        for id in changed {
            for &neighbor in &subgraph.nodes[&id].edges {
                if !queued[neighbor] {
                    queued[neighbor] = true;
                    queue.push_back(neighbor);
                }
            }
        }
    }
}
