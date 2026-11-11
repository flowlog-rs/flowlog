//! Per-stratum topology and contracted layout domains.
//!
//! The undirected graph contains arrangement variables; edges borrow their
//! original computation feeds. Contraction refines the arity groups that
//! define each layout domain. Layout selection remains unimplemented.

mod block_contraction;
mod refinement;
mod stratum_graph;

use tracing::trace;

use self::stratum_graph::StratumGraph;
use super::arrangement_graph::ArrangementGraph;

/// Builds and contracts one stratum's layout domains without changing the
/// graph's layouts or join directions.
pub(super) fn optimize(graph: &ArrangementGraph<'_>, stratum_index: usize) {
    let mut subgraph = StratumGraph::from_graph(graph, stratum_index);
    block_contraction::contract(graph, &mut subgraph);

    let edges = subgraph.edges.iter().enumerate();
    for (edge_index, edge) in edges {
        trace!(
            stratum_index,
            edge_index,
            input = ?edge.input,
            output = edge.output.index(),
            output_input_index = edge.output_input_index,
            feed = ?edge.feed,
            input_path = ?edge.input_path,
            "initialized undirected input incidence",
        );
    }
    for (arrangement, node) in &subgraph.nodes {
        trace!(
            stratum_index,
            arrangement = arrangement.index(),
            fixed = node.fixed,
            edges = ?node.edges,
            key = ?node.key,
            value = ?node.value,
            "initialized contracted layout domain",
        );
    }

    // TODO: Search these contracted groups once a layout selection policy is
    // defined.
}
