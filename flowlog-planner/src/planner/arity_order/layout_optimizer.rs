//! Per-stratum topology for layout solving.
//!
//! The undirected graph contains arrangement variables; edges borrow their
//! original computation feeds. Contraction refines the arity groups that
//! define the permutation domains for the reserved solving stage.

mod block_contraction;
mod refinement;
mod stratum_graph;

use tracing::trace;

use self::stratum_graph::StratumGraph;
use self::stratum_graph::group_permutations;
use super::arrangement_graph::ArrangementGraph;

/// Builds one stratum's solving graph and refines its field groups. The solver
/// remains unimplemented, leaving original layouts and join choices unchanged.
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
            first_key_order = ?group_permutations(&node.key).next(),
            first_value_order = ?group_permutations(&node.value).next(),
            "initialized arrangement layout domain",
        );
    }

    // TODO: choose and enumerate a layout-variable cut, then solve the forest
    // with messages in either direction. Read original flows at transitions
    // to enforce join keys and evaluate costs. Jointly evaluate edges from
    // each output input, preserving shared feed choices across consumers.
    // Enumerate arity groups and retain the original layout on ties.
}
