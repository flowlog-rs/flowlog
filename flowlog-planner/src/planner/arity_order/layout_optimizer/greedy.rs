//! Direct field alignment with randomized dedup restarts.
//!
//! Contracted domains and field mappings are prepared once. `optimize` visits
//! one neighbor per variable in each sweep; the cost model remains separate.

mod calc_cost;
mod direct_mapping;
mod feed_cost;
mod optimize;
mod variables;

use std::collections::BTreeMap;

use self::calc_cost::prepare_costs;
use self::direct_mapping::Mapping;
use self::variables::Variables;
use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementOrigin;
use super::LayoutChoice;
use super::block_contraction;
use super::stratum_graph::StratumGraph;
use crate::planner::PlanError;

/// Builds native contracted domains and field correspondence before search.
/// The original graph remains unchanged until selection finishes. Returns an
/// internal error for incompatible join keys or a residual traversal cycle.
pub(in crate::planner::arity_order) fn optimize(
    graph: &ArrangementGraph<'_>,
    stratum_count: usize,
) -> Result<LayoutChoice, PlanError> {
    let costs = prepare_costs(graph);
    let mut nodes = BTreeMap::new();
    for stratum_index in 0..stratum_count {
        let native =
            |origin: ArrangementOrigin| origin.stratum_index().unwrap_or(0) == stratum_index;
        let mut subgraph = StratumGraph::from_graph(graph, stratum_index);
        block_contraction::contract(graph, &mut subgraph);
        for (id, node) in subgraph.nodes {
            if native(graph.arrangements()[id.index()].origin()) {
                nodes.insert(id, node);
            }
        }
    }
    let variables = Variables::new(graph, &nodes)?;
    let mappings: Vec<_> = costs
        .iter()
        .map(|cost| Mapping::new(graph, &variables, cost))
        .collect();
    optimize::optimize(graph, &variables, &costs, &mappings, stratum_count)
}
