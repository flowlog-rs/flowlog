//! Direct field alignment with randomized dedup restarts.
//!
//! Field domains and mappings are prepared once. `optimize` visits
//! one neighbor per variable in each sweep; the cost model remains separate.

mod calc_cost;
mod direct_mapping;
mod feed_cost;
mod optimize;
mod variables;

use self::calc_cost::prepare_costs;
use self::direct_mapping::Mapping;
use self::variables::Variables;
use super::super::arrangement_graph::ArrangementGraph;
use super::LayoutChoice;
use crate::planner::PlanError;

/// Builds field domains and correspondence directly from the arrangement graph.
/// The original graph remains unchanged until selection finishes. Returns an
/// internal error for incompatible join keys or a residual traversal cycle.
pub(in crate::planner::arity_order) fn optimize(
    graph: &ArrangementGraph<'_>,
    stratum_count: usize,
) -> Result<LayoutChoice, PlanError> {
    let costs = prepare_costs(graph);
    let variables = Variables::new(graph)?;
    let mappings: Vec<_> = costs
        .iter()
        .map(|cost| Mapping::new(graph, &variables, cost))
        .collect();
    optimize::optimize(graph, &variables, &costs, &mappings, stratum_count)
}
