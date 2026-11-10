//! Per-stratum topology for layout solving.
//!
//! The undirected graph contains arrangement variables; edges borrow their
//! original computation feeds. Contraction refines the arity groups that
//! define permutation domains before shared-group epoch search. Candidates
//! are scored here without changing the original graph or stratum plans.

mod block_contraction;
mod calc_cost;
mod feed_cost;
mod group_merge;
mod optimize_cost;
mod refinement;
mod stratum_graph;

use std::collections::BTreeMap;

use tracing::debug;
use tracing::trace;

use self::stratum_graph::StratumGraph;
use super::arrangement_graph::ArrangementGraph;
use super::arrangement_graph::ArrangementId;
use super::arrangement_graph::AttributeLayout;
use super::arrangement_graph::JoinOrientation;
use crate::planner::PlanError;

// Arbitrary work units, not elapsed time. Each stratum receives an equal
// share; increasing this allows more candidate and join-direction scores.
const SEARCH_WORK_BUDGET: usize = 10_000_000;

/// Candidate choices retain original field IDs and feed indices. They do not
/// own or rewrite computations; absent entries keep the graph's choices.
#[derive(Debug, Default)]
pub(super) struct LayoutChoice {
    layouts: BTreeMap<ArrangementId, AttributeLayout>,
    join_orientations: BTreeMap<usize, JoinOrientation>,
}

impl LayoutChoice {
    #[must_use]
    fn layout<'layout>(
        &'layout self,
        graph: &'layout ArrangementGraph<'_>,
        id: ArrangementId,
    ) -> &'layout AttributeLayout {
        self.layouts
            .get(&id)
            .unwrap_or_else(|| graph.arrangements()[id.index()].layout())
    }

    #[must_use]
    fn orientation(&self, graph: &ArrangementGraph<'_>, feed_index: usize) -> JoinOrientation {
        self.join_orientations
            .get(&feed_index)
            .copied()
            .or_else(|| graph.feeds()[feed_index].join_orientation())
            .unwrap_or(JoinOrientation::Original)
    }
}

/// Builds one stratum's solving graph and reports its best modeled candidate.
/// Candidate choices are not applied to the original layouts or join sides.
/// `stratum_count` includes this stratum and must be nonzero.
/// Returns an internal error for incompatible mandatory key groups.
pub(super) fn optimize(
    graph: &ArrangementGraph<'_>,
    stratum_index: usize,
    stratum_count: usize,
) -> Result<(), PlanError> {
    let mut subgraph = StratumGraph::from_graph(graph, stratum_index);
    // Refinement creates independent identities when splitting a group;
    // running it after merging would disconnect the shared classes.
    block_contraction::contract(graph, &mut subgraph);
    let class_count = group_merge::merge(graph, &mut subgraph)?;

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
            "initialized merged layout domain",
        );
    }

    let work_budget = SEARCH_WORK_BUDGET / stratum_count;
    if let Some(result) =
        optimize_cost::optimize(graph, &subgraph, stratum_index, class_count, work_budget)
    {
        debug!(
            stratum_index,
            baseline_cost = ?result.baseline_cost,
            selected_cost = ?result.cost,
            valid_epochs = result.valid_epochs,
            choice = ?result.choice,
            "selected modeled layout candidate without applying it",
        );
    }
    Ok(())
}
