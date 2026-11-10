//! Per-stratum topology for layout solving.
//!
//! The undirected graph contains arrangement variables; edges borrow their
//! original computation feeds. Contraction refines the arity groups that
//! define permutation domains before shared-group epoch search. Selected
//! choices accumulate separately until all strata have been searched.

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

// Arbitrary work units, not elapsed time. Equal per-stratum ceilings bound
// total scoring work; smaller searches can use less than their ceiling.
const SEARCH_WORK_BUDGET: usize = 100_000_000;

/// Candidate choices retain original field IDs and feed indices. They do not
/// own or rewrite computations; absent entries keep the graph's choices.
#[derive(Debug, Clone, Default)]
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

    /// Installs accumulated choices after all scoring and refinement finish.
    /// Original field correspondence must remain available until then.
    pub(super) fn apply(self, graph: &mut ArrangementGraph<'_>) {
        for (id, layout) in self.layouts {
            *graph.arrangements_mut()[id.index()].layout_mut() = layout;
        }
        for (index, orientation) in self.join_orientations {
            *graph.feeds_mut()[index].join_orientation_mut() = Some(orientation);
        }
    }
}

/// Selects one stratum's layouts while retaining choices for other strata.
/// The original graph remains unchanged until all searches have finished.
/// `stratum_count` includes this stratum and must be nonzero.
/// Returns an internal error for incompatible mandatory key groups.
pub(super) fn optimize(
    graph: &ArrangementGraph<'_>,
    stratum_index: usize,
    stratum_count: usize,
    choice: &mut LayoutChoice,
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
    if let Some(result) = optimize_cost::optimize(
        graph,
        &subgraph,
        stratum_index,
        class_count,
        work_budget,
        choice,
    ) {
        debug!(
            stratum_index,
            baseline_cost = ?result.baseline_cost,
            selected_cost = ?result.cost,
            valid_epochs = result.valid_epochs,
            choice = ?result.choice,
            "selected layout candidate",
        );
        *choice = result.choice;
    }
    Ok(())
}
