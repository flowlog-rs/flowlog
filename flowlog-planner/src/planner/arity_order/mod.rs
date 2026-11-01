//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialize`] derives that graph from existing plans.
//! [`transformation_fusion`] composes projections while retaining shared
//! upstream feeds.
//! [`layout_validation`] checks selected identities and layout constraints.
//! [`stratum_rewritter`] applies choices across strata and propagates changed
//! field positions to consumers; [`layout_permutation`] performs each
//! operator's field, expression and collection metadata remapping.

mod arrangement_graph;
mod initialize;
mod layout_permutation;
mod layout_validation;
mod stratum_rewritter;
mod transformation_fusion;

use flowlog_parser::Program;
use initialize::initialize;
use stratum_rewritter::Selection;
use tracing::trace;

use super::PlanError;
use super::StratumPlanner;

/// Initializes one graph, selects layouts from consumers to producers, then
/// applies them in evaluation order. The solver is not implemented yet, so
/// selection preserves existing layouts and join directions.
///
/// # Errors
///
/// Returns an internal error when a binding lacks declaration or mutability
/// metadata, an aggregation lacks its declared min/max column, or a collection
/// producer cannot be resolved, or selected layouts are incompatible.
pub(super) fn optimize(plan: &mut [StratumPlanner], program: &Program) -> Result<(), PlanError> {
    // Step 1: initialize the graph with existing layouts and join directions.
    // Projection fusion stays within graph feeds; union inputs and recursive
    // dependencies retain their original producers.
    let graph = initialize(plan, program)?;
    for (node, order) in &graph.nodes {
        trace!(?node, ?order, "initialized arrangement order");
    }
    for (join, orientation) in &graph.joins {
        trace!(?join, ?orientation, "initialized join orientation");
    }
    for edge in &graph.edges {
        trace!(
            input = ?edge.input,
            output = ?edge.output,
            "initialized arrangement provenance",
        );
    }
    trace!(
        arrangements = graph.nodes.len(),
        edges = graph.edges.len(),
        "initialized attribute-ordering physical graph",
    );

    for stratum_index in (0..plan.len()).rev() {
        trace!(stratum_index, "physical layout solving is not implemented");

        // TODO: Step 2: eliminate fixed choices and contract only blocks whose
        // consecutive placement is proved to preserve an optimum.

        // TODO: Step 3: select a small-product cut of the undirected factor
        // graph and enumerate its assignments, leaving a forest each time.

        // TODO: Step 4: solve each forest with messages in both directions.
        // Enumerate legal K/V orders and join orientations during transitions,
        // coupling join key orders and sharing one target layout across union
        // feeds. Cache costs by arity and relative permutation. Keep upstream
        // and already selected downstream layouts fixed for this stratum.
        // Record choices in the graph before visiting the preceding stratum,
        // evaluate complete plan costs, and prefer main on ties.
    }

    // Step 5: release feed borrows before modifying the plans they reference.
    let selection = Selection {
        layouts: graph.nodes,
        joins: graph.joins,
    };
    drop(graph.edges);
    stratum_rewritter::apply(plan, program, selection)
}
