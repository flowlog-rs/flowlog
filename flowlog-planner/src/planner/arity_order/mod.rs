//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialize`] derives that graph from existing plans.

mod arrangement_graph;
mod initialize;

use flowlog_parser::Program;
use initialize::initialize;
use tracing::trace;

use super::StratumPlanner;

/// Returns the stratum unchanged after initializing its physical layout graph.
/// `upstream` contains earlier strata in evaluation order; `downstream` holds
/// already processed strata from last to nearest. The solver and layout
/// application are not implemented yet.
#[must_use]
pub(super) fn optimize(
    stratum: StratumPlanner,
    upstream: &[StratumPlanner],
    downstream: &[StratumPlanner],
    program: &Program,
) -> StratumPlanner {
    // Step 1: initialize the graph with existing layouts and join directions.
    // Projection fusion stays within graph feeds; union inputs and recursive
    // dependencies retain their original producers.
    let graph = initialize(&stratum, upstream, downstream, program);
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

    // TODO: Step 2: eliminate fixed choices and contract only blocks whose
    // consecutive placement is proved to preserve an optimum.

    // TODO: Step 3: select a small-product cut of the undirected factor
    // graph and enumerate its assignments, leaving a forest each time.

    // TODO: Step 4: solve each forest with messages in both directions.
    // Enumerate legal K/V orders and join orientations during transitions,
    // coupling join key orders and sharing one target layout across union
    // feeds. Compute costs on demand with a cache keyed by arity and relative
    // permutation. Read external and downstream layouts as constants, include
    // their conversion costs, and prefer main on ties.
    // Evaluate the complete plan cost for the selected layouts and directions.

    // TODO: Step 5: apply selected layouts and join directions, remap their
    // consumers across shared dependencies, and validate compatible keys.
    stratum
}
