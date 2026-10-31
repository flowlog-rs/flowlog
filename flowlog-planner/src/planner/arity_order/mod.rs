//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialize`] derives that graph from existing plans.
//! [`stratum_rewritter`] applies choices across strata and propagates changed
//! field positions to consumers; [`layout_permutation`] performs each
//! operator's field, expression and collection metadata remapping.

mod arrangement_graph;
mod initialize;
mod layout_permutation;
mod stratum_rewritter;

use flowlog_parser::Program;
use initialize::initialize;
use stratum_rewritter::Selection;
use tracing::trace;

use super::PlanError;
use super::StratumPlanner;

/// Returns the stratum with selected layouts and join directions applied.
/// `upstream` contains earlier strata in evaluation order; `downstream` holds
/// already processed strata from last to nearest. Downstream choices stay
/// fixed, while references to shared producers follow their changed layouts.
/// The solver is not implemented yet, so selection preserves the plan's
/// existing layouts and join directions.
///
/// # Errors
///
/// Returns an internal error for invalid layout choices, incompatible join
/// keys or invalid field references.
pub(super) fn optimize(
    mut stratum: StratumPlanner,
    upstream: &[StratumPlanner],
    downstream: &mut [StratumPlanner],
    program: &Program,
) -> Result<StratumPlanner, PlanError> {
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

    // Step 5: apply layouts and validate their compatibility.
    // stratum_rewritter visits producers before readers and propagates changed
    // field positions across shared dependencies.
    let selection = Selection {
        layouts: graph.nodes,
        joins: graph.joins,
    };
    // Feed expressions borrow the original plans; release them before
    // modifying the collections they reference.
    drop(graph.edges);
    stratum_rewritter::apply(&mut stratum, downstream, selection)?;
    Ok(stratum)
}
