//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialization`] derives that graph from existing plans and composes
//! projection feeds while retaining their original plan locations.
//! [`layout_optimizer`] builds each stratum's undirected solving graph and
//! refines its field groups before the reserved solving stage.
//! [`rewrite`] validates choices and propagates selected layouts and join
//! directions through the original stratum plans.

mod arrangement_graph;
mod initialization;
mod layout_optimizer;
mod rewrite;

use flowlog_parser::Program;
use initialization::build_graph;
use rewrite::LayoutAssignment;
use tracing::trace;

use super::PlanError;
use super::StratumPlanner;

/// Initializes one graph, selects layouts from consumers to producers, then
/// applies them in evaluation order. The solver is not implemented yet, so
/// the assignment preserves existing layouts and join directions.
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
    let graph = build_graph(plan, program)?;
    let arrangements = graph.arrangements().iter().enumerate();
    for (id, arrangement) in arrangements {
        trace!(
            id,
            origin = ?arrangement.origin(),
            layout = ?arrangement.layout(),
            inputs = ?arrangement.inputs(),
            "initialized arrangement",
        );
    }
    let feeds = graph.feeds().iter().enumerate();
    for (id, feed) in feeds {
        trace!(id, ?feed, "initialized feed");
    }
    trace!(
        arrangements = graph.arrangements().len(),
        feeds = graph.feeds().len(),
        "initialized attribute-ordering physical graph",
    );

    let solving_order = (0..plan.len()).rev();
    for stratum_index in solving_order {
        layout_optimizer::optimize(&graph, stratum_index);
    }

    // Step 5: release feed borrows before modifying the plans they reference.
    let assignment = LayoutAssignment::from_graph(graph);
    rewrite::apply(plan, program, assignment)
}
