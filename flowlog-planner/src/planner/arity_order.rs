//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialization`] derives that graph from existing plans and composes
//! projection feeds while retaining their original plan locations.

mod arrangement_graph;
mod initialization;

use flowlog_parser::Program;
use initialization::build_graph;
use tracing::trace;

use super::PlanError;
use super::StratumPlanner;

/// Initializes one graph for all strata without changing their plans and
/// traces it. Layout solving and its application to the plans are not
/// implemented yet, so the plans leave as they came.
///
/// # Errors
///
/// Returns an internal error when a binding lacks declaration or mutability
/// metadata, an aggregation lacks its declared min/max column, or a collection
/// producer cannot be resolved.
pub(super) fn optimize(plan: &[StratumPlanner], program: &Program) -> Result<(), PlanError> {
    let graph = build_graph(plan, program)?;
    let arrangements = graph.arrangements.iter().enumerate();
    for (id, arrangement) in arrangements {
        trace!(
            id,
            origin = ?arrangement.origin,
            layout = ?arrangement.layout,
            inputs = ?arrangement.inputs,
            "initialized arrangement",
        );
    }
    let feeds = graph.feeds.iter().enumerate();
    for (id, feed) in feeds {
        trace!(id, ?feed, "initialized feed");
    }
    trace!(
        arrangements = graph.arrangements.len(),
        feeds = graph.feeds.len(),
        "initialized attribute-ordering physical graph",
    );

    // TODO: solve the layouts stratum by stratum from the last one back,
    // consumers before producers, with the upstream and already chosen
    // downstream layouts fixed: contract the choices a fixed layout forces,
    // cut the factor graph into forests, and pass costs along each forest in
    // both directions, choosing key/value orders and join orientations. Then
    // apply the chosen layouts and orientations to the plans, remapping
    // shared consumers. The solver's design is what unblocks this.
    Ok(())
}
