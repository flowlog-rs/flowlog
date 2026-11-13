//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialization`] derives that graph from existing plans and composes
//! projection feeds while retaining their original plan locations.
//! [`layout_optimizer`] builds each stratum's undirected solving graph and
//! runs two direct-mapping sweeps within a work budget.
//! [`rewrite`] validates choices and propagates selected layouts and join
//! directions through the original stratum plans.

mod arrangement_graph;
mod initialization;
mod layout_optimizer;
mod rewrite;

use std::collections::HashMap;

use flowlog_parser::Program;
use initialization::build_graph;
use rewrite::LayoutAssignment;
use tracing::trace;

use self::arrangement_graph::ArrangementOrigin;
use super::PlanError;
use super::StratumPlanner;

/// Initializes one graph, selects layouts from consumers to producers, then
/// applies them in evaluation order. Returns declaration-column orders for
/// the input dedups; their raw inputs have no assumed sorting order.
///
/// # Errors
///
/// Returns an internal error when a binding lacks declaration or mutability
/// metadata, an aggregation lacks its declared min/max column, or a collection
/// producer cannot be resolved, or selected layouts are incompatible.
pub(super) fn optimize(
    plan: &mut [StratumPlanner],
    program: &Program,
) -> Result<HashMap<u64, Vec<usize>>, PlanError> {
    // Step 1: initialize the graph with existing layouts and join directions.
    // Projection fusion stays within graph feeds; union inputs and recursive
    // dependencies retain their original producers.
    let mut graph = build_graph(plan, program)?;
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

    let choice = layout_optimizer::optimize(&graph, plan.len())?;
    choice.apply(&mut graph);
    let input_orders: HashMap<u64, Vec<usize>> = graph
        .arrangements()
        .iter()
        .filter_map(|arrangement| match arrangement.origin() {
            ArrangementOrigin::InputDedup {
                relation_fingerprint,
            } => Some((relation_fingerprint, arrangement.layout().key().to_vec())),
            ArrangementOrigin::Transformation(_)
            | ArrangementOrigin::Dedup { .. }
            | ArrangementOrigin::Aggregation { .. }
            | ArrangementOrigin::AggregationExit { .. } => None,
        })
        .collect();

    // Step 5: release feed borrows before modifying the plans they reference.
    let assignment = LayoutAssignment::from_graph(graph);
    rewrite::apply(plan, program, assignment)?;
    Ok(input_orders)
}
