//! Physical attribute layouts across stratum plans and shared dependencies.
//!
//! [`arrangement_graph`] represents arrangements, their feeds and layout
//! choices.
//! [`initialization`] derives that graph from existing plans and composes
//! projection feeds while retaining their original plan locations.
//! [`layout_optimizer`] builds each stratum's undirected solving graph and
//! refines its field groups before the reserved solving stage.
//! [`rewrite`] validates the graph and propagates its layouts and join
//! directions through the original stratum plans. Layout selection is pending;
//! the graph retains the existing layouts and join directions.

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

/// Initializes layout domains from consumers to producers, then rewrites the
/// plan in evaluation order. Selection is pending, so existing layouts and
/// join directions are preserved. Returns declaration-column orders for input
/// dedups; their raw inputs have no assumed sorting order.
///
/// # Errors
///
/// Returns an internal error when a binding lacks declaration or mutability
/// metadata, an aggregation lacks its declared min/max column, a collection
/// producer cannot be resolved, or graph layouts are incompatible.
pub(super) fn optimize(
    plan: &mut [StratumPlanner],
    program: &Program,
) -> Result<HashMap<u64, Vec<usize>>, PlanError> {
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

    // Release feed borrows before modifying the plans they reference.
    let assignment = LayoutAssignment::from_graph(graph);
    rewrite::apply(plan, program, assignment)?;
    Ok(input_orders)
}
