//! Layout legality and the row orders required by binding inputs.
//!
//! Assignment validation couples all feeds of a binding. Per-transformation
//! validation checks key compatibility and resolves the original input sides.

use std::collections::HashMap;

use flowlog_parser::Program;

use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::JoinOrientation;
use super::super::arrangement_graph::Source;
use super::super::arrangement_graph::TransformationLocation;
use super::super::initialization::build_graph;
use super::LayoutAssignment;
use super::upstream_rewrite::RewrittenCollection;
use crate::planner::BindingOperation;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;

/// Validates arrangement choices and derives row orders for binding producers.
/// Returned orders use each transformation's original output-column positions,
/// not declaration column IDs. All feeds of a union must emit the same order.
/// Returns an internal error for invalid choices or conflicting shared outputs.
pub(super) fn assignment(
    assignment: &LayoutAssignment,
    plan: &[StratumPlanner],
    program: &Program,
) -> Result<HashMap<TransformationLocation, AttributeLayout>, PlanError> {
    let graph = build_graph(plan, program)?;
    let arrangement_by_origin: HashMap<_, _> = graph
        .arrangements()
        .iter()
        .map(|arrangement| (arrangement.origin(), arrangement))
        .collect();
    let mut binding_feed_layouts = HashMap::new();
    for (arrangement_origin, selected_layout) in &assignment.arrangement_layouts {
        let original_arrangement =
            arrangement_by_origin
                .get(arrangement_origin)
                .ok_or_else(|| {
                    PlanError::internal(format!(
                        "layout selects a missing boundary {arrangement_origin:?}"
                    ))
                })?;
        let sorted_columns = |columns: &[usize]| {
            let mut sorted = columns.to_vec();
            sorted.sort_unstable();
            sorted
        };
        if sorted_columns(selected_layout.key())
            != sorted_columns(original_arrangement.layout().key())
            || sorted_columns(selected_layout.value())
                != sorted_columns(original_arrangement.layout().value())
        {
            return Err(PlanError::internal(format!(
                "layout {selected_layout:?} does not permute the blocks of {arrangement_origin:?}"
            )));
        }
        if matches!(
            arrangement_origin,
            ArrangementOrigin::AggregationExit { .. }
        ) && selected_layout != original_arrangement.layout()
        {
            return Err(PlanError::internal(format!(
                "layout changes fixed aggregation exit {arrangement_origin:?}"
            )));
        }
    }
    for location in assignment.join_orientations.keys() {
        if !graph
            .feeds()
            .iter()
            .any(|feed| feed.location() == *location && feed.join_orientation().is_some())
        {
            return Err(PlanError::internal(format!(
                "orientation selects a non-join or missing step {location:?}"
            )));
        }
    }
    for arrangement in graph.arrangements() {
        let (stratum_index, relation_fingerprint) = match arrangement.origin() {
            ArrangementOrigin::Dedup {
                stratum_index,
                relation_fingerprint,
                ..
            }
            | ArrangementOrigin::Aggregation {
                stratum_index,
                relation_fingerprint,
                ..
            } => (stratum_index, relation_fingerprint),
            ArrangementOrigin::Transformation(_) | ArrangementOrigin::AggregationExit { .. } => {
                continue;
            }
        };
        let binding = &plan[stratum_index].relation_bindings()[&relation_fingerprint];
        let selected_layout = assignment
            .arrangement_layouts
            .get(&arrangement.origin())
            .unwrap_or_else(|| arrangement.layout());
        let input_arity = binding.inputs()[0].arity().1;
        // A binding names declaration columns, whereas a feed permutation
        // names positions in its original output. For example, changing
        // `(b,c,a)` to `(c,a,b)` requires feed positions `[1,2,0]`.
        let feed_column_order = match binding.operation() {
            BindingOperation::Dedup {
                order: original_binding_order,
            } => selected_layout
                .key()
                .iter()
                .map(|declaration_column| {
                    original_binding_order
                        .iter()
                        .position(|column| column == declaration_column)
                        .expect("validated binding column")
                })
                .collect(),
            BindingOperation::Aggregation { .. } => (0..input_arity).collect(),
        };
        let required_feed_layout = AttributeLayout::from_columns(vec![], feed_column_order);
        // An existing seed projection can emit the chosen input order itself.
        // Relation seeds keep their resolved source and are aligned separately.
        let binding_sources = arrangement.inputs().iter().enumerate();
        for (source_index, source) in binding_sources {
            let is_seed = source_index == 0 && binding.seed().is_some();
            if is_seed && !matches!(source, Source::Feed(_)) {
                continue;
            }
            match *source {
                Source::Feed(id) => {
                    let location = graph.feeds()[id.index()].location();
                    if let Some(previous_requirement) =
                        binding_feed_layouts.insert(location, required_feed_layout.clone())
                        && previous_requirement != required_feed_layout
                    {
                        return Err(PlanError::internal(format!(
                            "shared producer {location:?} has incompatible binding layouts"
                        )));
                    }
                }
                Source::Arrangement(id) => {
                    let source_arrangement = &graph.arrangements()[id.index()];
                    let source_layout = assignment
                        .arrangement_layouts
                        .get(&source_arrangement.origin())
                        .unwrap_or_else(|| source_arrangement.layout());
                    let source_column_order: Vec<usize> = source_layout
                        .key()
                        .iter()
                        .chain(source_layout.value())
                        .copied()
                        .collect();
                    if source_column_order != required_feed_layout.value() {
                        return Err(PlanError::internal(format!(
                            "binding 0x{relation_fingerprint:016x} conflicts with its arranged producer"
                        )));
                    }
                }
                Source::External(_) => {
                    if !required_feed_layout
                        .value()
                        .iter()
                        .copied()
                        .eq(0..input_arity)
                    {
                        return Err(PlanError::internal(format!(
                            "binding 0x{relation_fingerprint:016x} requires a reordered external rule input"
                        )));
                    }
                }
            }
        }
    }
    Ok(binding_feed_layouts)
}

/// Checks a transformation's output before indexing either original block.
/// Returns an internal error for duplicate, missing or cross-block columns.
pub(super) fn output_layout(
    transformation: &Transformation,
    output_order: &AttributeLayout,
) -> Result<(), PlanError> {
    let (key_arity, value_arity) = transformation.output().arity();
    let mut sorted_key_columns = output_order.key().to_vec();
    let mut sorted_value_columns = output_order.value().to_vec();
    sorted_key_columns.sort_unstable();
    sorted_value_columns.sort_unstable();
    if !sorted_key_columns.iter().copied().eq(0..key_arity)
        || !sorted_value_columns
            .iter()
            .copied()
            .eq(key_arity..key_arity + value_arity)
    {
        return Err(PlanError::internal(format!(
            "layout {output_order:?} does not permute the blocks of collection 0x{:016x}",
            transformation.output().fingerprint(),
        )));
    }
    Ok(())
}

/// Returns input rewrites in original side order, with matching key orders.
/// The result has one entry per existing input; join orientation is applied
/// later, so expression references still resolve against their original side.
/// Returns an internal error for incompatible keys or an illegal side swap.
pub(super) fn transformation(
    transformation: &Transformation,
    join_orientation: JoinOrientation,
    rewritten_outputs: &HashMap<u64, RewrittenCollection>,
) -> Result<Vec<RewrittenCollection>, PlanError> {
    if join_orientation == JoinOrientation::Swapped
        && !matches!(
            transformation,
            Transformation::JnToRow { .. } | Transformation::JnToKv { .. }
        )
    {
        return Err(PlanError::internal(format!(
            "{} does not allow swapping inputs",
            transformation.operation_name(),
        )));
    }
    let input_rewrites = if transformation.is_unary() {
        vec![RewrittenCollection::resolve_upstream_output(
            transformation.unary_input(),
            rewritten_outputs,
        )?]
    } else {
        let (left, right) = transformation.binary_input();
        vec![
            RewrittenCollection::resolve_upstream_output(left, rewritten_outputs)?,
            RewrittenCollection::resolve_upstream_output(right, rewritten_outputs)?,
        ]
    };
    if let [left_rewrite, right_rewrite] = input_rewrites.as_slice()
        && left_rewrite.column_order().key() != right_rewrite.column_order().key()
    {
        return Err(PlanError::internal(format!(
            "{} has incompatible join key orders {:?} and {:?}",
            transformation.operation_name(),
            left_rewrite.column_order().key(),
            right_rewrite.column_order().key(),
        )));
    }
    Ok(input_rewrites)
}
