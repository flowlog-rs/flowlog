//! Local refinement of consecutive field groups across one edge.
//!
//! Field correspondence carries partitions between endpoints and shared join
//! keys. Changed arrangement IDs let the caller reschedule dependent edges.

use std::collections::BTreeMap;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementId;
use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::Feed;
use super::super::arrangement_graph::Source;
use super::stratum_graph::ArityGroup;
use super::stratum_graph::ArrangementNode;
use super::stratum_graph::Edge;
use crate::planner::FactorArgument;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;

/// A field's position inside its current group, or a reason not to group it.
#[derive(Debug, Clone, Copy)]
enum Label {
    Group {
        is_key: bool,
        group: usize,
        position: usize,
    },
    Absent,
    Separate,
}

impl Label {
    /// Returns `true` if the next field preserves this group's internal order.
    #[must_use]
    fn continues(self, next: Self) -> bool {
        match self {
            Self::Group {
                is_key,
                group,
                position,
            } => matches!(
                next,
                Self::Group { is_key: next_key, group: next_group, position: next_position }
                    if is_key == next_key && group == next_group && position < next_position
            ),
            Self::Absent => matches!(next, Self::Absent),
            Self::Separate => false,
        }
    }
}

/// Lineage to one edge's input. Other join inputs have no column on this path;
/// computed expressions and aggregate answers cannot preserve a field order.
#[derive(Debug, Clone, Copy)]
enum Lineage {
    Column { field: usize },
    OtherInput,
    Computed,
}

/// Reconciles the feed's shared keys, refines the output, then maps its updated
/// partition back to the input. New input-key boundaries reach the other side
/// before returning. Returns every changed arrangement ID, possibly repeated;
/// fixed layouts remain unchanged.
#[must_use = "changed arrangements must reschedule their dependent edges"]
pub(super) fn refine_edge(
    graph: &ArrangementGraph<'_>,
    edge: &Edge<'_, '_>,
    nodes: &mut BTreeMap<ArrangementId, ArrangementNode>,
) -> Vec<ArrangementId> {
    let mut changed = Vec::new();
    if let Some(feed) = edge.feed {
        refine_shared_keys(graph, feed, &edge.input_path, nodes, &mut changed);
    }
    let output = &nodes[&edge.output];
    let columns: Vec<(usize, Lineage)> = output
        .key
        .iter()
        .chain(&output.value)
        .flat_map(|group| &group.arities)
        .map(|&field| (field, column_lineage(graph, edge, field)))
        .collect();
    let input_labels = if let Source::Arrangement(id) = edge.input {
        field_labels(&nodes[&id])
    } else {
        BTreeMap::new()
    };
    let downstream_labels: BTreeMap<usize, Label> = columns
        .iter()
        .map(|&(output, lineage)| {
            let label = match lineage {
                Lineage::Column { field } => {
                    input_labels.get(&field).copied().unwrap_or(Label::Separate)
                }
                Lineage::OtherInput => Label::Absent,
                Lineage::Computed => Label::Separate,
            };
            (output, label)
        })
        .collect();
    if let Some(output) = nodes.get_mut(&edge.output)
        && refine_node(output, &downstream_labels)
    {
        changed.push(edge.output);
    }

    let Source::Arrangement(input) = edge.input else {
        return changed;
    };
    let output_labels = field_labels(&nodes[&edge.output]);
    let mut upstream_labels = BTreeMap::new();
    if columns
        .iter()
        .any(|(_, lineage)| matches!(lineage, Lineage::Computed))
    {
        // Without column lineage, leave the input uncontracted. This
        // retains all layouts but also splits unrelated input fields.
        upstream_labels = field_labels(&nodes[&input])
            .into_keys()
            .map(|field| (field, Label::Separate))
            .collect();
    } else {
        for (output, lineage) in columns {
            if let Lineage::Column { field } = lineage {
                let label = output_labels[&output];
                if upstream_labels.insert(field, label).is_some() {
                    upstream_labels.insert(field, Label::Separate);
                }
            }
        }
    }
    if let Some(node) = nodes.get_mut(&input)
        && refine_node(node, &upstream_labels)
    {
        changed.push(input);
    }
    // Mapping the output partition back can create new input-key boundaries.
    if let Some(feed) = edge.feed {
        refine_shared_keys(graph, feed, &edge.input_path, nodes, &mut changed);
    }
    changed
}

/// Reconciles keys at binary feeds along this input path, including joins
/// beneath projections whose outputs no longer contain the shared key.
fn refine_shared_keys(
    graph: &ArrangementGraph<'_>,
    feed: &Feed<'_>,
    path: &[usize],
    nodes: &mut BTreeMap<ArrangementId, ArrangementNode>,
    changed: &mut Vec<ArrangementId>,
) {
    if let [Source::Arrangement(left), Source::Arrangement(right)] = feed.inputs()
        && nodes.contains_key(left)
        && nodes.contains_key(right)
    {
        for (source, target) in [(*left, *right), (*right, *left)] {
            let source_labels = field_labels(&nodes[&source]);
            let source_keys = graph.arrangements()[source.index()].layout().key();
            let target_keys = graph.arrangements()[target.index()].layout().key();
            let labels: BTreeMap<usize, Label> = source_keys
                .iter()
                .zip(target_keys)
                .map(|(source, &target)| (target, source_labels[source]))
                .collect();
            if let Some(node) = nodes.get_mut(&target)
                && refine_node(node, &labels)
            {
                changed.push(target);
            }
        }
    }
    if let Some((&port, remaining)) = path.split_first()
        && let Source::Feed(id) = feed.inputs()[port]
    {
        refine_shared_keys(graph, &graph.feeds()[id.index()], remaining, nodes, changed);
    }
}

#[must_use]
fn field_labels(node: &ArrangementNode) -> BTreeMap<usize, Label> {
    let mut labels = BTreeMap::new();
    for (is_key, groups) in [(true, &node.key), (false, &node.value)] {
        let groups = groups.iter().enumerate();
        for (group, fields) in groups {
            let fields = fields.arities.iter().enumerate();
            for (position, &field) in fields {
                labels.insert(
                    field,
                    Label::Group {
                        is_key,
                        group,
                        position,
                    },
                );
            }
        }
    }
    labels
}

/// Splits contiguous runs only, preserving every existing group boundary and
/// the original layout. Absent columns share a label; unknown columns split.
/// Returns `true` if the partition gains a boundary.
#[must_use]
fn refine_node(node: &mut ArrangementNode, labels: &BTreeMap<usize, Label>) -> bool {
    let previous_groups = node.key.len() + node.value.len();
    for groups in [&mut node.key, &mut node.value] {
        let mut refined = Vec::new();
        for group in std::mem::take(groups) {
            let runs = group.arities.chunk_by(|left, right| {
                let left = labels.get(left).copied().unwrap_or(Label::Absent);
                let right = labels.get(right).copied().unwrap_or(Label::Absent);
                left.continues(right)
            });
            for run in runs {
                refined.push(ArityGroup {
                    arities: run.to_vec(),
                });
            }
        }
        *groups = refined;
    }
    node.key.len() + node.value.len() > previous_groups
}

#[must_use]
fn column_lineage(graph: &ArrangementGraph<'_>, edge: &Edge<'_, '_>, field: usize) -> Lineage {
    let Some(feed) = edge.feed else {
        return Lineage::Column { field };
    };
    let output = &graph.arrangements()[edge.output.index()];
    let position = match output.origin() {
        ArrangementOrigin::InputDedup { .. } | ArrangementOrigin::Dedup { .. } => {
            output.layout().key().iter().position(|&id| id == field)
        }
        ArrangementOrigin::Transformation(_)
        | ArrangementOrigin::Aggregation { .. }
        | ArrangementOrigin::AggregationExit { .. } => Some(field),
    };
    let Some(position) = position else {
        return Lineage::Computed;
    };
    follow_column(graph, feed, &edge.input_path, position)
}

/// Resolves one output column along the retained input-port path. Plain column
/// references preserve lineage; aggregate values read the new answer row.
#[must_use]
fn follow_column(
    graph: &ArrangementGraph<'_>,
    feed: &Feed<'_>,
    path: &[usize],
    output_position: usize,
) -> Lineage {
    let Some((&port, remaining)) = path.split_first() else {
        return Lineage::Computed;
    };
    let step = feed.step().as_ref();
    let flow = step.flow();
    let expression = if output_position < flow.key().len() {
        flow.key().get(output_position)
    } else {
        flow.value().get(output_position - flow.key().len())
    };
    let Some(expression) = expression else {
        return Lineage::Computed;
    };
    if !expression.rest.is_empty() || !matches!(expression.init, FactorArgument::Var(_)) {
        let arguments = expression.transformation_arguments();
        let other_side = !arguments.is_empty()
            && arguments.iter().all(|argument| {
                matches!(
                    argument,
                    TransformationArgument::Jn((left, false, _)) if usize::from(!*left) != port
                )
            });
        return if other_side {
            Lineage::OtherInput
        } else {
            Lineage::Computed
        };
    }
    let FactorArgument::Var(argument) = expression.init else {
        return Lineage::Computed;
    };
    let (is_key, index) = match argument {
        TransformationArgument::KV(slot) => slot,
        TransformationArgument::Jn((left, is_key, index)) => {
            if !is_key && usize::from(!left) != port {
                return Lineage::OtherInput;
            }
            (is_key, index)
        }
    };
    if matches!(flow, TransformationFlow::Aggregate { .. }) && !is_key {
        return Lineage::Computed;
    }
    let input = if step.is_unary() {
        step.unary_input()
    } else {
        let (left, right) = step.binary_input();
        if port == 0 { left } else { right }
    };
    let source = feed.inputs()[port];
    match source {
        Source::Feed(id) => {
            let position = if is_key {
                index
            } else {
                input.arity().0 + index
            };
            follow_column(graph, &graph.feeds()[id.index()], remaining, position)
        }
        Source::Arrangement(id) => {
            let arrangement = &graph.arrangements()[id.index()];
            let field = match arrangement.origin() {
                ArrangementOrigin::Transformation(_) => {
                    if is_key {
                        index
                    } else {
                        input.arity().0 + index
                    }
                }
                ArrangementOrigin::InputDedup { .. }
                | ArrangementOrigin::Dedup { .. }
                | ArrangementOrigin::Aggregation { .. }
                | ArrangementOrigin::AggregationExit { .. } => {
                    // Relation readers use declaration-indexed row slots,
                    // independently of the binding's physical key order.
                    index
                }
            };
            Lineage::Column { field }
        }
    }
}
