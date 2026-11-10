//! Static sharing of ordering identities between compatible arity groups.
//!
//! Join keys merge first. Plain copies then merge greedily without putting
//! distinct groups of one arrangement in the same class. Physical layouts
//! and the original feed graph remain unchanged.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::rc::Rc;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementId;
use super::super::arrangement_graph::Source;
use super::refinement::copied_input_field;
use super::stratum_graph::ArrangementNode;
use super::stratum_graph::Edge;
use super::stratum_graph::StratumGraph;
use crate::planner::PlanError;

/// Merges corresponding key blocks, then whole copied blocks in reverse edge
/// order. Refined partitions must be complete and cannot split afterward.
/// The copy merges are maximal for this traversal, not globally maximum.
/// Returns the number of classes after assigning dense class indices in
/// arrangement order, visiting each key before its value.
///
/// # Errors
///
/// Returns an internal error for incompatible mandatory key partitions or
/// key classes that would contain distinct groups of one arrangement.
pub(super) fn merge(
    graph: &ArrangementGraph<'_>,
    subgraph: &mut StratumGraph<'_, '_>,
) -> Result<usize, PlanError> {
    for edge in &subgraph.edges {
        merge_join_keys(graph, edge, &mut subgraph.nodes)?;
    }
    let edges = subgraph.edges.iter().rev();
    for edge in edges {
        let Source::Arrangement(input) = edge.input else {
            continue;
        };
        let output = &subgraph.nodes[&edge.output];
        let group_count = output.key.len() + output.value.len();
        for group_index in 0..group_count {
            let output = &subgraph.nodes[&edge.output];
            let output_group = if group_index < output.key.len() {
                &output.key[group_index]
            } else {
                &output.value[group_index - output.key.len()]
            };
            let copied_fields: Option<Vec<usize>> = output_group
                .arities
                .iter()
                .map(|&field| copied_input_field(graph, edge, field))
                .collect();
            let Some(copied_fields) = copied_fields else {
                continue;
            };
            let input = &subgraph.nodes[&input];
            let mut input_groups = input.key.iter().chain(&input.value);
            let Some(input_group) = input_groups.find(|group| group.arities == copied_fields)
            else {
                continue;
            };
            let upstream_set = Rc::clone(&input_group.merged_set);
            let downstream_set = Rc::clone(&output_group.merged_set);
            try_merge(&mut subgraph.nodes, &upstream_set, &downstream_set);
        }
    }
    let mut classes: Vec<Rc<BTreeSet<ArrangementId>>> = Vec::new();
    for node in subgraph.nodes.values_mut() {
        let groups = node.key.iter_mut().chain(&mut node.value);
        for group in groups {
            group.class = classes
                .iter()
                .position(|class| Rc::ptr_eq(class, &group.merged_set))
                .unwrap_or_else(|| {
                    classes.push(Rc::clone(&group.merged_set));
                    classes.len() - 1
                });
        }
    }
    Ok(classes.len())
}

/// Equates keys even when a projection hides the binary feed or its key.
/// Ordinary joins, negative joins and grouped aggregation all require the
/// same key permutation on their two arranged inputs.
fn merge_join_keys(
    graph: &ArrangementGraph<'_>,
    edge: &Edge<'_, '_>,
    nodes: &mut BTreeMap<ArrangementId, ArrangementNode>,
) -> Result<(), PlanError> {
    let Some(mut feed) = edge.feed else {
        return Ok(());
    };
    let mut path = edge.input_path.as_slice();
    loop {
        if let [Source::Arrangement(left), Source::Arrangement(right)] = feed.inputs()
            && nodes.contains_key(left)
            && nodes.contains_key(right)
        {
            let left_keys = &nodes[left].key;
            let right_keys = &nodes[right].key;
            if left_keys.len() != right_keys.len()
                || left_keys
                    .iter()
                    .zip(right_keys)
                    .any(|(left, right)| left.arities.len() != right.arities.len())
            {
                return Err(PlanError::internal(format!(
                    "arrangements {} and {} have incompatible key partitions after refinement",
                    left.index(),
                    right.index(),
                )));
            }
            let key_groups = left_keys.len();
            for group_index in 0..key_groups {
                let left_set = Rc::clone(&nodes[left].key[group_index].merged_set);
                let right_set = Rc::clone(&nodes[right].key[group_index].merged_set);
                if !try_merge(nodes, &left_set, &right_set) {
                    return Err(PlanError::internal(format!(
                        "key group {group_index} of arrangements {} and {} \
                         would merge distinct groups of one arrangement",
                        left.index(),
                        right.index(),
                    )));
                }
            }
        }
        let Some((&port, remaining)) = path.split_first() else {
            break;
        };
        let Source::Feed(id) = feed.inputs()[port] else {
            break;
        };
        feed = &graph.feeds()[id.index()];
        path = remaining;
    }
    Ok(())
}

/// Returns `true` if the classes already match or their disjoint owner sets
/// can merge. Every alias is replaced together, so no group retains a stale
/// owner set. Different allocations with equal contents remain distinct.
fn try_merge(
    nodes: &mut BTreeMap<ArrangementId, ArrangementNode>,
    left: &Rc<BTreeSet<ArrangementId>>,
    right: &Rc<BTreeSet<ArrangementId>>,
) -> bool {
    if Rc::ptr_eq(left, right) {
        return true;
    }
    if !left.is_disjoint(right) {
        return false;
    }
    let merged_set: BTreeSet<ArrangementId> = left.union(right).copied().collect();
    let merged_set = Rc::new(merged_set);
    for node in nodes.values_mut() {
        let groups = node.key.iter_mut().chain(&mut node.value);
        for group in groups {
            if Rc::ptr_eq(&group.merged_set, left) || Rc::ptr_eq(&group.merged_set, right) {
                group.merged_set = Rc::clone(&merged_set);
            }
        }
    }
    true
}
