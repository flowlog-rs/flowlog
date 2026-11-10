//! Bounded search over shared group priorities and ordinary join directions.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;

use tracing::debug;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::AttributeLayout;
use super::super::arrangement_graph::JoinOrientation;
use super::super::arrangement_graph::Source;
use super::LayoutChoice;
use super::calc_cost::Cost;
use super::calc_cost::calc_cost;
use super::calc_cost::cost_work;
use super::calc_cost::prepare_costs;
use super::feed_cost::FeedCost;
use super::stratum_graph::ArityGroup;
use super::stratum_graph::StratumGraph;

// A fixed, arbitrary seed makes the bounded search reproducible.
const SEED: u64 = 0x6a09_e667_f3bc_c909;

#[derive(Debug)]
pub(super) struct SearchResult {
    pub(super) choice: LayoutChoice,
    pub(super) cost: Cost,
    pub(super) baseline_cost: Cost,
    pub(super) valid_epochs: usize,
}

/// Returns the best modeled candidate, retaining the original on ties.
/// Groups must already have dense class IDs assigned by merging.
/// Returns `None` when the budget cannot fund an initial round or the
/// original candidate cannot be scored.
#[must_use]
pub(super) fn optimize(
    graph: &ArrangementGraph<'_>,
    subgraph: &StratumGraph<'_, '_>,
    stratum_index: usize,
    class_count: usize,
    work_budget: usize,
) -> Option<SearchResult> {
    let costs = prepare_costs(graph, subgraph);
    let feeds = selected_feeds(graph, &costs);
    let joins: Vec<usize> = feeds
        .iter()
        .copied()
        .filter(|&index| {
            let feed = &graph.feeds()[index];
            feed.join_orientation().is_some() && feed.location().stratum_index() == stratum_index
        })
        .collect();
    let score_work = cost_work(graph, &costs, work_budget);
    // Reserve one full round even for rejected candidates: an initial score
    // and at most one new score per join. Division avoids product overflow.
    let rounds = (work_budget / score_work) / (joins.len() + 1);
    debug!(
        stratum_index,
        work_budget, score_work, rounds, "estimated layout search work",
    );
    if rounds == 0 {
        return None;
    }
    let successors = ordering_constraints(graph, subgraph, &feeds, class_count);
    let mut indegrees = vec![0; class_count];
    for neighbors in &successors {
        for &neighbor in neighbors {
            indegrees[neighbor] += 1;
        }
    }
    let mut cache = HashMap::new();
    let mut best = LayoutChoice::default();
    let baseline_cost = calc_cost(graph, &costs, &best, &mut cache)?;
    let mut best_cost =
        choose_orientations(graph, &costs, &mut best, &joins, baseline_cost, &mut cache);
    let has_layout_choices = subgraph
        .nodes
        .values()
        .any(|node| !node.fixed && (node.key.len() > 1 || node.value.len() > 1));
    // The original layout and its direction sweep consume the first round.
    let epochs = if has_layout_choices { rounds - 1 } else { 0 };
    let mut valid_epochs = 0;
    for epoch in 0..epochs {
        if best_cost == Cost::default() {
            break;
        }
        let Some(ranks) = random_ranks(&successors, &indegrees, epoch) else {
            break;
        };
        let mut candidate = LayoutChoice::default();
        for (&id, node) in &subgraph.nodes {
            if !node.fixed {
                candidate.layouts.insert(
                    id,
                    AttributeLayout::from_columns(
                        ordered_fields(&node.key, &ranks),
                        ordered_fields(&node.value, &ranks),
                    ),
                );
            }
        }
        let Some(cost) = calc_cost(graph, &costs, &candidate, &mut cache) else {
            continue;
        };
        valid_epochs += 1;
        let cost = choose_orientations(graph, &costs, &mut candidate, &joins, cost, &mut cache);
        if cost < best_cost {
            best = candidate;
            best_cost = cost;
        }
    }
    Some(SearchResult {
        choice: best,
        cost: best_cost,
        baseline_cost,
        valid_epochs,
    })
}

/// Includes computations beneath projections and all consumers' contributions.
#[must_use]
fn selected_feeds(graph: &ArrangementGraph<'_>, costs: &[FeedCost]) -> BTreeSet<usize> {
    let mut pending: Vec<Source> = costs.iter().map(|cost| cost.source).collect();
    let mut feeds = BTreeSet::new();
    while let Some(source) = pending.pop() {
        if let Source::Feed(id) = source
            && feeds.insert(id.index())
        {
            pending.extend_from_slice(graph.feeds()[id.index()].inputs());
        }
    }
    feeds
}

/// Preserves key order when a free join input meets a fixed arrangement.
#[must_use]
fn ordering_constraints(
    graph: &ArrangementGraph<'_>,
    subgraph: &StratumGraph<'_, '_>,
    feeds: &BTreeSet<usize>,
    class_count: usize,
) -> Vec<BTreeSet<usize>> {
    let mut successors = vec![BTreeSet::new(); class_count];
    for &index in feeds {
        let [Source::Arrangement(left), Source::Arrangement(right)] = graph.feeds()[index].inputs()
        else {
            continue;
        };
        for (fixed, free) in [(*left, *right), (*right, *left)] {
            let (Some(fixed_node), Some(free_node)) =
                (subgraph.nodes.get(&fixed), subgraph.nodes.get(&free))
            else {
                continue;
            };
            if fixed_node.fixed && !free_node.fixed {
                // Optional copy sharing must not pin every free region.
                for pair in free_node.key.windows(2) {
                    successors[pair[0].class].insert(pair[1].class);
                }
            }
        }
    }
    successors
}

/// Samples a total order satisfying the fixed join-key constraints.
#[must_use]
fn random_ranks(
    successors: &[BTreeSet<usize>],
    indegrees: &[usize],
    epoch: usize,
) -> Option<Vec<usize>> {
    let mut incoming = indegrees.to_vec();
    let mut priorities = Vec::with_capacity(incoming.len());
    let mut ready = BTreeSet::new();
    let counts = incoming.iter().enumerate();
    for (class, &count) in counts {
        let mut hasher = DefaultHasher::new();
        (SEED, epoch, class).hash(&mut hasher);
        let priority = hasher.finish();
        priorities.push(priority);
        if count == 0 {
            ready.insert((priority, class));
        }
    }
    let mut ranks = vec![0; incoming.len()];
    let mut visited = 0;
    while let Some((_, class)) = ready.pop_first() {
        ranks[class] = visited;
        visited += 1;
        for &successor in &successors[class] {
            incoming[successor] -= 1;
            if incoming[successor] == 0 {
                ready.insert((priorities[successor], successor));
            }
        }
    }
    (visited == incoming.len()).then_some(ranks)
}

#[must_use]
fn ordered_fields(groups: &[ArityGroup], ranks: &[usize]) -> Vec<usize> {
    let mut order: Vec<&ArityGroup> = groups.iter().collect();
    order.sort_unstable_by_key(|group| ranks[group.class]);
    order
        .into_iter()
        .flat_map(|group| group.arities.iter().copied())
        .collect()
}

fn choose_orientations(
    graph: &ArrangementGraph<'_>,
    costs: &[FeedCost],
    choice: &mut LayoutChoice,
    joins: &[usize],
    mut cost: Cost,
    cache: &mut HashMap<Vec<usize>, u128>,
) -> Cost {
    for &index in joins {
        if cost == Cost::default() {
            break;
        }
        let original = choice.orientation(graph, index);
        let opposite = match original {
            JoinOrientation::Original => JoinOrientation::Swapped,
            JoinOrientation::Swapped => JoinOrientation::Original,
        };
        choice.join_orientations.insert(index, opposite);
        if let Some(candidate_cost) = calc_cost(graph, costs, choice, cache)
            && candidate_cost < cost
        {
            cost = candidate_cost;
        } else {
            choice.join_orientations.insert(index, original);
        }
    }
    cost
}
