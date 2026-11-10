//! Sums feed conversion costs separately for feedback and one-time work.
//!
//! Preparation identifies distinct arrangement inputs and their feedback
//! dependencies. Candidate scoring evaluates only their conversion costs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use super::super::arrangement_graph::ArrangementGraph;
use super::super::arrangement_graph::ArrangementOrigin;
use super::super::arrangement_graph::Scope;
use super::super::arrangement_graph::Source;
use super::LayoutChoice;
use super::feed_cost::FeedCost;
use super::feed_cost::feed_cost;
use super::feed_cost::feed_work;
use super::stratum_graph::StratumGraph;

/// Lexicographic priority keeps feedback conversions ahead of one-time work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct Cost {
    pub(super) recursive: u128,
    pub(super) non_recursive: u128,
}

/// Prepares one contribution per arrangement input, retaining whether its
/// source depends on the output stratum's feedback loop.
#[must_use]
pub(super) fn prepare_costs(
    graph: &ArrangementGraph<'_>,
    subgraph: &StratumGraph<'_, '_>,
) -> Vec<FeedCost> {
    let (arrangement_dependencies, feed_dependencies) = recursive_dependencies(graph);
    let contributions: BTreeSet<_> = subgraph
        .edges
        .iter()
        .map(|edge| (edge.output, edge.output_input_index))
        .collect();
    let mut costs = Vec::with_capacity(contributions.len());
    for (output, input_index) in contributions {
        let arrangement = &graph.arrangements()[output.index()];
        let source = arrangement.inputs()[input_index];
        let recursive = if matches!(
            arrangement.origin(),
            ArrangementOrigin::AggregationExit { .. }
        ) {
            false
        } else {
            let stratum_index = arrangement.origin().stratum_index();
            match source {
                Source::Arrangement(id) => {
                    arrangement_dependencies[id.index()].contains(&stratum_index)
                }
                Source::Feed(id) => feed_dependencies[id.index()].contains(&stratum_index),
                Source::External(_) => false,
            }
        };
        costs.push(FeedCost::new(graph, output, input_index, recursive));
    }
    costs
}

/// Estimates one complete score, retaining source traversal and discarded
/// projection fields even when the inversion lookup would hit its cache.
/// Values above the available budget are capped at `limit + 1`.
#[must_use]
pub(super) fn cost_work(graph: &ArrangementGraph<'_>, costs: &[FeedCost], limit: usize) -> usize {
    let mut work: usize = 0;
    for term in costs {
        work = work.saturating_add(feed_work(graph, term, limit));
        if work > limit {
            return limit.saturating_add(1);
        }
    }
    work.max(1)
}

/// Returns no score when a contribution cannot be represented or its exact
/// binary inversion count exceeds the supported 64-coordinate domain.
#[must_use]
pub(super) fn calc_cost(
    graph: &ArrangementGraph<'_>,
    costs: &[FeedCost],
    choice: &LayoutChoice,
    cache: &mut HashMap<Vec<usize>, u128>,
) -> Option<Cost> {
    let mut cost = Cost::default();
    for term in costs {
        let contribution = feed_cost(graph, term, choice, cache)?;
        if term.recursive {
            cost.recursive = cost.recursive.checked_add(contribution)?;
        } else {
            cost.non_recursive = cost.non_recursive.checked_add(contribution)?;
        }
    }
    Some(cost)
}

/// Reserved recursive bindings anchor feedback; feed IDs are dependency
/// ordered. Leaving a static aggregation's loop cuts its feedback dependence.
#[must_use]
fn recursive_dependencies(
    graph: &ArrangementGraph<'_>,
) -> (Vec<BTreeSet<usize>>, Vec<BTreeSet<usize>>) {
    let mut arrangements = vec![BTreeSet::new(); graph.arrangements().len()];
    let mut outputs: BTreeMap<usize, Vec<usize>> = BTreeMap::new();
    let indexed_arrangements = graph.arrangements().iter().enumerate();
    for (index, arrangement) in indexed_arrangements {
        match arrangement.origin() {
            ArrangementOrigin::Dedup {
                stratum_index,
                scope: Scope::Recursive,
                ..
            }
            | ArrangementOrigin::Aggregation {
                stratum_index,
                scope: Scope::Recursive,
                ..
            } => {
                arrangements[index].insert(stratum_index);
            }
            ArrangementOrigin::Transformation(_) => {
                for source in arrangement.inputs() {
                    if let Source::Feed(id) = source {
                        outputs.entry(id.index()).or_default().push(index);
                    }
                }
            }
            ArrangementOrigin::Dedup {
                scope: Scope::NonRecursive,
                ..
            }
            | ArrangementOrigin::Aggregation {
                scope: Scope::NonRecursive,
                ..
            }
            | ArrangementOrigin::AggregationExit { .. } => {}
        }
    }
    let mut feeds: Vec<BTreeSet<usize>> = Vec::with_capacity(graph.feeds().len());
    let indexed_feeds = graph.feeds().iter().enumerate();
    for (index, feed) in indexed_feeds {
        let mut dependencies = BTreeSet::new();
        for source in feed.inputs() {
            match *source {
                Source::External(_) => {}
                Source::Arrangement(id) => dependencies.extend(&arrangements[id.index()]),
                Source::Feed(id) => dependencies.extend(&feeds[id.index()]),
            }
        }
        if let Some(outputs) = outputs.get(&index) {
            for &output in outputs {
                arrangements[output].clone_from(&dependencies);
            }
        }
        feeds.push(dependencies);
    }
    (arrangements, feeds)
}
