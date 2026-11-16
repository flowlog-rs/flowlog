//! Sums feed conversion costs separately for feedback and one-time work.
//!
//! Preparation identifies distinct arrangement inputs and their feedback
//! dependencies. Candidate scoring evaluates only their conversion costs.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::ArrangementId;
use super::super::super::arrangement_graph::ArrangementOrigin;
use super::super::super::arrangement_graph::FeedId;
use super::super::super::arrangement_graph::Scope;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
use super::feed_cost::FeedCost;
use super::feed_cost::feed_cost;
use super::feed_cost::feed_work;
use crate::planner::Transformation;

/// Prepares one contribution per arrangement input and per join row buffer,
/// retaining whether its source depends on the output stratum's feedback loop.
#[must_use]
pub(super) fn prepare_costs(graph: &ArrangementGraph<'_>) -> Vec<FeedCost> {
    let (arrangement_dependencies, feed_dependencies) = recursive_dependencies(graph);
    let contributions: Vec<_> = graph
        .arrangements()
        .iter()
        .enumerate()
        .flat_map(|(output, arrangement)| {
            (0..arrangement.inputs().len()).map(move |input| (ArrangementId::new(output), input))
        })
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
                Source::Unit => false,
                Source::Arrangement(id) => stratum_index
                    .is_some_and(|index| arrangement_dependencies[id.index()].contains(&index)),
                Source::Feed(id) => stratum_index
                    .is_some_and(|index| feed_dependencies[id.index()].contains(&index)),
            }
        };
        costs.push(FeedCost::new(graph, output, input_index, recursive));
    }
    let feeds = graph.feeds().iter().enumerate();
    for (index, feed) in feeds {
        if matches!(feed.step().as_ref(), Transformation::JnToRow { .. }) {
            let recursive = feed_dependencies[index].contains(&feed.location().stratum_index());
            costs.push(FeedCost::join(graph, FeedId::new(index), recursive));
        }
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

/// Exact scoring with a shared permutation cache and a bounded work counter.
/// Every score, including normalization and observations, consumes work.
#[derive(Debug)]
pub(super) struct CostEvaluator {
    remaining_work: usize,
    cache: HashMap<Vec<usize>, u128>,
    pub(super) evaluations: usize,
}

impl CostEvaluator {
    #[must_use]
    pub(super) fn new(work_budget: usize) -> Self {
        Self {
            remaining_work: work_budget,
            cache: HashMap::new(),
            evaluations: 0,
        }
    }

    /// Returns `true` if another score may fit the remaining work budget.
    #[must_use]
    pub(super) fn has_budget(&self) -> bool {
        self.remaining_work > 0
    }

    /// Sums raw inversions without recursive weighting. Returns `None` for
    /// an unrepresentable conversion, sum overflow, or exhausted work budget.
    #[must_use]
    pub(super) fn score(
        &mut self,
        graph: &ArrangementGraph<'_>,
        costs: &[FeedCost],
        choice: &LayoutChoice,
    ) -> Option<u128> {
        let work = cost_work(graph, costs, self.remaining_work);
        if work > self.remaining_work {
            self.remaining_work = 0;
            return None;
        }
        self.remaining_work -= work;
        self.evaluations += 1;
        let mut total = 0_u128;
        for cost in costs {
            total = total.checked_add(feed_cost(graph, cost, choice, &mut self.cache)?)?;
        }
        Some(total)
    }
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
            ArrangementOrigin::InputDedup { .. }
            | ArrangementOrigin::Dedup {
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
                Source::Unit => {}
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
