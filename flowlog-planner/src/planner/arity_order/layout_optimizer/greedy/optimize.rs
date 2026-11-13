//! Random dedup restarts and two direct-mapping sweeps per phase.
//!
//! Phase definitions are prepared once. Local acceptance uses one fixed edge;
//! recording uses the whole phase, with downstream recursive work preferred.

use std::collections::BTreeSet;

use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use tracing::debug;

use super::super::super::arrangement_graph::ArrangementGraph;
use super::super::super::arrangement_graph::ArrangementId;
use super::super::super::arrangement_graph::ArrangementOrigin;
use super::super::super::arrangement_graph::Scope;
use super::super::super::arrangement_graph::Source;
use super::super::LayoutChoice;
use super::calc_cost::CostEvaluator;
use super::direct_mapping::Mapping;
use super::direct_mapping::Neighbor;
use super::direct_mapping::flip_join;
use super::feed_cost::FeedCost;
use super::variables::Variable;
use super::variables::Variables;
use crate::planner::PlanError;

// Arbitrary search ceilings retain the previous scoring-work budget while
// replacing each iterative local search with a single mapped candidate.
const EPOCHS: usize = 32;
const WORK_BUDGET: usize = 100_000_000;

#[derive(Debug)]
struct Visit {
    variable: Variable,
    neighbors: Vec<usize>,
}

#[derive(Debug)]
struct Phase {
    stratum_index: usize,
    recursive: bool,
    costs: Vec<FeedCost>,
    frozen: BTreeSet<Variable>,
    /// Downstream-neighbor visits first, then upstream-neighbor visits.
    visits: [Vec<Visit>; 2],
}

/// Keeps the original plan on ties or if no complete epoch fits the budget.
/// Returns an internal error if cutting recursive heads leaves a cycle.
pub(super) fn optimize(
    graph: &ArrangementGraph<'_>,
    variables: &Variables,
    costs: &[FeedCost],
    mappings: &[Mapping],
    stratum_count: usize,
) -> Result<LayoutChoice, PlanError> {
    let order = topological_order(graph, variables, costs, mappings)?;
    let phases = prepare_phases(graph, variables, costs, mappings, &order, stratum_count);
    let frozen = variables.fixed();
    let seeds: BTreeSet<_> = graph
        .arrangements()
        .iter()
        .enumerate()
        .filter_map(|(index, arrangement)| {
            if matches!(
                arrangement.origin(),
                ArrangementOrigin::InputDedup { .. } | ArrangementOrigin::Dedup { .. }
            ) {
                variables
                    .row(graph, ArrangementId::new(index))
                    .filter(|&id| !frozen.contains(&Variable::Layout(id)))
            } else {
                None
            }
        })
        .collect();
    let mut evaluator = CostEvaluator::new(WORK_BUDGET);
    let mut best = LayoutChoice::default();
    let baseline: Option<Vec<_>> = phases
        .iter()
        .map(|phase| evaluator.score(graph, &phase.costs, &best))
        .collect();
    let Some(mut best_priority) = baseline else {
        return Ok(best);
    };
    let baseline = best_priority.clone();
    let mut random = SmallRng::seed_from_u64(0);
    for epoch in 0..EPOCHS {
        if !evaluator.has_budget() || best_priority.iter().all(|&cost| cost == 0) {
            break;
        }
        let mut choice = LayoutChoice::default();
        for &seed in &seeds {
            // Shuffling complete groups cannot violate a layout domain.
            variables
                .randomize(graph, &mut choice, seed, &mut random)
                .expect("random initialization only permutes complete groups");
        }
        let mut priority = Vec::new();
        for phase in &phases {
            let Some(cost) = evaluator.score(graph, &phase.costs, &choice) else {
                break;
            };
            let search = PhaseSearch {
                graph,
                variables,
                costs,
                mappings,
                phase,
                best: Candidate {
                    choice: choice.clone(),
                    cost,
                },
            };
            let Some(candidate) = search.run(choice, &mut evaluator, &mut random) else {
                return Ok(best);
            };
            choice = candidate.choice;
            priority.push(candidate.cost);
        }
        if priority.len() != phases.len() {
            break;
        }
        if priority < best_priority {
            best = choice;
            best_priority = priority;
        }
        debug!(
            epoch,
            ?best_priority,
            evaluations = evaluator.evaluations,
            "finished direct-mapping layout epoch"
        );
    }
    debug!(
        ?baseline,
        ?best_priority,
        evaluations = evaluator.evaluations,
        "selected direct-mapping layout candidate"
    );
    Ok(best)
}

/// Prepares visits and freezes complete interfaces in phase-priority order.
/// These dependencies remain the same across randomized restarts.
#[must_use]
fn prepare_phases(
    graph: &ArrangementGraph<'_>,
    variables: &Variables,
    costs: &[FeedCost],
    mappings: &[Mapping],
    order: &[Variable],
    stratum_count: usize,
) -> Vec<Phase> {
    let mut phases = Vec::new();
    let mut frozen = variables.fixed();
    let strata = (0..stratum_count).rev();
    for stratum_index in strata {
        for recursive in [true, false] {
            let selected: Vec<_> = costs
                .iter()
                .enumerate()
                .filter_map(|(index, cost)| {
                    (cost.stratum_index(graph) == Some(stratum_index)
                        && cost.recursive == recursive)
                        .then_some(index)
                })
                .collect();
            if selected.is_empty() {
                continue;
            }
            let inputs: Vec<_> = selected
                .iter()
                .map(|&index| mappings[index].input_variables.clone())
                .collect();
            let outputs: Vec<BTreeSet<_>> = selected
                .iter()
                .map(|&index| {
                    costs[index]
                        .output()
                        .into_iter()
                        .flat_map(|output| variables.arrangement(output))
                        .collect()
                })
                .collect();
            let interface: BTreeSet<_> = inputs.iter().chain(&outputs).flatten().copied().collect();
            let mut visits = [Vec::new(), Vec::new()];
            for (pass, sweep) in visits.iter_mut().enumerate() {
                let mut traversal = order.to_vec();
                if pass == 0 {
                    traversal.reverse();
                }
                for variable in traversal {
                    if frozen.contains(&variable) {
                        continue;
                    }
                    let scopes = if pass == 0 || matches!(variable, Variable::Join(_)) {
                        &inputs
                    } else {
                        &outputs
                    };
                    let neighbors: Vec<_> = selected
                        .iter()
                        .zip(scopes)
                        .filter_map(|(&index, scope)| scope.contains(&variable).then_some(index))
                        .collect();
                    if !neighbors.is_empty() {
                        sweep.push(Visit {
                            variable,
                            neighbors,
                        });
                    }
                }
            }
            phases.push(Phase {
                stratum_index,
                recursive,
                costs: selected.iter().map(|&index| costs[index].clone()).collect(),
                frozen: frozen.clone(),
                visits,
            });
            frozen.extend(interface);
        }
    }
    phases
}

#[derive(Debug)]
struct Candidate {
    choice: LayoutChoice,
    cost: u128,
}

/// A phase's walk and its best full-phase snapshot are independent states.
#[derive(Debug)]
struct PhaseSearch<'search, 'plan> {
    graph: &'search ArrangementGraph<'plan>,
    variables: &'search Variables,
    costs: &'search [FeedCost],
    mappings: &'search [Mapping],
    phase: &'search Phase,
    best: Candidate,
}

impl PhaseSearch<'_, '_> {
    /// Tries relevant unfrozen join directions for the selected neighbor.
    /// Join ties retain the current direction. Failure discards the candidate.
    #[must_use]
    fn evaluate(
        &self,
        index: usize,
        mut choice: LayoutChoice,
        evaluator: &mut CostEvaluator,
    ) -> Option<Candidate> {
        let objective = std::slice::from_ref(&self.costs[index]);
        let mut cost = evaluator.score(self.graph, objective, &choice)?;
        for &join in &self.mappings[index].joins {
            if self.phase.frozen.contains(&Variable::Join(join)) {
                continue;
            }
            flip_join(self.graph, &mut choice, join);
            let flipped = evaluator.score(self.graph, objective, &choice)?;
            if flipped < cost {
                cost = flipped;
            } else {
                flip_join(self.graph, &mut choice, join);
            }
        }
        Some(Candidate { choice, cost })
    }

    /// Records every accepted state with the original complete phase objective.
    /// Equal full-phase scores retain the earlier snapshot.
    #[must_use]
    fn observe(&mut self, choice: &LayoutChoice, evaluator: &mut CostEvaluator) -> Option<()> {
        let cost = evaluator.score(self.graph, &self.phase.costs, choice)?;
        if cost < self.best.cost {
            self.best = Candidate {
                choice: choice.clone(),
                cost,
            };
        }
        Some(())
    }

    /// Scores one mapped candidate against one neighbor, drawn once per visit.
    /// The first sweep accepts neutral alignment; the second requires a gain.
    #[must_use]
    fn visit(
        &mut self,
        visit: &Visit,
        neighbor: Neighbor,
        choice: &mut LayoutChoice,
        evaluator: &mut CostEvaluator,
        random: &mut SmallRng,
    ) -> Option<()> {
        let index = visit.neighbors[random.gen_range(0..visit.neighbors.len())];
        let Some(current) = self.evaluate(index, choice.clone(), evaluator) else {
            return Some(());
        };
        self.observe(&current.choice, evaluator)?;
        *choice = current.choice;
        let mut aligned = choice.clone();
        match visit.variable {
            Variable::Join(feed) => flip_join(self.graph, &mut aligned, feed),
            Variable::Layout(id) => {
                let Some(requested) = self.mappings[index].requested_columns(
                    self.graph,
                    &self.costs[index],
                    &aligned,
                    neighbor,
                ) else {
                    return Some(());
                };
                self.variables
                    .align(self.graph, &mut aligned, id, &requested)?;
            }
        }
        let Some(candidate) = self.evaluate(index, aligned, evaluator) else {
            return Some(());
        };
        let accept = candidate.cost < current.cost
            || (neighbor == Neighbor::Downstream && candidate.cost == current.cost);
        if accept && candidate.choice != *choice {
            self.observe(&candidate.choice, evaluator)?;
            *choice = candidate.choice;
        }
        Some(())
    }

    #[must_use]
    fn run(
        mut self,
        mut choice: LayoutChoice,
        evaluator: &mut CostEvaluator,
        random: &mut SmallRng,
    ) -> Option<Candidate> {
        let directions = [Neighbor::Downstream, Neighbor::Upstream];
        let sweeps = directions.into_iter().zip(&self.phase.visits);
        for (neighbor, visits) in sweeps {
            for visit in visits {
                if !evaluator.has_budget() {
                    return None;
                }
                self.visit(visit, neighbor, &mut choice, evaluator, random)?;
            }
        }
        debug!(
            stratum_index = self.phase.stratum_index,
            recursive = self.phase.recursive,
            selected_cost = self.best.cost,
            "finished direct-mapping layout sweeps"
        );
        Some(self.best)
    }
}

/// Cuts incoming recursive-head dependencies only for traversal. The original
/// sources and every feedback contribution remain in scoring and alignment.
fn topological_order(
    graph: &ArrangementGraph<'_>,
    variables: &Variables,
    costs: &[FeedCost],
    mappings: &[Mapping],
) -> Result<Vec<Variable>, PlanError> {
    let mut parents = vec![BTreeSet::new(); graph.arrangements().len()];
    let mut joins = vec![BTreeSet::new(); parents.len()];
    for cost in costs {
        let Some(output) = cost.output() else {
            continue;
        };
        let output = output.index();
        let sources = cost.physical_sources(graph);
        for source in &sources {
            if let Source::Feed(feed) = source
                && graph.feeds()[feed.index()].join_orientation().is_some()
            {
                joins[output].insert(feed.index());
            }
        }
        if matches!(
            graph.arrangements()[output].origin(),
            ArrangementOrigin::Dedup {
                scope: Scope::Recursive,
                ..
            } | ArrangementOrigin::Aggregation {
                scope: Scope::Recursive,
                ..
            }
        ) {
            continue;
        }
        for source in sources {
            if let Source::Arrangement(id) = source {
                parents[output].insert(id);
            }
        }
    }
    let mut ready: BTreeSet<_> = parents
        .iter()
        .enumerate()
        .filter_map(|(index, parents)| parents.is_empty().then_some(ArrangementId::new(index)))
        .collect();
    let mut children = vec![Vec::new(); parents.len()];
    let indexed = parents.iter().enumerate();
    for (index, parents) in indexed {
        for parent in parents {
            children[parent.index()].push(ArrangementId::new(index));
        }
    }
    let mut order = Vec::new();
    let mut seen = BTreeSet::new();
    let mut visited = 0;
    while let Some(arrangement) = ready.pop_first() {
        visited += 1;
        let local = joins[arrangement.index()]
            .iter()
            .copied()
            .map(Variable::Join)
            .chain(variables.arrangement(arrangement));
        for variable in local {
            if seen.insert(variable) {
                order.push(variable);
            }
        }
        for &child in &children[arrangement.index()] {
            parents[child.index()].remove(&arrangement);
            if parents[child.index()].is_empty() {
                ready.insert(child);
            }
        }
    }
    if visited != parents.len() {
        let blocked: Vec<_> = parents
            .iter()
            .enumerate()
            .filter_map(|(index, parents)| (!parents.is_empty()).then_some(index))
            .collect();
        return Err(PlanError::internal(format!(
            "layout traversal remains cyclic after cutting recursive heads; blocked arrangements {blocked:?}",
        )));
    }
    for mapping in mappings {
        for &join in &mapping.joins {
            let variable = Variable::Join(join);
            if seen.insert(variable) {
                order.push(variable);
            }
        }
    }
    Ok(order)
}
