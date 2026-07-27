//! Finds dependency cycles and orders them for evaluation.
//!
//! Edges point from consumers to the rules they depend on.
//! [`compute_sccs`] groups mutually dependent rules into [`Component`]s.
//! [`merge_strata`] then places dependencies before their consumers and
//! combines independent non-recursive components.

use std::collections::BTreeSet;
use std::collections::HashSet;

use crate::stratifier::dependency_graph::DependencyGraph;

/// Rules that must be evaluated together.
#[derive(Debug, Clone)]
pub(super) struct Component {
    rule_ids: Vec<usize>,
    recursive: bool,
}

impl Component {
    /// Returns rule IDs in graph traversal order.
    #[must_use]
    pub(super) fn rule_ids(&self) -> &[usize] {
        &self.rule_ids
    }

    /// Returns `true` if the rules form a dependency cycle.
    #[must_use]
    pub(super) fn is_recursive(&self) -> bool {
        self.recursive
    }
}

/// Groups mutually dependent rules with Kosaraju's two-pass algorithm.
///
/// Every rule appears in exactly one component. A component is recursive
/// when it contains multiple rules or one rule with a self-dependency.
#[must_use]
pub(super) fn compute_sccs(graph: &DependencyGraph) -> Vec<Component> {
    let dependencies = graph.dependencies();
    let rule_count = dependencies.len();

    // Reverse finish order makes the second pass encounter one component
    // at a time in the transposed graph.
    let mut finish_order = Vec::with_capacity(rule_count);
    let mut visited = vec![false; rule_count];
    for rule_id in 0..rule_count {
        visit_dependencies(dependencies, &mut visited, &mut finish_order, rule_id);
    }
    finish_order.reverse();

    // Reversing every edge confines a second-pass traversal to one component.
    let transposed = transpose(dependencies);
    let mut assigned = vec![false; rule_count];
    let mut component_rules = Vec::new();
    for rule_id in finish_order {
        if assigned[rule_id] {
            continue;
        }

        let mut rules = Vec::new();
        collect_component(&transposed, &mut assigned, &mut rules, rule_id);
        component_rules.push(rules);
    }

    component_rules
        .into_iter()
        .map(|rule_ids| {
            let recursive = rule_ids.len() > 1
                || rule_ids
                    .first()
                    .is_some_and(|&rule_id| dependencies[rule_id].contains(&rule_id));

            Component {
                rule_ids,
                recursive,
            }
        })
        .collect()
}

/// Returns `true` if an edge stays within one recursive component.
///
/// `source` and `target` must come from the graph passed to [`compute_sccs`].
#[must_use]
pub(super) fn is_recursive_edge(components: &[Component], source: usize, target: usize) -> bool {
    components.iter().any(|component| {
        component.recursive
            && component.rule_ids.contains(&source)
            && component.rule_ids.contains(&target)
    })
}

/// Orders components after their dependencies.
///
/// Independent non-recursive components in the same ready set are combined.
/// Recursive components retain separate fixpoint boundaries.
#[must_use]
pub(super) fn merge_strata(components: Vec<Component>, graph: &DependencyGraph) -> Vec<Component> {
    let dependencies = graph.dependencies();
    let mut pending = components;
    let mut merged = Vec::new();

    while !pending.is_empty() {
        let pending_rules: HashSet<usize> = pending
            .iter()
            .flat_map(|component| component.rule_ids.iter().copied())
            .collect();
        let pending_count = pending.len();
        let mut blocked = Vec::new();
        let mut non_recursive_rules = Vec::new();
        let mut recursive_components = Vec::new();

        for component in pending {
            if has_pending_dependency(&component, dependencies, &pending_rules) {
                blocked.push(component);
            } else if component.recursive {
                recursive_components.push(component);
            } else {
                non_recursive_rules.extend(component.rule_ids);
            }
        }

        debug_assert!(
            blocked.len() < pending_count,
            "the component graph must be acyclic"
        );

        if !non_recursive_rules.is_empty() {
            merged.push(Component {
                rule_ids: non_recursive_rules,
                recursive: false,
            });
        }
        merged.extend(recursive_components);
        pending = blocked;
    }

    merged
}

fn has_pending_dependency(
    component: &Component,
    dependencies: &[BTreeSet<usize>],
    pending_rules: &HashSet<usize>,
) -> bool {
    component.rule_ids.iter().any(|&rule_id| {
        dependencies[rule_id].iter().any(|dependency| {
            pending_rules.contains(dependency) && !component.rule_ids.contains(dependency)
        })
    })
}

fn transpose(dependencies: &[BTreeSet<usize>]) -> Vec<BTreeSet<usize>> {
    let mut transposed = vec![BTreeSet::new(); dependencies.len()];
    for (source, targets) in dependencies.iter().enumerate() {
        for &target in targets {
            transposed[target].insert(source);
        }
    }
    transposed
}

fn visit_dependencies(
    dependencies: &[BTreeSet<usize>],
    visited: &mut [bool],
    finish_order: &mut Vec<usize>,
    rule_id: usize,
) {
    if visited[rule_id] {
        return;
    }

    visited[rule_id] = true;
    for &dependency in &dependencies[rule_id] {
        visit_dependencies(dependencies, visited, finish_order, dependency);
    }
    finish_order.push(rule_id);
}

fn collect_component(
    transposed: &[BTreeSet<usize>],
    assigned: &mut [bool],
    component: &mut Vec<usize>,
    rule_id: usize,
) {
    if assigned[rule_id] {
        return;
    }

    assigned[rule_id] = true;
    component.push(rule_id);
    for &consumer in &transposed[rule_id] {
        collect_component(transposed, assigned, component, consumer);
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    fn graph(rule_count: usize, edges: &[(usize, usize)]) -> DependencyGraph {
        DependencyGraph::from_edges(rule_count, edges)
    }

    #[test]
    fn two_rule_cycle_is_one_recursive_component() {
        let components = compute_sccs(&graph(2, &[(0, 1), (1, 0)]));

        assert_eq!(components.len(), 1);
        assert!(components[0].is_recursive());
        assert!(is_recursive_edge(&components, 0, 1));
    }

    #[test]
    fn self_dependency_makes_single_rule_component_recursive() {
        let components = compute_sccs(&graph(2, &[(1, 0), (1, 1)]));

        assert_eq!(components.len(), 2);
        assert!(is_recursive_edge(&components, 1, 1));
        assert!(!is_recursive_edge(&components, 0, 0));
    }

    #[test]
    fn independent_rules_are_separate_non_recursive_components() {
        let components = compute_sccs(&graph(3, &[]));

        assert_eq!(components.len(), 3);
        assert!(components.iter().all(|component| !component.is_recursive()));
    }

    #[test]
    fn merge_combines_ready_non_recursive_components() {
        let graph = graph(3, &[]);
        let merged = merge_strata(compute_sccs(&graph), &graph);

        assert_eq!(merged.len(), 1);
        assert!(!merged[0].is_recursive());
        assert_eq!(merged[0].rule_ids().len(), 3);
    }

    #[test]
    fn merge_keeps_recursive_component_separate() {
        let graph = graph(3, &[(0, 1), (1, 0)]);
        let merged = merge_strata(compute_sccs(&graph), &graph);

        assert_eq!(merged.len(), 2);
        let recursive = merged
            .iter()
            .find(|component| component.is_recursive())
            .expect("recursive stratum");
        let non_recursive = merged
            .iter()
            .find(|component| !component.is_recursive())
            .expect("non-recursive stratum");
        assert_eq!(recursive.rule_ids().len(), 2);
        assert_eq!(non_recursive.rule_ids(), [2]);
    }

    #[test]
    fn merge_places_dependencies_before_consumers() {
        let graph = graph(4, &[(0, 1), (1, 2), (2, 3), (3, 2)]);
        let merged = merge_strata(compute_sccs(&graph), &graph);

        assert_eq!(merged.len(), 3);
        assert!(merged[0].is_recursive());
        assert!(!merged[1].is_recursive());
        assert!(!merged[2].is_recursive());
        let mut recursive_pair = merged[0].rule_ids().to_vec();
        recursive_pair.sort_unstable();
        assert_eq!(recursive_pair, vec![2, 3]);
        assert_eq!(merged[1].rule_ids(), [1]);
        assert_eq!(merged[2].rule_ids(), [0]);
    }
}
