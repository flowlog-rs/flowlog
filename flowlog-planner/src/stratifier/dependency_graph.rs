//! Builds dense rule dependency graphs for stratification.

use std::collections::BTreeSet;
use std::collections::HashMap;

use flowlog_parser::FlowLogRule;
use flowlog_parser::Predicate;

/// Dependency relationships between a program's rules.
///
/// An edge points from a consumer to a rule producing one of its body
/// relations. Ordered sets make later component traversal deterministic.
#[derive(Debug, Clone)]
pub(super) struct DependencyGraph {
    dependencies: Vec<BTreeSet<usize>>,
    negative_edges: BTreeSet<(usize, usize)>,
}

impl DependencyGraph {
    /// Returns dependencies at the corresponding rule ID.
    ///
    /// Every dependency is a valid index in this slice.
    #[must_use]
    pub(super) fn dependencies(&self) -> &[BTreeSet<usize>] {
        &self.dependencies
    }

    /// Returns negative dependency edges in deterministic order.
    #[must_use]
    pub(super) fn negative_edges(&self) -> &BTreeSet<(usize, usize)> {
        &self.negative_edges
    }

    /// Returns the intra-slice dependency graph for `rules`.
    ///
    /// Rule IDs are 0-based local indices into `rules`. Only intra-slice
    /// edges are tracked; a body atom whose relation no rule in the slice
    /// derives adds no edge.
    #[must_use]
    pub(super) fn from_rules(rules: &[FlowLogRule]) -> Self {
        let head_to_rule_map = Self::build_head_to_rule_map(rules);

        let mut dependencies: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); rules.len()];
        let mut negative_edges: BTreeSet<(usize, usize)> = BTreeSet::new();

        for (rule_id, (rule_dependencies, rule)) in dependencies.iter_mut().zip(rules).enumerate() {
            for predicate in rule.rhs() {
                let (atom_name, is_negative) = match predicate {
                    Predicate::PositiveAtom(atom) => (atom.name(), false),
                    Predicate::NegativeAtom(atom) => (atom.name(), true),
                    Predicate::Compare(_) => continue,
                };
                let Some(dep_ids) = head_to_rule_map.get(atom_name) else {
                    continue;
                };
                for &dep_id in dep_ids {
                    rule_dependencies.insert(dep_id);
                    if is_negative {
                        negative_edges.insert((rule_id, dep_id));
                    }
                }
            }
        }

        Self {
            dependencies,
            negative_edges,
        }
    }

    #[cfg(test)]
    pub(super) fn from_edges(rule_count: usize, edges: &[(usize, usize)]) -> Self {
        let mut dependencies = vec![BTreeSet::new(); rule_count];
        for &(source, target) in edges {
            dependencies[source].insert(target);
        }
        Self {
            dependencies,
            negative_edges: BTreeSet::new(),
        }
    }

    fn build_head_to_rule_map(rules: &[FlowLogRule]) -> HashMap<String, Vec<usize>> {
        let mut map: HashMap<String, Vec<usize>> = HashMap::new();
        for (id, rule) in rules.iter().enumerate() {
            map.entry(rule.head().name().to_string())
                .or_default()
                .push(id);
        }
        map
    }
}
