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
}

impl DependencyGraph {
    /// Returns dependencies at the corresponding rule ID.
    ///
    /// Every dependency is a valid index in this slice.
    #[must_use]
    pub(super) fn dependencies(&self) -> &[BTreeSet<usize>] {
        &self.dependencies
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
        for (rule_dependencies, rule) in dependencies.iter_mut().zip(rules) {
            for atom_name in body_reads(rule.rhs()) {
                if let Some(dep_ids) = head_to_rule_map.get(atom_name) {
                    rule_dependencies.extend(dep_ids);
                }
            }
        }
        Self { dependencies }
    }

    #[cfg(test)]
    pub(super) fn from_edges(rule_count: usize, edges: &[(usize, usize)]) -> Self {
        let mut dependencies = vec![BTreeSet::new(); rule_count];
        for &(source, target) in edges {
            dependencies[source].insert(target);
        }
        Self { dependencies }
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

/// The relation each atom in `predicates` reads, negated or not; an
/// aggregate reads the atoms of its body. Recursion through a negation or
/// an aggregate is rejected once strata are known, so all reads count
/// alike here.
fn body_reads(predicates: &[Predicate]) -> Vec<&str> {
    let mut reads = Vec::new();
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                reads.push(atom.name());
            }
            Predicate::Compare(_) => {}
            Predicate::Aggregate(aggregate) => reads.extend(body_reads(aggregate.body())),
        }
    }
    reads
}
