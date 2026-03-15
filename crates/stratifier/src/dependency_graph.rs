//! Dependency graph construction for FlowLog Datalog programs.

use itertools::Itertools;
use parser::{logic::FlowLogRule, Predicate};
use std::collections::{BTreeSet, HashMap, HashSet};
use std::fmt;

/// Represents the dependency relationships between rules in a single segment.
///
/// Rule IDs are 0-based local indices into the slice passed to [`DependencyGraph::from_rules`].
/// Only intra-segment edges are tracked; cross-segment references are ignored by callers
/// (treated as already-computed EDB from prior segments).
#[derive(Debug, Clone)]
pub(super) struct DependencyGraph {
    /// All dependencies: every rule whose head predicate appears in the body of another rule.
    dependency_map: HashMap<usize, HashSet<usize>>,

    /// Edges caused by negation. Used to detect unstratifiable programs
    /// (negation through recursion). Ordered for deterministic error reporting.
    negative_edges: BTreeSet<(usize, usize)>,
}

impl DependencyGraph {
    /// Returns map from rule ID to the set of rule IDs it depends on (polarity-agnostic).
    #[must_use]
    pub(super) fn dependency_map(&self) -> &HashMap<usize, HashSet<usize>> {
        &self.dependency_map
    }

    /// Returns the set of dependency edges introduced by negation.
    #[must_use]
    pub(super) fn negative_edges(&self) -> &BTreeSet<(usize, usize)> {
        &self.negative_edges
    }

    /// Build a dependency graph for the given rules.
    ///
    /// Rule IDs are 0-based local indices into `rules`.  Only intra-slice edges are tracked;
    /// references to relations not defined in the slice are silently ignored.
    #[must_use]
    pub(super) fn from_rules(rules: &[FlowLogRule]) -> Self {
        let head_to_rule_map = Self::build_head_to_rule_map(rules);

        let mut dependency_map: HashMap<usize, HashSet<usize>> =
            (0..rules.len()).map(|i| (i, HashSet::new())).collect();
        let mut negative_edges: BTreeSet<(usize, usize)> = BTreeSet::new();

        for (rule_id, rule) in rules.iter().enumerate() {
            for predicate in rule.rhs() {
                let (atom_name, is_negative) = match predicate {
                    Predicate::PositiveAtomPredicate(atom) => (atom.name(), false),
                    Predicate::NegativeAtomPredicate(atom) => (atom.name(), true),
                    _ => continue,
                };
                if let Some(dep_ids) = head_to_rule_map.get(atom_name) {
                    for &dep_id in dep_ids {
                        dependency_map.get_mut(&rule_id).unwrap().insert(dep_id);
                        if is_negative {
                            negative_edges.insert((rule_id, dep_id));
                        }
                    }
                }
            }
        }

        Self { dependency_map, negative_edges }
    }

    fn build_head_to_rule_map(rules: &[FlowLogRule]) -> HashMap<String, Vec<usize>> {
        let mut map: HashMap<String, Vec<usize>> = HashMap::new();
        for (id, rule) in rules.iter().enumerate() {
            map.entry(rule.head().name().to_string()).or_default().push(id);
        }
        map
    }
}

impl fmt::Display for DependencyGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\nDependency Graph:")?;
        writeln!(f, "{}", "-".repeat(45))?;
        for (rule_id, deps) in self.dependency_map().iter().sorted_by_key(|x| x.0) {
            if deps.is_empty() {
                writeln!(f, "Rule {}: []", rule_id)?;
            } else {
                let dep_str = deps.iter().sorted().map(ToString::to_string).join(", ");
                writeln!(f, "Rule {}: [{}]", rule_id, dep_str)?;
            }
        }
        Ok(())
    }
}
