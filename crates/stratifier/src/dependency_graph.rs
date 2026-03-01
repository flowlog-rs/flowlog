//! Dependency graph construction for FlowLog Datalog programs.

use itertools::Itertools;
use parser::{logic::FlowLogRule, Predicate, Program};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Represents the dependency relationships between rules.
#[derive(Debug, Clone)]
pub(super) struct DependencyGraph {
    /// All dependencies: every rule whose head predicate appears in the body of another rule.
    dependency_map: HashMap<usize, HashSet<usize>>,

    /// Edges caused by negation. Used to detect unstratifiable
    /// programs (negation through recursion).
    negative_edges: HashSet<(usize, usize)>,
}

impl DependencyGraph {
    /// Returns map from rule ID to the set of rule IDs it depends on (polarity-agnostic).
    #[must_use]
    pub(super) fn dependency_map(&self) -> &HashMap<usize, HashSet<usize>> {
        &self.dependency_map
    }

    /// Returns the set of dependency edges introduced by negation.
    #[must_use]
    pub(super) fn negative_edges(&self) -> &HashSet<(usize, usize)> {
        &self.negative_edges
    }

    /// Constructs a dependency graph from a program.
    #[must_use]
    pub(super) fn from_program(program: &Program) -> Self {
        let rules = program.rules();

        let head_to_rule_ids_map = Self::build_head_to_rule_map(rules);

        let mut dependency_map: HashMap<usize, HashSet<usize>> =
            (0..rules.len()).map(|i| (i, HashSet::new())).collect();
        let mut negative_edges: HashSet<(usize, usize)> = HashSet::new();

        for (rule_id, rule) in rules.iter().enumerate() {
            Self::analyze_rule_dependencies(
                rule_id,
                rule,
                &head_to_rule_ids_map,
                &mut dependency_map,
                &mut negative_edges,
            );
        }

        Self {
            dependency_map,
            negative_edges,
        }
    }

    /// Builds mapping from relation names to rule IDs that define them.
    fn build_head_to_rule_map(rules: &[FlowLogRule]) -> HashMap<String, Vec<usize>> {
        let mut head_to_rule_ids_map: HashMap<String, Vec<usize>> = HashMap::new();

        for (rule_id, rule) in rules.iter().enumerate() {
            // Multiple rules can define the same relation.
            head_to_rule_ids_map
                .entry(rule.head().name().to_string())
                .or_default()
                .push(rule_id);
        }

        head_to_rule_ids_map
    }

    fn analyze_rule_dependencies(
        rule_id: usize,
        rule: &FlowLogRule,
        head_to_rule_ids_map: &HashMap<String, Vec<usize>>,
        dependency_map: &mut HashMap<usize, HashSet<usize>>,
        negative_edges: &mut HashSet<(usize, usize)>,
    ) {
        for predicate in rule.rhs() {
            // Determine the atom name and whether the dependency is negative
            let (atom_name, is_negative) = match predicate {
                Predicate::PositiveAtomPredicate(atom) => (atom.name(), false),
                Predicate::NegativeAtomPredicate(atom) => (atom.name(), true),
                // Other predicate types (constraints, comparisons, etc.) - skip dependency analysis
                _ => continue,
            };

            // For both positive and negative atoms, check if they reference IDB relations
            // If so, add dependency (the rule needs these atoms to be computed first)
            if let Some(dependency_rule_ids) = head_to_rule_ids_map.get(atom_name) {
                for &dep_rule_id in dependency_rule_ids {
                    dependency_map
                        .get_mut(&rule_id)
                        .expect("Stratifier error: rule ID should exist in map")
                        .insert(dep_rule_id);

                    // Track negative edges separately for stratification validation
                    if is_negative {
                        negative_edges.insert((rule_id, dep_rule_id));
                    }
                }
            }
        }
    }
}

impl fmt::Display for DependencyGraph {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\nDependency Graph:")?;
        writeln!(f, "{}", "-".repeat(45))?;

        for (rule_id, dependent_rule_ids) in self.dependency_map().iter().sorted_by_key(|x| x.0) {
            if dependent_rule_ids.is_empty() {
                writeln!(f, "Rule {}: []", rule_id)?;
            } else {
                let dependent_rule_ids_str = dependent_rule_ids
                    .iter()
                    .sorted()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", ");
                writeln!(f, "Rule {}: [{}]", rule_id, dependent_rule_ids_str)?;
            }
        }
        Ok(())
    }
}
