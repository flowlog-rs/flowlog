//! Dependency graph construction for Macaron Datalog programs.

use itertools::Itertools;
use parser::{logic::MacaronRule, Predicate, Program};
use std::collections::{HashMap, HashSet};
use std::fmt;

/// Represents the dependency relationships between rules.
#[derive(Debug, Clone)]
pub(super) struct DependencyGraph {
    /// All dependencies: every rule whose head predicate appears in the body of another rule.
    dependency_map: HashMap<usize, HashSet<usize>>,
}

impl DependencyGraph {
    /// Returns map from rule ID to the set of rule IDs it depends on (polarity-agnostic).
    #[must_use]
    pub(super) fn dependency_map(&self) -> &HashMap<usize, HashSet<usize>> {
        &self.dependency_map
    }

    /// Constructs a dependency graph from a program.
    #[must_use]
    pub(super) fn from_program(program: &Program) -> Self {
        let rules = program.rules();

        let head_to_rule_ids_map = Self::build_head_to_rule_map(rules);

        let mut dependency_map: HashMap<usize, HashSet<usize>> =
            (0..rules.len()).map(|i| (i, HashSet::new())).collect();

        for (rule_id, rule) in rules.iter().enumerate() {
            Self::analyze_rule_dependencies(
                rule_id,
                rule,
                &head_to_rule_ids_map,
                &mut dependency_map,
            );
        }

        Self { dependency_map }
    }

    /// Builds mapping from relation names to rule IDs that define them.
    fn build_head_to_rule_map(rules: &[MacaronRule]) -> HashMap<String, Vec<usize>> {
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
        rule: &MacaronRule,
        head_to_rule_ids_map: &HashMap<String, Vec<usize>>,
        dependency_map: &mut HashMap<usize, HashSet<usize>>,
    ) {
        for predicate in rule.rhs() {
            // Determine the atom name based on predicate type and handle dependencies
            let atom_name = match predicate {
                Predicate::PositiveAtomPredicate(atom) | Predicate::NegatedAtomPredicate(atom) => {
                    atom.name()
                }
                // Other predicate types (constraints, comparisons, etc.) - skip dependency analysis
                _ => continue,
            };

            // For both positive and negated atoms, check if they reference IDB relations
            // If so, add positive dependency (the rule needs these atoms to be computed first)
            if let Some(dependency_rule_ids) = head_to_rule_ids_map.get(atom_name) {
                // Add dependency (polarity-agnostic): current rule depends on rules defining this atom
                dependency_map
                    .get_mut(&rule_id)
                    .expect("Rule ID should exist in map")
                    .extend(dependency_rule_ids.iter().copied());
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
