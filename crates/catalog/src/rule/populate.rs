//! Rule metadata population operations.
//!
//! This module contains functions for populating various metadata structures
//! needed for rule processing, including argument signatures, variable mappings,
//! and dependency relationships between atoms.

use std::collections::{HashMap, HashSet};

use super::Catalog;
use crate::atom::{AtomArgumentSignature, AtomSignature};
use crate::filter::Filters;
use parser::{AtomArg, Predicate};

/// Internal API for populating all metadata fields given a parsed rule.
impl Catalog {
    /// Populates all metadata required for rule processing.
    pub(crate) fn populate_all_metadata(&mut self) {
        // 1. Build signatures, filters, fingerprints, and collect comparison predicates
        self.populate_argument_signatures();

        // 2. Map each variable to the positive atoms (by rhs index) where it occurs
        self.populate_argument_presence_in_positive_atom_map();

        // 3. Detect arguments that can be pruned (appear only once and not in head)
        self.populate_unused_arguments();

        // 4. Compute superset relationships among atoms and comparisons
        self.populate_supersets();

        // 5. Cache head argument objects by their string form
        self.head_arguments_map = self
            .rule
            .head()
            .head_arguments()
            .iter()
            .map(|ha| (ha.to_string(), ha.clone()))
            .collect();
    }

    /// Populates argument signatures and filters for positive/negative atoms and comparisons.
    fn populate_argument_signatures(&mut self) {
        // Tracks vars that are already bound (safe) before encountering them in a negated atom
        let mut is_safe_set = HashSet::new();

        // Map of later-occurring var signature to first-occurring signature (for equality filters)
        let mut local_var_eq_map = HashMap::new();

        // Tracks first occurrence of each variable within a single atom (reset per atom)
        let mut local_var_first_occurrence_map: HashMap<String, AtomArgumentSignature> =
            HashMap::new();

        // Map of argument signature to constant literal
        let mut local_const_map = HashMap::new();

        // Placeholders (wildcards) encountered
        let mut local_placeholder_set = HashSet::new();

        // Partition RHS predicates into positive atoms, negated atoms, and comparisons
        let (positive_atoms, negated_atoms, comparison_predicates): (Vec<_>, Vec<_>, Vec<_>) =
            self.rule.rhs().iter().enumerate().fold(
                (Vec::new(), Vec::new(), Vec::new()),
                |(mut pos, mut neg, mut comp), (i, p)| {
                    match p {
                        Predicate::PositiveAtomPredicate(a) => {
                            pos.push(a);
                            self.positive_atom_rhs_ids.push(i);
                        }
                        Predicate::NegatedAtomPredicate(a) => {
                            neg.push(a);
                            self.negated_atom_rhs_ids.push(i);
                        }
                        Predicate::ComparePredicate(expr) => comp.push(expr.clone()),
                        Predicate::BoolPredicate(_) => {}
                    };
                    (pos, neg, comp)
                },
            );

        // Process positive atoms: record fingerprints, build argument signatures, record var/const/placeholder, collect var set
        for (rhs_id, atom) in positive_atoms.iter().enumerate() {
            let mut atom_sigs = Vec::new();
            let mut atom_var_str_set = HashSet::new();
            for (arg_id, arg) in atom.arguments().iter().enumerate() {
                let sig = AtomArgumentSignature::new(AtomSignature::new(true, rhs_id), arg_id);
                atom_sigs.push(sig);
                match arg {
                    AtomArg::Var(v) => {
                        // Mark variable as safe (can be used in later negated atoms)
                        is_safe_set.insert(v);
                        atom_var_str_set.insert(v.to_string());
                        self.signature_to_argument_str_map
                            .insert(sig, v.to_string());
                        if let Some(first) = local_var_first_occurrence_map.get(v) {
                            // Equal to a previous occurrence inside the same atom
                            local_var_eq_map.insert(sig, *first);
                        } else {
                            local_var_first_occurrence_map.insert(v.to_string(), sig);
                        }
                    }
                    AtomArg::Const(c) => {
                        // Record constant binding
                        local_const_map.insert(sig, c.to_owned());
                    }
                    AtomArg::Placeholder => {
                        // Record placeholder _
                        local_placeholder_set.insert(sig);
                    }
                }
            }

            self.positive_atom_fingerprints.push(atom.fingerprint());
            self.positive_atom_argument_signatures.push(atom_sigs);
            self.positive_atom_argument_vars_str_sets
                .push(atom_var_str_set);

            // Reset first-occurrence tracking for next atom
            local_var_first_occurrence_map.clear();
        }

        // Process negated atoms: only already-safe vars are allowed
        for (neg_rhs_id, atom) in negated_atoms.iter().enumerate() {
            let mut neg_sigs = Vec::new();
            let mut neg_var_str_set = HashSet::new();
            for (arg_id, arg) in atom.arguments().iter().enumerate() {
                let sig = AtomArgumentSignature::new(AtomSignature::new(false, neg_rhs_id), arg_id);
                neg_sigs.push(sig);
                match arg {
                    AtomArg::Var(v) => {
                        if is_safe_set.contains(v) {
                            neg_var_str_set.insert(v.to_string());
                            self.signature_to_argument_str_map
                                .insert(sig, v.to_string());
                            if let Some(first) = local_var_first_occurrence_map.get(v) {
                                local_var_eq_map.insert(sig, *first);
                            } else {
                                local_var_first_occurrence_map.insert(v.to_string(), sig);
                            }
                        } else {
                            panic!(
                                "Catalog error: unsafe var detected at negation !{} of rule {}",
                                atom, self.rule
                            );
                        }
                    }
                    AtomArg::Const(c) => {
                        local_const_map.insert(sig, c.to_owned());
                    }
                    AtomArg::Placeholder => {
                        local_placeholder_set.insert(sig);
                    }
                }
            }
            self.negated_atom_fingerprints.push(atom.fingerprint());
            self.negated_atom_argument_signatures.push(neg_sigs);
            self.negated_atom_argument_vars_str_sets
                .push(neg_var_str_set);

            // Reset first-occurrence tracking for next atom
            local_var_first_occurrence_map.clear();
        }

        // Build filter structures (variable equalities, constant bindings, placeholders)
        self.filters = Filters::new(local_var_eq_map, local_const_map, local_placeholder_set);
        // Build variable string sets for each comparison predicate (used for superset analysis)
        self.comparison_predicates_vars_str_set = comparison_predicates
            .iter()
            .map(|c| {
                c.vars_set()
                    .into_iter()
                    .cloned()
                    .collect::<HashSet<String>>()
            })
            .collect();
        self.comparison_predicates = comparison_predicates;
    }

    /// Creates a map of which variables appear in which positive atoms.
    fn populate_argument_presence_in_positive_atom_map(&mut self) {
        for (rhs_id, sigs) in self.positive_atom_argument_signatures.iter().enumerate() {
            for sig in sigs {
                // Skip non-binding argument kinds (constants, equality-propagated vars, placeholders)
                if self.filters.is_const_or_var_eq_or_placeholder(sig) {
                    continue; // Not a primary binding occurrence
                }
                if let Some(var) = self.signature_to_argument_str_map.get(sig) {
                    // Allocate the per-variable presence vector lazily with appropriate length
                    let entry = self
                        .argument_presence_in_positive_atom_map
                        .entry(var.clone())
                        .or_insert(vec![None; self.positive_atom_argument_signatures.len()]);
                    // Only record if not already set for this atom index
                    if entry[rhs_id].is_none() {
                        entry[rhs_id] = Some(*sig);
                    }
                } else {
                    panic!("Cataslog error: signature {:?} absent", sig);
                }
            }
        }
    }

    /// Identifies arguments that appear only once and aren't in the head.
    ///
    /// Variables that appear only once in the rule body (excluding the head) can be
    /// considered unused and potentially pruned for optimization purposes.
    fn populate_unused_arguments(&mut self) {
        let mut variable_counts = HashMap::new();

        // Count occurrences in positive atoms
        for vars_set in &self.positive_atom_argument_vars_str_sets {
            for variable in vars_set {
                *variable_counts.entry(variable.clone()).or_insert(0) += 1;
            }
        }

        // Count occurrences in negated atoms
        for vars_set in &self.negated_atom_argument_vars_str_sets {
            for variable in vars_set {
                *variable_counts.entry(variable.clone()).or_insert(0) += 1;
            }
        }

        // Count occurrences in comparison predicates
        for comparison_predicate in &self.comparison_predicates {
            for variable in comparison_predicate.vars_set() {
                *variable_counts.entry(variable.clone()).or_insert(0) += 1;
            }
        }

        // Collect all head variables (never considered unused, even if single-occurrence)
        let head_variables = self.head_arguments_strs();

        // Identify and group unused arguments by their atom signature
        for (signature, variable) in &self.signature_to_argument_str_map {
            let appears_only_once = matches!(variable_counts.get(variable), Some(1));
            let not_in_head = !head_variables.contains(variable);

            if appears_only_once && not_in_head {
                // Group unused argument signatures by their atom signature
                self.unused_arguments_per_atom
                    .entry(*signature.atom_signature())
                    .or_default()
                    .push(*signature);
            }
        }
    }

    /// Finds superset relationships between predicates based on their variable sets.
    fn populate_supersets(&mut self) {
        let pos_var_sets = &self.positive_atom_argument_vars_str_sets;

        // For each positive atom: list indices of other positive atoms whose var set is a superset
        self.positive_supersets = (0..pos_var_sets.len())
            .map(|i| {
                pos_var_sets
                    .iter()
                    .enumerate()
                    .filter(|(j, set)| i != *j && pos_var_sets[i].is_subset(set))
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();

        // For each negated atom: list indices of positive atoms that contain all its vars
        self.negated_supersets = self
            .negated_atom_argument_vars_str_sets
            .iter()
            .map(|set| {
                pos_var_sets
                    .iter()
                    .enumerate()
                    .filter(|(_, ps)| set.is_subset(ps))
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();

        // For each comparison predicate: positive atoms whose var set covers it
        self.comparison_supersets = self
            .comparison_predicates_vars_str_set
            .iter()
            .map(|set| {
                pos_var_sets
                    .iter()
                    .enumerate()
                    .filter(|(_, ps)| set.is_subset(ps))
                    .map(|(j, _)| j)
                    .collect()
            })
            .collect();
    }
}
