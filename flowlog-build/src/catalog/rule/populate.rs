//! Rule metadata population operations.
//!
//! This module contains functions for populating various metadata structures
//! needed for rule processing, including argument signatures, variable mappings,
//! and dependency relationships between atoms.

use std::collections::{HashMap, HashSet};

use super::Catalog;
use crate::catalog::{
    AtomArgumentSignature, AtomSignature, CatalogError, Filters, UnsafePredicateKind,
};
use flowlog_parser::{AtomArg, Predicate};

/// Internal API for populating all metadata fields given a parsed rule.
impl Catalog {
    /// Populates all metadata required for rule processing.
    pub(super) fn populate_all_metadata(&mut self) -> Result<(), CatalogError> {
        // 1. Build signatures, filters, fingerprints, and collect comparison predicates
        self.populate_argument_signatures()?;

        // 2. Map each variable to the positive atoms (by rhs index) where it occurs
        self.populate_argument_presence_in_positive_atom_map()?;

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

        Ok(())
    }

    /// Populates argument signatures and filters for positive/negative atoms and comparisons.
    fn populate_argument_signatures(&mut self) -> Result<(), CatalogError> {
        // Tracks vars that are already bound (safe) before encountering them in a negative atom
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

        // Partition RHS predicates into positive atoms, negative atoms, and
        // comparisons.
        let mut positive_atoms = Vec::new();
        let mut negative_atoms = Vec::new();
        let mut comparison_predicates = Vec::new();
        for (i, p) in self.rule.rhs().iter().enumerate() {
            match p {
                Predicate::PositiveAtom(a) => {
                    positive_atoms.push(a);
                    self.positive_atom_rhs_ids.push(i);
                }
                Predicate::NegativeAtom(a) => {
                    negative_atoms.push(a);
                    self.negative_atom_rhs_ids.push(i);
                }
                Predicate::Compare(expr) => comparison_predicates.push(expr.clone()),
            }
        }

        // Process positive atoms: record fingerprints, build argument signatures, record var/const/placeholder, collect var set
        for (pos_rhs_id, atom) in positive_atoms.iter().enumerate() {
            let mut atom_sigs = Vec::new();
            let mut atom_var_str_set = HashSet::new();
            for (arg_id, arg) in atom.arguments().iter().enumerate() {
                let sig = AtomArgumentSignature::new(AtomSignature::new(true, pos_rhs_id), arg_id);
                atom_sigs.push(sig);
                match arg {
                    AtomArg::Var(v) => {
                        // Mark variable as safe (can be used in later negative atoms)
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

        // Range-restriction: every variable in a negative atom or comparison
        // must appear in some positive body atom. Display strings and spans
        // are fetched only on the error path.
        let rule_span = self.rule.span();
        for atom in &negative_atoms {
            for arg in atom.arguments() {
                if let AtomArg::Var(v) = arg
                    && !is_safe_set.contains(v)
                {
                    return Err(CatalogError::UnsafeVariable {
                        kind: UnsafePredicateKind::Negation,
                        predicate: format!("!{atom}"),
                        predicate_span: atom.span(),
                        rule_span,
                        var: v.clone(),
                    });
                }
            }
        }
        for comp in &comparison_predicates {
            for v in comp.vars_set() {
                if !is_safe_set.contains(v) {
                    return Err(CatalogError::UnsafeVariable {
                        kind: UnsafePredicateKind::Comparison,
                        predicate: comp.to_string(),
                        predicate_span: comp.span(),
                        rule_span,
                        var: v.clone(),
                    });
                }
            }
        }

        // Process negative atoms: populate signature metadata (safety already verified above).
        for (neg_rhs_id, atom) in negative_atoms.iter().enumerate() {
            let mut neg_sigs = Vec::new();
            let mut neg_var_str_set = HashSet::new();
            for (arg_id, arg) in atom.arguments().iter().enumerate() {
                let sig = AtomArgumentSignature::new(AtomSignature::new(false, neg_rhs_id), arg_id);
                neg_sigs.push(sig);
                match arg {
                    AtomArg::Var(v) => {
                        neg_var_str_set.insert(v.to_string());
                        self.signature_to_argument_str_map
                            .insert(sig, v.to_string());
                        if let Some(first) = local_var_first_occurrence_map.get(v) {
                            local_var_eq_map.insert(sig, *first);
                        } else {
                            local_var_first_occurrence_map.insert(v.to_string(), sig);
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
            self.negative_atom_fingerprints.push(atom.fingerprint());
            self.negative_atom_argument_signatures.push(neg_sigs);
            self.negative_atom_argument_vars_str_sets
                .push(neg_var_str_set);

            local_var_first_occurrence_map.clear();
        }

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

        Ok(())
    }

    /// Creates a map of which variables appear in which positive atoms.
    fn populate_argument_presence_in_positive_atom_map(&mut self) -> Result<(), CatalogError> {
        let n_atoms = self.positive_atom_argument_signatures.len();
        for (rhs_id, sigs) in self.positive_atom_argument_signatures.iter().enumerate() {
            for sig in sigs {
                // Skip non-binding argument kinds (constants, equality-propagated
                // vars, placeholders) — only primary binding occurrences count.
                if self.filters.is_const_or_var_eq_or_placeholder(sig) {
                    continue;
                }
                let Some(var) = self.signature_to_argument_str_map.get(sig) else {
                    return Err(CatalogError::internal(format!(
                        "argument signature {sig} absent from signature_to_argument_str_map"
                    )));
                };
                let entry = self
                    .argument_presence_in_positive_atom_map
                    .entry(var.clone())
                    .or_insert_with(|| vec![None; n_atoms]);
                // Only record the first binding occurrence per atom index.
                if entry[rhs_id].is_none() {
                    entry[rhs_id] = Some(*sig);
                }
            }
        }
        Ok(())
    }

    /// Identifies arguments that appear only once and aren't in the head.
    ///
    /// Variables that appear only once in the rule body (excluding the head) can be
    /// considered unused and potentially pruned for optimization purposes.
    fn populate_unused_arguments(&mut self) {
        let mut variable_counts: HashMap<String, u32> = HashMap::new();
        let mut bump = |v: &String| {
            *variable_counts.entry(v.clone()).or_insert(0) += 1;
        };

        for vars_set in &self.positive_atom_argument_vars_str_sets {
            vars_set.iter().for_each(&mut bump);
        }
        for vars_set in &self.negative_atom_argument_vars_str_sets {
            vars_set.iter().for_each(&mut bump);
        }
        for comparison_predicate in &self.comparison_predicates {
            comparison_predicate
                .vars_set()
                .into_iter()
                .for_each(&mut bump);
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

        // Indices of positive atoms whose var set is a superset of `needle`,
        // optionally excluding one self-index.
        let pos_supersets_of = |needle: &HashSet<String>, exclude: Option<usize>| -> Vec<usize> {
            pos_var_sets
                .iter()
                .enumerate()
                .filter(|(j, ps)| Some(*j) != exclude && needle.is_subset(ps))
                .map(|(j, _)| j)
                .collect()
        };

        // For each positive atom: other positive atoms whose var set is a superset
        self.positive_supersets = (0..pos_var_sets.len())
            .map(|i| pos_supersets_of(&pos_var_sets[i], Some(i)))
            .collect();

        // For each negative atom: positive atoms that contain all its vars
        self.negative_supersets = self
            .negative_atom_argument_vars_str_sets
            .iter()
            .map(|set| pos_supersets_of(set, None))
            .collect();

        // For each comparison predicate: positive atoms whose var set covers it
        self.comparison_supersets = self
            .comparison_predicates_vars_str_set
            .iter()
            .map(|set| pos_supersets_of(set, None))
            .collect();
    }
}
