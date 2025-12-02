//! Rule Modification Operations
//!
//! This module provides the catalog modification API for transforming logical rules
//! during query planning. It implements three core transformation operations:
//!
//! 1. **Projection**: Remove specified arguments from atoms
//! 2. **Join**: Combine multiple atoms into new atoms
//! 3. **Comparison**: Apply comparison predicates to atoms and generate filtered results
//!
//! All operations maintain catalog consistency by updating internal metadata mappings
//! and rebuilding the underlying rule structure.

use super::Catalog;
use crate::atom::{AtomArgumentSignature, AtomSignature};
use parser::{Atom, AtomArg, FlowLogRule, Predicate};

/// Public API for modifying rules and updating catalog metadata accordingly.
impl Catalog {
    /// Map an EDB atom to a required key/value layout.
    /// This function do not change the arity of the atom.
    /// Only premap of an original atom should use this function.
    pub fn map_modify(
        &mut self,
        atom_signature: AtomSignature,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) {
        // Find the global RHS position of the target atom
        let rhs_index = self.rhs_index_from_signature(atom_signature);

        // Create a new mapped atom with the same arguments but a new name
        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtomPredicate(atom) | Predicate::NegativeAtomPredicate(atom) => {
                let new_atom = Atom::new(
                    &new_atom_name,
                    atom.arguments().to_vec(),
                    new_atom_fingerprint,
                );
                match &self.rule.rhs()[rhs_index] {
                    Predicate::PositiveAtomPredicate(_) => {
                        Predicate::PositiveAtomPredicate(new_atom)
                    }
                    Predicate::NegativeAtomPredicate(_) => {
                        Predicate::NegativeAtomPredicate(new_atom)
                    }
                    _ => unreachable!(),
                }
            }
            other => {
                panic!(
                    "Catalog error: Target predicate at rhs index {} is not an positive atom: {}",
                    rhs_index, other
                )
            }
        };

        // Replace the original atom with the mapped atom and update the rule
        self.update_rule_in_place(rhs_index, new_atom);
    }

    /// Projects out specified arguments from an atom, creating a new atom with reduced arity.
    pub fn projection_modify(
        &mut self,
        atom_signature: AtomSignature,
        arguments_to_delete: Vec<AtomArgumentSignature>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) {
        // Validate that all argument signatures belong to the target atom
        for arg_sig in &arguments_to_delete {
            if *arg_sig.atom_signature() != atom_signature {
                panic!("Catalog error: All argument signatures must belong to the specified atom");
            }
        }

        // Find the global RHS position of the target atom
        let rhs_index = self.rhs_index_from_signature(atom_signature);

        // Convert argument signatures to IDs and sort in reverse order
        // (reverse order ensures removal doesn't affect subsequent indices)
        let mut arg_ids_to_delete: Vec<usize> = arguments_to_delete
            .iter()
            .map(|s| s.argument_id())
            .collect();
        arg_ids_to_delete.sort_unstable();
        arg_ids_to_delete.reverse();

        // Create the new projected atom
        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtomPredicate(atom) | Predicate::NegativeAtomPredicate(atom) => {
                // Validate argument bounds before removal
                for &arg_id in &arg_ids_to_delete {
                    if arg_id >= atom.arity() {
                        panic!("Catalog error: Argument id {} out of bounds for atom '{}' with arity {}", arg_id, atom.name(), atom.arity());
                    }
                }

                // Remove arguments in reverse order to preserve indices
                let mut new_args = atom.arguments().to_vec();
                arg_ids_to_delete.iter().for_each(|&arg_id| {
                    new_args.remove(arg_id);
                });

                // Create new atom
                let new_atom = Atom::new(&new_atom_name, new_args, new_atom_fingerprint);
                match &self.rule.rhs()[rhs_index] {
                    Predicate::PositiveAtomPredicate(_) => {
                        Predicate::PositiveAtomPredicate(new_atom)
                    }
                    Predicate::NegativeAtomPredicate(_) => {
                        Predicate::NegativeAtomPredicate(new_atom)
                    }
                    _ => unreachable!("Catalog error: unexpected predicate type in projection"),
                }
            }
            other => {
                panic!(
                    "Catalog error: Target predicate at rhs index {} is not an atom: {}",
                    rhs_index, other
                )
            }
        };

        // Replace the original atom with the projected atom and update the rule
        self.update_rule_in_place(rhs_index, new_atom);
    }

    /// Joins multiple atoms into new atoms with specified argument mappings.
    pub fn join_modify(
        &mut self,
        left_atom_signature: AtomSignature,
        right_atom_signatures: Vec<AtomSignature>,
        new_arguments_list: Vec<Vec<AtomArgumentSignature>>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) {
        // Ensure all parameter vectors have matching lengths
        let num_right_atoms = right_atom_signatures.len();
        if new_arguments_list.len() != num_right_atoms
            || new_names.len() != num_right_atoms
            || new_fingerprints.len() != num_right_atoms
        {
            panic!("Catalog error: right_atom_signatures, new_arguments_list, new_names, and new_fingerprints must have the same length");
        }

        // Find and validate the left atom
        let left_rhs_index = self.rhs_index_from_signature(left_atom_signature);

        // Ensure left predicate is an atom (not a comparison or filter)
        match &self.rule.rhs()[left_rhs_index] {
            Predicate::PositiveAtomPredicate(_) | Predicate::NegativeAtomPredicate(_) => {}
            _ => panic!("Catalog error: Left predicate must be an atom"),
        }

        // Find and validate all right atoms
        let right_indices: Vec<usize> = right_atom_signatures
            .iter()
            .map(|&sig| {
                let idx = self.rhs_index_from_signature(sig);
                // Ensure each right predicate is an atom
                match &self.rule.rhs()[idx] {
                    Predicate::PositiveAtomPredicate(_) | Predicate::NegativeAtomPredicate(_) => {}
                    _ => panic!("Catalog error: Right predicate must be an atom"),
                }
                idx
            })
            .collect();

        // Create new joined atoms from the argument mappings
        let new_joined_atoms: Vec<Predicate> = (0..num_right_atoms)
            .map(|i| {
                // Convert argument signatures to actual argument strings
                let new_atom_args: Vec<AtomArg> = new_arguments_list[i]
                    .iter()
                    .map(|arg_sig| {
                        self.signature_to_argument_str_map
                            .get(arg_sig)
                            .map(|arg_str| AtomArg::Var(arg_str.clone()))
                            .unwrap_or_else(|| {
                                panic!(
                                    "Catalog error: Argument signature {:?} not found in signature map",
                                    arg_sig
                                )
                            })
                    })
                    .collect();

                // Create the new joined atom
                let new_atom = Atom::new(&new_names[i], new_atom_args, new_fingerprints[i]);
                Predicate::PositiveAtomPredicate(new_atom)
            })
            .collect();

        // Remove left atoms and update new joined atoms
        self.remove_and_update_rule(left_rhs_index, right_indices, new_joined_atoms);
    }

    /// Applies a comparison predicate to atoms, creating filtered versions of the atoms.
    pub fn comparison_modify(
        &mut self,
        comparison_index: usize,
        right_atom_signatures: Vec<AtomSignature>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) {
        // Ensure all parameter vectors have matching lengths
        let num_atoms = right_atom_signatures.len();
        if new_names.len() != num_atoms || new_fingerprints.len() != num_atoms {
            panic!("Catalog error: right_atom_signatures, new_names, and new_fingerprints must have the same length");
        }

        // Get the comparison predicate and find its position in the rule RHS
        let comparison_predicate = &self.comparison_predicates[comparison_index];
        let comparison_rhs_index = self
            .rule
            .rhs()
            .iter()
            .enumerate()
            .find_map(|(idx, p)| match p {
                Predicate::ComparePredicate(expr) if expr == comparison_predicate => Some(idx),
                _ => None,
            })
            .unwrap_or_else(|| {
                panic!(
                    "Catalog error: Comparison predicate at index {} not found in rule RHS",
                    comparison_index
                )
            });

        // Find and validate all target atoms for the comparison
        let right_indices: Vec<usize> = right_atom_signatures
            .iter()
            .map(|&sig| {
                let idx = self.rhs_index_from_signature(sig);
                // Ensure the target predicate is an atom
                match &self.rule.rhs()[idx] {
                    Predicate::PositiveAtomPredicate(_) | Predicate::NegativeAtomPredicate(_) => {}
                    _ => panic!("Catalog error: Right predicate must be an atom"),
                }
                idx
            })
            .collect();

        // Create filtered atoms by copying original atom arguments
        let new_filtered_atoms: Vec<Predicate> = right_indices
            .iter()
            .enumerate()
            .map(|(i, &atom_idx)| {
                // Extract arguments from the original atom
                let new_atom_args = match &self.rule.rhs()[atom_idx] {
                    Predicate::PositiveAtomPredicate(atom)
                    | Predicate::NegativeAtomPredicate(atom) => atom.arguments().to_vec(),
                    _ => panic!("Catalog error: Expected atom predicate"),
                };

                // Create the new filtered atom with copied arguments
                let new_atom = Atom::new(&new_names[i], new_atom_args, new_fingerprints[i]);
                Predicate::PositiveAtomPredicate(new_atom)
            })
            .collect();

        // Remove comparison predicate, then update rule with new filtered atoms
        self.remove_and_update_rule(comparison_rhs_index, right_indices, new_filtered_atoms);
    }

    // ========================================================================================
    // === PRIVATE HELPER METHODS ===
    // ========================================================================================

    /// Update the rule by replacing the predicate at the specified index.
    /// Note that we take global RHS index here, not positive or negative index.
    fn update_rule_in_place(&mut self, global_rhs_idx: usize, new_predicate: Predicate) {
        let mut new_rhs = self.rule.rhs().to_vec();

        // Update the rule
        new_rhs[global_rhs_idx] = new_predicate;
        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs, self.rule.is_planning());
        self.update_rule(&new_rule);
    }

    /// Remove specified indices from RHS, add new predicates, and update the rule.
    /// Indices are removed in descending order to preserve correctness.
    /// Note that we take global RHS indices here, not positive or negative indices.
    fn remove_and_update_rule(
        &mut self,
        global_rhs_index_to_remove: usize,
        global_rhs_indices_to_update: Vec<usize>,
        new_predicates: Vec<Predicate>,
    ) {
        let mut new_rhs = self.rule.rhs().to_vec();

        // Update the rule with new predicates
        global_rhs_indices_to_update
            .iter()
            .enumerate()
            .for_each(|(i, idx)| {
                new_rhs[*idx] = new_predicates[i].clone();
            });
        new_rhs.remove(global_rhs_index_to_remove);

        // Add new predicates and update the rule
        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs, self.rule.is_planning());
        self.update_rule(&new_rule);
    }
}
