//! Rule Modification Operations
//!
//! This module provides the catalog modification API for transforming
//! logical rules during query planning. Each public method rewrites the
//! rule's RHS to reflect a planning step and re-syncs the catalog's
//! internal metadata via [`Catalog::update_rule`].
//!
//! Operations:
//!
//! 1. **Map** ([`Catalog::map_modify`]): Rename an atom while keeping its
//!    arity intact (used for EDB premap layout adjustments).
//! 2. **Projection** ([`Catalog::projection_modify`]): Drop specified
//!    arguments from an atom, reducing its arity.
//! 3. **SIP** ([`Catalog::sip_modify`]): Reorder a positive atom's
//!    arguments so join keys come first.
//! 4. **Join** ([`Catalog::join_modify`]): Replace a left atom and a set
//!    of right atoms with new joined atoms.
//! 5. **Comparison** ([`Catalog::comparison_modify`]): Fold a comparison
//!    predicate into the atoms it filters, producing renamed copies.
//! 6. **FnCall** ([`Catalog::fn_call_modify`]): Same as comparison, but
//!    folding a function-call predicate.

use super::Catalog;
use crate::catalog::{AtomArgumentSignature, AtomSignature, CatalogError};
use flowlog_parser::{Atom, AtomArg, FlowLogRule, Predicate};

/// Public API for modifying rules and updating catalog metadata accordingly.
impl Catalog {
    /// Map an EDB atom to a required key/value layout.
    ///
    /// Does not change the atom's arity; only the premap of an original
    /// atom should use this function.
    pub(crate) fn map_modify(
        &mut self,
        atom_signature: AtomSignature,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        // Find the global RHS position of the target atom
        let rhs_index = self.rhs_index_from_signature(atom_signature);

        // Create a new mapped atom with the same arguments but a new name
        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtom(atom) => Predicate::PositiveAtom(Atom::new(
                &new_atom_name,
                atom.arguments().to_vec(),
                new_atom_fingerprint,
            )),
            Predicate::NegativeAtom(atom) => Predicate::NegativeAtom(Atom::new(
                &new_atom_name,
                atom.arguments().to_vec(),
                new_atom_fingerprint,
            )),
            other => {
                return Err(CatalogError::internal(format!(
                    "map_modify: target predicate at rhs index {rhs_index} is not an atom: {other}"
                )));
            }
        };

        // Replace the original atom with the mapped atom and update the rule
        self.update_rule_in_place(rhs_index, new_atom)
    }

    /// Projects out specified arguments from an atom, creating a new atom with reduced arity.
    pub(crate) fn projection_modify(
        &mut self,
        atom_signature: AtomSignature,
        arguments_to_delete: Vec<AtomArgumentSignature>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        // Validate that all argument signatures belong to the target atom
        for arg_sig in &arguments_to_delete {
            if *arg_sig.atom_signature() != atom_signature {
                return Err(CatalogError::internal(format!(
                    "projection_modify: argument signature {arg_sig:?} does not belong \
                     to target atom {atom_signature:?}"
                )));
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

        let build_projected_atom = |atom: &Atom| -> Result<Atom, CatalogError> {
            for &arg_id in &arg_ids_to_delete {
                if arg_id >= atom.arity() {
                    return Err(CatalogError::internal(format!(
                        "projection_modify: argument id {arg_id} out of bounds for atom \
                         `{}` with arity {}",
                        atom.name(),
                        atom.arity()
                    )));
                }
            }
            let mut new_args = atom.arguments().to_vec();
            for &arg_id in &arg_ids_to_delete {
                new_args.remove(arg_id);
            }
            Ok(Atom::new(&new_atom_name, new_args, new_atom_fingerprint))
        };

        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtom(atom) => Predicate::PositiveAtom(build_projected_atom(atom)?),
            Predicate::NegativeAtom(atom) => Predicate::NegativeAtom(build_projected_atom(atom)?),
            other => {
                return Err(CatalogError::internal(format!(
                    "projection_modify: target predicate at rhs index {rhs_index} \
                     is not an atom: {other}"
                )));
            }
        };

        // Replace the original atom with the projected atom and update the rule
        self.update_rule_in_place(rhs_index, new_atom)
    }

    /// SIP optimization, left atoms project to key and semijoin on right atom.
    /// It should not modify the arguments on the left side, but it may reorder the arguments on
    /// the right side to put the join keys in front.
    pub(crate) fn sip_modify(
        &mut self,
        right_atom_signature: AtomSignature,
        new_argument_list: Vec<AtomArgumentSignature>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        let rhs_index = self.rhs_index_from_signature(right_atom_signature);

        // Verify the target is a positive atom
        if !matches!(self.rule.rhs()[rhs_index], Predicate::PositiveAtom(_)) {
            return Err(CatalogError::internal(format!(
                "sip_modify: target predicate at rhs index {rhs_index} is not a positive atom: {}",
                self.rule.rhs()[rhs_index]
            )));
        }

        // Create the new SIP atom by reordering arguments according to new arguments
        let new_atom_args = self.lookup_arg_vars(&new_argument_list, "sip_modify")?;

        let new_atom = Atom::new(&new_atom_name, new_atom_args, new_atom_fingerprint);
        self.update_rule_in_place(rhs_index, Predicate::PositiveAtom(new_atom))
    }

    /// Joins multiple atoms into new atoms with specified argument mappings.
    pub(crate) fn join_modify(
        &mut self,
        left_atom_signature: AtomSignature,
        right_atom_signatures: Vec<AtomSignature>,
        new_arguments_list: Vec<Vec<AtomArgumentSignature>>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) -> Result<(), CatalogError> {
        // Ensure all parameter vectors have matching lengths
        let num_right_atoms = right_atom_signatures.len();
        if new_arguments_list.len() != num_right_atoms
            || new_names.len() != num_right_atoms
            || new_fingerprints.len() != num_right_atoms
        {
            return Err(CatalogError::internal(format!(
                "join_modify: parameter length mismatch — right_atom_signatures={}, \
                 new_arguments_list={}, new_names={}, new_fingerprints={}",
                num_right_atoms,
                new_arguments_list.len(),
                new_names.len(),
                new_fingerprints.len()
            )));
        }

        // Find and validate the left atom
        let left_rhs_index = self.rhs_index_from_signature(left_atom_signature);

        // Ensure left predicate is an atom (not a comparison or filter)
        match &self.rule.rhs()[left_rhs_index] {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            other => {
                return Err(CatalogError::internal(format!(
                    "join_modify: left predicate at rhs index {left_rhs_index} \
                     is not an atom: {other}"
                )));
            }
        }

        // Find and validate all right atoms
        let right_indices =
            self.validate_atom_rhs_indices(&right_atom_signatures, "join_modify")?;

        let mut new_joined_atoms = Vec::with_capacity(num_right_atoms);
        for i in 0..num_right_atoms {
            let new_atom_args = self.lookup_arg_vars(&new_arguments_list[i], "join_modify")?;
            let new_atom = Atom::new(&new_names[i], new_atom_args, new_fingerprints[i]);
            new_joined_atoms.push(Predicate::PositiveAtom(new_atom));
        }

        // Remove left atoms and update new joined atoms
        self.remove_and_update_rule(left_rhs_index, right_indices, new_joined_atoms)
    }

    /// Applies a comparison predicate to atoms, creating filtered versions of the atoms.
    pub(crate) fn comparison_modify(
        &mut self,
        comparison_index: usize,
        right_atom_signatures: Vec<AtomSignature>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) -> Result<(), CatalogError> {
        // Ensure all parameter vectors have matching lengths
        let num_atoms = right_atom_signatures.len();
        if new_names.len() != num_atoms || new_fingerprints.len() != num_atoms {
            return Err(CatalogError::internal(format!(
                "comparison_modify: parameter length mismatch — right_atom_signatures={}, \
                 new_names={}, new_fingerprints={}",
                num_atoms,
                new_names.len(),
                new_fingerprints.len()
            )));
        }

        // Get the comparison predicate and find its position in the rule RHS
        let comparison_predicate = &self.comparison_predicates[comparison_index];
        let comparison_rhs_index = self
            .rule
            .rhs()
            .iter()
            .enumerate()
            .find_map(|(idx, p)| match p {
                Predicate::Compare(expr) if expr == comparison_predicate => Some(idx),
                _ => None,
            })
            .ok_or_else(|| {
                CatalogError::internal(format!(
                    "comparison_modify: comparison predicate at index {comparison_index} \
                     not found in rule RHS"
                ))
            })?;

        // Find and validate all target atoms for the comparison
        let right_indices =
            self.validate_atom_rhs_indices(&right_atom_signatures, "comparison_modify")?;

        // Create filtered atoms by copying original atom arguments
        let new_filtered_atoms = self.build_renamed_atom_copies(
            &right_indices,
            &new_names,
            &new_fingerprints,
            "comparison_modify",
        )?;

        // Remove comparison predicate, then update rule with new filtered atoms
        self.remove_and_update_rule(comparison_rhs_index, right_indices, new_filtered_atoms)
    }

    /// Applies a fn_call predicate to atoms, creating filtered versions of the atoms.
    pub(crate) fn fn_call_modify(
        &mut self,
        fn_call_index: usize,
        right_atom_signatures: Vec<AtomSignature>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) -> Result<(), CatalogError> {
        // Ensure all parameter vectors have matching lengths
        let num_atoms = right_atom_signatures.len();
        if new_names.len() != num_atoms || new_fingerprints.len() != num_atoms {
            return Err(CatalogError::internal(format!(
                "fn_call_modify: parameter length mismatch — right_atom_signatures={}, \
                 new_names={}, new_fingerprints={}",
                num_atoms,
                new_names.len(),
                new_fingerprints.len()
            )));
        }

        // Get the fn_call predicate and find its position in the rule RHS
        let fn_call_predicate = &self.fn_call_predicates[fn_call_index];
        let fn_call_rhs_index = self
            .rule
            .rhs()
            .iter()
            .enumerate()
            .find_map(|(idx, p)| match p {
                Predicate::FnCall(fc) if fc == fn_call_predicate => Some(idx),
                _ => None,
            })
            .ok_or_else(|| {
                CatalogError::internal(format!(
                    "fn_call_modify: fn_call predicate at index {fn_call_index} \
                     not found in rule RHS"
                ))
            })?;

        // Find and validate all target atoms
        let right_indices =
            self.validate_atom_rhs_indices(&right_atom_signatures, "fn_call_modify")?;

        // Create filtered atoms by copying original atom arguments
        let new_filtered_atoms = self.build_renamed_atom_copies(
            &right_indices,
            &new_names,
            &new_fingerprints,
            "fn_call_modify",
        )?;

        // Remove fn_call predicate, then update rule with new filtered atoms
        self.remove_and_update_rule(fn_call_rhs_index, right_indices, new_filtered_atoms)
    }

    // ========================================================================================
    // === PRIVATE HELPER METHODS ===
    // ========================================================================================

    /// Resolve each `AtomSignature` to its global RHS index and verify that
    /// the predicate at that index is a positive or negative atom.
    ///
    /// `context` is used as a prefix for the error message so callers can
    /// be identified at the failure site.
    fn validate_atom_rhs_indices(
        &self,
        signatures: &[AtomSignature],
        context: &str,
    ) -> Result<Vec<usize>, CatalogError> {
        signatures
            .iter()
            .map(|&sig| {
                let idx = self.rhs_index_from_signature(sig);
                match &self.rule.rhs()[idx] {
                    Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => Ok(idx),
                    other => Err(CatalogError::internal(format!(
                        "{context}: right predicate at rhs index {idx} is not an atom: {other}"
                    ))),
                }
            })
            .collect()
    }

    /// Look up each argument signature in `signature_to_argument_str_map`
    /// and wrap the resulting variable name in `AtomArg::Var`.
    fn lookup_arg_vars(
        &self,
        signatures: &[AtomArgumentSignature],
        context: &str,
    ) -> Result<Vec<AtomArg>, CatalogError> {
        signatures
            .iter()
            .map(|arg_sig| {
                self.signature_to_argument_str_map
                    .get(arg_sig)
                    .cloned()
                    .map(AtomArg::Var)
                    .ok_or_else(|| {
                        CatalogError::internal(format!(
                            "{context}: argument signature {arg_sig:?} not found in signature map"
                        ))
                    })
            })
            .collect()
    }

    /// Build a positive atom for each RHS index by copying the existing
    /// atom's arguments and assigning the matching name and fingerprint.
    ///
    /// Callers should have already validated that each index points at an
    /// atom (e.g. via [`Self::validate_atom_rhs_indices`]); the inner
    /// match is kept defensive so a stale index still surfaces a clean
    /// internal error instead of a panic.
    fn build_renamed_atom_copies(
        &self,
        indices: &[usize],
        new_names: &[String],
        new_fingerprints: &[u64],
        context: &str,
    ) -> Result<Vec<Predicate>, CatalogError> {
        indices
            .iter()
            .enumerate()
            .map(|(i, &atom_idx)| {
                let args = match &self.rule.rhs()[atom_idx] {
                    Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                        atom.arguments().to_vec()
                    }
                    other => {
                        return Err(CatalogError::internal(format!(
                            "{context}: expected atom predicate at rhs index {atom_idx}, got: {other}"
                        )));
                    }
                };
                let new_atom = Atom::new(&new_names[i], args, new_fingerprints[i]);
                Ok(Predicate::PositiveAtom(new_atom))
            })
            .collect()
    }

    /// Update the rule by replacing the predicate at the specified index.
    /// Note that we take global RHS index here, not positive or negative index.
    fn update_rule_in_place(
        &mut self,
        global_rhs_idx: usize,
        new_predicate: Predicate,
    ) -> Result<(), CatalogError> {
        let mut new_rhs = self.rule.rhs().to_vec();
        new_rhs[global_rhs_idx] = new_predicate;
        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs);
        self.update_rule(&new_rule)
    }

    /// Remove specified indices from RHS, add new predicates, and update the rule.
    /// Indices are removed in descending order to preserve correctness.
    /// Note that we take global RHS indices here, not positive or negative indices.
    fn remove_and_update_rule(
        &mut self,
        global_rhs_index_to_remove: usize,
        global_rhs_indices_to_update: Vec<usize>,
        new_predicates: Vec<Predicate>,
    ) -> Result<(), CatalogError> {
        let mut new_rhs = self.rule.rhs().to_vec();

        for (idx, pred) in global_rhs_indices_to_update.into_iter().zip(new_predicates) {
            new_rhs[idx] = pred;
        }
        new_rhs.remove(global_rhs_index_to_remove);

        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs);
        self.update_rule(&new_rule)
    }
}
