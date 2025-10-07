//! Core logic for rule planning.
//!
//! This module implements the core rule planning algorithm, focusing on joining
//! two positive atoms and applying optimization transformations in a fixed-point loop.
//!
//! Core logic depends on optimizer give the index of the two atoms to join.

use std::collections::{HashMap, HashSet};

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use tracing::trace;

// =========================================================================
// Core Rule Planning Logic
// =========================================================================
impl RulePlanner {
    /// This is the main entry point for the rule planning process. It performs a join
    /// between two positive atoms and then applies optimization transformations in a
    /// fixed-point loop until no more optimizations can be applied.
    pub fn core(&mut self, catalog: &mut Catalog, join_tuple_index: (usize, usize)) {
        trace!(
            "Join:\n  LHS atom: ({}, {})\n RHS atom: ({}, {})",
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.0)],
            catalog.positive_atom_rhs_id(join_tuple_index.0),
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.1)],
            catalog.positive_atom_rhs_id(join_tuple_index.1),
        );

        // Execute the initial join between the two selected atoms
        self.apply_join(catalog, join_tuple_index);
        trace!("Catalog:\n{}", catalog);
        trace!("{}", "-".repeat(60));

        // Apply optimization transformations until fixed point
        loop {
            // 1) Apply semijoin optimizations and comparison pushdown
            // These optimizations can create new opportunities for projection
            if self.apply_semijoin(catalog) {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Remove unused arguments to reduce data volume
            // This must come after semijoins as they may eliminate argument usage
            if self.remove_unused_arguments(catalog) {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Fixed point reached - no more optimizations possible
            break;
        }
    }

    /// Applies a join transformation between two positive atoms.
    fn apply_join(&mut self, catalog: &mut Catalog, join_tuple_index: (usize, usize)) {
        let current_transformation_index = self.transformation_infos.len();
        let (lhs_idx, rhs_idx) = join_tuple_index;

        // Extract LHS atom information and register as consumer
        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_idx);
        let left_atom_signature = AtomSignature::new(true, lhs_idx);
        let left_atom_argument_signatures = catalog.positive_atom_argument_signature(lhs_idx);

        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            lhs_pos_fp,
            current_transformation_index,
        );

        // Extract RHS atom information and register as consumer
        let rhs_pos_fp = catalog.positive_atom_fingerprint(rhs_idx);
        let right_atom_signatures = vec![AtomSignature::new(true, rhs_idx)];
        let right_atom_argument_signatures = catalog.positive_atom_argument_signature(rhs_idx);

        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            rhs_pos_fp,
            current_transformation_index,
        );

        // Partition arguments into join keys and payload values
        let (lhs_keys, lhs_vals, rhs_vals) = Self::partition_shared_keys(
            catalog,
            left_atom_argument_signatures,
            right_atom_argument_signatures,
        );
        trace!(
            "Join keys: {:?}",
            lhs_keys
                .iter()
                .map(|pos| (
                    pos,
                    catalog.signature_to_argument_str(pos.init().signature().unwrap())
                ))
                .collect::<Vec<_>>()
        );
        trace!(
            "Join LHS values: {:?}",
            lhs_vals
                .iter()
                .map(|pos| (
                    pos,
                    catalog.signature_to_argument_str(pos.init().signature().unwrap())
                ))
                .collect::<Vec<_>>()
        );
        trace!(
            "Join RHS values: {:?}",
            rhs_vals
                .iter()
                .map(|pos| (
                    pos,
                    catalog.signature_to_argument_str(pos.init().signature().unwrap())
                ))
                .collect::<Vec<_>>()
        );

        // Construct output argument list: keys + LHS values + RHS values
        let new_arguments_list: Vec<AtomArgumentSignature> = lhs_keys
            .iter()
            .chain(lhs_vals.iter())
            .chain(rhs_vals.iter())
            .map(|pos| pos.init().signature().unwrap())
            .cloned()
            .collect();
        let output_value_start_idx = lhs_keys.len();

        // Create the join transformation with proper key-value layouts
        let tx = TransformationInfo::join_to_kv(
            lhs_pos_fp,                                              // LHS input fingerprint
            rhs_pos_fp,                                              // RHS input fingerprint
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals.clone()), // LHS layout (keys + values)
            KeyValueLayout::new(Vec::new(), rhs_vals.clone()),       // RHS layout (values only)
            KeyValueLayout::new(
                lhs_keys,
                lhs_vals.iter().chain(rhs_vals.iter()).cloned().collect(), // output layout
            ),
            vec![], // no additional comparison constraints
        );

        // Generate descriptive name and register the transformation
        let new_name = format!("atom_pos{}_join_atom_pos{}", lhs_idx, rhs_idx);
        let new_fp = tx.output_info_fp();

        self.insert_producer(new_fp, current_transformation_index);

        trace!("Join transformation:\n      {}", tx);

        // Store layout information and register transformation
        self.kv_layouts.insert(new_fp, output_value_start_idx);
        self.transformation_infos.push(tx);

        // Update catalog with the new joined atom
        catalog.join_modify(
            left_atom_signature,
            right_atom_signatures,
            vec![new_arguments_list],
            vec![new_name],
            vec![new_fp],
        );
    }

    /// Partitions atom arguments into join keys and remaining values.
    fn partition_shared_keys(
        catalog: &Catalog,
        lhs_sigs: &[AtomArgumentSignature],
        rhs_sigs: &[AtomArgumentSignature],
    ) -> (Vec<ArithmeticPos>, Vec<ArithmeticPos>, Vec<ArithmeticPos>) {
        // Build mapping from argument names to RHS signatures for efficient lookup
        let mut rhs_sigs_to_name = HashMap::new();
        for sig in rhs_sigs {
            let name = catalog.signature_to_argument_str(sig);
            rhs_sigs_to_name.insert(name, sig);
        }

        // Partition LHS arguments into join keys and remaining values
        let mut left_keys = Vec::new();
        let mut left_remains = Vec::new();
        let mut matched_names = HashSet::new();

        for sig in lhs_sigs {
            let name = catalog.signature_to_argument_str(sig);
            if rhs_sigs_to_name.contains_key(name) {
                // This variable appears in both atoms - it's a join key
                left_keys.push(ArithmeticPos::from_var_signature(*sig));
                matched_names.insert(name.as_str());
            } else {
                // This variable only appears in LHS - it's a payload value
                left_remains.push(ArithmeticPos::from_var_signature(*sig))
            }
        }

        // Collect RHS arguments that don't participate in join (RHS payload)
        let mut right_remains = Vec::new();
        for sig in rhs_sigs {
            let name = catalog.signature_to_argument_str(sig);
            if !matched_names.contains(name.as_str()) {
                right_remains.push(ArithmeticPos::from_var_signature(*sig));
            }
        }

        (left_keys, left_remains, right_remains)
    }
}
