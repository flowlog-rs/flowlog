//! Common utilities for rule planning.
//!
//! This module provides shared functionality for rule planning operations including:
//! - Semijoin operations (positive and anti-semijoin)
//! - Comparison predicate pushdown
//! - Projection and unused argument removal
//! - Producer-consumer relationship management

use std::collections::HashSet;
use tracing::trace;

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};

// =========================================================================
// Semijoin & Comparison Operations
// =========================================================================
impl RulePlanner {
    /// Attempts to apply semijoin or comparison pushdown optimizations.
    ///
    /// This method tries the following optimizations in order:
    /// 1. Comparison pushdown: When a comparison predicate can be pushed to atoms
    /// 2. Positive semijoin: When a positive atom has positive supersets
    /// 3. Anti-semijoin: When a negated atom has positive supersets  
    ///
    /// The reason why we try comparison pushdown first is that it can avoid fuse a comparison
    /// with a neg join producer, which is undefined in my understanding.
    ///
    /// Returns `true` if any optimization was applied, `false` otherwise.
    pub(super) fn apply_semijoin(&mut self, catalog: &mut Catalog) -> bool {
        // (1) Comparison predicate pushdown
        // When a comparison can be evaluated against specific atoms
        if let Some((lhs_comp_idx, rhs_pos_indices)) = catalog
            .comparison_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            trace!(
                "Comparison pushdown:\n  Comparison: {}\n  RHS atoms: {:?}",
                catalog.comparison_predicate(lhs_comp_idx),
                rhs_pos_indices
                    .iter()
                    .map(|&i| (
                        &catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)],
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_comparison_pushdown(catalog, lhs_comp_idx, &rhs_pos_indices);
        }

        // (2) Positive semijoin optimization
        // When a positive atom has positive supersets, we can join them
        if let Some((lhs_pos_idx, rhs_pos_indices)) = catalog
            .positive_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            trace!(
                "Positive semijoin:\n  LHS atom: ({}, {})\n  RHS atoms: {:?}",
                catalog.rule().rhs()[catalog.positive_atom_rhs_id(lhs_pos_idx)],
                catalog.positive_atom_rhs_id(lhs_pos_idx),
                rhs_pos_indices
                    .iter()
                    .map(|&i| (
                        &catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)],
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_positive_semijoin(catalog, lhs_pos_idx, &rhs_pos_indices);
        }

        // (3) Anti-semijoin optimization
        // When a negated atom has positive supersets, we can anti-join them
        if let Some((lhs_neg_idx, rhs_pos_indices)) = catalog
            .negated_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            trace!(
                "Anti-semijoin:\n  LHS negated atom: ({}, !{})\n  RHS atoms: {:?}",
                catalog.rule().rhs()[catalog.negated_atom_rhs_id(lhs_neg_idx)],
                catalog.negated_atom_rhs_id(lhs_neg_idx),
                rhs_pos_indices
                    .iter()
                    .map(|&i| (
                        &catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)],
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_anti_semijoin(catalog, lhs_neg_idx, &rhs_pos_indices);
        }

        false
    }

    /// Applies positive semijoin optimization.
    ///
    /// Positive semijoin: Joins the LHS atom with each RHS atom and keeps only the RHS.
    fn apply_positive_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        // Extract LHS atom information
        let lhs_pos_args = catalog
            .positive_atom_argument_signature(lhs_pos_idx)
            .clone();
        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_pos_idx);
        let left_atom_signature = AtomSignature::new(true, lhs_pos_idx);
        let lhs_arg_names = Self::arg_names_set(&lhs_pos_args, catalog);

        // Build join keys from LHS arguments - these become the join condition
        let lhs_keys: Vec<ArithmeticPos> = lhs_pos_args
            .iter()
            .map(|&s| ArithmeticPos::from_var_signature(s))
            .collect();
        trace!(
            "Semijoin keys: {:?}",
            lhs_keys
                .iter()
                .map(|pos| (
                    pos,
                    catalog.signature_to_argument_str(pos.init().signature().unwrap())
                ))
                .collect::<Vec<_>>()
        );

        // Initialize collections for new atoms created by the semijoin
        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut new_arg_lists = Vec::new();
        let mut right_sigs = Vec::new();

        // Process each RHS atom for semijoin
        for &rhs_idx in rhs_pos_indices {
            let current_transformation_index = self.transformation_infos.len();

            // Register LHS atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                lhs_pos_fp,
                current_transformation_index,
            );

            // Extract RHS atom information
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Register RHS atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                current_transformation_index,
            );

            // Store join result argument list and signature for catalog update
            new_arg_lists.push(rhs_args.clone());
            right_sigs.push(AtomSignature::new(true, rhs_idx));

            // Build RHS arithmetic positions excluding those already in LHS keys to avoid duplication
            let rhs_vals: Vec<ArithmeticPos> = rhs_args
                .iter()
                .filter(|&sig| !lhs_arg_names.contains(catalog.signature_to_argument_str(sig)))
                .map(|&s| ArithmeticPos::from_var_signature(s))
                .collect();
            trace!(
                "Semijoin RHS values: {:?}",
                rhs_vals
                    .iter()
                    .map(|pos| (
                        pos,
                        catalog.signature_to_argument_str(pos.init().signature().unwrap())
                    ))
                    .collect::<Vec<_>>()
            );

            // Create the join transformation
            let tx = TransformationInfo::join_to_kv(
                lhs_pos_fp,
                rhs_fp,
                KeyValueLayout::new(lhs_keys.clone(), Vec::new()), // LHS: keys only
                KeyValueLayout::new(Vec::new(), rhs_vals.clone()), // RHS: values only
                KeyValueLayout::new(lhs_keys.clone(), rhs_vals),
                vec![], // no additional comparisons
            );

            // Generate descriptive name for the new atom
            let new_name = format!("atom_pos{}_pos_semijoin_atom_pos{}", lhs_pos_idx, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            trace!("Positive semijoin transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Store the key-value layout split point
            self.kv_layouts.insert(new_fp, lhs_keys.len());
            self.transformation_infos.push(tx);
        }

        // Update catalog with the new joined atoms
        catalog.join_modify(
            left_atom_signature,
            right_sigs,
            new_arg_lists,
            new_names,
            new_fps,
        );
        true
    }

    /// Applies anti-semijoin optimization.
    ///
    /// Anti-semijoin: Keeps LHS rows whose join keys do NOT exist in any RHS atom.
    fn apply_anti_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_neg_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        // Extract LHS negated atom information
        let lhs_neg_args = catalog.negated_atom_argument_signature(lhs_neg_idx).clone();
        let lhs_neg_fp = catalog.negated_atom_fingerprint(lhs_neg_idx);
        let left_atom_signature = AtomSignature::new(false, lhs_neg_idx);
        let lhs_arg_names = Self::arg_names_set(&lhs_neg_args, catalog);

        // Build join keys from LHS arguments - these become the join condition
        let lhs_keys: Vec<ArithmeticPos> = lhs_neg_args
            .iter()
            .map(|&s| ArithmeticPos::from_var_signature(s))
            .collect();
        trace!(
            "Semijoin keys: {:?}",
            lhs_keys
                .iter()
                .map(|pos| (
                    pos,
                    catalog.signature_to_argument_str(pos.init().signature().unwrap())
                ))
                .collect::<Vec<_>>()
        );

        // Initialize collections for new atoms created by the anti-semijoin
        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut new_arg_lists = Vec::new();
        let mut right_sigs = Vec::new();

        // Process each RHS atom for anti-semijoin
        for &rhs_idx in rhs_pos_indices {
            let current_transformation_index = self.transformation_infos.len();

            // Register LHS negated atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                lhs_neg_fp,
                current_transformation_index,
            );

            // Extract RHS atom information
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Register RHS atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                current_transformation_index,
            );

            // Store join result argument list and signature for catalog update
            new_arg_lists.push(rhs_args.clone());
            right_sigs.push(AtomSignature::new(true, rhs_idx));

            // Build RHS arithmetic positions excluding those already in LHS keys to avoid duplication
            let rhs_vals: Vec<ArithmeticPos> = rhs_args
                .iter()
                .filter(|&sig| !lhs_arg_names.contains(catalog.signature_to_argument_str(sig)))
                .map(|&s| ArithmeticPos::from_var_signature(s))
                .collect();
            trace!(
                "Semijoin RHS values: {:?}",
                rhs_vals
                    .iter()
                    .map(|pos| (
                        pos,
                        catalog.signature_to_argument_str(pos.init().signature().unwrap())
                    ))
                    .collect::<Vec<_>>()
            );

            // Create the anti-join transformation
            let tx = TransformationInfo::anti_join_to_kv(
                lhs_neg_fp,
                rhs_fp,
                KeyValueLayout::new(lhs_keys.clone(), Vec::new()), // LHS: keys only
                KeyValueLayout::new(Vec::new(), rhs_vals.clone()), // RHS: values only
                KeyValueLayout::new(lhs_keys.clone(), rhs_vals),
            );

            // Generate descriptive name for the new atom
            let new_name = format!("atom_neg{}_neg_semijoin_atom_pos{}", lhs_neg_idx, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Update producer transformation index
            self.insert_producer(new_fp, current_transformation_index);

            trace!("Anti semijoin transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Store the key-value layout split point
            self.kv_layouts.insert(new_fp, lhs_keys.len());
            self.transformation_infos.push(tx);
        }

        // Update catalog with the new anti-joined atoms
        catalog.join_modify(
            left_atom_signature,
            right_sigs,
            new_arg_lists,
            new_names,
            new_fps,
        );
        true
    }

    /// Pushes down a comparison predicate onto RHS atoms that fully cover its variables.
    ///
    /// Comparison pushdown moves evaluation of comparison predicates closer to data sources,
    /// reducing the volume of data processed in subsequent operations. This is particularly
    /// effective for selective predicates that can eliminate many tuples early.
    fn apply_comparison_pushdown(
        &mut self,
        catalog: &mut Catalog,
        lhs_comp_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        // Extract comparison predicate information
        let comparison_exprs = catalog.comparison_predicate(lhs_comp_idx);

        // Initialize collections for new atoms created by the comparison pushdown
        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut right_sigs = Vec::new();

        // Process each RHS atom for comparison pushdown
        for &rhs_idx in rhs_pos_indices {
            let current_transformation_index = self.transformation_infos.len();

            // Extract RHS atom information
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Register RHS atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                current_transformation_index,
            );

            // Store signature for catalog update
            right_sigs.push(AtomSignature::new(true, rhs_idx));

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(rhs_fp, &rhs_args);

            let tx = TransformationInfo::kv_to_kv(
                rhs_fp,
                KeyValueLayout::new(in_keys.clone(), in_vals.clone()),
                KeyValueLayout::new(in_keys.clone(), in_vals),
                vec![], // no const-eq
                vec![], // no var-eq
                vec![catalog.resolve_comparison_predicates(rhs_idx, lhs_comp_idx)],
            );

            // Generate descriptive name for the new atom
            let new_name = format!("comparison_{}_filter_atom_pos{}", comparison_exprs, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            trace!("Comparison transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Store the key-value layout split point
            self.kv_layouts.insert(new_fp, in_keys.len());
            self.transformation_infos.push(tx);
        }

        catalog.comparison_modify(lhs_comp_idx, right_sigs, new_names, new_fps);
        true
    }
}

// =========================================================================
// Projection & Unused Argument Removal
// =========================================================================
impl RulePlanner {
    /// Removes arguments across atoms that are provably unused for outputs.
    pub(super) fn remove_unused_arguments(&mut self, catalog: &mut Catalog) -> bool {
        // Get groups of unused arguments per atom from catalog analysis
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return false;
        }

        let mut applied = false;

        // Process each atom that has unused arguments
        for (atom_signature, to_delete) in groups.clone() {
            let current_transformation_index = self.transformation_infos.len();

            trace!(
                "Unused-arg removal:\n  Atom: {}, {}\n  To delete: {:?}",
                catalog.rule().rhs()[catalog.rhs_index_from_signature(atom_signature)],
                atom_signature,
                to_delete
                    .iter()
                    .map(|sig| (catalog.signature_to_argument_str(sig), sig))
                    .collect::<Vec<_>>()
            );

            // Resolve atom information from signature
            let (args, atom_fp, atom_id) = catalog.resolve_atom(&atom_signature);

            // Register atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                atom_fp,
                current_transformation_index,
            );

            // Create set of positions to drop for efficient filtering
            let drop_set: HashSet<ArithmeticPos> = to_delete
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect();

            // Get current key-value layout for this atom
            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

            // Filter out unused arguments from both keys and values
            let out_keys: Vec<ArithmeticPos> = in_keys
                .iter()
                .filter(|&sig| !drop_set.contains(sig))
                .cloned()
                .collect();
            let out_vals: Vec<ArithmeticPos> = in_vals
                .iter()
                .filter(|&sig| !drop_set.contains(sig))
                .cloned()
                .collect();

            trace!(
                "Output KV layout: keys={:?}, values={:?}",
                out_keys,
                out_vals
            );

            let out_value_start_idx = out_keys.len();

            // Create projection transformation that removes unused arguments
            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                KeyValueLayout::new(in_keys, in_vals), // input layout
                KeyValueLayout::new(out_keys, out_vals), // output layout (projected)
                vec![],                                // no constant equality constraints
                vec![],                                // no variable equality constraints
                vec![],                                // no comparison constraints
            );

            // Generate descriptive name for projected atom
            let new_name = Self::projection_name(&atom_signature, atom_id);
            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            trace!("Unused transformation:\n      {}", tx);

            // Store the key-value layout split point
            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);

            // Modify the catalog to reflect the projected atom
            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp);

            applied = true;
        }

        applied
    }
}

// =========================================================================
// Private Utilities
// =========================================================================
impl RulePlanner {
    /// Build output payload expressions excluding a specific argument signature.
    #[inline]
    pub(super) fn out_values_excluding(
        args: &[AtomArgumentSignature],
        drop_sig: AtomArgumentSignature,
    ) -> Vec<ArithmeticPos> {
        args.iter()
            .filter(|&sig| *sig != drop_sig)
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect()
    }

    /// Return cached KV split (keys|values) for an atom or initialize it to a row layout.
    /// Cache stores the split index (prefix length) of the keys.
    pub(super) fn get_or_init_row_kv_layout(
        &mut self,
        atom_fp: u64,
        args: &[AtomArgumentSignature],
    ) -> (Vec<ArithmeticPos>, Vec<ArithmeticPos>) {
        if let Some(&split_idx) = self.kv_layouts.get(&atom_fp) {
            let all: Vec<ArithmeticPos> = args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect();
            let (keys, vals) = all.split_at(split_idx);
            trace!(
                "Cached KV layout [atom 0x{:016x}]: keys={:?}, values={:?}",
                atom_fp,
                keys,
                vals
            );
            return (keys.to_vec(), vals.to_vec());
        }

        // Default: row-based layout (no keys, all values).
        let keys = Vec::new();
        let vals: Vec<ArithmeticPos> = args
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();

        trace!(
            "Initialize KV layout [atom 0x{:016x}]: keys=[], values={:?}",
            atom_fp,
            vals
        );

        self.kv_layouts.insert(atom_fp, 0);
        (keys, vals)
    }

    /// Generates a consistent projection name based on atom polarity and RHS identifier.
    #[inline]
    pub(super) fn projection_name(atom_signature: &AtomSignature, atom_id: usize) -> String {
        if atom_signature.is_positive() {
            format!("atom_{}_proj", atom_id)
        } else {
            format!("neg_atom_{}_proj", atom_id)
        }
    }

    /// Collects display strings for argument signatures to identify duplicates in RHS columns.
    pub(super) fn arg_names_set(
        args: &[AtomArgumentSignature],
        catalog: &Catalog,
    ) -> HashSet<String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
            .collect()
    }
}

// =========================================================================
// Producer-Consumer Relationship Management
// =========================================================================
impl RulePlanner {
    /// Registers a producer transformation for a given fingerprint.
    ///
    /// Each transformation fingerprint can have exactly one producer - the transformation
    /// that generates data with that fingerprint.
    pub(super) fn insert_producer(&mut self, producer_fp: u64, producer_idx: usize) {
        // It is impossible to have two producers for the same transformation fingerprint.
        assert!(
            self.producer_consumer
                .insert(producer_fp, (producer_idx, None))
                .is_none(),
            "Producer already exists for transformation fingerprint: {:016x}",
            producer_fp
        );
    }

    /// Registers a consumer transformation for data with a given fingerprint.
    ///
    /// Multiple transformations can consume the same data (i.e., multiple consumers per producer).
    /// This function tracks these consumption relationships, but ignores original atom fingerprints
    /// since those represent input data rather than transformation outputs.
    ///
    /// # Arguments
    /// * `original_atom_fp` - Set of fingerprints for original input atoms
    /// * `producer_fp` - Fingerprint of the data being consumed
    /// * `consumer_idx` - Index of the transformation that consumes this data
    pub(super) fn insert_consumer(
        &mut self,
        original_atom_fp: &HashSet<u64>,
        producer_fp: u64,
        consumer_idx: usize,
    ) {
        // If this is an original atom fingerprint (read-in atom),
        // it's not produced by any transformation, so just return
        if original_atom_fp.contains(&producer_fp) {
            return;
        }

        self.producer_consumer
            .entry(producer_fp)
            .and_modify(|(_, consumers)| {
                consumers.get_or_insert_with(Vec::new).push(consumer_idx);
            })
            .or_insert_with(|| {
                panic!(
                    "No producer for transformation fingerprint: {:#018x}",
                    producer_fp
                )
            });
    }

    #[inline]
    pub(super) fn producer_index(&self, fp: u64) -> usize {
        self.producer_consumer
            .get(&fp)
            .map(|(idx, _)| *idx)
            .unwrap_or_else(|| panic!("No producer for transformation fingerprint: {:#018x}", fp))
    }

    #[inline]
    pub(super) fn consumer_indices(&self, fp: u64) -> Vec<usize> {
        self.producer_consumer
            .get(&fp)
            .and_then(|(_, consumers)| consumers.clone())
            .unwrap_or_default()
    }
}
