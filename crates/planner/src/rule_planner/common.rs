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
    /// 3. Anti-semijoin: When a negative atom has positive supersets  
    ///
    /// The reason why we try comparison pushdown first is that it can avoid fuse a comparison
    /// with a neg join producer, which is undefined in my understanding.
    ///
    /// WARNING: this method should be called as a total instead of calling individual
    /// semijoin or comparison methods, because they may interfere with each other in fuse logic,
    /// as the reason showed above.
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
        // When a positive atom has positive supersets, we can join them.
        // Note we need premap for both LHS and RHS atoms if they are original EDBs (row format).
        if let Some((lhs_pos_idx, rhs_pos_indices)) = catalog
            .positive_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            self.apply_positive_semijoin_premap(catalog, lhs_pos_idx, &rhs_pos_indices);

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
        // When a negative atom has positive supersets, we can anti-join them.
        // Note we need premap for both LHS and RHS atoms if they are original EDBs (row format).
        if let Some((lhs_neg_idx, rhs_pos_indices)) = catalog
            .negative_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            self.apply_anti_semijoin_premap(catalog, lhs_neg_idx, &rhs_pos_indices);

            trace!(
                "Anti-semijoin:\n  LHS negative atom: ({}, !{})\n  RHS atoms: {:?}",
                catalog.rule().rhs()[catalog.negative_atom_rhs_id(lhs_neg_idx)],
                catalog.negative_atom_rhs_id(lhs_neg_idx),
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

    /// Apply premap for positive semijoin optimization if needed..
    fn apply_positive_semijoin_premap(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_indices: &[usize],
    ) {
        // Even for empty key key-value layout, we still need premap for EDB relations.
        // ((), (value)) is not the same as (value) in differential dataflow.

        // Process LHS atom for positive semijoin.
        if catalog
            .original_atom_fingerprints()
            .contains(&catalog.positive_atom_fingerprint(lhs_pos_idx))
        {
            // LHS atom is an EDB, need to create a map
            self.create_edb_premap_transformations(catalog, lhs_pos_idx, true);
        }

        // Process each RHS atom for positive semijoin.
        for &rhs_idx in rhs_pos_indices {
            if catalog
                .original_atom_fingerprints()
                .contains(&catalog.positive_atom_fingerprint(rhs_idx))
            {
                // RHS atom is not in key-only format; need to create a map
                self.create_edb_premap_transformations(catalog, rhs_idx, true);
            }
        }
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
        let lhs_key_names: Vec<String> = lhs_pos_args
            .iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
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

            // Build RHS arithmetic positions; keys must mirror the LHS key ordering
            let rhs_named_positions: Vec<(String, ArithmeticPos)> = rhs_args
                .iter()
                .map(|&sig| {
                    (
                        catalog.signature_to_argument_str(&sig).clone(),
                        ArithmeticPos::from_var_signature(sig),
                    )
                })
                .collect();

            let find_rhs_pos = |target: &str| -> ArithmeticPos {
                rhs_named_positions
                    .iter()
                    .find(|(name, _)| name == target)
                    .unwrap_or_else(|| {
                        panic!(
                            "Planner error: RHS missing key {} for positive semijoin",
                            target
                        )
                    })
                    .1
                    .clone()
            };

            let rhs_keys: Vec<ArithmeticPos> = lhs_key_names
                .iter()
                .map(|name| find_rhs_pos(name))
                .collect();

            let rhs_vals: Vec<ArithmeticPos> = rhs_named_positions
                .iter()
                .filter(|(name, _)| !lhs_arg_names.contains(name))
                .map(|(_, pos)| pos.clone())
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
                KeyValueLayout::new(rhs_keys, rhs_vals.clone()),   // RHS: keys aligned + values
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

            // Store the transformation info
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

    /// Apply premap for anti-semijoin optimization.
    fn apply_anti_semijoin_premap(
        &mut self,
        catalog: &mut Catalog,
        lhs_neg_idx: usize,
        rhs_pos_indices: &[usize],
    ) {
        // Even for empty key key-value layout, we still need premap for EDB relations.
        // ((), (value)) is not the same as (value) in differential dataflow.

        // Process LHS atom for anti-semijoin.
        if catalog
            .original_atom_fingerprints()
            .contains(&catalog.negative_atom_fingerprint(lhs_neg_idx))
        {
            // LHS atom is not in key-only format; need to create a map
            self.create_edb_premap_transformations(catalog, lhs_neg_idx, false);
        }

        // Process each RHS atom for anti-semijoin.
        for &rhs_idx in rhs_pos_indices {
            if catalog
                .original_atom_fingerprints()
                .contains(&catalog.positive_atom_fingerprint(rhs_idx))
            {
                // RHS atom is not in key-only format; need to create a map
                self.create_edb_premap_transformations(catalog, rhs_idx, true);
            }
        }
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
        // Extract LHS negative atom information
        let lhs_neg_args = catalog
            .negative_atom_argument_signature(lhs_neg_idx)
            .clone();
        let lhs_neg_fp = catalog.negative_atom_fingerprint(lhs_neg_idx);
        let left_atom_signature = AtomSignature::new(false, lhs_neg_idx);
        let lhs_arg_names = Self::arg_names_set(&lhs_neg_args, catalog);

        // Build join keys from LHS arguments - these become the join condition
        let lhs_keys: Vec<ArithmeticPos> = lhs_neg_args
            .iter()
            .map(|&s| ArithmeticPos::from_var_signature(s))
            .collect();
        let lhs_key_names: Vec<String> = lhs_neg_args
            .iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
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

            // Register LHS negative atom as consumer of this transformation
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

            // Build RHS arithmetic positions; keys must mirror the LHS key ordering
            let rhs_named_positions: Vec<(String, ArithmeticPos)> = rhs_args
                .iter()
                .map(|&sig| {
                    (
                        catalog.signature_to_argument_str(&sig).clone(),
                        ArithmeticPos::from_var_signature(sig),
                    )
                })
                .collect();

            let find_rhs_pos = |target: &str| -> ArithmeticPos {
                rhs_named_positions
                    .iter()
                    .find(|(name, _)| name == target)
                    .unwrap_or_else(|| {
                        panic!(
                            "Planner error: RHS missing key {} for anti-semijoin",
                            target
                        )
                    })
                    .1
                    .clone()
            };

            let rhs_keys: Vec<ArithmeticPos> = lhs_key_names
                .iter()
                .map(|name| find_rhs_pos(name))
                .collect();

            let rhs_vals: Vec<ArithmeticPos> = rhs_named_positions
                .iter()
                .filter(|(name, _)| !lhs_arg_names.contains(name))
                .map(|(_, pos)| pos.clone())
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
                KeyValueLayout::new(rhs_keys, rhs_vals.clone()),   // RHS: values only
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

            // Store the transformation info
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

            let in_vals = rhs_args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect::<Vec<_>>();

            let tx = TransformationInfo::kv_to_kv(
                rhs_fp,
                catalog.original_atom_fingerprints().contains(&rhs_fp),
                KeyValueLayout::new(vec![], in_vals.clone()),
                KeyValueLayout::new(vec![], in_vals),
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

            // Store the transformation info
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

            // Get current values for this atom
            let in_vals = args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect::<Vec<_>>();

            // Filter out unused arguments from both keys and values
            let out_vals: Vec<ArithmeticPos> = in_vals
                .iter()
                .filter(|&sig| !drop_set.contains(sig))
                .cloned()
                .collect();

            trace!("Output KV layout: keys=[], values={:?}", out_vals);

            // Create projection transformation that removes unused arguments
            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                catalog.original_atom_fingerprints().contains(&atom_fp),
                KeyValueLayout::new(vec![], in_vals), // input layout
                KeyValueLayout::new(vec![], out_vals), // output layout (projected)
                vec![],                               // no constant equality constraints
                vec![],                               // no variable equality constraints
                vec![],                               // no comparison constraints
            );

            // Generate descriptive name for projected atom
            let new_name = Self::projection_name(&atom_signature, atom_id);
            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            trace!("Unused transformation:\n      {}", tx);

            // Store the transformation info
            self.transformation_infos.push(tx);

            // Modify the catalog to reflect the projected atom
            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp);

            applied = true;
        }

        applied
    }
}

// =========================================================================
// Premap for EDB relations
// =========================================================================
impl RulePlanner {
    /// Creates premap transformations for EDB relations that are not in row format.
    /// We do not care about real key-value layout output here, as these premap is just
    /// an indicator we need to map the EDB from read-in row format.
    /// The key-value layout adjustment is handled in later fuse phase if needed.
    pub(super) fn create_edb_premap_transformations(
        &mut self,
        catalog: &mut Catalog,
        atom_idx: usize,
        is_positive: bool,
    ) {
        // Both positive and negative atoms can be pre-mapped.
        let edb_fp = if is_positive {
            catalog.positive_atom_fingerprint(atom_idx)
        } else {
            catalog.negative_atom_fingerprint(atom_idx)
        };
        let edb_args = if is_positive {
            catalog.positive_atom_argument_signature(atom_idx)
        } else {
            catalog.negative_atom_argument_signature(atom_idx)
        };
        let edb_atom_signature = AtomSignature::new(is_positive, atom_idx);
        let current_transformation_index = self.transformation_infos.len();

        let edb_layout = KeyValueLayout::new(
            Vec::new(),
            edb_args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect(),
        );

        let tx = TransformationInfo::kv_to_kv(
            edb_fp,
            true,
            edb_layout.clone(), // input layout
            edb_layout,         // output row layout
            vec![],             // no constant equality constraints
            vec![],             // no variable equality constraints
            vec![],             // no comparison constraints
        );

        // Generate descriptive name for the projected atom
        let new_name = format!("edb_{}_premap", edb_fp);
        let new_fp = tx.output_info_fp();

        // Store the transformation info
        self.transformation_infos.push(tx);

        // Register this transformation as consumer of EDB atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            edb_fp,
            current_transformation_index,
        );

        // Register this transformation as a producer
        self.insert_producer(new_fp, current_transformation_index);

        // Update catalog to reflect the premap atom
        catalog.map_modify(edb_atom_signature, new_name, new_fp);
    }
}

// =========================================================================
// Private Utilities
// =========================================================================
impl RulePlanner {
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
    fn arg_names_set(args: &[AtomArgumentSignature], catalog: &Catalog) -> HashSet<String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
            .collect()
    }
}

// =========================================================================
// Producer-Consumer Relationship Management
// =========================================================================
impl RulePlanner {
    /// Registers a producer transformation for data with a given fingerprint.
    ///
    /// Multiple transformations can produce the same data (i.e., multiple producers per output).
    /// We will deduplicate in the later fusion logic.
    pub(super) fn insert_producer(&mut self, producer_fp: u64, producer_idx: usize) {
        self.producer_consumer
            .entry(producer_fp)
            .and_modify(|(producers, _)| producers.push(producer_idx))
            .or_insert((vec![producer_idx], vec![]));
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
                consumers.push(consumer_idx);
            })
            .or_insert_with(|| {
                panic!(
                    "No producer for transformation fingerprint: {:#018x}",
                    producer_fp
                )
            });
    }

    /// Retrieves the indices of producer transformations for a given fingerprint.
    #[inline]
    pub(super) fn producer_indices(&self, fp: u64) -> Vec<usize> {
        self.producer_consumer
            .get(&fp)
            .map(|(producers, _)| producers.clone())
            .unwrap_or_else(|| panic!("No producer for transformation fingerprint: {:#018x}", fp))
    }

    /// Retrieves the indices of consumer transformations for a given fingerprint.
    #[inline]
    pub(super) fn consumer_indices(&self, fp: u64) -> Vec<usize> {
        self.producer_consumer
            .get(&fp)
            .map(|(_, consumers)| consumers.clone())
            .unwrap_or_else(|| panic!("No consumers for transformation fingerprint: {:#018x}", fp))
    }
}
