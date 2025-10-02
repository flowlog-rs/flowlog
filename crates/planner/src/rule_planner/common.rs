use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use std::collections::HashSet;
use tracing::trace;

impl RulePlanner {
    // =========================================================================
    // Semijoin & comparison pass (private)
    // =========================================================================

    /// Try (in order): positive semijoin, anti-semijoin, comparison pushdown.
    pub(super) fn apply_semijoin(&mut self, catalog: &mut Catalog) -> bool {
        // (1) Positive predicate has positive supersets
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
                        catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)].clone(),
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_positive_semijoin(catalog, lhs_pos_idx, &rhs_pos_indices);
        }

        // (2) Negated predicate has positive supersets
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
                        catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)].clone(),
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_anti_semijoin(catalog, lhs_neg_idx, &rhs_pos_indices);
        }

        // (3) Comparison predicate has positive supersets
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
                        catalog.rule().rhs()[catalog.positive_atom_rhs_id(i)].clone(),
                        catalog.positive_atom_rhs_id(i)
                    ))
                    .collect::<Vec<_>>()
            );
            return self.apply_comparison_pushdown(catalog, lhs_comp_idx, &rhs_pos_indices);
        }

        false
    }

    /// Positive semijoin: join LHS with RHS and keep only RHS.
    fn apply_positive_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        let lhs_pos_args = catalog
            .positive_atom_argument_signature(lhs_pos_idx)
            .clone();
        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_pos_idx);
        let left_atom_signature = AtomSignature::new(true, lhs_pos_idx);

        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut new_arg_lists = Vec::new();
        let mut right_sigs = Vec::new();

        // Join keys (LHS arguments become keys).
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

        // In output, values start after keys.
        let out_value_start_idx = lhs_keys.len();

        // Precompute LHS arg names to exclude duplicates from RHS payload.
        let lhs_arg_names = Self::arg_names_set(&lhs_pos_args, catalog);

        for &rhs_idx in rhs_pos_indices {
            let transformation_index = self.get_current_transformation_index();

            // Update consumer transformation index for LHS atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                lhs_pos_fp,
                transformation_index,
            );

            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Update consumer transformation index for RHS atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                transformation_index,
            );

            new_arg_lists.push(rhs_args.clone());
            right_sigs.push(AtomSignature::new(true, rhs_idx));

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

            let tx = TransformationInfo::join_to_kv(
                lhs_pos_fp,
                rhs_fp,
                KeyValueLayout::new(lhs_keys.clone(), Vec::new()), // join keys from LHS
                KeyValueLayout::new(Vec::new(), rhs_vals.clone()), // RHS payload as values
                KeyValueLayout::new(lhs_keys.clone(), rhs_vals),   // output = keys + payload
                vec![],                                            // no extra comparisons
            );

            let new_name = format!("atom_pos{}_pos_semijoin_atom_pos{}", lhs_pos_idx, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Update producer transformation index
            self.insert_producer(new_fp, transformation_index);

            trace!("Positive semijoin transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);
        }

        catalog.join_modify(
            left_atom_signature,
            right_sigs,
            new_arg_lists,
            new_names,
            new_fps,
        );
        true
    }

    /// Anti-semijoin: keep LHS rows whose join-keys NOT exist in RHS.
    /// Carries through unique RHS payload columns.
    fn apply_anti_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_neg_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        let lhs_neg_args = catalog.negated_atom_argument_signature(lhs_neg_idx).clone();
        let lhs_neg_fp = catalog.negated_atom_fingerprint(lhs_neg_idx);
        let left_atom_signature = AtomSignature::new(false, lhs_neg_idx);

        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut new_arg_lists = Vec::new();
        let mut right_sigs = Vec::new();

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

        let lhs_arg_names = Self::arg_names_set(&lhs_neg_args, catalog);
        let out_value_start_idx = lhs_keys.len();

        for &rhs_idx in rhs_pos_indices {
            let transformation_index = self.get_current_transformation_index();

            // Update consumer transformation index for LHS atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                lhs_neg_fp,
                transformation_index,
            );

            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Update consumer transformation index for RHS atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                transformation_index,
            );

            new_arg_lists.push(rhs_args.clone());
            right_sigs.push(AtomSignature::new(true, rhs_idx));

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

            let tx = TransformationInfo::anti_join_to_kv(
                lhs_neg_fp,
                rhs_fp,
                KeyValueLayout::new(lhs_keys.clone(), Vec::new()),
                KeyValueLayout::new(Vec::new(), rhs_vals.clone()),
                KeyValueLayout::new(lhs_keys.clone(), rhs_vals),
            );

            let new_name = format!("atom_neg{}_neg_semijoin_atom_pos{}", lhs_neg_idx, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Update producer transformation index
            self.insert_producer(new_fp, transformation_index);

            trace!("Anti semijoin transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);
        }

        catalog.join_modify(
            left_atom_signature,
            right_sigs,
            new_arg_lists,
            new_names,
            new_fps,
        );
        true
    }

    /// Push down a comparison predicate onto RHS atoms that fully cover its variables.
    fn apply_comparison_pushdown(
        &mut self,
        catalog: &mut Catalog,
        lhs_comp_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> bool {
        let comparison_exprs = catalog.comparison_predicate(lhs_comp_idx);

        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut right_sigs = Vec::new();

        for &rhs_idx in rhs_pos_indices {
            let transformation_index = self.get_current_transformation_index();
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

            // Update consumer transformation index for RHS atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                transformation_index,
            );

            right_sigs.push(AtomSignature::new(true, rhs_idx));

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(rhs_fp, &rhs_args);

            let out_value_start_idx = in_keys.len();

            let tx = TransformationInfo::kv_to_kv(
                rhs_fp,
                KeyValueLayout::new(in_keys.clone(), in_vals.clone()),
                KeyValueLayout::new(in_keys, in_vals),
                vec![], // no const-eq
                vec![], // no var-eq
                vec![catalog.resolve_comparison_predicates(rhs_idx, lhs_comp_idx)],
            );

            let new_name = format!("comparison_{}_filter_atom_pos{}", comparison_exprs, rhs_idx);
            let new_fp = tx.output_info_fp();

            // Update producer transformation index
            self.insert_producer(new_fp, transformation_index);

            trace!("Comparison transformation:\n      {}", tx);

            new_names.push(new_name);
            new_fps.push(new_fp);

            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);
        }

        catalog.comparison_modify(lhs_comp_idx, right_sigs, new_names, new_fps);
        true
    }

    // =========================================================================
    // Projection / unused args (private)
    // =========================================================================

    /// Remove arguments across atoms that are provably unused for outputs.
    pub(super) fn remove_unused_arguments(&mut self, catalog: &mut Catalog) -> bool {
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return false;
        }

        let mut applied = false;

        for (atom_signature, to_delete) in groups.clone() {
            let transformation_index = self.get_current_transformation_index();
            trace!(
                "Unused-arg removal:\n  Atom: {}, {}\n  To delete: {:?}",
                catalog.rule().rhs()[catalog.rhs_index_from_signature(atom_signature)],
                atom_signature,
                to_delete
                    .iter()
                    .map(|sig| (catalog.signature_to_argument_str(sig), sig))
                    .collect::<Vec<_>>()
            );

            let (args, atom_fp, atom_id) = catalog.resolve_atom(&atom_signature);

            // Update consumer transformation index for the atom
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                atom_fp,
                transformation_index,
            );

            let drop_set: HashSet<ArithmeticPos> = to_delete
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect();

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

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

            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                KeyValueLayout::new(in_keys, in_vals),
                KeyValueLayout::new(out_keys, out_vals),
                vec![],
                vec![],
                vec![],
            );

            let new_name = Self::projection_name(&atom_signature, atom_id);
            let new_fp = tx.output_info_fp();

            // Update producer transformation index
            self.insert_producer(new_fp, transformation_index);

            trace!("Unused transformation:\n      {}", tx);

            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp);

            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);

            applied = true;
        }

        applied
    }

    // =========================================================================
    // TransformationInfo small utilities (private)
    // =========================================================================

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
                "Cached KV layout [atom 0x{:x}]: keys={:?}, values={:?}",
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
            "Initialize KV layout [atom 0x{:x}]: keys=[], values={:?}",
            atom_fp,
            vals
        );

        self.kv_layouts.insert(atom_fp, 0);
        (keys, vals)
    }

    /// Construct a consistent projection name based on polarity and rhs-id.
    #[inline]
    pub(super) fn projection_name(atom_signature: &AtomSignature, atom_id: usize) -> String {
        if atom_signature.is_positive() {
            format!("atom_{}_proj", atom_id)
        } else {
            format!("neg_atom_{}_proj", atom_id)
        }
    }

    /// Collect display strings for signatures, used to exclude duplicate RHS columns.
    /// Note: we return owned `String`s to avoid lifetime coupling to `catalog`.
    pub(super) fn arg_names_set(
        args: &[AtomArgumentSignature],
        catalog: &Catalog,
    ) -> HashSet<String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
            .collect()
    }

    // =========================================================================
    // Producer Consumer utilities (private)
    // =========================================================================

    /// Get the transformation index
    pub(super) fn get_current_transformation_index(&self) -> usize {
        self.transformation_infos.len()
    }

    /// Create a producer mapping for a transformation fingerprint.
    pub(super) fn insert_producer(&mut self, producer_fp: u64, producer_idx: usize) {
        // It is impossible to have two producers for the same transformation fingerprint.
        assert!(
            self.producer_consumer
                .insert(producer_fp, (producer_idx, None))
                .is_none(),
            "Producer already exists for transformation fingerprint: {:x}",
            producer_fp
        );
    }

    /// Create a consumer mapping for a transformation fingerprint.
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
}
