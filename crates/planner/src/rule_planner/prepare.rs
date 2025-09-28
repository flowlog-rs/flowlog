use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use parser::ConstType;
use std::collections::HashSet;
use tracing::trace;

impl RulePlanner {
    // =========================================================================
    // Public API
    // =========================================================================

    /// Run the alpha-elimination prepare phase for a single rule.
    ///
    /// This repeatedly applies (in order):
    /// 1) Local filters (var=var, var=const, placeholder)
    /// 2) (Anti-)semijoins and comparison pushdown
    /// 3) Projection that removes unused arguments
    ///
    /// The loop stops when a full iteration makes no changes.
    pub fn prepare(&mut self, catalog: &mut Catalog) {
        let mut step = 0;

        loop {
            // 1) Try filters first to maximize early pruning and simplify later steps.
            if self.apply_filter(catalog) {
                step += 1;
                trace!("Prepare Step {}: filter applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Then (anti-)semijoins and comparison pushdown.
            if self.apply_semijoin(catalog) {
                step += 1;
                trace!("Prepare Step {}: semijoin applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 3) Finally, remove any arguments that no longer contribute to outputs.
            if self.remove_unused_arguments(catalog) {
                step += 1;
                trace!("Prepare Step {}: unused arguments removed", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Nothing else to do in this phase.
            break;
        }
    }

    // =========================================================================
    // Filter pass (private)
    // =========================================================================

    /// Try to apply any available filter in priority order.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> bool {
        // (1) var == var
        if let Some((&a, &b)) = catalog.filters().var_eq_map().iter().next() {
            return self.apply_var_equality_filter(catalog, a, b);
        }

        // (2) var == const
        if let Some((&var_sig, const_val)) = catalog.filters().const_map().iter().next() {
            return self.apply_const_equality_filter(catalog, var_sig, const_val.clone());
        }

        // (3) placeholder
        if let Some(&var_sig) = catalog.filters().placeholder_set().iter().next() {
            return self.apply_placeholder_filter(catalog, var_sig);
        }

        false
    }

    /// Apply a variable equality filter (var1 == var2) by filtering and projecting
    /// away one of the equal variables.
    fn apply_var_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        a: AtomArgumentSignature,
        b: AtomArgumentSignature,
    ) -> bool {
        trace!(
            "Variables equality filter:\n  Atom: {}\n  Arguments: {} == {}",
            catalog.rhs_index_from_signature(*a.atom_signature()),
            a,
            b
        );

        // Canonicalize the kept/dropped order by signature then arg-id.
        let (var1_sig, var2_sig) =
            if (*a.atom_signature(), a.argument_id()) <= (*b.atom_signature(), b.argument_id()) {
                (a, b)
            } else {
                (b, a)
            };

        let atom_signature = var1_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        // We drop var2_sig from the output payload.
        let out_vals = Self::out_values_excluding(args, var2_sig);
        trace!("Output values after dropping {}: {:?}", var2_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![],                     // no const-eq
            vec![(var1_sig, var2_sig)], // var-eq
            vec![],                     // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Var-eq transformation:\n     {}", tx);

        catalog.projection_modify(*atom_signature, vec![var2_sig], new_name, new_fp);

        // Cache new layout and record the transformation.
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Apply a constant equality filter (var == const) and project away the filtered column.
    fn apply_const_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
        const_val: ConstType,
    ) -> bool {
        trace!(
            "Constant equality filter:\n  Atom: {}\n  Arguments: {} == {}",
            catalog.rhs_index_from_signature(*var_sig.atom_signature()),
            var_sig,
            const_val
        );

        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![(var_sig, const_val)], // const-eq
            vec![],                     // no var-eq
            vec![],                     // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Const-eq transformation:\n     {}", tx);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Apply a "placeholder" filter and project away its column.
    fn apply_placeholder_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
    ) -> bool {
        trace!(
            "Placeholder filter:\n  Atom: {}\n  Arguments: {}",
            catalog.rhs_index_from_signature(*var_sig.atom_signature()),
            var_sig
        );

        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![], // no const-eq
            vec![], // no var-eq
            vec![], // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Placeholder transformation:\n      {}", tx);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    // =========================================================================
    // Semijoin & comparison pass (private)
    // =========================================================================

    /// Try (in order): positive semijoin, anti-semijoin, comparison pushdown.
    fn apply_semijoin(&mut self, catalog: &mut Catalog) -> bool {
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
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

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
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

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
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

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
    fn remove_unused_arguments(&mut self, catalog: &mut Catalog) -> bool {
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return false;
        }

        let mut applied = false;

        for (atom_signature, to_delete) in groups.clone() {
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

            trace!("Unused transformation:\n      {}", tx);

            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp);

            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);

            applied = true;
        }

        applied
    }

    // =========================================================================
    // Small utilities (private)
    // =========================================================================

    /// Build output payload expressions excluding a specific argument signature.
    #[inline]
    fn out_values_excluding(
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
    fn get_or_init_row_kv_layout(
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
    fn projection_name(atom_signature: &AtomSignature, atom_id: usize) -> String {
        if atom_signature.is_positive() {
            format!("atom_{}_proj", atom_id)
        } else {
            format!("neg_atom_{}_proj", atom_id)
        }
    }

    /// Collect display strings for signatures, used to exclude duplicate RHS columns.
    /// Note: we return owned `String`s to avoid lifetime coupling to `catalog`.
    fn arg_names_set(args: &[AtomArgumentSignature], catalog: &Catalog) -> HashSet<String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str(sig).clone())
            .collect()
    }
}
