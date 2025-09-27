//! Rule planner implementing the per-rule preparation planning.

use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use parser::ConstType;
use std::collections::{HashMap, HashSet};
use tracing::trace;

/// Rule planner for the per-rule planning.
#[derive(Debug, Default)]
pub struct RulePlanner {
    /// The list of transformation info generated during the alpha elimination prepare phase.
    transformation_infos: Vec<TransformationInfo>,

    /// The map of key-value layout indexed by transformation fingerprint.
    /// We cache the index of the start of value columns.
    kv_layouts: HashMap<u64, usize>,
}

impl RulePlanner {
    /// Creates a new empty `RulePlanner`.
    pub fn new() -> Self {
        Self {
            transformation_infos: Vec::new(),
            kv_layouts: HashMap::new(),
        }
    }

    /// Executes the alpha-elimination prepare phase of per-rule planning.
    pub fn prepare(&mut self, catalog: &mut Catalog) {
        let mut step = 0;

        loop {
            // Try to apply all the filters first, this gives us the most opportunities for simplification.
            if self.apply_filter(catalog) {
                step += 1;
                trace!("Prepare Step {}: filter applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Try to apply semijoin, this includes positive semijoin, negated semijoin and comparisons.
            if self.apply_semijoin(catalog) {
                step += 1;
                trace!("Prepare Step {}: semijoin applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Try to remove arguments not contributing to the output.
            if self.remove_unused_arguments(catalog) {
                step += 1;
                trace!("Prepare Step {}: unused arguments removed", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Neither step could apply — we've done alpha elimination.
            break;
        }
    }

    pub fn core(&mut self, _catalog: &mut Catalog) {
        todo!("Implement the core planning phase");
    }

    /// NOTE: keeps original API (clone) to avoid breaking callers.
    pub fn transformation_infos(&self) -> Vec<TransformationInfo> {
        self.transformation_infos.clone()
    }

    /// Applies available filters in priority order, mutating the catalog if applied.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> bool {
        // Priority 1: Variable equality filter (var1 == var2)
        if let Some((&a, &b)) = catalog.filters().var_eq_map().iter().next() {
            return self.apply_var_equality_filter(catalog, a, b);
        }

        // Priority 2: Constant equality filter (var == const)
        if let Some((&var_sig, const_val)) = catalog.filters().const_map().iter().next() {
            return self.apply_const_equality_filter(catalog, var_sig, const_val.clone());
        }

        // Priority 3: Placeholder filters
        if let Some(&var_sig) = catalog.filters().placeholder_set().iter().next() {
            return self.apply_placeholder_filter(catalog, var_sig);
        }

        false
    }

    /// Applies variable equality filter (var1 == var2).
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

        // Canonicalize order: (smaller atom sig, then smaller arg id) comes first.
        let (var1_sig, var2_sig) =
            if (*a.atom_signature(), a.argument_id()) <= (*b.atom_signature(), b.argument_id()) {
                (a, b)
            } else {
                (b, a)
            };

        let atom_signature = var1_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var2_sig);
        trace!("Output values after dropping {}: {:?}", var2_sig, out_vals);

        // Build a "filter then project-away-dropped-var" KV→KV transformation.
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

        trace!("Var-eq transformation: {} (0x{:x})", new_name, new_fp);

        catalog.projection_modify(*atom_signature, vec![var2_sig], new_name, new_fp);

        // Update key-value layout cache
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Applies constant equality filter (var == const).
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
            vec![(var_sig, const_val)],
            vec![], // no var-eq
            vec![], // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Const-eq transformation: {} (0x{:x})", new_name, new_fp);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        // Update key-value layout cache
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Applies placeholder filter.
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

        trace!("Placeholder transformation: {} (0x{:x})", new_name, new_fp);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        // Update key-value layout cache
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Removes unused arguments across atoms (batched).
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

            trace!("Unused transformation: {} (0x{:x})", new_name, new_fp);

            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp);

            // Update key-value layout cache
            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);
            applied = true;
        }

        applied
    }

    /// Second step after applying the first filter: prepare via (anti-)semijoin or comparison pushdown.
    /// Returns true if something was applied.
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

    /// Applies positive semijoin transformation.
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

        let out_value_start_idx = lhs_keys.len();

        // Precompute set of LHS argument strings for "value-only" cols on RHS.
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
                KeyValueLayout::new(Vec::new(), rhs_vals.clone()), // RHS "payload" as values
                KeyValueLayout::new(lhs_keys.clone(), rhs_vals),   // output preserves keys+payload
                vec![],                                            // no extra comparisons
            );

            let new_name = format!("atom_pos{}_pos_semijoin_atom_pos{}", lhs_pos_idx, rhs_idx);
            let new_fp = tx.output_info_fp();

            trace!(
                "Positive semijoin transformation: {} (0x{:x})",
                new_name,
                new_fp
            );

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Update key-value layout cache
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

    /// Applies anti-semijoin transformation.
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

            trace!(
                "Anti semijoin transformation: {} (0x{:x})",
                new_name,
                new_fp
            );

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Update key-value layout cache
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

    /// Applies comparison pushdown transformation.
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

            trace!("Comparison transformation: {} (0x{:x})", new_name, new_fp);

            new_names.push(new_name);
            new_fps.push(new_fp);

            // Update key-value layout cache
            self.kv_layouts.insert(new_fp, out_value_start_idx);
            self.transformation_infos.push(tx);
        }

        catalog.comparison_modify(lhs_comp_idx, right_sigs, new_names, new_fps);
        true
    }

    // -------- Helper Methods ------------------------------------------------

    /// Build output value expressions excluding a specific argument signature.
    fn out_values_excluding(
        args: &[AtomArgumentSignature],
        drop_sig: AtomArgumentSignature,
    ) -> Vec<ArithmeticPos> {
        args.iter()
            .filter(|&sig| *sig != drop_sig)
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect()
    }

    /// Returns an existing KV layout for atom fingerprint, or initializes a row-based (no key) layout.
    /// We cache only the split index: before that index = keys, from that index = values.
    fn get_or_init_row_kv_layout(
        &mut self,
        atom_fp: u64,
        args: &[AtomArgumentSignature],
    ) -> (Vec<ArithmeticPos>, Vec<ArithmeticPos>) {
        if let Some(&split_idx) = self.kv_layouts.get(&atom_fp) {
            // Already cached: split according to stored index
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

        // Not cached: default row-based (all values, no keys)
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

        // Cache split index (0 means no keys, all values)
        self.kv_layouts.insert(atom_fp, 0);

        (keys, vals)
    }

    /// Formats a new projection name based on atom kind and id.
    fn projection_name(atom_signature: &AtomSignature, atom_id: usize) -> String {
        if atom_signature.is_positive() {
            format!("atom_{}_proj", atom_id)
        } else {
            format!("neg_atom_{}_proj", atom_id)
        }
    }

    /// Builds a set of display strings for the given argument signatures (used for RHS payload pick).
    fn arg_names_set<'a>(
        args: &'a [AtomArgumentSignature],
        catalog: &'a Catalog,
    ) -> HashSet<&'a String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str(sig))
            .collect()
    }
}
