//! Rule planner implementing the per-rule preparation planning.

use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use std::collections::{HashMap, HashSet};

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
        loop {
            // Try to apply the next available filter; continue looping on success.
            if self.apply_filter(catalog) {
                continue;
            }

            // Try semijoin-like prep step.
            if self.apply_semijoin(catalog) {
                continue;
            }

            // Try to remove arguments not contributing to the output.
            if self.remove_unused_arguments(catalog) {
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
        let filters = catalog.filters();

        // Priority 1: Variable equality filter (var1 == var2)
        if let Some((&a, &b)) = filters.var_eq_map().iter().next() {
            // Canonicalize order: (smaller atom sig, then smaller arg id) comes first.
            let (var1_sig, var2_sig) = if (*a.atom_signature(), a.argument_id())
                <= (*b.atom_signature(), b.argument_id())
            {
                (a, b)
            } else {
                (b, a)
            };

            let atom_signature = var1_sig.atom_signature();
            let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);
            let out_vals = Self::out_values_excluding(args, var2_sig);

            // Build a “filter then project-away-dropped-var” KV→KV transformation.
            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                KeyValueLayout::new(in_keys, in_vals),
                KeyValueLayout::new(Vec::new(), out_vals),
                vec![],                     // no const-eq
                vec![(var1_sig, var2_sig)], // var-eq
                vec![],                     // no comparisons
            );

            let new_name = Self::projection_name(atom_signature, atom_id);
            catalog.projection_modify(
                *atom_signature,
                vec![var2_sig],
                new_name,
                tx.output_info_fp(),
            );

            // Update key-value layout cache
            self.kv_layouts.insert(tx.output_info_fp(), 0);

            self.transformation_infos.push(tx);
            return true;
        }

        // Priority 2: Constant equality filter (var == const)
        if let Some((&var_sig, const_val)) = filters.const_map().iter().next() {
            let atom_signature = var_sig.atom_signature();
            let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);
            let out_vals = Self::out_values_excluding(args, var_sig);

            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                KeyValueLayout::new(in_keys, in_vals),
                KeyValueLayout::new(Vec::new(), out_vals),
                vec![(var_sig, const_val.clone())],
                vec![], // no var-eq
                vec![], // no comparisons
            );

            let new_name = Self::projection_name(atom_signature, atom_id);
            catalog.projection_modify(
                *atom_signature,
                vec![var_sig],
                new_name,
                tx.output_info_fp(),
            );

            // Update key-value layout cache
            self.kv_layouts.insert(tx.output_info_fp(), 0);

            self.transformation_infos.push(tx);
            return true;
        }

        // Priority 3: Placeholder filters
        if let Some(&var_sig) = filters.placeholder_set().iter().next() {
            let atom_signature = var_sig.atom_signature();
            let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

            let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);
            let out_vals = Self::out_values_excluding(args, var_sig);

            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                KeyValueLayout::new(in_keys, in_vals),
                KeyValueLayout::new(Vec::new(), out_vals),
                vec![], // no const-eq
                vec![], // no var-eq
                vec![], // no comparisons
            );

            let new_name = Self::projection_name(atom_signature, atom_id);
            catalog.projection_modify(
                *atom_signature,
                vec![var_sig],
                new_name,
                tx.output_info_fp(),
            );

            // Update key-value layout cache
            self.kv_layouts.insert(tx.output_info_fp(), 0);

            self.transformation_infos.push(tx);
            return true;
        }

        false
    }

    /// Removes unused arguments across atoms (batched).
    fn remove_unused_arguments(&mut self, catalog: &mut Catalog) -> bool {
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return false;
        }

        let mut applied = false;
        for (atom_signature, to_delete) in groups.clone() {
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
            catalog.projection_modify(atom_signature, to_delete, new_name, tx.output_info_fp());

            // Update key-value layout cache
            self.kv_layouts
                .insert(tx.output_info_fp(), out_value_start_idx);

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
        {
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
            let out_value_start_idx = lhs_keys.len();

            // Precompute set of LHS argument strings for “value-only” cols on RHS.
            let lhs_arg_names = Self::arg_names_set(&lhs_pos_args, catalog);

            for &rhs_idx in rhs_pos_indices {
                let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
                let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

                new_arg_lists.push(rhs_args.clone());
                right_sigs.push(AtomSignature::new(true, rhs_idx));

                let rhs_vals: Vec<ArithmeticPos> = rhs_args
                    .iter()
                    .filter(|&sig| {
                        !lhs_arg_names
                            .contains(catalog.signature_to_argument_str_map().get(sig).unwrap())
                    })
                    .map(|&s| ArithmeticPos::from_var_signature(s))
                    .collect();

                let tx = TransformationInfo::join_to_kv(
                    lhs_pos_fp,
                    rhs_fp,
                    KeyValueLayout::new(lhs_keys.clone(), Vec::new()), // join keys from LHS
                    KeyValueLayout::new(Vec::new(), rhs_vals.clone()), // RHS “payload” as values
                    KeyValueLayout::new(lhs_keys.clone(), rhs_vals), // output preserves keys+payload
                    vec![],                                          // no extra comparisons
                );

                new_names.push(format!(
                    "atom_{}_pos_semijoin_atom_{}",
                    lhs_pos_idx, rhs_idx
                ));
                new_fps.push(tx.output_info_fp());

                // Update key-value layout cache
                self.kv_layouts
                    .insert(tx.output_info_fp(), out_value_start_idx);

                self.transformation_infos.push(tx);
            }

            catalog.join_modify(
                left_atom_signature,
                right_sigs,
                new_arg_lists,
                new_names,
                new_fps,
            );
            return true;
        }

        // (2) Negated predicate has positive supersets
        if let Some((lhs_neg_idx, rhs_pos_indices)) = catalog
            .negated_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
        {
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
            let lhs_arg_names = Self::arg_names_set(&lhs_neg_args, catalog);
            let out_value_start_idx = lhs_keys.len();

            for &rhs_idx in rhs_pos_indices {
                let rhs_args = catalog.positive_atom_argument_signature(rhs_idx).clone();
                let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx);

                new_arg_lists.push(rhs_args.clone());
                right_sigs.push(AtomSignature::new(true, rhs_idx));

                let rhs_vals: Vec<ArithmeticPos> = rhs_args
                    .iter()
                    .filter(|&sig| {
                        !lhs_arg_names
                            .contains(catalog.signature_to_argument_str_map().get(sig).unwrap())
                    })
                    .map(|&s| ArithmeticPos::from_var_signature(s))
                    .collect();

                let tx = TransformationInfo::anti_join_to_kv(
                    lhs_neg_fp,
                    rhs_fp,
                    KeyValueLayout::new(lhs_keys.clone(), Vec::new()),
                    KeyValueLayout::new(Vec::new(), rhs_vals.clone()),
                    KeyValueLayout::new(lhs_keys.clone(), rhs_vals),
                );

                new_names.push(format!(
                    "atom_{}_neg_semijoin_atom_{}",
                    lhs_neg_idx, rhs_idx
                ));
                new_fps.push(tx.output_info_fp());

                // Update key-value layout cache
                self.kv_layouts
                    .insert(tx.output_info_fp(), out_value_start_idx);

                self.transformation_infos.push(tx);
            }

            catalog.join_modify(
                left_atom_signature,
                right_sigs,
                new_arg_lists,
                new_names,
                new_fps,
            );
            return true;
        }

        // (3) Comparison predicate has positive supersets
        if let Some((lhs_comp_idx, rhs_pos_indices)) = catalog
            .comparison_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
        {
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

                new_names.push(format!(
                    "comparison_{}_filter_atom_{}",
                    comparison_exprs, rhs_idx
                ));
                new_fps.push(tx.output_info_fp());

                // Update key-value layout cache
                self.kv_layouts
                    .insert(tx.output_info_fp(), out_value_start_idx);

                self.transformation_infos.push(tx);
            }

            catalog.comparison_modify(lhs_comp_idx, right_sigs, new_names, new_fps);
            return true;
        }

        false
    }

    // -------- Helpers -------------------------------------------------------

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

    /// Returns an existing KV layout for atom fingerprint, or initializes a row-based (no key) layout
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
            return (keys.to_vec(), vals.to_vec());
        }

        // Not cached: default row-based (all values, no keys)
        let keys = Vec::new();
        let vals: Vec<ArithmeticPos> = args
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();

        // Cache split index (0 means no keys, all values)
        self.kv_layouts.insert(atom_fp, 0);

        (keys, vals)
    }

    /// Formats a new projection name based on atom kind and id.
    fn projection_name(atom_signature: &AtomSignature, atom_id: usize) -> String {
        let base = if atom_signature.is_positive() {
            format!("atom_{}", atom_id)
        } else {
            format!("neg_atom_{}", atom_id)
        };
        format!("{}_proj", base)
    }

    /// Builds a set of display strings for the given argument signatures (used for RHS payload pick).
    fn arg_names_set<'a>(
        args: &'a [AtomArgumentSignature],
        catalog: &'a Catalog,
    ) -> HashSet<&'a String> {
        args.iter()
            .map(|sig| catalog.signature_to_argument_str_map().get(sig).unwrap())
            .collect()
    }
}
