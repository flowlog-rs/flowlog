//! Rule planner implementing the per-rule preparation planning.

use crate::fake_transformation::FakeTransformation;
use crate::transformation::Transformation;
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use std::collections::HashSet;

/// Rule planner for the per-rule planning.
#[derive(Debug, Default)]
pub struct RulePlanner {
    /// The list of transformations generated during the alpha elimination prepare phase.
    fake_prepare_transformations: Vec<FakeTransformation>,

    /// The map of key-value layout indexed by fake transformation signature.
    fake_kv_layouts: std::collections::HashMap<u64, (Vec<ArithmeticPos>, Vec<ArithmeticPos>)>,

    /// The list of transformations generated during the core plan phase.
    core_transformations: Vec<Transformation>,
}

impl RulePlanner {
    /// Creates a new empty `RulePlanner`.
    pub fn new() -> Self {
        Self {
            fake_prepare_transformations: Vec::new(),
            core_transformations: Vec::new(),
            fake_kv_layouts: std::collections::HashMap::new(),
        }
    }

    /// Executes the alpha elimination prepare phase of per-rule planning.
    pub fn prepare(&mut self, catalog: &mut Catalog) {
        loop {
            // Try to apply the next available filter; continue looping on success
            if self.apply_filter(catalog) {
                continue;
            }

            // Try the second step (TODO: implement)
            if self.apply_semijoin(catalog) {
                continue;
            }

            // Try to remove arguments not contributing to the output
            if self.remove_unused_arguments(catalog) {
                continue;
            }

            // Neither step could apply — we've done alpha elimination
            break;
        }
    }

    pub fn core(&mut self, _catalog: &mut Catalog) {
        todo!("Implement the core planning phase");
    }

    pub fn fake_transformations(&self) -> Vec<FakeTransformation> {
        self.fake_prepare_transformations.clone()
    }

    /// Applies the first available filter and updates the catalog.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> bool {
        let filters = catalog.filters();

        // Priority 1: Variable equality filter (var1 == var2)
        if let Some((&sig_a, &sig_b)) = filters.var_eq_map().iter().next() {
            // Always order pair so that var1_sig < var2_sig (by atom signature, then argument id)
            let (var1_sig, var2_sig) = if (*sig_a.atom_signature(), sig_a.argument_id())
                <= (*sig_b.atom_signature(), sig_b.argument_id())
            {
                (sig_a, sig_b)
            } else {
                (sig_b, sig_a)
            };

            // Step 1: Locate the target atom and fetch its args
            let atom_signature = var1_sig.atom_signature();
            let (args, atom_fg, atom_id) = catalog.resolve_atom(atom_signature);

            // Step 2: Build the input key-value layout
            let (input_key_exprs, input_value_exprs) = self
                .fake_kv_layouts
                .get(&atom_fg)
                .cloned()
                .unwrap_or_else(|| {
                    let input_key_exprs = Vec::new(); // Row-based input
                    let input_value_exprs: Vec<ArithmeticPos> = args
                        .iter()
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();
                    self.fake_kv_layouts.insert(
                        atom_fg,
                        (input_key_exprs.clone(), input_value_exprs.clone()),
                    );
                    (input_key_exprs, input_value_exprs)
                });

            // Step 3: Create default output key-value layout as row based (no keys).
            let (fake_output_key_exprs, fake_output_value_exprs): (
                Vec<ArithmeticPos>,
                Vec<ArithmeticPos>,
            ) = (Vec::new(), Self::out_values_excluding(args, var2_sig));

            // Step 4: Create the fake transformation
            let fk_tx = FakeTransformation::fake_kv_to_kv(
                atom_fg,
                input_key_exprs,
                input_value_exprs,
                fake_output_key_exprs,
                fake_output_value_exprs,
                vec![],                     // no const-eq constraints here
                vec![(var1_sig, var2_sig)], // var-eq constraint
                vec![],                     // no comparisons here
            );

            // Step 4: Mutate catalog — project away the constrained var and update its identity
            let new_name = format!(
                "{}_proj",
                if atom_signature.is_positive() {
                    format!("atom_{}", atom_id)
                } else {
                    format!("neg_atom_{}", atom_id)
                }
            );
            catalog.projection_modify(
                *atom_signature,
                vec![var2_sig],
                new_name,
                fk_tx.fake_output_sig(),
            );

            // Step 6: Record the transformation in the prepared phase pipeline
            self.fake_prepare_transformations.push(fk_tx);

            return true;
        }
        // Priority 2: Constant equality filters (var == const)
        else if let Some((&var_sig, const_val)) = filters.const_map().iter().next() {
            // Step 1: Locate the target atom and fetch its (args, fingerprint)
            let atom_signature = var_sig.atom_signature();
            let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

            // Step 2: Build the input key-value layout
            let (input_key_exprs, input_value_exprs) = self
                .fake_kv_layouts
                .get(&atom_fp)
                .cloned()
                .unwrap_or_else(|| {
                    let input_key_exprs = Vec::new(); // Row-based input
                    let input_value_exprs: Vec<ArithmeticPos> = args
                        .iter()
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();
                    self.fake_kv_layouts.insert(
                        atom_fp,
                        (input_key_exprs.clone(), input_value_exprs.clone()),
                    );
                    (input_key_exprs, input_value_exprs)
                });

            // Step 3: Create default output key-value layout as row based (no keys).
            let (fake_output_key_exprs, fake_output_value_exprs): (
                Vec<ArithmeticPos>,
                Vec<ArithmeticPos>,
            ) = (Vec::new(), Self::out_values_excluding(args, var_sig));

            // Step 4: Create the fake transformation
            let fk_tx = FakeTransformation::fake_kv_to_kv(
                atom_fp,
                input_key_exprs,
                input_value_exprs,
                fake_output_key_exprs,
                fake_output_value_exprs,
                vec![(var_sig, const_val.clone())], // no const-eq constraints here
                vec![],                             // var-eq constraint
                vec![],                             // no comparisons here
            );

            // Step 5: Mutate catalog — project away the constrained var and update its identity
            let new_name = format!(
                "{}_proj",
                if atom_signature.is_positive() {
                    format!("atom_{}", atom_id)
                } else {
                    format!("neg_atom_{}", atom_id)
                }
            );
            catalog.projection_modify(
                *atom_signature,
                vec![var_sig],
                new_name,
                fk_tx.fake_output_sig(),
            );

            // Step 6: Record the transformation in the prepared phase pipeline
            self.fake_prepare_transformations.push(fk_tx);

            return true;
        }
        // Priority 3: Placeholder filters
        else if let Some(&var_sig) = filters.placeholder_set().iter().next() {
            // Step 1: Locate the target atom and fetch its (args, fingerprint)
            let atom_signature = var_sig.atom_signature();
            let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

            // Step 2: Build the input key-value layout
            let (input_key_exprs, input_value_exprs) = self
                .fake_kv_layouts
                .get(&atom_fp)
                .cloned()
                .unwrap_or_else(|| {
                    let input_key_exprs = Vec::new(); // Row-based input
                    let input_value_exprs: Vec<ArithmeticPos> = args
                        .iter()
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();
                    self.fake_kv_layouts.insert(
                        atom_fp,
                        (input_key_exprs.clone(), input_value_exprs.clone()),
                    );
                    (input_key_exprs, input_value_exprs)
                });

            // Step 3: Create default output key-value layout as row based (no keys).
            let (fake_output_key_exprs, fake_output_value_exprs): (
                Vec<ArithmeticPos>,
                Vec<ArithmeticPos>,
            ) = (Vec::new(), Self::out_values_excluding(args, var_sig));

            // Step 4: Create the fake transformation
            let fk_tx = FakeTransformation::fake_kv_to_kv(
                atom_fp,
                input_key_exprs,
                input_value_exprs,
                fake_output_key_exprs,
                fake_output_value_exprs,
                vec![], // no const-eq constraints here
                vec![], // var-eq constraint
                vec![], // no comparisons here
            );

            // Step 5: Mutate catalog — project away the constrained var and update its identity
            let new_name = format!(
                "{}_proj",
                if atom_signature.is_positive() {
                    format!("atom_{}", atom_id)
                } else {
                    format!("neg_atom_{}", atom_id)
                }
            );
            catalog.projection_modify(
                *atom_signature,
                vec![var_sig],
                new_name,
                fk_tx.fake_output_sig(),
            );

            // Step 6: Record the transformation in the prepared phase pipeline
            self.fake_prepare_transformations.push(fk_tx);

            return true;
        }

        false
    }

    fn remove_unused_arguments(&mut self, catalog: &mut Catalog) -> bool {
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return false;
        }

        let mut applied_any = false;

        for (atom_signature, to_delete) in groups.clone() {
            // Resolve current atom state
            let (args, atom_fp, atom_id) = catalog.resolve_atom(&atom_signature);

            // Build projected output excluding all to_delete
            let drop_set: HashSet<AtomArgumentSignature> = to_delete.iter().copied().collect();
            let (input_key_exprs, input_value_exprs) = self
                .fake_kv_layouts
                .get(&atom_fp)
                .cloned()
                .unwrap_or_else(|| {
                    let input_key_exprs = Vec::new(); // Row-based input
                    let input_value_exprs: Vec<ArithmeticPos> = args
                        .iter()
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();
                    self.fake_kv_layouts.insert(
                        atom_fp,
                        (input_key_exprs.clone(), input_value_exprs.clone()),
                    );
                    (input_key_exprs, input_value_exprs)
                });
            let out_values: Vec<ArithmeticPos> = args
                .iter()
                .filter(|&sig| !drop_set.contains(sig))
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect();

            // Build transformation and compute new fingerprint
            let fk_tx = FakeTransformation::fake_kv_to_kv(
                atom_fp,
                input_key_exprs,
                input_value_exprs,
                vec![],
                out_values,
                vec![],
                vec![],
                vec![],
            );

            // Mutate catalog for this atom
            let new_name = format!(
                "{}_proj",
                if atom_signature.is_positive() {
                    format!("atom_{}", atom_id)
                } else {
                    format!("neg_atom_{}", atom_id)
                }
            );
            catalog.projection_modify(atom_signature, to_delete, new_name, fk_tx.fake_output_sig());

            // Record the transformation
            self.fake_prepare_transformations.push(fk_tx);
            applied_any = true;
        }

        applied_any
    }

    /// Second step after applying the first filter.
    /// TODO: implement the additional preparation action; return true if something was applied.
    fn apply_semijoin(&mut self, catalog: &mut Catalog) -> bool {
        // (1) Positive predicate has positive supersets
        let pos_supersets = catalog.positive_supersets();
        let neg_supersets = catalog.negated_supersets();
        let comp_supersets = catalog.comparison_supersets();

        if let Some((lhs_pos_idx, rhs_pos_indices)) = pos_supersets
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
        {
            let mut new_names: Vec<String> = Vec::new();
            let mut new_fingerprints: Vec<u64> = Vec::new();
            let mut new_arguments_list: Vec<Vec<AtomArgumentSignature>> = Vec::new();
            let mut right_atom_signatures: Vec<AtomSignature> = Vec::new();

            let lhs_pos_args = catalog.positive_atom_argument_signatures()[lhs_pos_idx].clone();
            let lhs_pos_fp = catalog.positive_atom_fingerprints()[lhs_pos_idx];
            let left_atom_signature = AtomSignature::new(true, lhs_pos_idx);
            for super_pos_idx in rhs_pos_indices {
                let rhs_pos_args =
                    catalog.positive_atom_argument_signatures()[*super_pos_idx].clone();
                new_arguments_list.push(rhs_pos_args.clone());
                let rhs_pos_fp = catalog.positive_atom_fingerprints()[*super_pos_idx];
                right_atom_signatures.push(AtomSignature::new(true, *super_pos_idx));

                let input_key_exprs: Vec<ArithmeticPos> = lhs_pos_args
                    .iter()
                    .map(|&sig| ArithmeticPos::from_var_signature(sig))
                    .collect();
                let input_key_str = lhs_pos_args
                    .iter()
                    .map(|sig| catalog.signature_to_argument_str_map().get(sig).unwrap())
                    .collect::<HashSet<_>>();
                let input_value_exprs: Vec<ArithmeticPos> = rhs_pos_args
                    .iter()
                    .filter(|&sig| {
                        !input_key_str
                            .contains(catalog.signature_to_argument_str_map().get(sig).unwrap())
                    })
                    .map(|&sig| ArithmeticPos::from_var_signature(sig))
                    .collect();

                let (fake_output_key_exprs, fake_output_value_exprs) =
                    (input_key_exprs.clone(), input_value_exprs.clone());

                let fk_tx = FakeTransformation::fake_join_to_kv(
                    lhs_pos_fp,
                    rhs_pos_fp,
                    input_key_exprs,
                    vec![],
                    input_value_exprs,
                    fake_output_key_exprs,
                    fake_output_value_exprs,
                    vec![], // no comparisons here
                );

                new_names.push(format!(
                    "atom_{}_pos_semijoin_atom_{}",
                    lhs_pos_idx, super_pos_idx
                ));
                new_fingerprints.push(fk_tx.fake_output_sig());
                self.fake_prepare_transformations.push(fk_tx);
            }

            catalog.join_modify(
                left_atom_signature,
                right_atom_signatures,
                new_arguments_list,
                new_names,
                new_fingerprints,
            );

            return true;
        } else {
            // 2) Negated predicate has positive supersets

            if let Some((lhs_neg_idx, rhs_pos_indices)) = neg_supersets
                .iter()
                .enumerate()
                .find(|(_, v)| !v.is_empty())
            {
                let mut new_names: Vec<String> = Vec::new();
                let mut new_fingerprints: Vec<u64> = Vec::new();
                let mut new_arguments_list: Vec<Vec<AtomArgumentSignature>> = Vec::new();
                let mut right_atom_signatures: Vec<AtomSignature> = Vec::new();

                let lhs_neg_args = catalog.negated_atom_argument_signatures()[lhs_neg_idx].clone();
                let lhs_neg_fp = catalog.negated_atom_fingerprints()[lhs_neg_idx];
                let left_atom_signature = AtomSignature::new(false, lhs_neg_idx);
                for super_pos_idx in rhs_pos_indices {
                    let rhs_pos_args =
                        catalog.positive_atom_argument_signatures()[*super_pos_idx].clone();
                    new_arguments_list.push(rhs_pos_args.clone());
                    let rhs_pos_fp = catalog.positive_atom_fingerprints()[*super_pos_idx];
                    right_atom_signatures.push(AtomSignature::new(true, *super_pos_idx));

                    let input_key_exprs: Vec<ArithmeticPos> = lhs_neg_args
                        .iter()
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();
                    let input_key_str = lhs_neg_args
                        .iter()
                        .map(|sig| catalog.signature_to_argument_str_map().get(sig).unwrap())
                        .collect::<HashSet<_>>();
                    let input_value_exprs: Vec<ArithmeticPos> = rhs_pos_args
                        .iter()
                        .filter(|&sig| {
                            !input_key_str
                                .contains(catalog.signature_to_argument_str_map().get(sig).unwrap())
                        })
                        .map(|&sig| ArithmeticPos::from_var_signature(sig))
                        .collect();

                    let (fake_output_key_exprs, fake_output_value_exprs) =
                        (input_key_exprs.clone(), input_value_exprs.clone());

                    let fk_tx = FakeTransformation::fake_join_to_kv(
                        lhs_neg_fp,
                        rhs_pos_fp,
                        input_key_exprs,
                        vec![],
                        input_value_exprs,
                        fake_output_key_exprs,
                        fake_output_value_exprs,
                        vec![], // no comparisons here
                    );

                    new_names.push(format!(
                        "atom_{}_neg_semijoin_atom_{}",
                        lhs_neg_idx, super_pos_idx
                    ));
                    new_fingerprints.push(fk_tx.fake_output_sig());
                    self.fake_prepare_transformations.push(fk_tx);
                }

                catalog.join_modify(
                    left_atom_signature,
                    right_atom_signatures,
                    new_arguments_list,
                    new_names,
                    new_fingerprints,
                );

                return true;
            } else {
                // 3) Comparison predicate has positive supersets
                if let Some((lhs_comp_idx, rhs_pos_indices)) = comp_supersets
                    .iter()
                    .enumerate()
                    .find(|(_, v)| !v.is_empty())
                {
                    let mut new_names: Vec<String> = Vec::new();
                    let mut new_fingerprints: Vec<u64> = Vec::new();
                    let mut right_atom_signatures: Vec<AtomSignature> = Vec::new();

                    let comparison_exprs = catalog.comparison_predicates()[lhs_comp_idx].clone();
                    for super_pos_idx in rhs_pos_indices {
                        let rhs_pos_args =
                            catalog.positive_atom_argument_signatures()[*super_pos_idx].clone();
                        let rhs_pos_fp = catalog.positive_atom_fingerprints()[*super_pos_idx];
                        right_atom_signatures.push(AtomSignature::new(true, *super_pos_idx));

                        let (input_key_exprs, input_value_exprs) = self
                            .fake_kv_layouts
                            .get(&rhs_pos_fp)
                            .cloned()
                            .unwrap_or_else(|| {
                                let input_key_exprs = Vec::new(); // Row-based input
                                let input_value_exprs: Vec<ArithmeticPos> = rhs_pos_args
                                    .iter()
                                    .map(|&sig| ArithmeticPos::from_var_signature(sig))
                                    .collect();
                                self.fake_kv_layouts.insert(
                                    rhs_pos_fp,
                                    (input_key_exprs.clone(), input_value_exprs.clone()),
                                );
                                (input_key_exprs, input_value_exprs)
                            });

                        let (fake_output_key_exprs, fake_output_value_exprs) =
                            (input_key_exprs.clone(), input_value_exprs.clone());

                        let fk_tx = FakeTransformation::fake_kv_to_kv(
                            rhs_pos_fp,
                            input_key_exprs,
                            input_value_exprs,
                            fake_output_key_exprs,
                            fake_output_value_exprs,
                            vec![], // no const-eq constraints here
                            vec![], // var-eq constraint
                            vec![catalog
                                .comparison_predicates_filter_pos(*super_pos_idx, lhs_comp_idx)], // no comparisons here
                        );

                        new_names.push(format!(
                            "comparison_{}_filter_atom_{}",
                            comparison_exprs, super_pos_idx
                        ));
                        new_fingerprints.push(fk_tx.fake_output_sig());
                        self.fake_prepare_transformations.push(fk_tx);
                    }

                    catalog.comparison_modify(
                        lhs_comp_idx,
                        right_atom_signatures,
                        new_names,
                        new_fingerprints,
                    );

                    return true;
                }
            }
        }

        false
    }

    /// Build output value expressions excluding a specific argument signature.
    fn out_values_excluding(
        args: &[AtomArgumentSignature],
        drop_sig: AtomArgumentSignature,
    ) -> Vec<ArithmeticPos> {
        args.iter()
            .filter(|&sig| sig != &drop_sig)
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect()
    }
}
