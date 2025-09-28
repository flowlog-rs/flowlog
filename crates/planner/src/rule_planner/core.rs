use std::collections::{HashMap, HashSet};

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};
use tracing::trace;

impl RulePlanner {
    pub fn core(&mut self, catalog: &mut Catalog, join_tuple_index: (usize, usize)) {
        trace!(
            "Join:\n  LHS atom: ({}, {})\n RHS atom: ({}, {})",
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.0)],
            catalog.positive_atom_rhs_id(join_tuple_index.0),
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.1)],
            catalog.positive_atom_rhs_id(join_tuple_index.1),
        );
        self.apply_join(catalog, join_tuple_index);
        trace!("Catalog:\n{}", catalog);
        trace!("{}", "-".repeat(60));

        loop {
            // 1) First (anti-)semijoins and comparison pushdown.
            if self.apply_semijoin(catalog) {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Second, remove any arguments that no longer contribute to outputs.
            if self.remove_unused_arguments(catalog) {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Nothing else to do in this phase.
            break;
        }
    }

    fn apply_join(&mut self, catalog: &mut Catalog, join_tuple_index: (usize, usize)) {
        let (lhs_idx, rhs_idx) = join_tuple_index;

        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_idx);
        let left_atom_signature = AtomSignature::new(true, lhs_idx);
        let left_atom_argument_signatures = catalog.positive_atom_argument_signature(lhs_idx);

        let rhs_pos_fp = catalog.positive_atom_fingerprint(rhs_idx);
        let right_atom_signatures = vec![AtomSignature::new(true, rhs_idx)];
        let right_atom_argument_signatures = catalog.positive_atom_argument_signature(rhs_idx);

        let (lhs_keys, lhs_vals, rhs_vals) = Self::partition_shared_keys(
            catalog,
            &left_atom_argument_signatures,
            &right_atom_argument_signatures,
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
        let new_arguments_list: Vec<AtomArgumentSignature> = lhs_keys
            .iter()
            .chain(lhs_vals.iter())
            .chain(rhs_vals.iter())
            .map(|pos| pos.init().signature().unwrap())
            .cloned()
            .collect();
        let output_value_start_idx = lhs_keys.len();

        let tx = TransformationInfo::join_to_kv(
            lhs_pos_fp,
            rhs_pos_fp,
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals.clone()), // join keys from LHS
            KeyValueLayout::new(Vec::new(), rhs_vals.clone()),       // RHS payload as values
            KeyValueLayout::new(
                lhs_keys,
                lhs_vals.iter().chain(rhs_vals.iter()).cloned().collect(),
            ), // output = keys + payload
            vec![],                                                  // no extra comparisons
        );

        let new_name = format!("atom_pos{}_join_atom_pos{}", lhs_idx, rhs_idx);
        let new_fp = tx.output_info_fp();

        trace!("Join transformation:\n      {}", tx);

        self.kv_layouts.insert(new_fp, output_value_start_idx);
        self.transformation_infos.push(tx);

        catalog.join_modify(
            left_atom_signature,
            right_atom_signatures,
            vec![new_arguments_list],
            vec![new_name],
            vec![new_fp],
        );
    }

    fn partition_shared_keys(
        catalog: &Catalog,
        lhs_sigs: &[AtomArgumentSignature],
        rhs_sigs: &[AtomArgumentSignature],
    ) -> (Vec<ArithmeticPos>, Vec<ArithmeticPos>, Vec<ArithmeticPos>) {
        let mut rhs_sigs_to_name = HashMap::new();
        for sig in rhs_sigs {
            let name = catalog.signature_to_argument_str(sig);
            rhs_sigs_to_name.insert(name, sig);
        }

        let mut left_keys = Vec::new();
        let mut left_remains = Vec::new();
        let mut matched_names = HashSet::new();

        for sig in lhs_sigs {
            let name = catalog.signature_to_argument_str(sig);
            if rhs_sigs_to_name.contains_key(name) {
                left_keys.push(ArithmeticPos::from_var_signature(*sig));
                matched_names.insert(name.as_str());
            } else {
                left_remains.push(ArithmeticPos::from_var_signature(*sig))
            }
        }

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
