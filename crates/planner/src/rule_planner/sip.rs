//! **Side-Information Passing (SIP)** optimization for rule planning.
//!
//! SIP leverages variable bindings that are already available in one atom to
//! filter another atom *before* the main join takes place.  Concretely, for a
//! pair of atoms `(L, R)` that share variables, SIP:
//!
//! 1. **Projects** `L` down to the shared join-key columns.
//! 2. **Semijoins** the projected keys against `R`, producing a smaller
//!    version of `R` that only retains tuples whose keys appear in `L`.
//!
//! The filtered `R` then replaces the original atom in the catalog so that
//! downstream planning operates on the reduced collection.
//!
//! > **Note:** This is currently an ad-hoc, per-pair optimization.  A more
//! > general SIP framework (e.g. sideways information-passing strategies for
//! > full rules) is left for future work.

use crate::{transformation::KeyValueLayout, TransformationInfo};

use super::RulePlanner;
use catalog::{
    arithmetic::FactorPos, ArithmeticPos, AtomSignature, AtomArgumentSignature,
    Catalog, JoinPredicates, KvPredicates,
};

use tracing::trace;
use tracing::debug;

// =========================================================================
// SIP Optimization
// =========================================================================
impl RulePlanner {
    /// Entry point for applying SIP optimizations to the current rule plan.
    pub fn apply_sip(&mut self, catalog: &mut Catalog, use_approx_sip: bool) {
        let positive_atom_numbers = catalog.positive_atom_number();

        // Only apply SIP if there are at least 3 positive atoms
        if positive_atom_numbers > 1 {
            // Left -> Right foward pass.
            for left_atom_idx in 0..positive_atom_numbers {
                for right_atom_idx in (left_atom_idx + 1)..positive_atom_numbers {
                    if catalog.check_sip_pair(left_atom_idx, right_atom_idx) {
                        trace!(
                            "SIP: forward atom_pos{} -> atom_pos{}",
                            left_atom_idx,
                            right_atom_idx
                        );
                        self.apply_sip_premaps(catalog, (left_atom_idx, right_atom_idx));
                        if use_approx_sip {
                            self.apply_sip_projection_approx_semijoin(catalog, left_atom_idx, right_atom_idx);
                        } else {
                            self.apply_sip_projection_semijoin(catalog, left_atom_idx, right_atom_idx);
                        }
                        trace!("Catalog:\n{}", catalog);
                        trace!("{}", "-".repeat(60));
                    }
                }
            }

            // Right -> Left backward pass.
            for left_atom_idx in (0..positive_atom_numbers).rev() {
                for right_atom_idx in (0..left_atom_idx).rev() {
                    if catalog.check_sip_pair(left_atom_idx, right_atom_idx) {
                        trace!(
                            "SIP: backward atom_pos{} -> atom_pos{}",
                            left_atom_idx,
                            right_atom_idx
                        );
                        self.apply_sip_premaps(catalog, (left_atom_idx, right_atom_idx));
                        if use_approx_sip {
                            self.apply_sip_projection_approx_semijoin(catalog, left_atom_idx, right_atom_idx);
                        } else {
                            self.apply_sip_projection_semijoin(catalog, left_atom_idx, right_atom_idx);
                        }
                        trace!("Catalog:\n{}", catalog);
                        trace!("{}", "-".repeat(60));
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Ensures that EDB atoms referenced by a SIP pair have identity premap
    /// transformations so they present a proper key/value layout.
    fn apply_sip_premaps(&mut self, catalog: &mut Catalog, sip_pair: (usize, usize)) {
        let (lhs_idx, rhs_idx) = sip_pair;

        let lhs_is_original = catalog
            .original_atom_fingerprints()
            .contains(&catalog.positive_atom_fingerprint(lhs_idx));
        let rhs_is_original = catalog
            .original_atom_fingerprints()
            .contains(&catalog.positive_atom_fingerprint(rhs_idx));

        if lhs_is_original {
            self.create_edb_premap_transformations(catalog, lhs_idx, true);
        }
        if rhs_is_original {
            self.create_edb_premap_transformations(catalog, rhs_idx, true);
        }
    }

    /// Builds a two-step **project → semijoin** plan that filters the RHS
    /// atom using shared join keys from the LHS atom.
    ///
    /// After this method returns, the RHS atom in `catalog` has been replaced
    /// by the semijoin result (same arguments, new fingerprint).
    fn apply_sip_projection_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_idx: usize,
    ) {
        let base_idx = self.transformation_infos.len();
        let originals = catalog.original_atom_fingerprints();

        // Atom metadata
        let left_fp = catalog.positive_atom_fingerprint(lhs_pos_idx);
        let left_arg_sigs = catalog.positive_atom_argument_signature(lhs_pos_idx);

        let right_fp = catalog.positive_atom_fingerprint(rhs_pos_idx);
        let right_atom_sig = AtomSignature::new(true, rhs_pos_idx);
        let right_arg_sigs = catalog.positive_atom_argument_signature(rhs_pos_idx);

        // Register both atoms as consumers of their respective inputs.
        // The projection consumes LHS; the semijoin consumes the result of projection and RHS.
        self.insert_consumer(originals, left_fp, base_idx);
        self.insert_consumer(originals, right_fp, base_idx + 1);

        // Partition arguments into shared keys and remaining values
        let (lhs_keys, lhs_vals, rhs_keys, rhs_vals) =
            Self::partition_shared_keys(catalog, left_arg_sigs, right_arg_sigs);

        Self::trace_sip_partitions(catalog, &lhs_keys, &lhs_vals, &rhs_vals);

        // ---- Step 1: Project LHS → join keys only ----
        let proj_tx = TransformationInfo::kv_to_kv(
            left_fp,
            originals.contains(&left_fp),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals),
            KeyValueLayout::new(lhs_keys.clone(), vec![]),
            KvPredicates::default(),
        )
        .into_sip_projection();
        let proj_fp = proj_tx.output_info_fp();

        self.insert_producer(proj_fp, base_idx);
        self.transformation_infos.push(proj_tx);
        // No catalog modification needed — the projection is an internal
        // intermediate result consumed only by the semijoin below.

        // ---- Step 2: Semijoin projected-LHS ⋉ RHS ----
        self.insert_consumer(originals, proj_fp, base_idx + 1);

        // Rebuild the projected LHS keys with new signatures for the semijoin operator.
        let lhs_new_keys: Vec<ArithmeticPos> = lhs_keys
            .iter()
            .enumerate()
            .map(|(idx, pos)| {
                let sig = pos.init().as_var_signature().unwrap();
                let new_sig = AtomArgumentSignature::new(*sig.atom_signature(), idx);
                ArithmeticPos::from_var_signature(new_sig)
            })
            .collect();

        let semijoin_tx = TransformationInfo::join_to_kv(
            proj_fp,
            right_fp,
            KeyValueLayout::new(lhs_new_keys.clone(), vec![]),
            KeyValueLayout::new(rhs_keys.clone(), rhs_vals.clone()),
            KeyValueLayout::new(lhs_new_keys.clone(), rhs_vals.clone()),
            JoinPredicates::default(),
        );
        let semijoin_fp = semijoin_tx.output_info_fp();
        let semijoin_name = format!(
            "atom_pos{}_sip_semijoin_atom_pos{}",
            lhs_pos_idx, rhs_pos_idx
        );

        self.insert_producer(semijoin_fp, base_idx + 1);
        self.transformation_infos.push(semijoin_tx);

        // Replace the RHS atom in the catalog with the filtered version.
        let new_arguments_list = rhs_keys
            .iter()
            .chain(rhs_vals.iter())
            .map(|pos| pos.init().as_var_signature().unwrap())
            .cloned()
            .collect();

        catalog.sip_modify(
            right_atom_sig,
            new_arguments_list,
            semijoin_name,
            semijoin_fp,
        );
    }

    fn apply_sip_projection_approx_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_idx: usize,
    ) {
        let originals = catalog.original_atom_fingerprints();

        // Atom metadata
        let left_fp = catalog.positive_atom_fingerprint(lhs_pos_idx);
        let left_arg_sigs = catalog.positive_atom_argument_signature(lhs_pos_idx);

        let right_fp = catalog.positive_atom_fingerprint(rhs_pos_idx);
        let right_atom_sig = AtomSignature::new(true, rhs_pos_idx);
        let right_arg_sigs = catalog.positive_atom_argument_signature(rhs_pos_idx);

        debug!("left_fp: {:x}", left_fp);
        debug!("right_fp: {:x}", right_fp);

        // Partition arguments into shared keys and remaining values
        let (lhs_keys, lhs_vals, rhs_keys, rhs_vals) =
            Self::partition_shared_keys(catalog, left_arg_sigs, right_arg_sigs);

        Self::trace_sip_partitions(catalog, &lhs_keys, &lhs_vals, &rhs_vals);

        // ---- Step 1: Project LHS → single hashed join key ----
        let lhs_hash_key = Self::hash_key_expression(&lhs_keys);

        let next_idx = self.transformation_infos.len();
        self.insert_consumer(originals, left_fp, next_idx);

        let lhs_hmap_tx = TransformationInfo::kv_to_kv(
            left_fp,
            originals.contains(&left_fp),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals),
            KeyValueLayout::new(vec![lhs_hash_key.clone()], vec![]),
            KvPredicates::default(),
        )
        .into_sip_projection();
        let lhs_hmap_fp = lhs_hmap_tx.output_info_fp();
        self.insert_producer(lhs_hmap_fp, next_idx);
        self.transformation_infos.push(lhs_hmap_tx);
        debug!("lhs_hmap_fp: {:x}", lhs_hmap_fp);
        debug!("lhs_hmap_idx: {:?}", next_idx);

        // ---- Step 2: Project RHS → hashed join key + RHS payload ----
        let rhs_hash_key = Self::hash_key_expression(&rhs_keys);
        let mut rhs_kv = Vec::new();
        for sig in right_arg_sigs {
            rhs_kv.push(ArithmeticPos::from_var_signature(*sig))
        }

        let next_idx = self.transformation_infos.len();
        self.insert_consumer(originals, right_fp, next_idx);
        let rhs_hmap_tx = TransformationInfo::kv_to_kv(
            right_fp,
            originals.contains(&right_fp),
            KeyValueLayout::new(rhs_keys.clone(), rhs_vals.clone()),
            KeyValueLayout::new(vec![rhs_hash_key.clone()], rhs_kv.clone()),
            KvPredicates::default(),
        )
        .into_sip_projection();
        let rhs_hmap_fp = rhs_hmap_tx.output_info_fp();
        self.insert_producer(rhs_hmap_fp, next_idx);
        self.transformation_infos.push(rhs_hmap_tx);
        debug!("rhs_hmap_fp: {:x}", rhs_hmap_fp);
        debug!("rhs_hmap_idx: {:?}", next_idx);

        // ---- Step 3: Semijoin projected-LHS ⋉ projected-RHS ----
        let next_idx = self.transformation_infos.len();
        self.insert_consumer(originals, lhs_hmap_fp, next_idx);
        self.insert_consumer(originals, rhs_hmap_fp, next_idx);

        let pre_join_lhs_key = vec![lhs_hash_key.clone()];
        let pre_join_rhs_key = vec![rhs_hash_key.clone()];
        let pre_join_rhs_vals = rhs_kv.clone();

        let lhs_new_keys: Vec<ArithmeticPos> = pre_join_lhs_key
            .iter()
            .enumerate()
            .map(|(idx, pos)| {
                let binding = pos
                    .signatures();
                let sig = binding
                    .first()
                    .expect("fncall key must contain at least one variable");
                let new_sig = AtomArgumentSignature::new(*sig.atom_signature(), idx);
                ArithmeticPos::from_var_signature(new_sig)
            })
            .collect();

        let rhs_new_keys: Vec<ArithmeticPos> = pre_join_rhs_key
            .iter()
            .enumerate()
            .map(|(idx, pos)| {
                let binding = pos
                    .signatures();
                let sig = binding
                    .first()
                    .expect("fncall key must contain at least one variable");
                let new_sig = AtomArgumentSignature::new(*sig.atom_signature(), idx);
                ArithmeticPos::from_var_signature(new_sig)
            })
            .collect();

        let rhs_new_vals: Vec<ArithmeticPos> = pre_join_rhs_vals
            .iter()
            .enumerate()
            .map(|(idx, pos)| {
                let binding = pos
                    .signatures();
                let sig = binding
                    .first()
                    .expect("fncall key must contain at least one variable");
                let new_sig = AtomArgumentSignature::new(*sig.atom_signature(), idx + 1);
                ArithmeticPos::from_var_signature(new_sig)
            })
            .collect();

        let semijoin_tx = TransformationInfo::join_to_kv(
            lhs_hmap_fp,
            rhs_hmap_fp,
            KeyValueLayout::new(lhs_new_keys, vec![]),
            KeyValueLayout::new(rhs_new_keys, rhs_new_vals.clone()),
            KeyValueLayout::new(vec![], rhs_new_vals.clone()),
            JoinPredicates::default(),
        );
        let semijoin_fp = semijoin_tx.output_info_fp();
        let semijoin_name = format!(
            "atom_pos{}_sip_semijoin_atom_pos{}",
            lhs_pos_idx, rhs_pos_idx
        );

        self.insert_producer(semijoin_fp, next_idx);
        self.transformation_infos.push(semijoin_tx);

        debug!("semijoin_fp: {:x}", semijoin_fp);
        debug!("semijoin_idx: {:?}", next_idx);

        let rhs_new_vals: Vec<ArithmeticPos> = rhs_new_vals
            .iter()
            .enumerate()
            .map(|(idx, pos)| {
                let binding = pos
                    .signatures();
                let sig = binding
                    .first()
                    .expect("fncall key must contain at least one variable");
                let new_sig = AtomArgumentSignature::new(*sig.atom_signature(), idx);
                ArithmeticPos::from_var_signature(new_sig)
            })
            .collect();

        // Replace the RHS atom in the catalog with the filtered version.
        let new_arguments_list = rhs_new_vals
            .iter()
            .map(|pos| pos.init().as_var_signature().unwrap())
            .cloned()
            .collect();

        catalog.sip_modify(
            right_atom_sig,
            new_arguments_list,
            semijoin_name,
            semijoin_fp,
        );
    }

    fn hash_key_expression(keys: &[ArithmeticPos]) -> ArithmeticPos {
        ArithmeticPos::new(
            FactorPos::FnCall {
                name: "hash".to_string(),
                args: keys.to_vec(),
            },
            vec![],
        )
    }

    // ------------------------------------------------------------------
    // Tracing
    // ------------------------------------------------------------------
    fn trace_sip_partitions(
        catalog: &Catalog,
        lhs_keys: &[ArithmeticPos],
        lhs_vals: &[ArithmeticPos],
        rhs_vals: &[ArithmeticPos],
    ) {
        let fmt = |pos: &ArithmeticPos| {
            (
                pos.clone(),
                catalog.signature_to_argument_str(pos.init().as_var_signature().unwrap()),
            )
        };

        trace!(
            "SIP semijoin keys: {:?}",
            lhs_keys.iter().map(fmt).collect::<Vec<_>>()
        );
        trace!(
            "SIP semijoin LHS values: {:?}",
            lhs_vals.iter().map(fmt).collect::<Vec<_>>()
        );
        trace!(
            "SIP semijoin RHS values: {:?}",
            rhs_vals.iter().map(fmt).collect::<Vec<_>>()
        );
    }
}
