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
use catalog::{ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog};

use tracing::trace;

// =========================================================================
// SIP Optimization
// =========================================================================
impl RulePlanner {
    /// Entry point for applying SIP optimizations to the current rule plan.
    pub fn apply_sip(&mut self, catalog: &mut Catalog) {
        unimplemented!("SIP optimization is not implemented yet");
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
        let proj_tx = TransformationInfo::kv_to_kv_sip_projection(
            left_fp,
            originals.contains(&left_fp),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals),
            KeyValueLayout::new(lhs_keys.clone(), vec![]),
            vec![],
            vec![],
            vec![],
        );
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
                let sig = pos.init().signature().unwrap();
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
            vec![],
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
            .map(|pos| pos.init().signature().unwrap())
            .cloned()
            .collect();

        catalog.sip_modify(
            right_atom_sig,
            new_arguments_list,
            semijoin_name,
            semijoin_fp,
        );
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
                catalog.signature_to_argument_str(pos.init().signature().unwrap()),
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
