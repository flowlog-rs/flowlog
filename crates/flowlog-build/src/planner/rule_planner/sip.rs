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

use super::RulePlanner;
use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog, JoinPredicates, KvPredicates,
};
use crate::common::{proj_name, semijoin_name};
use crate::planner::{KeyValueLayout, PlanError, TransformationInfo};
use tracing::trace;

// =========================================================================
// SIP Optimization
// =========================================================================
impl RulePlanner {
    /// Entry point for applying SIP optimizations to the current rule plan.
    pub(crate) fn apply_sip(&mut self, catalog: &mut Catalog) -> Result<(), PlanError> {
        let n = catalog.positive_atom_number();

        // SIP only pays off once there's a third atom that can benefit from
        // the filtered result — for 2-atom rules a semijoin would just
        // duplicate the join itself.
        if n <= 2 {
            return Ok(());
        }

        // Forward pass: each atom filters every later atom it shares vars with.
        for left in 0..n {
            for right in (left + 1)..n {
                self.try_apply_sip_pair(catalog, left, right, "forward")?;
            }
        }

        // Backward pass: every later atom gets to filter every earlier one.
        for left in (0..n).rev() {
            for right in (0..left).rev() {
                self.try_apply_sip_pair(catalog, left, right, "backward")?;
            }
        }

        Ok(())
    }

    /// If `(left, right)` shares variables, run premaps + projection-semijoin
    /// for the pair; otherwise no-op. `direction` is purely for trace output.
    fn try_apply_sip_pair(
        &mut self,
        catalog: &mut Catalog,
        left: usize,
        right: usize,
        direction: &str,
    ) -> Result<(), PlanError> {
        if !catalog.check_sip_pair(left, right) {
            return Ok(());
        }
        trace!("SIP: {direction} atom_pos{} -> atom_pos{}", left, right);
        self.apply_sip_premaps(catalog, (left, right))?;
        self.apply_sip_projection_semijoin(catalog, left, right)?;
        trace!("Catalog:\n{}", catalog);
        trace!("{}", "-".repeat(60));
        Ok(())
    }

    // ------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------

    /// Ensures that EDB atoms referenced by a SIP pair have identity premap
    /// transformations so they present a proper key/value layout.
    fn apply_sip_premaps(
        &mut self,
        catalog: &mut Catalog,
        sip_pair: (usize, usize),
    ) -> Result<(), PlanError> {
        // The two atoms are distinct, and premapping one cannot change the
        // other's fingerprint, so a single forward pass is sufficient.
        for idx in [sip_pair.0, sip_pair.1] {
            let fp = catalog.positive_atom_fingerprint(idx);
            if catalog.original_atom_fingerprints().contains(&fp) {
                self.create_edb_premap_transformations(catalog, idx, true)?;
            }
        }
        Ok(())
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
    ) -> Result<(), PlanError> {
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
        self.insert_consumer(originals, left_fp, base_idx)?;
        self.insert_consumer(originals, right_fp, base_idx + 1)?;

        // Partition arguments into shared keys and remaining values
        let (lhs_keys, lhs_vals, rhs_keys, rhs_vals) =
            Self::partition_shared_keys(catalog, left_arg_sigs, right_arg_sigs);

        Self::trace_sip_partitions(catalog, &lhs_keys, &lhs_vals, &rhs_vals);

        // ---- Step 1: Project LHS → join keys only ----
        let left_name = catalog.positive_atom_name(lhs_pos_idx)?.to_string();
        let lhs_key_names = RulePlanner::attrs_from_positions(&lhs_keys, catalog);
        let proj_name = proj_name(&left_name, &lhs_key_names);
        let proj_tx = TransformationInfo::kv_to_kv(
            left_fp,
            left_name,
            proj_name.clone(),
            originals.contains(&left_fp),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals),
            KeyValueLayout::new(lhs_keys.clone(), vec![]),
            KvPredicates::default(),
        )
        .into_sip_projection()?;
        let proj_fp = proj_tx.output_info_fp();

        self.insert_producer(proj_fp, base_idx);
        self.transformation_infos.push(proj_tx);
        // No catalog modification needed — the projection is an internal
        // intermediate result consumed only by the semijoin below.

        // ---- Step 2: Semijoin projected-LHS ⋉ RHS ----
        self.insert_consumer(originals, proj_fp, base_idx + 1)?;

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

        let right_name = catalog.positive_atom_name(rhs_pos_idx)?.to_string();
        let semijoin_name = semijoin_name(&proj_name, &right_name, &lhs_key_names);
        let semijoin_tx = TransformationInfo::join_to_kv(
            proj_fp,
            proj_name,
            right_fp,
            right_name,
            semijoin_name.clone(),
            KeyValueLayout::new(lhs_new_keys.clone(), vec![]),
            KeyValueLayout::new(rhs_keys.clone(), rhs_vals.clone()),
            KeyValueLayout::new(lhs_new_keys.clone(), rhs_vals.clone()),
            JoinPredicates::default(),
        );
        let semijoin_fp = semijoin_tx.output_info_fp();

        self.insert_producer(semijoin_fp, base_idx + 1);
        self.transformation_infos.push(semijoin_tx);

        // Replace the RHS atom in the catalog with the filtered version.
        let new_arguments_list = rhs_keys
            .iter()
            .chain(rhs_vals.iter())
            .map(|pos| *pos.init().as_var_signature().unwrap())
            .collect();

        catalog.sip_modify(
            right_atom_sig,
            new_arguments_list,
            semijoin_name,
            semijoin_fp,
        )?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;

    /// Two-atom rule falls below the `positive_atom_numbers > 2` gate in
    /// sip.rs:37. SIP must produce zero transformations. Removing that
    /// guard would duplicate every pairwise semijoin for simple 2-atom
    /// rules — perf regression with no correctness signal.
    #[test]
    fn apply_sip_skips_two_atom_rule() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32)\n\
            .decl B(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x), B(x).\n",
        );
        planner.apply_sip(&mut catalog).expect("sip");
        assert_eq!(
            planner.transformation_infos().len(),
            0,
            "SIP must not run on 2-atom rules"
        );
        // Post-state: catalog must also be untouched — SIP that
        // silently modified atom state while emitting nothing would
        // desync the catalog from the empty transformation_infos.
        assert_eq!(catalog.positive_atom_number(), 2);
    }

    /// Three-atom rule with shared vars across adjacent pairs — forward
    /// pass must emit projection+semijoin pairs. Counts `is_sip_projection`
    /// to prove forward pass actually fired (not just returned Ok(())).
    #[test]
    fn apply_sip_forward_pass_fires() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl C(a: int32, b: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .decl Out(x: int32, z: int32)\n\
            .output Out\n\
            Out(x, z) :- A(x), B(x, y), C(y, z).\n",
        );
        planner.apply_sip(&mut catalog).expect("sip");

        let proj_count = planner
            .transformation_infos()
            .iter()
            .filter(|t| t.is_sip_projection())
            .count();
        assert!(
            proj_count >= 2,
            "expected at least 2 SIP projections from forward pass (A→B, B→C), got {proj_count}"
        );
        // SIP replaces atoms with filtered semijoin results; atom count
        // stays at 3. A bug in sip_modify that dropped atoms would
        // silently corrupt planning for the subsequent core pass.
        assert_eq!(catalog.positive_atom_number(), 3);
    }

    /// The backward pass in sip.rs:56 gives later atoms the chance to
    /// filter earlier ones. If it were removed, atom A (first in the body)
    /// could only ever be a SIP *source*, never a *target* — losing a
    /// valid optimization. We assert a strictly higher projection count
    /// than forward-only to prove backward ran.
    #[test]
    fn apply_sip_backward_pass_filters_earlier_atom() {
        // Chain A(y), B(y, z), C(z):
        //   forward  : SIP B from A, SIP C from B     → 2 projections
        //   backward : SIP B from C, SIP A from B     → 2 projections
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl C(a: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .decl Out(z: int32)\n\
            .output Out\n\
            Out(z) :- A(y), B(y, z), C(z).\n",
        );
        planner.apply_sip(&mut catalog).expect("sip");

        let proj_count = planner
            .transformation_infos()
            .iter()
            .filter(|t| t.is_sip_projection())
            .count();
        assert!(
            proj_count >= 4,
            "forward alone yields 2 projections; with backward expect ≥4, got {proj_count}"
        );
        assert_eq!(catalog.positive_atom_number(), 3);
    }
}
