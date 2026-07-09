//! Core logic for rule planning.
//!
//! This module implements the core rule planning algorithm, focusing on joining
//! two positive atoms and applying optimization transformations in a fixed-point loop.
//!
//! Core logic relies on optimizer to give the index of the two atoms to join.

use tracing::trace;

use super::RulePlanner;
use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, AtomSignature, Catalog, JoinPredicates,
};
use crate::planner::PlanError;
use crate::planner::{KeyValueLayout, TransformationInfo};

// =========================================================================
// Core Planning
// =========================================================================
impl RulePlanner {
    /// This is the main entry point for the rule planning process. It performs a join
    /// between two positive atoms and then applies optimization transformations in a
    /// fixed-point loop until no more optimizations can be applied.
    pub(crate) fn core(
        &mut self,
        catalog: &mut Catalog,
        join_tuple_index: (usize, usize),
    ) -> Result<(), PlanError> {
        trace!(
            "Join:\n  LHS atom: ({}, {})\n RHS atom: ({}, {})",
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.0)],
            catalog.positive_atom_rhs_id(join_tuple_index.0),
            catalog.rule().rhs()[catalog.positive_atom_rhs_id(join_tuple_index.1)],
            catalog.positive_atom_rhs_id(join_tuple_index.1),
        );

        // Premap EDB atoms to match required key/value layouts
        self.apply_join_premaps(catalog, join_tuple_index)?;

        // Execute the initial join between the two selected atoms
        self.apply_join(catalog, join_tuple_index)?;
        trace!("Catalog:\n{}", catalog);
        trace!("{}", "-".repeat(60));

        // Apply optimization transformations until fixed point
        loop {
            // 1) Apply semijoin optimizations and comparison pushdown
            // These optimizations can create new opportunities for projection
            if self.apply_semijoin(catalog)? {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Remove unused arguments to reduce data volume
            // This must come after semijoins as they may eliminate argument usage
            if self.remove_unused_arguments(catalog)? {
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Fixed point reached - no more optimizations possible
            break;
        }
        Ok(())
    }

    /// Premaps EDB atoms to match required key/value layouts.
    fn apply_join_premaps(
        &mut self,
        catalog: &mut Catalog,
        join_tuple_index: (usize, usize),
    ) -> Result<(), PlanError> {
        let (lhs_idx, rhs_idx) = join_tuple_index;
        for idx in [lhs_idx, rhs_idx] {
            if catalog
                .original_atom_fingerprints()
                .contains(&catalog.positive_atom_fingerprint(idx))
            {
                self.create_edb_premap_transformations(catalog, idx, true)?;
            }
        }
        Ok(())
    }

    /// Applies a join transformation between two positive atoms.
    fn apply_join(
        &mut self,
        catalog: &mut Catalog,
        join_tuple_index: (usize, usize),
    ) -> Result<(), PlanError> {
        let current_transformation_index = self.transformation_infos.len();
        let (lhs_idx, rhs_idx) = join_tuple_index;

        // Extract LHS atom information and register as consumer
        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_idx);
        let left_atom_signature = AtomSignature::new(true, lhs_idx);
        let left_atom_argument_signatures = catalog.positive_atom_argument_signature(lhs_idx);

        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            lhs_pos_fp,
            current_transformation_index,
        )?;

        // Extract RHS atom information and register as consumer
        let rhs_pos_fp = catalog.positive_atom_fingerprint(rhs_idx);
        let right_atom_signatures = vec![AtomSignature::new(true, rhs_idx)];
        let right_atom_argument_signatures = catalog.positive_atom_argument_signature(rhs_idx);

        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            rhs_pos_fp,
            current_transformation_index,
        )?;

        // Partition arguments into join keys and payload values
        let (mut lhs_keys, lhs_vals, mut rhs_keys, rhs_vals) = Self::partition_shared_keys(
            catalog,
            left_atom_argument_signatures,
            right_atom_argument_signatures,
        );

        // Equi-join fusion (see `Catalog::equijoin_keys_for_pair`): when the two
        // atoms share no bare variable — which would otherwise force an empty-key
        // cross-join plus a post-join filter, i.e. an O(|L|·|R|) blow-up — key the
        // join on a spanning equality `L == R` computed from the two sides, where
        // `L` is grounded wholly in one atom and `R` in the other. This turns e.g.
        // the DOOP `ContextResponse` join `hctx.0 == hctxValue` (a tuple
        // projection) or an arithmetic `x + 1 == y + 2` into a real hash join.
        //
        // The companion layout change in `fuse.rs`/`transformation/info.rs`
        // (`refactor_output_key_value_layout`) carries the *computed* key
        // `ArithmeticPos` through to the input-arrangement reshape so it
        // materializes the derived key instead of collapsing to the base column.
        // Fresh-variable equalities (`z = g(x)`) are desugared to bindings before
        // planning, so only both-sides-grounded equalities ever reach here — the
        // only ones safe to fuse. The join closure keeps the equality as a
        // redundant residual, so correctness holds even for unforeseen shapes.
        const ENABLE_EQUIJOIN_FUSION: bool = true;
        let mut fused_equijoin = false;
        if ENABLE_EQUIJOIN_FUSION && lhs_keys.is_empty() {
            for (l, r) in catalog.equijoin_keys_for_pair(lhs_idx, rhs_idx) {
                lhs_keys.push(l);
                rhs_keys.push(r);
                fused_equijoin = true;
            }
        }

        fn labelled(positions: &[ArithmeticPos], catalog: &Catalog) -> Vec<String> {
            positions
                .iter()
                .map(|pos| match pos.init().as_var_signature() {
                    Some(sig) => catalog.signature_to_argument_str(sig).clone(),
                    None => pos.to_string(),
                })
                .collect()
        }
        trace!("Join keys: {:?}", labelled(&lhs_keys, catalog));
        trace!("Join LHS values: {:?}", labelled(&lhs_vals, catalog));
        trace!("Join RHS values: {:?}", labelled(&rhs_vals, catalog));

        // Construct output argument list. A fused equi-join keys on a spanning
        // equality whose sides may be computed (e.g. `hctx.0`) and are not
        // necessarily distinct output columns, so the join emits just the two
        // payloads (the base variables already live in the values); a plain
        // shared-variable join instead emits its single shared key once.
        let new_arguments_list: Vec<AtomArgumentSignature> = if fused_equijoin {
            lhs_vals
                .iter()
                .chain(rhs_vals.iter())
                .map(|pos| *pos.init().as_var_signature().unwrap())
                .collect()
        } else {
            lhs_keys
                .iter()
                .chain(lhs_vals.iter())
                .chain(rhs_vals.iter())
                .map(|pos| *pos.init().as_var_signature().unwrap())
                .collect()
        };

        // Create the join transformation with proper key-value layouts
        let lhs_name = catalog.positive_atom_name(lhs_idx)?.to_string();
        let rhs_name = catalog.positive_atom_name(rhs_idx)?.to_string();
        let lhs_key_names = Self::attrs_from_positions(&lhs_keys, catalog);
        let new_name = Self::join_name(&lhs_name, &rhs_name, &lhs_key_names);
        // The joined output keeps a shared-variable key (if any); a fused
        // equi-join produces an unkeyed payload (re-keyed later as needed).
        let output_key = if fused_equijoin {
            Vec::new()
        } else {
            lhs_keys.clone()
        };
        let tx = TransformationInfo::join_to_kv(
            lhs_pos_fp,
            lhs_name,
            rhs_pos_fp,
            rhs_name,
            new_name.clone(),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals.clone()),
            KeyValueLayout::new(rhs_keys.clone(), rhs_vals.clone()),
            KeyValueLayout::new(
                output_key,
                lhs_vals.iter().chain(rhs_vals.iter()).cloned().collect(),
            ),
            JoinPredicates::default(),
        );

        let new_fp = tx.output_info_fp();

        self.insert_producer(new_fp, current_transformation_index);

        trace!("Join transformation:\n{}", tx);

        // Store the transformation info
        self.transformation_infos.push(tx);

        // Update catalog with the new joined atom
        catalog.join_modify(
            left_atom_signature,
            right_atom_signatures,
            vec![new_arguments_list],
            vec![new_name],
            vec![new_fp],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;
    use super::*;

    /// `Out(x, z) :- A(x, y), B(y, z).` — shared var `y` is the join key.
    /// Core must emit a `JoinToKV` whose output layout has `y` as the sole
    /// join key, `x` (from A) and `z` (from B) as values, and both input
    /// layouts keyed on `y`. A broken `partition_shared_keys` would route
    /// `x` or `z` to the key position (cross product with wrong semantics)
    /// or route `y` to values (no join at all, just stapling two streams).
    ///
    /// Signatures are captured before `core()` runs because the pass
    /// calls `update_rule` under the hood, rebuilding the catalog's
    /// sig→name map around the joined atom. We compare by sig identity,
    /// which pins each slot to the exact source-level argument regardless
    /// of any post-join name remapping.
    #[test]
    fn core_join_emits_join_to_kv_with_shared_key_as_join_key() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl Out(x: int32, z: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, z) :- A(x, y), B(y, z).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        // Pin each source var to its pre-core argument signature.
        let a_sigs = catalog.positive_atom_argument_signature(0).clone();
        let b_sigs = catalog.positive_atom_argument_signature(1).clone();
        let x_in_a = a_sigs[0];
        let y_in_a = a_sigs[1];
        let y_in_b = b_sigs[0];
        let z_in_b = b_sigs[1];

        planner.core(&mut catalog, (0, 1)).expect("core");

        let join = planner
            .transformation_infos()
            .iter()
            .find(|t| matches!(t, TransformationInfo::JoinToKV { .. }))
            .expect("JoinToKV transformation missing");

        let sig_of = |pos: &crate::catalog::ArithmeticPos| {
            *pos.init().as_var_signature().expect("var signature")
        };

        let out = join.output_kv_layout();
        assert_eq!(out.key().len(), 1, "exactly one join key");
        assert_eq!(
            sig_of(&out.key()[0]),
            y_in_a,
            "join key must be `y` from LHS (A's arg 1)"
        );
        assert_eq!(out.value().len(), 2, "two payload values");
        assert_eq!(
            sig_of(&out.value()[0]),
            x_in_a,
            "first output value must be `x` from LHS"
        );
        assert_eq!(
            sig_of(&out.value()[1]),
            z_in_b,
            "second output value must be `z` from RHS"
        );

        // Both input layouts must also be keyed on `y` — if either side
        // was keyed on its own local var, the join degenerates.
        let (left, right) = join.input_kv_layout();
        let right = right.expect("JoinToKV has a right input layout");
        assert_eq!(sig_of(&left.key()[0]), y_in_a, "LHS input keyed on `y`");
        assert_eq!(sig_of(&right.key()[0]), y_in_b, "RHS input keyed on `y`");
        assert_eq!(sig_of(&left.value()[0]), x_in_a, "LHS payload is `x`");
        assert_eq!(sig_of(&right.value()[0]), z_in_b, "RHS payload is `z`");

        // Post-state: core must leave the catalog reduced to one atom
        // (the join result) and flagged planned. A planner that emitted
        // the right JoinToKV but forgot to call `catalog.join_modify`
        // would fail here but pass the structural checks above.
        assert_eq!(
            catalog.positive_atom_number(),
            1,
            "two atoms must collapse into one after the join"
        );
        assert!(
            catalog.is_planned(),
            "catalog should be flagged planned after a complete 2-atom join"
        );
    }

    /// `Out(x, y) :- A(x), B(y), x + 1 = y + 2.` — the two atoms share no
    /// bare variable, so a name-only join key search finds nothing and would
    /// emit an empty-key cross-join with `x+1 = y+2` as a post-filter (an
    /// O(|A|·|B|) blow-up). Equi-join fusion must instead key the join on the
    /// spanning equality: the LHS input keyed on the *computed* `x + 1`, the
    /// RHS on `y + 2`. This is the general form of the DOOP tuple-projection
    /// join (`hctx.0 = hctxValue`) that the fusion was built to accelerate.
    #[test]
    fn core_fuses_spanning_computed_equality_into_join_key() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y), x + 1 = y + 2.\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        let x_in_a = catalog.positive_atom_argument_signature(0)[0];
        let y_in_b = catalog.positive_atom_argument_signature(1)[0];

        planner.core(&mut catalog, (0, 1)).expect("core");

        let join = planner
            .transformation_infos()
            .iter()
            .find(|t| matches!(t, TransformationInfo::JoinToKV { .. }))
            .expect("JoinToKV transformation missing");

        let (left, right) = join.input_kv_layout();
        let right = right.expect("JoinToKV has a right input layout");

        // The join must be keyed (not an empty-key cross product) …
        assert_eq!(left.key().len(), 1, "LHS keyed on the fused equality");
        assert_eq!(right.key().len(), 1, "RHS keyed on the fused equality");

        // … and each key must be a *computed* expression (`x+1`, `y+2`),
        // not a bare column — that is the whole point of the fusion.
        assert!(
            !left.key()[0].is_plain_var(),
            "LHS join key must be the computed `x + 1`, not a bare variable"
        );
        assert!(
            !right.key()[0].is_plain_var(),
            "RHS join key must be the computed `y + 2`, not a bare variable"
        );

        // The computed keys must be rooted at the correct source variables.
        assert!(
            left.key()[0].signatures().contains(&&x_in_a),
            "LHS key `x + 1` must reference `x` from A"
        );
        assert!(
            right.key()[0].signatures().contains(&&y_in_b),
            "RHS key `y + 2` must reference `y` from B"
        );

        assert_eq!(
            catalog.positive_atom_number(),
            1,
            "two atoms must collapse into one after the fused join"
        );
    }
}
