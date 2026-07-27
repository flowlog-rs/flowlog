//! Core logic for rule planning.
//!
//! This module implements the core rule planning algorithm, focusing on joining
//! two positive atoms and applying optimization transformations in a fixed-point loop.
//!
//! Core logic relies on optimizer to give the index of the two atoms to join.

use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::Catalog;
use crate::catalog::JoinPredicates;
use crate::catalog::KvPredicates;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

// =========================================================================
// Core Planning
// =========================================================================
impl RulePlanner {
    /// This is the main entry point for the rule planning process. It performs a join
    /// between two positive atoms and then applies optimization transformations in a
    /// fixed-point loop until no more optimizations can be applied.
    pub fn core(
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

        // Turn spanning equalities into shared join columns when the two
        // atoms would otherwise cross-product
        self.apply_equijoin_fusion(catalog, join_tuple_index)?;

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

    /// Fuse spanning equalities between the two join atoms into shadow-column
    /// join keys, consuming the fused equalities.
    fn apply_equijoin_fusion(
        &mut self,
        catalog: &mut Catalog,
        join_tuple_index: (usize, usize),
    ) -> Result<(), PlanError> {
        // `A(x), B(y), x + 1 = y + 2` shares no variable, so the join would
        // be an empty-key cross product with the equality as a post-join
        // filter. Materializing each side as a column under one shared name
        // — `π⁺[x+1->k](A) ⋈[k] π⁺[y+2->k](B)` — turns it into a hash join,
        // and the equality is dropped: the shadow-column match *is* the
        // equality.
        let (lhs_idx, rhs_idx) = join_tuple_index;

        // Gate: only pairs with no shared variable (an otherwise-keyed join
        // is fine as-is; the equality stays a cheap post-join filter).
        if catalog.check_sip_pair(lhs_idx, rhs_idx) {
            return Ok(());
        }

        let fusable = catalog.equijoin_keys_for_pair(lhs_idx, rhs_idx);
        if fusable.is_empty() {
            return Ok(());
        }

        // One shadow column per equality side that needs it: when a side is
        // already a bare variable, the other side shadows *its name* (no new
        // column on the bare side); when both sides are computed, both shadow
        // a fresh name.
        let mut lhs_shadows: Vec<(String, ArithmeticPos)> = Vec::new();
        let mut rhs_shadows: Vec<(String, ArithmeticPos)> = Vec::new();
        let consumed: Vec<usize> = fusable.iter().map(|&(comp_id, ..)| comp_id).collect();
        for (comp_id, l, r) in fusable {
            match (l.plain_var(), r.plain_var()) {
                (_, Some(sig)) => {
                    lhs_shadows.push((catalog.signature_to_argument_str(&sig).clone(), l));
                }
                (Some(sig), None) => {
                    rhs_shadows.push((catalog.signature_to_argument_str(&sig).clone(), r));
                }
                (None, None) => {
                    let name = catalog.fresh_equijoin_key_name(comp_id);
                    lhs_shadows.push((name.clone(), l));
                    rhs_shadows.push((name, r));
                }
            }
        }

        for (atom_idx, shadows) in [(lhs_idx, lhs_shadows), (rhs_idx, rhs_shadows)] {
            if !shadows.is_empty() {
                self.create_shadow_column_premap(catalog, atom_idx, shadows)?;
            }
        }

        catalog.consume_comparisons(&consumed)?;
        Ok(())
    }

    /// Premap an atom to a copy carrying its `(name, expr)` shadow columns (`π⁺`).
    fn create_shadow_column_premap(
        &mut self,
        catalog: &mut Catalog,
        atom_idx: usize,
        shadows: Vec<(String, ArithmeticPos)>,
    ) -> Result<(), PlanError> {
        let current_transformation_index = self.transformation_infos.len();
        let atom_fp = catalog.positive_atom_fingerprint(atom_idx);

        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        )?;

        let input_name = catalog.positive_atom_name(atom_idx)?.to_string();
        let new_name = Self::shadow_name(&input_name, &shadows);
        let (names, exprs): (Vec<String>, Vec<ArithmeticPos>) = shadows.into_iter().unzip();

        // Output layout: the atom's columns in order, then the shadow columns.
        let in_vals: Vec<ArithmeticPos> = catalog
            .positive_atom_argument_signature(atom_idx)
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();
        let mut out_vals = in_vals.clone();
        out_vals.extend(exprs);

        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            input_name,
            new_name.clone(),
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(Vec::new(), in_vals),
            KeyValueLayout::new(Vec::new(), out_vals),
            KvPredicates::default(),
        );

        let new_fp = tx.output_info_fp();
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Shadow-column premap transformation:\n{}", tx);
        self.transformation_infos.push(tx);

        catalog.append_arguments_modify(
            AtomSignature::new(true, atom_idx),
            names,
            new_name,
            new_fp,
        )?;
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
        let (lhs_keys, lhs_vals, rhs_keys, rhs_vals) = Self::partition_shared_keys(
            catalog,
            left_atom_argument_signatures,
            right_atom_argument_signatures,
        );
        fn labelled<'a>(
            positions: &'a [ArithmeticPos],
            catalog: &'a Catalog,
        ) -> Vec<(&'a ArithmeticPos, &'a String)> {
            positions
                .iter()
                .map(|pos| {
                    (
                        pos,
                        catalog.signature_to_argument_str(pos.init().as_var_signature().unwrap()),
                    )
                })
                .collect()
        }
        trace!("Join keys: {:?}", labelled(&lhs_keys, catalog));
        trace!("Join LHS values: {:?}", labelled(&lhs_vals, catalog));
        trace!("Join RHS values: {:?}", labelled(&rhs_vals, catalog));

        // Construct output argument list: keys + LHS values + RHS values
        let new_arguments_list: Vec<AtomArgumentSignature> = lhs_keys
            .iter()
            .chain(lhs_vals.iter())
            .chain(rhs_vals.iter())
            .map(|pos| *pos.init().as_var_signature().unwrap())
            .collect();

        // Create the join transformation with proper key-value layouts
        let lhs_name = catalog.positive_atom_name(lhs_idx)?.to_string();
        let rhs_name = catalog.positive_atom_name(rhs_idx)?.to_string();
        let lhs_key_names = Self::attrs_from_positions(&lhs_keys, catalog);
        let new_name = Self::join_name(&lhs_name, &rhs_name, &lhs_key_names);
        let tx = TransformationInfo::join_to_kv(
            lhs_pos_fp,
            lhs_name,
            rhs_pos_fp,
            rhs_name,
            new_name.clone(),
            KeyValueLayout::new(lhs_keys.clone(), lhs_vals.clone()),
            KeyValueLayout::new(rhs_keys, rhs_vals.clone()),
            KeyValueLayout::new(
                lhs_keys,
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

    /// The single `JoinToKV` a planned 2-atom join must contain.
    fn find_join(planner: &RulePlanner) -> &TransformationInfo {
        planner
            .transformation_infos()
            .iter()
            .find(|t| matches!(t, TransformationInfo::JoinToKV { .. }))
            .expect("JoinToKV transformation missing")
    }

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

        let join = find_join(&planner);

        let sig_of = |pos: &ArithmeticPos| *pos.init().as_var_signature().expect("var signature");

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

    /// Shared body for the equi-join fusion tests: plan the single rule of
    /// `program`, then assert the join is keyed (`key_len` keys per side),
    /// `n_computed_cols` computed shadow columns were materialized, and the
    /// equality was consumed (catalog fully planned, no residual filter).
    fn assert_equijoin_fused(program: &str, key_len: usize, n_computed_cols: usize) {
        let (mut planner, mut catalog) = test_setup(program);
        planner.prepare(&mut catalog).expect("prepare");
        planner.core(&mut catalog, (0, 1)).expect("core");

        let join = find_join(&planner);
        let (left, right) = join.input_kv_layout();
        let right = right.expect("JoinToKV has a right input layout");
        assert_eq!(left.key().len(), key_len, "LHS keyed, not cross product");
        assert_eq!(right.key().len(), key_len, "RHS keyed, not cross product");
        // Join keys are plain columns of the (premapped) atoms; the computed
        // expressions live in the premaps, not the join.
        assert!(left.key().iter().all(|p| p.plain_var().is_some()));
        assert!(right.key().iter().all(|p| p.plain_var().is_some()));

        let computed_cols: usize = planner
            .transformation_infos()
            .iter()
            .filter(|t| matches!(t, TransformationInfo::KVToKV { .. }))
            .map(|t| {
                t.output_kv_layout()
                    .value()
                    .iter()
                    .filter(|p| p.plain_var().is_none())
                    .count()
            })
            .sum();
        assert_eq!(
            computed_cols, n_computed_cols,
            "materialized computed columns"
        );

        assert!(
            catalog.is_planned(),
            "equality must be consumed by the fusion — planned, no residual"
        );
    }

    /// `Out(x, y) :- A(x), B(y), x + 1 = y + 2.` — no shared variable, but a
    /// spanning arithmetic equality. Both sides materialize a computed column
    /// under one fresh shared name and the join keys on it.
    #[test]
    fn core_fuses_spanning_arithmetic_equality() {
        assert_equijoin_fused(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y), x + 1 = y + 2.\n",
            1,
            2,
        );
    }

    /// `x = y` with distinct names: one premap materializes `x` under the
    /// name `y` (a plain column copy — not computed), making it a shared var.
    #[test]
    fn core_fuses_plain_var_spanning_equality() {
        assert_equijoin_fused(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y), x = y.\n",
            1,
            0,
        );
    }

    /// Tuple destructure `c = (v, w)` desugars to the spanning projection
    /// equality `c.0 = v` (the DOOP context shape). The lhs materializes the
    /// projection; the rhs keys on `v` directly.
    #[test]
    fn core_fuses_tuple_projection_equality() {
        assert_equijoin_fused(
            "\
            .type P = (a: int32, b: int32)\n\
            .decl Base(a: int32, b: int32)\n\
            .decl Mk(c: P)\n\
            .decl Val(v: int32, t: int32)\n\
            .decl Out(v: int32, t: int32)\n\
            .input Base(IO=\"file\", filename=\"Base.csv\", delimiter=\",\")\n\
            .input Val(IO=\"file\", filename=\"Val.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(v, t) :- Mk(c), Val(v, t), c = (v, w).\n\
            Mk(c) :- Base(a, b), c = (a, b).\n",
            1,
            1,
        );
    }

    /// Two spanning equalities between one pair → composite two-column key;
    /// also exercises descending-order comparison consumption.
    #[test]
    fn core_fuses_composite_equijoin_key() {
        assert_equijoin_fused(
            "\
            .decl A(x: int32, w: int32)\n\
            .decl B(y: int32, z: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x, w), B(y, z), x + 1 = y + 2, w = z + 3.\n",
            2,
            3,
        );
    }

    /// No connecting condition at all: the empty-key cross product must be
    /// preserved untouched — fusion only fires on a consumable equality.
    #[test]
    fn core_preserves_genuine_cross_product() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner.core(&mut catalog, (0, 1)).expect("core");

        let join = find_join(&planner);
        let (left, right) = join.input_kv_layout();
        assert!(left.key().is_empty(), "no fabricated join key");
        assert!(right.expect("right layout").key().is_empty());
        assert!(catalog.is_planned());
    }
}
