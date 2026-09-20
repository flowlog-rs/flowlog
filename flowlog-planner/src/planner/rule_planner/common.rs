//! Common utilities for rule planning.
//!
//! This module provides shared functionality for rule planning operations including:
//! - Semijoin operations (positive and anti-semijoin)
//! - Comparison predicate pushdown
//! - Projection and unused argument removal
//! - Producer-consumer relationship management

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;

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
// Semijoin & Comparison Operations
// =========================================================================
impl RulePlanner {
    /// Attempts to apply semijoin or comparison pushdown optimizations.
    ///
    /// This method tries the following optimizations in order:
    /// 1. Comparison pushdown: When a comparison predicate can be pushed to atoms
    /// 2. Positive semijoin: When a positive atom has positive supersets
    /// 3. Anti-semijoin: When a negative atom has positive supersets
    ///
    /// A comparison is pushed into every superset, since it lowers to a
    /// stateless filter. A semijoin or antijoin is pushed into exactly one
    /// superset, chosen by [`RulePlanner::next_superset_pair`]: each copy is an
    /// arrangement, and a later join between two atoms that both contain the
    /// filtered variables enforces the constraint on the unfiltered side by
    /// itself.
    ///
    /// The reason why we try comparison pushdown first is that it can avoid fuse a comparison
    /// with a neg join producer, which is undefined in my understanding.
    ///
    /// WARNING: this method should be called as a total instead of calling individual
    /// semijoin or comparison methods, because they may interfere with each other in fuse logic,
    /// as the reason showed above.
    ///
    /// Returns `true` if any optimization was applied, `false` otherwise.
    pub(super) fn apply_semijoin(&mut self, catalog: &mut Catalog) -> Result<bool, PlanError> {
        // (1) Comparison predicate pushdown
        // When a comparison can be evaluated against specific atoms
        if let Some((lhs_comp_idx, rhs_pos_indices)) = catalog
            .comparison_supersets()
            .iter()
            .enumerate()
            .find(|(_, v)| !v.is_empty())
            .map(|(idx, indices)| (idx, indices.clone()))
        {
            let comparison = catalog.comparison_predicate(lhs_comp_idx)?;
            let rhs_atoms = rhs_pos_indices
                .iter()
                .map(|&index| {
                    Ok((
                        catalog.positive_atom_name(index)?.to_string(),
                        catalog.positive_atom_rhs_id(index)?,
                    ))
                })
                .collect::<Result<Vec<_>, PlanError>>()?;
            trace!(
                "Comparison pushdown:\n  Comparison: {}\n  RHS atoms: {:?}",
                comparison, rhs_atoms
            );
            return self.apply_comparison_pushdown(catalog, lhs_comp_idx, &rhs_pos_indices);
        }

        // (2) Positive semijoin
        // The LHS atom folds into one superset and leaves the rule.
        if let Some((lhs_pos_idx, rhs_pos_idx)) =
            self.next_superset_pair(catalog, catalog.positive_supersets())?
        {
            self.premap_original_atom(catalog, lhs_pos_idx, true)?;
            self.premap_original_atom(catalog, rhs_pos_idx, true)?;
            trace!(
                "Positive semijoin:\n  LHS atom: ({}, {})\n  RHS atom: ({}, {})",
                catalog.positive_atom_name(lhs_pos_idx)?,
                catalog.positive_atom_rhs_id(lhs_pos_idx)?,
                catalog.positive_atom_name(rhs_pos_idx)?,
                catalog.positive_atom_rhs_id(rhs_pos_idx)?,
            );
            return self.apply_positive_semijoin(catalog, lhs_pos_idx, rhs_pos_idx);
        }

        // (3) Anti-semijoin
        // The negative atom folds into one positive superset and leaves the rule.
        if let Some((lhs_neg_idx, rhs_pos_idx)) =
            self.next_superset_pair(catalog, catalog.negative_supersets())?
        {
            self.premap_original_atom(catalog, lhs_neg_idx, false)?;
            self.premap_original_atom(catalog, rhs_pos_idx, true)?;
            trace!(
                "Anti-semijoin:\n  LHS negative atom: ({}, !{})\n  RHS atom: ({}, {})",
                catalog.negative_atom_name(lhs_neg_idx)?,
                catalog.negative_atom_rhs_id(lhs_neg_idx)?,
                catalog.positive_atom_name(rhs_pos_idx)?,
                catalog.positive_atom_rhs_id(rhs_pos_idx)?,
            );
            return self.apply_anti_semijoin(catalog, lhs_neg_idx, rhs_pos_idx);
        }

        Ok(false)
    }

    /// Picks the next atom to fold into a superset, as `(atom index,
    /// superset index)`, or `None` when no atom has a superset.
    ///
    /// `supersets[i]` lists, in body order, the positive atoms whose variable
    /// sets contain those of atom `i`. The atom is the first one with any
    /// superset. Its superset ranks static before recursive, then fewest
    /// arguments, then body order. A static target keeps the semijoin or
    /// antijoin out of the fixpoint loop. The other supersets are enforced
    /// for free when they join the chosen one, so the rank only decides
    /// which filtered copy gets materialized, and the smallest one is the
    /// cheapest; arity stands in for size. Nothing here reads the join
    /// order, which the optimizer may change later.
    // TODO: rank by tuple count instead of arity once the optimizer's
    // `relation_cardinality` table is populated.
    fn next_superset_pair(
        &self,
        catalog: &Catalog,
        supersets: &[Vec<usize>],
    ) -> Result<Option<(usize, usize)>, PlanError> {
        let Some((lhs, candidates)) = supersets.iter().enumerate().find(|(_, c)| !c.is_empty())
        else {
            return Ok(None);
        };
        let rank = |index: usize| -> Result<(bool, usize), PlanError> {
            let recursive = self.derives_from_recursive(catalog.positive_atom_fingerprint(index)?);
            let arity = catalog.positive_atom_argument_signature(index)?.len();
            Ok((recursive, arity))
        };
        // Candidates come in body order and `<` is strict, so the earliest
        // one wins a tie.
        let mut rhs = candidates[0];
        let mut rhs_rank = rank(rhs)?;
        for &index in &candidates[1..] {
            let index_rank = rank(index)?;
            if index_rank < rhs_rank {
                rhs = index;
                rhs_rank = index_rank;
            }
        }
        Ok(Some((lhs, rhs)))
    }

    /// Returns `true` if the collection changes inside the stratum's
    /// fixpoint: it is a recursive body atom or is produced from one.
    fn derives_from_recursive(&self, fp: u64) -> bool {
        if self.recursive_relations.contains(&fp) {
            return true;
        }
        self.producer_consumer
            .get(&fp)
            .is_some_and(|(producers, _)| {
                producers.iter().any(|&index| {
                    let (left, right) = self.transformation_infos[index].input_info_fp();
                    self.derives_from_recursive(left)
                        || right.is_some_and(|right| self.derives_from_recursive(right))
                })
            })
    }

    /// Premaps an atom that is still in row format before it joins.
    ///
    /// Only original atoms need this. Even for an empty key, `((), (value))`
    /// is not the same as `(value)` in differential dataflow.
    fn premap_original_atom(
        &mut self,
        catalog: &mut Catalog,
        atom_idx: usize,
        is_positive: bool,
    ) -> Result<(), PlanError> {
        let fp = if is_positive {
            catalog.positive_atom_fingerprint(atom_idx)?
        } else {
            catalog.negative_atom_fingerprint(atom_idx)?
        };
        if catalog.original_atom_fingerprints().contains(&fp) {
            self.create_edb_premap_transformations(catalog, atom_idx, is_positive)?;
        }
        Ok(())
    }

    /// Applies positive semijoin optimization.
    ///
    /// Positive semijoin: Joins the LHS atom with the RHS atom and keeps only the RHS.
    fn apply_positive_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_pos_idx: usize,
        rhs_pos_idx: usize,
    ) -> Result<bool, PlanError> {
        let current_transformation_index = self.transformation_infos.len();

        // Extract LHS atom information
        let lhs_pos_args = catalog
            .positive_atom_argument_signature(lhs_pos_idx)?
            .to_vec();
        let lhs_pos_fp = catalog.positive_atom_fingerprint(lhs_pos_idx)?;
        // Build join keys from LHS arguments - these become the join condition
        let lhs_keys: Vec<ArithmeticPos> = lhs_pos_args
            .iter()
            .map(|&s| ArithmeticPos::from_var_signature(s))
            .collect();
        let lhs_key_names: Vec<String> = lhs_pos_args
            .iter()
            .map(|sig| {
                catalog
                    .signature_to_argument_str(sig)
                    .map(str::to_string)
                    .map_err(PlanError::from)
            })
            .collect::<Result<_, _>>()?;
        trace!("Semijoin keys: {:?}", lhs_key_names);

        // Register both atoms as consumers of this transformation
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            lhs_pos_fp,
            current_transformation_index,
        )?;
        let rhs_args = catalog
            .positive_atom_argument_signature(rhs_pos_idx)?
            .to_vec();
        let rhs_fp = catalog.positive_atom_fingerprint(rhs_pos_idx)?;
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            rhs_fp,
            current_transformation_index,
        )?;

        // Build RHS atom argument signatures; keys must mirror the LHS key ordering
        let (new_rhs_args, rhs_keys, rhs_vals) =
            Self::reorder_rhs_arguments(&rhs_args, &lhs_key_names, catalog, "positive semijoin")?;
        trace!(
            "Semijoin RHS values: {:?}",
            Self::attrs_from_positions(&rhs_vals, catalog)?
        );

        // Create the join transformation
        let lhs_name = catalog.positive_atom_name(lhs_pos_idx)?.to_string();
        let rhs_name = catalog.positive_atom_name(rhs_pos_idx)?.to_string();
        let new_name = Self::semijoin_name(&lhs_name, &rhs_name, &lhs_key_names);
        let tx = TransformationInfo::join_to_kv(
            lhs_pos_fp,
            lhs_name,
            rhs_fp,
            rhs_name,
            new_name.clone(),
            KeyValueLayout::new(lhs_keys.clone(), Vec::new()), // LHS: keys only
            KeyValueLayout::new(rhs_keys, rhs_vals.clone()),   // RHS: keys aligned + values
            KeyValueLayout::new(lhs_keys, rhs_vals),
            JoinPredicates::default(), // no additional comparisons and fn call predicates
        );
        let new_fp = tx.output_info_fp();
        self.insert_producer(new_fp, current_transformation_index);
        self.push_transformation(tx, catalog, "Positive semijoin")?;
        self.folded_filters.push(lhs_pos_fp);

        // Update catalog with the new joined atom
        catalog.join_modify(
            AtomSignature::new(true, lhs_pos_idx),
            AtomSignature::new(true, rhs_pos_idx),
            &new_rhs_args,
            &new_name,
            new_fp,
        )?;
        Ok(true)
    }

    /// Applies anti-semijoin optimization.
    ///
    /// Anti-semijoin: Keeps RHS rows whose join keys do NOT exist in the LHS atom.
    fn apply_anti_semijoin(
        &mut self,
        catalog: &mut Catalog,
        lhs_neg_idx: usize,
        rhs_pos_idx: usize,
    ) -> Result<bool, PlanError> {
        let current_transformation_index = self.transformation_infos.len();

        // Extract LHS negative atom information
        let lhs_neg_args = catalog
            .negative_atom_argument_signature(lhs_neg_idx)?
            .to_vec();
        let lhs_neg_fp = catalog.negative_atom_fingerprint(lhs_neg_idx)?;
        // Build join keys from LHS arguments - these become the join condition
        let lhs_keys: Vec<ArithmeticPos> = lhs_neg_args
            .iter()
            .map(|&s| ArithmeticPos::from_var_signature(s))
            .collect();
        let lhs_key_names: Vec<String> = lhs_neg_args
            .iter()
            .map(|sig| {
                catalog
                    .signature_to_argument_str(sig)
                    .map(str::to_string)
                    .map_err(PlanError::from)
            })
            .collect::<Result<_, _>>()?;
        trace!("Semijoin keys: {:?}", lhs_key_names);

        // Register both atoms as consumers of this transformation
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            lhs_neg_fp,
            current_transformation_index,
        )?;
        let rhs_args = catalog
            .positive_atom_argument_signature(rhs_pos_idx)?
            .to_vec();
        let rhs_fp = catalog.positive_atom_fingerprint(rhs_pos_idx)?;
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            rhs_fp,
            current_transformation_index,
        )?;

        let (new_rhs_args, rhs_keys, rhs_vals) =
            Self::reorder_rhs_arguments(&rhs_args, &lhs_key_names, catalog, "anti-semijoin")?;
        trace!(
            "Semijoin RHS values: {:?}",
            Self::attrs_from_positions(&rhs_vals, catalog)?
        );

        // Create the anti-join transformation
        let lhs_name = catalog.negative_atom_name(lhs_neg_idx)?.to_string();
        let rhs_name = catalog.positive_atom_name(rhs_pos_idx)?.to_string();
        let new_name = Self::antijoin_name(&lhs_name, &rhs_name, &lhs_key_names);
        let tx = TransformationInfo::anti_join_to_kv(
            lhs_neg_fp,
            lhs_name,
            rhs_fp,
            rhs_name,
            new_name.clone(),
            KeyValueLayout::new(lhs_keys, Vec::new()), // LHS: keys only
            KeyValueLayout::new(rhs_keys.clone(), rhs_vals.clone()), // RHS: values only
            KeyValueLayout::new(rhs_keys, rhs_vals),
        );
        let new_fp = tx.output_info_fp();
        self.insert_producer(new_fp, current_transformation_index);
        self.push_transformation(tx, catalog, "Anti semijoin")?;
        self.folded_filters.push(lhs_neg_fp);

        // Update catalog with the new anti-joined atom
        catalog.join_modify(
            AtomSignature::new(false, lhs_neg_idx),
            AtomSignature::new(true, rhs_pos_idx),
            &new_rhs_args,
            &new_name,
            new_fp,
        )?;
        Ok(true)
    }

    /// Pushes down a comparison predicate onto RHS atoms that fully cover its variables.
    ///
    /// Comparison pushdown moves evaluation of comparison predicates closer to data sources,
    /// reducing the volume of data processed in subsequent operations. This is particularly
    /// effective for selective predicates that can eliminate many tuples early.
    fn apply_comparison_pushdown(
        &mut self,
        catalog: &mut Catalog,
        lhs_comp_idx: usize,
        rhs_pos_indices: &[usize],
    ) -> Result<bool, PlanError> {
        // Initialize collections for new atoms created by the comparison pushdown
        let mut new_names = Vec::new();
        let mut new_fps = Vec::new();
        let mut right_sigs = Vec::new();

        // Process each RHS atom for comparison pushdown
        for &rhs_idx in rhs_pos_indices {
            let current_transformation_index = self.transformation_infos.len();

            // Extract RHS atom information
            let rhs_args = catalog.positive_atom_argument_signature(rhs_idx)?.to_vec();
            let rhs_fp = catalog.positive_atom_fingerprint(rhs_idx)?;

            // Register RHS atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                rhs_fp,
                current_transformation_index,
            )?;

            // Store signature for catalog update
            right_sigs.push(AtomSignature::new(true, rhs_idx));

            let in_vals = rhs_args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect::<Vec<_>>();

            let input_name = catalog.positive_atom_name(rhs_idx)?.to_string();
            let cond = catalog.comparison_predicate(lhs_comp_idx)?.to_string();
            let new_name = Self::filter_name(&input_name, &cond);
            let tx = TransformationInfo::kv_to_kv(
                rhs_fp,
                input_name,
                new_name.clone(),
                catalog.original_atom_fingerprints().contains(&rhs_fp),
                KeyValueLayout::new(vec![], in_vals.clone()),
                KeyValueLayout::new(vec![], in_vals),
                KvPredicates {
                    compare_exprs: vec![
                        catalog.resolve_comparison_predicates(rhs_idx, lhs_comp_idx)?,
                    ],
                    ..Default::default() // no const-eq, no var-eq and fn call predicates
                },
            );

            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            new_names.push(new_name);
            new_fps.push(new_fp);

            self.push_transformation(tx, catalog, "Comparison")?;
        }

        catalog.comparison_modify(lhs_comp_idx, right_sigs, new_names, new_fps)?;
        Ok(true)
    }
}

// =========================================================================
// Projection & Unused Argument Removal
// =========================================================================
impl RulePlanner {
    /// Removes arguments across atoms that are provably unused for outputs.
    pub(super) fn remove_unused_arguments(
        &mut self,
        catalog: &mut Catalog,
    ) -> Result<bool, PlanError> {
        // Get groups of unused arguments per atom from catalog analysis
        let groups = catalog.unused_arguments_per_atom();
        if groups.is_empty() {
            return Ok(false);
        }

        let mut applied = false;

        // Process each atom that has unused arguments
        for (atom_signature, to_delete) in groups.clone() {
            let current_transformation_index = self.transformation_infos.len();

            let rhs_index = catalog.rhs_index_from_signature(atom_signature)?;
            let predicate = catalog.rule().rhs().get(rhs_index).ok_or_else(|| {
                PlanError::internal(format!(
                    "unused-argument atom body index {rhs_index} is out of bounds for length {}",
                    catalog.rule().rhs().len()
                ))
            })?;
            let deleted = to_delete
                .iter()
                .map(|sig| Ok((catalog.signature_to_argument_str(sig)?.to_string(), *sig)))
                .collect::<Result<Vec<_>, PlanError>>()?;
            trace!(
                "Unused-arg removal:\n  Atom: {}, {}\n  To delete: {:?}",
                predicate, atom_signature, deleted
            );

            // Resolve atom information from signature
            let (args, atom_fp, _atom_id, input_name) = catalog.resolve_atom(&atom_signature)?;
            let input_name = input_name.to_string();

            // Register atom as consumer of this transformation
            self.insert_consumer(
                catalog.original_atom_fingerprints(),
                atom_fp,
                current_transformation_index,
            )?;

            // Create set of positions to drop for efficient filtering
            let drop_set: HashSet<ArithmeticPos> = to_delete
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect();

            // Get current values for this atom
            let in_vals = args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect::<Vec<_>>();

            // Filter out unused arguments from both keys and values
            let out_vals: Vec<ArithmeticPos> = in_vals
                .iter()
                .filter(|&sig| !drop_set.contains(sig))
                .cloned()
                .collect();

            trace!("Output KV layout: keys=[], values={:?}", out_vals);

            // Create projection transformation that removes unused arguments
            let kept_attrs = Self::attrs_from_positions(&out_vals, catalog)?;
            let new_name = Self::proj_name(&input_name, &kept_attrs);
            let tx = TransformationInfo::kv_to_kv(
                atom_fp,
                input_name,
                new_name.clone(),
                catalog.original_atom_fingerprints().contains(&atom_fp),
                KeyValueLayout::new(vec![], in_vals),
                KeyValueLayout::new(vec![], out_vals),
                KvPredicates::default(), // no const-eq, var-eq, comparisons and fn_call predicates
            );

            let new_fp = tx.output_info_fp();

            // Register this transformation as a producer
            self.insert_producer(new_fp, current_transformation_index);

            self.push_transformation(tx, catalog, "Unused")?;

            // Modify the catalog to reflect the projected atom
            catalog.projection_modify(atom_signature, to_delete, new_name, new_fp)?;

            applied = true;
        }

        Ok(applied)
    }
}

// =========================================================================
// Premap for EDB relations
// =========================================================================
impl RulePlanner {
    /// Creates premap transformations for EDB relations that are not in row format.
    /// We do not care about real key-value layout output here, as these premap is just
    /// an indicator we need to map the EDB from read-in row format.
    /// The key-value layout adjustment is handled in later fuse phase if needed.
    pub(super) fn create_edb_premap_transformations(
        &mut self,
        catalog: &mut Catalog,
        atom_idx: usize,
        is_positive: bool,
    ) -> Result<(), PlanError> {
        // Both positive and negative atoms can be pre-mapped.
        let (edb_fp, edb_args) = if is_positive {
            (
                catalog.positive_atom_fingerprint(atom_idx)?,
                catalog.positive_atom_argument_signature(atom_idx)?,
            )
        } else {
            (
                catalog.negative_atom_fingerprint(atom_idx)?,
                catalog.negative_atom_argument_signature(atom_idx)?,
            )
        };
        let edb_atom_signature = AtomSignature::new(is_positive, atom_idx);
        let current_transformation_index = self.transformation_infos.len();

        let edb_layout = KeyValueLayout::new(
            Vec::new(),
            edb_args
                .iter()
                .map(|&sig| ArithmeticPos::from_var_signature(sig))
                .collect(),
        );

        // EDB premap is a layout-only pass-through: keep the atom's current
        // hierarchical name as both input and output name.
        let edb_name = if is_positive {
            catalog.positive_atom_name(atom_idx)?.to_string()
        } else {
            catalog.negative_atom_name(atom_idx)?.to_string()
        };
        let tx = TransformationInfo::kv_to_kv(
            edb_fp,
            edb_name.clone(),
            edb_name.clone(),
            true,
            edb_layout.clone(),
            edb_layout,
            KvPredicates::default(), // no const-eq, var-eq, comparisons and fn_call predicates
        );

        let new_name = edb_name;
        let new_fp = tx.output_info_fp();

        self.push_transformation(tx, catalog, "Premap")?;

        // Register this transformation as consumer of EDB atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            edb_fp,
            current_transformation_index,
        )?;

        // Register this transformation as a producer
        self.insert_producer(new_fp, current_transformation_index);

        // Update catalog to reflect the premap atom
        catalog.map_modify(edb_atom_signature, new_name, new_fp)?;
        Ok(())
    }
}

// =========================================================================
// Private Utilities
// =========================================================================
impl RulePlanner {
    /// Hierarchical name wrappers. These describe the logical operation
    /// applied to the input(s), so each transformation's `output_name`
    /// records the full construction path from EDBs.
    #[inline]
    pub(super) fn proj_name(input_name: &str, attrs: &[String]) -> String {
        format!("π[{}]({})", attrs.join(","), input_name)
    }

    #[inline]
    pub(super) fn filter_name(input_name: &str, cond: &str) -> String {
        format!("σ[{}]({})", cond, input_name)
    }

    /// Label for a shadow-column premap: all input columns plus one computed
    /// `expr->name` column per fused equality side.
    #[inline]
    pub(super) fn shadow_name(input_name: &str, shadows: &[(String, ArithmeticPos)]) -> String {
        let bindings: Vec<String> = shadows
            .iter()
            .map(|(name, expr)| format!("{expr}->{name}"))
            .collect();
        format!("π⁺[{}]({})", bindings.join(","), input_name)
    }

    /// Extract named-attribute names from `ArithmeticPos` entries.
    /// Non-binding signatures (placeholders, const-eq, var-eq) and
    /// non-variable arithmetic expressions are skipped — they carry no
    /// user-visible name and only add noise to the rendered operator label.
    pub(super) fn attrs_from_positions(
        positions: &[ArithmeticPos],
        catalog: &Catalog,
    ) -> Result<Vec<String>, PlanError> {
        let filters = catalog.filters();
        positions
            .iter()
            .filter_map(|pos| pos.init().as_var_signature())
            .filter(|sig| !filters.is_const_or_var_eq_or_placeholder(sig))
            .map(|sig| {
                catalog
                    .signature_to_argument_str(sig)
                    .map(str::to_string)
                    .map_err(PlanError::from)
            })
            .collect()
    }

    #[inline]
    pub(super) fn join_name(left: &str, right: &str, keys: &[String]) -> String {
        format!("({} ⋈[{}] {})", left, keys.join(","), right)
    }

    #[inline]
    pub(super) fn semijoin_name(left: &str, right: &str, keys: &[String]) -> String {
        format!("({} ⋉[{}] {})", left, keys.join(","), right)
    }

    #[inline]
    pub(super) fn antijoin_name(left: &str, right: &str, keys: &[String]) -> String {
        format!("({} ▷[{}] {})", left, keys.join(","), right)
    }

    /// Orders RHS arguments so join keys come first in the same order as the LHS keys.
    #[allow(clippy::type_complexity)]
    fn reorder_rhs_arguments(
        rhs_args: &[AtomArgumentSignature],
        lhs_key_names: &[String],
        catalog: &Catalog,
        context: &str,
    ) -> Result<
        (
            Vec<AtomArgumentSignature>,
            Vec<ArithmeticPos>,
            Vec<ArithmeticPos>,
        ),
        PlanError,
    > {
        let mut remaining: Vec<(String, AtomArgumentSignature)> = rhs_args
            .iter()
            .map(|&sig| Ok((catalog.signature_to_argument_str(&sig)?.to_string(), sig)))
            .collect::<Result<_, PlanError>>()?;
        let mut ordered = Vec::with_capacity(rhs_args.len());

        for key_name in lhs_key_names {
            let position = remaining
                .iter()
                .position(|(name, _)| name == key_name)
                .ok_or_else(|| {
                    PlanError::internal(format!(
                        "reorder_rhs_arguments: RHS missing key {key_name} for {context}"
                    ))
                })?;
            let (_, sig) = remaining.remove(position);
            ordered.push(sig);
        }

        for (_, sig) in remaining {
            ordered.push(sig);
        }

        let key_count = lhs_key_names.len();
        let rhs_keys = ordered[..key_count]
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();
        let rhs_vals = ordered[key_count..]
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect();

        Ok((ordered, rhs_keys, rhs_vals))
    }

    /// Partitions atom arguments into join keys and remaining values.
    #[allow(clippy::type_complexity)]
    pub(super) fn partition_shared_keys(
        catalog: &Catalog,
        lhs_sigs: &[AtomArgumentSignature],
        rhs_sigs: &[AtomArgumentSignature],
    ) -> Result<
        (
            Vec<ArithmeticPos>,
            Vec<ArithmeticPos>,
            Vec<ArithmeticPos>,
            Vec<ArithmeticPos>,
        ),
        PlanError,
    > {
        // Build mapping from argument names to RHS signatures for efficient lookup
        let mut rhs_name_to_sig = HashMap::new();
        for sig in rhs_sigs {
            let name = catalog.signature_to_argument_str(sig)?.to_string();
            rhs_name_to_sig.insert(name, *sig);
        }

        // Partition LHS arguments into join keys and remaining values
        let mut left_keys = Vec::new();
        let mut left_remains = Vec::new();
        let mut matched_names = Vec::new(); // Keep order for right_keys

        for sig in lhs_sigs {
            let name = catalog.signature_to_argument_str(sig)?.to_string();
            if rhs_name_to_sig.contains_key(&name) {
                // This variable appears in both atoms - it's a join key
                left_keys.push(ArithmeticPos::from_var_signature(*sig));
                matched_names.push(name.clone());
            } else {
                // This variable only appears in LHS - it's a payload value
                left_remains.push(ArithmeticPos::from_var_signature(*sig))
            }
        }

        // Build right_keys in the same order as left_keys
        let right_keys: Vec<ArithmeticPos> = matched_names
            .iter()
            .map(|name| {
                let sig = rhs_name_to_sig[name];
                ArithmeticPos::from_var_signature(sig)
            })
            .collect();

        // Collect RHS arguments that don't participate in join (RHS payload)
        let mut right_remains = Vec::new();
        for sig in rhs_sigs {
            let name = catalog.signature_to_argument_str(sig)?.to_string();
            if !matched_names.contains(&name) {
                right_remains.push(ArithmeticPos::from_var_signature(*sig));
            }
        }

        Ok((left_keys, left_remains, right_keys, right_remains))
    }
}

// =========================================================================
// Producer-Consumer Relationship Management
// =========================================================================
impl RulePlanner {
    /// Records the variable names behind `tx`'s input and output positions,
    /// logs it under `label`, and appends it to the plan.
    ///
    /// Every planning site must append through here, before the catalog
    /// rewrite that consumes the transformation: the names resolve through
    /// the catalog's current argument signatures, which that rewrite
    /// renumbers. A constant or placeholder argument has no name and is
    /// skipped; any other unresolved position is an internal error.
    pub(super) fn push_transformation(
        &mut self,
        mut tx: TransformationInfo,
        catalog: &Catalog,
        label: &str,
    ) -> Result<(), PlanError> {
        let (left, right) = tx.input_kv_layout();
        let layouts = [Some(left), right, Some(tx.output_kv_layout())];
        let filters = catalog.filters();
        let mut names = BTreeMap::new();
        for position in layouts
            .into_iter()
            .flatten()
            .flat_map(|layout| layout.key().iter().chain(layout.value()))
        {
            let factors = std::iter::once(position.init())
                .chain(position.rest().iter().map(|(_, factor)| factor));
            for signature in factors.filter_map(|factor| factor.as_var_signature()) {
                if filters.const_map().contains_key(signature)
                    || filters.placeholder_set().contains(signature)
                {
                    continue;
                }
                let name = catalog.signature_to_argument_str(signature)?;
                names.insert(*signature, name.to_string());
            }
        }
        tx.set_variables(names);
        trace!("{label} transformation:\n{tx}");
        self.transformation_infos.push(tx);
        Ok(())
    }

    /// Registers a producer transformation for data with a given fingerprint.
    ///
    /// Multiple transformations can produce the same data (i.e., multiple producers per output).
    /// We will deduplicate in the later fusion logic.
    pub(super) fn insert_producer(&mut self, producer_fp: u64, producer_idx: usize) {
        self.producer_consumer
            .entry(producer_fp)
            .and_modify(|(producers, _)| producers.push(producer_idx))
            .or_insert_with(|| (vec![producer_idx], vec![]));
    }

    /// Registers a consumer transformation for data with a given fingerprint.
    ///
    /// Multiple transformations can consume the same data (i.e., multiple consumers per producer).
    /// This function tracks these consumption relationships, but ignores original atom fingerprints
    /// since those represent input data rather than transformation outputs.
    ///
    /// # Arguments
    /// * `original_atom_fp` - Set of fingerprints for original input atoms
    /// * `producer_fp` - Fingerprint of the data being consumed
    /// * `consumer_idx` - Index of the transformation that consumes this data
    ///
    /// Returns an internal `PlanError` if a consumer registers for a non-original
    /// fingerprint that has no producer yet (planner bug: inconsistent graph).
    pub(super) fn insert_consumer(
        &mut self,
        original_atom_fp: &BTreeSet<u64>,
        producer_fp: u64,
        consumer_idx: usize,
    ) -> Result<(), PlanError> {
        // If this is an original atom fingerprint (read-in atom),
        // it's not produced by any transformation, so just return
        if original_atom_fp.contains(&producer_fp) {
            return Ok(());
        }

        match self.producer_consumer.get_mut(&producer_fp) {
            Some((_, consumers)) => {
                consumers.push(consumer_idx);
                Ok(())
            }
            None => Err(PlanError::internal(format!(
                "insert_consumer: no producer for transformation fingerprint {producer_fp:#018x}"
            ))),
        }
    }

    /// Rebuilds the producer-consumer map from the transformation list,
    /// after a phase has inserted, removed, or rewired transformations.
    pub(super) fn rebuild_producer_consumer(
        &mut self,
        original_atom_fp: &BTreeSet<u64>,
    ) -> Result<(), PlanError> {
        // Clear caches
        self.producer_consumer.clear();

        let count = self.transformation_infos.len();
        trace!(
            "[rebuild_producer_consumer] rebuilding for {} transformations",
            count
        );

        // First pass: register all producers
        for index in 0..count {
            let output_fp = self.transformation_infos[index].output_info_fp();
            self.insert_producer(output_fp, index);
            trace!(
                "[rebuild_producer_consumer] producer: idx {} -> fp {:#018x}",
                index, output_fp
            );
        }

        // Second pass: register all consumers for each input fingerprint
        for index in 0..count {
            let (left_fp, right_fp_opt) = self.transformation_infos[index].input_info_fp();
            for input_fp in [Some(left_fp), right_fp_opt].into_iter().flatten() {
                self.insert_consumer(original_atom_fp, input_fp, index)?;
            }
        }

        // Detailed mapping summary
        for (fp, (prod_idx, consumers)) in &self.producer_consumer {
            trace!(
                "[rebuild_producer_consumer] mapping: fp {:#018x} -> producer {:?}, consumers {:?}",
                fp, prod_idx, consumers
            );
        }

        trace!(
            "[rebuild_producer_consumer] done: {} producer-consumer entries",
            self.producer_consumer.len(),
        );
        Ok(())
    }

    /// Index of the transformation producing the collection `fp`, or
    /// `None` for an original atom.
    pub(super) fn producer(&self, fp: u64) -> Option<usize> {
        self.producer_consumer
            .get(&fp)
            .and_then(|(producers, _)| producers.first().copied())
    }

    /// Retrieves the indices of producer transformations for a given fingerprint.
    #[inline]
    pub(super) fn producer_indices(&self, fp: u64) -> Result<Vec<usize>, PlanError> {
        self.producer_consumer
            .get(&fp)
            .map(|(producers, _)| producers.clone())
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "producer_indices: no producer for transformation fingerprint {fp:#018x}"
                ))
            })
    }

    /// Retrieves the indices of consumer transformations for a given fingerprint.
    #[inline]
    pub(super) fn consumer_indices(&self, fp: u64) -> Result<Vec<usize>, PlanError> {
        self.producer_consumer
            .get(&fp)
            .map(|(_, consumers)| consumers.clone())
            .ok_or_else(|| {
                PlanError::internal(format!(
                    "consumer_indices: no consumers for transformation fingerprint {fp:#018x}"
                ))
            })
    }
}

/// Shared setup helper for planner phase tests.
///
/// Parses a program source and builds a fresh `(RulePlanner, Catalog)` for
/// its first rule. Runs the full `parse`, so the typechecker pins every
/// literal to a concrete type, matching `tests/catalog_errors.rs`.
#[cfg(test)]
pub(super) fn test_setup(src: &str) -> (RulePlanner, Catalog) {
    test_setup_recursive(src, &[])
}

/// [`test_setup`] for a rule in a recursive stratum: the body atoms whose
/// relation is named in `recursive` are marked as feeding the fixpoint.
#[cfg(test)]
pub(super) fn test_setup_recursive(src: &str, recursive: &[&str]) -> (RulePlanner, Catalog) {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use tempfile::NamedTempFile;

    let mut tmp = NamedTempFile::new().expect("tempfile");
    tmp.write_all(src.as_bytes()).expect("write");
    let mut sm = SourceMap::new();
    let program = flowlog_parser::parse(
        &tmp.path().to_string_lossy(),
        &[],
        &mut sm,
        &mut Config::default(),
    )
    .expect("parse failed");
    let rule = program.rules()[0].clone();
    let catalog = Catalog::from_rule(&rule).expect("catalog build failed");
    let recursive_fps: Vec<u64> = (0..catalog.positive_atom_number())
        .filter(|&index| recursive.contains(&catalog.positive_atom_name(index).unwrap()))
        .map(|index| catalog.positive_atom_fingerprint(index).unwrap())
        .collect();
    let planner = RulePlanner::new(rule, &recursive_fps);
    (planner, catalog)
}
