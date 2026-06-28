//! Fuse logic for rule planner.
//!
//! Warning: you should not modify this file unless you are very sure about what you are doing.
//!
//! This module implements the logic to fuse map transformations into their producers
//! to reduce the number of transformation steps and improve performance.
//! It also ensures that key-value layout requirements are correctly propagated
//! through the transformation pipeline.
//!
//! Recommended rules when reasoning about fusion order:
//! 1. Always apply base filters before any further operations.
//! 2. Always perform possible comparisons before any semijoins.
//!
//! Not following these rules might introduce subtle bugs.

use std::collections::{BTreeMap, HashSet, VecDeque};
use tracing::trace;

use super::RulePlanner;
use crate::catalog::{
    ArithmeticPos, AtomArgumentSignature, AtomSignature, ComparisonExprPos, FactorPos,
    FnCallPredicatePos, KvPredicates,
};
use flowlog_parser::ConstType;
use crate::planner::{KeyValueLayout, PlanError, TransformationInfo};

/// Ordered consumer indices alongside their key/value index selections.
/// (minimum consumer id, consumer ids, key indices, value indices)
type ConsumerLayout = (usize, Vec<usize>, Vec<usize>, Vec<usize>);
/// Assigned producer indices with their consumers and key/value index selections.
/// (assigned producer ids, consumer ids, key indices, value indices)
type LayoutAssignment = (Vec<usize>, Vec<usize>, Vec<usize>, Vec<usize>);

// =========================================================================
// Fusion
// =========================================================================
impl RulePlanner {
    /// Run fusion passes (map fusion and KV-layout fusion) on
    /// the planned transformation infos.
    pub(crate) fn fuse(&mut self, original_atom_fp: &HashSet<u64>) -> Result<(), PlanError> {
        trace!(
            "Transformation infos before fusion:\n{}",
            self.transformation_infos_dump()
        );
        self.fuse_map(original_atom_fp)?;
        self.fuse_kv_layout(original_atom_fp)?;
        trace!(
            "Transformation infos after fusion:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }
}

impl RulePlanner {
    /// Fuse map transformation infos.
    ///
    /// Map transformations that directly consume the output of other transformations
    /// (and are not neg joins) can be fused into their producers.
    fn fuse_map(&mut self, original_atom_fp: &HashSet<u64>) -> Result<(), PlanError> {
        let mut fused_map_indices = Vec::new();

        // Iterate in reverse order so consumers are processed before their producers.
        for index in (0..self.transformation_infos.len()).rev() {
            let Some(TransformationInfo::KVToKV {
                input_info_fp,
                output_info_fp,
                output_name,
                output_kv_layout,
                predicates,
                is_sip_projection,
                ..
            }) = self.transformation_infos.get(index)
            else {
                continue;
            };

            // Do not fuse SIP projection transformations
            if *is_sip_projection {
                continue;
            }

            // Do not fuse if the input is from an EDB
            if original_atom_fp.contains(input_info_fp) {
                trace!(
                    "[fuse_map] skip at idx {}: input is original atom {:#018x}",
                    index, *input_info_fp
                );
                continue;
            }

            let input_fp = *input_info_fp;
            let output_fp = *output_info_fp;
            let fused_map_name = output_name.clone();
            let out_kv_layout = output_kv_layout.clone();
            let predicates = predicates.clone();

            let input_producer_indices = self.producer_indices(input_fp)?;
            let mut input_producer_output_fp = 0u64;
            for &input_producer_index in &input_producer_indices {
                // Short-lived borrow to check if producer is a neg join
                let producer_tx = &self.transformation_infos[input_producer_index];
                if producer_tx.is_neg_join()
                    && (!predicates.compare_exprs.is_empty()
                        || !predicates.fn_call_preds.is_empty())
                {
                    // We always apply possible comparisons/fn_call before neg joins, so it is impossible
                    // to fuse a map with a neg join producer if there are any comparisons or fn_call predicates.
                    return Err(PlanError::internal(
                        "fuse_map: impossible fusion of map with neg join producer",
                    ));
                }

                trace!(
                    "[fuse_map] fuse at idx {}: input {:#018x} -> output {:#018x}; producer idx {}",
                    index, input_fp, output_fp, input_producer_index
                );

                // Extract output key/value argument ids from ArithmeticPos expressions
                let (key_argument_ids, value_argument_ids) =
                    out_kv_layout.extract_argument_ids_from_layout();
                trace!(
                    "[fuse_map]   -> key ids: {:?}, value ids: {:?}",
                    key_argument_ids, value_argument_ids
                );

                // Apply fused layout + comparisons + fn_call predicates to producer, and get new output fp
                input_producer_output_fp = self.apply_fused_layout_filters_cmps(
                    input_producer_index,
                    &key_argument_ids,
                    &value_argument_ids,
                    &predicates,
                    fused_map_name.clone(),
                )?;
            }

            let output_consumer_indices = self.consumer_indices(output_fp)?;

            // Update all consumers to point to the producer's new output
            for &output_consumer_index in &output_consumer_indices {
                let consumer_tx = &mut self.transformation_infos[output_consumer_index];
                consumer_tx.update_input_fake_info_fp(input_producer_output_fp, &output_fp);

                // Update the producer-consumer mapping
                self.insert_consumer(
                    original_atom_fp,
                    input_producer_output_fp,
                    output_consumer_index,
                )?;
                trace!(
                    "[fuse_map]   -> updated consumer idx {} to input {:#018x}",
                    output_consumer_index, input_producer_output_fp
                );
                // Note: No need to update the input key-value layout of consumers here.
                // They will be updated when processed as join producers in later iterations.
            }

            // Update the producer_consumer map
            fused_map_indices.push(index);
        }

        // Remove fused maps in reverse order to avoid shifting indices
        for index in fused_map_indices {
            self.transformation_infos.remove(index);
        }

        trace!(
            "Transformation infos after map fusion:\n{}",
            self.transformation_infos_dump()
        );

        // After removing fused maps, rebuild the producer-consumer
        self.rebuild_producer_consumer(original_atom_fp)?;
        Ok(())
    }

    /// Fuse correct key-value layout requirements from downstream transformation infos
    /// to upstream transformations.
    fn fuse_kv_layout(&mut self, original_atom_fp: &HashSet<u64>) -> Result<(), PlanError> {
        // Collect output fingerprints in transformation order, keeping only
        // the first occurrence of each. Order matters for sharing
        // optimization: a different processing order may yield different
        // fingerprints for the same plan operations.
        let mut seen: HashSet<u64> = HashSet::new();
        let tx_fps: Vec<u64> = self
            .transformation_infos
            .iter()
            .map(|tx| tx.output_info_fp())
            .filter(|fp| seen.insert(*fp))
            .collect();

        for tx_fp in tx_fps {
            // Copy out the producer index and current consumers (if any), then mutate
            let Some((producer_indices, consumers)) = self.producer_consumer.get(&tx_fp).cloned()
            else {
                // No producer found - likely an original atom; ignore
                continue;
            };

            if consumers.is_empty() {
                // No consumers - likely a final output; ignore
                continue;
            }

            let consumer_layouts = self.collect_consumer_layout_indices(&consumers, tx_fp)?;
            let producer_consumer_assignments =
                Self::assign_layout_to_producer(tx_fp, &producer_indices, &consumer_layouts)?;

            for (producers, consumers, key_indices, value_indices) in producer_consumer_assignments
            {
                trace!(
                    "[fuse_kv_layout] fuse at producer fp {:#018x} -> consumers {:?}; key ids: {:?}, value ids: {:?}",
                    tx_fp, consumers, key_indices, value_indices
                );
                // Update producer layout and fingerprint
                let mut new_output_fp = 0u64;
                for producer_idx in producers {
                    new_output_fp = {
                        let producer_tx = &mut self.transformation_infos[producer_idx];
                        producer_tx.refactor_output_key_value_layout(&key_indices, &value_indices);
                        producer_tx.update_output_fake_sig();
                        producer_tx.output_info_fp()
                    };
                }

                // Update consumers to use new fingerprint
                for consumer_idx in consumers {
                    self.transformation_infos[consumer_idx]
                        .update_input_fake_info_fp(new_output_fp, &tx_fp);
                }
            }
        }

        // After updating kv-layouts, rebuild the producer-consumer
        self.rebuild_producer_consumer(original_atom_fp)?;
        Ok(())
    }
}

// -----------------------------
// Small helpers (private)
// -----------------------------
impl RulePlanner {
    /// Build a new output layout from argument ids, update the producer's layout and comparisons,
    /// then return the new output fingerprint.
    #[inline]
    fn apply_fused_layout_filters_cmps(
        &mut self,
        producer_idx: usize,
        key_argument_ids: &[usize],
        value_argument_ids: &[usize],
        predicates: &KvPredicates,
        fused_map_output_name: String,
    ) -> Result<u64, PlanError> {
        // Build the new output layout by selecting positions from the current producer output
        let all_positions = self.collect_output_positions(producer_idx);
        let new_out_kv_layout = self.generate_layout_from_argument_ids(
            &all_positions,
            key_argument_ids,
            value_argument_ids,
        )?;

        let remapped_const_eq =
            Self::remap_const_eq_constraints(&all_positions, &predicates.const_eq)?;
        let remapped_var_eq = Self::remap_var_eq_constraints(&all_positions, &predicates.var_eq)?;
        let remapped_cmps = Self::remap_comparisons(&all_positions, &predicates.compare_exprs)?;
        let remapped_fn_calls =
            Self::remap_fn_call_preds(&all_positions, &predicates.fn_call_preds)?;

        // Update producer output layout, predicates, name and fingerprint.
        // The producer now semantically emits what the fused map used to emit,
        // so its output_name inherits the map's.
        {
            let producer_tx = &mut self.transformation_infos[producer_idx];
            producer_tx.update_output_key_value_layout(new_out_kv_layout);
            if !predicates.const_eq.is_empty() || !predicates.var_eq.is_empty() {
                producer_tx
                    .update_const_eq_and_var_eq_constraints(remapped_const_eq, remapped_var_eq)?;
            }
            if !predicates.compare_exprs.is_empty() {
                producer_tx.update_comparisons(remapped_cmps)?;
            }
            if !predicates.fn_call_preds.is_empty() {
                producer_tx.update_fn_call_preds(remapped_fn_calls)?;
            }
            producer_tx.update_output_name(fused_map_output_name);
            producer_tx.update_output_fake_sig();
        }

        // Return the new output fingerprint
        let new_fp = self.transformation_infos[producer_idx].output_info_fp();
        self.insert_producer(new_fp, producer_idx);
        Ok(new_fp)
    }

    // Collect all output positions (keys + values) from an upstream transformation.
    #[inline]
    fn collect_output_positions(&self, producer_idx: usize) -> Vec<ArithmeticPos> {
        let layout = self.transformation_infos[producer_idx].output_kv_layout();
        layout
            .key()
            .iter()
            .chain(layout.value().iter())
            .cloned()
            .collect()
    }

    // Generate a KeyValueLayout from argument ids by selecting from the provided positions.
    #[inline]
    fn generate_layout_from_argument_ids(
        &self,
        positions: &[ArithmeticPos],
        key_ids: &[usize],
        value_ids: &[usize],
    ) -> Result<KeyValueLayout, PlanError> {
        let pick = |id: &usize, kind: &str| -> Result<ArithmeticPos, PlanError> {
            positions.get(*id).cloned().ok_or_else(|| {
                PlanError::internal(format!(
                    "generate_layout_from_argument_ids: missing {kind} argument id {id} in output layout ({} positions)",
                    positions.len()
                ))
            })
        };
        let new_key = key_ids
            .iter()
            .map(|id| pick(id, "key"))
            .collect::<Result<Vec<_>, _>>()?;
        let new_value = value_ids
            .iter()
            .map(|id| pick(id, "value"))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(KeyValueLayout::new(new_key, new_value))
    }

    /// Remap comparison expressions by converting each variable signature to the
    /// corresponding ArithmeticPos from the provided positions and rebuilding.
    fn remap_comparisons(
        positions: &[ArithmeticPos],
        cmps: &[ComparisonExprPos],
    ) -> Result<Vec<ComparisonExprPos>, PlanError> {
        cmps.iter()
            .map(|c| {
                let left = Self::remap_arithmetic(positions, c.left())?;
                let right = Self::remap_arithmetic(positions, c.right())?;
                Ok(ComparisonExprPos::from_parts(
                    left,
                    c.operator().clone(),
                    right,
                ))
            })
            .collect()
    }

    /// Remap fn_call predicate positions by converting each variable signature to the
    /// corresponding ArithmeticPos from the provided positions and rebuilding.
    fn remap_fn_call_preds(
        positions: &[ArithmeticPos],
        fn_calls: &[FnCallPredicatePos],
    ) -> Result<Vec<FnCallPredicatePos>, PlanError> {
        fn_calls
            .iter()
            .map(|fc| {
                let new_args = fc
                    .args()
                    .iter()
                    .map(|a| Self::remap_arithmetic(positions, a))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(FnCallPredicatePos::new(
                    fc.name().to_string(),
                    new_args,
                    fc.is_negated(),
                ))
            })
            .collect()
    }

    /// Remap an ArithmeticPos by resolving each variable signature through `positions`.
    fn remap_arithmetic(
        positions: &[ArithmeticPos],
        expr: &ArithmeticPos,
    ) -> Result<ArithmeticPos, PlanError> {
        for sig in expr.signatures() {
            let id = sig.argument_id();
            let pos = positions.get(id).ok_or_else(|| {
                PlanError::internal(format!(
                    "remap_arithmetic: missing argument id {id} in positions"
                ))
            })?;
            if !pos.rest().is_empty() {
                return Err(PlanError::internal(format!(
                    "remap_arithmetic: expected single-factor position for argument id {id}, got compound expression"
                )));
            }
        }
        Ok(expr.map_vars(&|sig| positions[sig.argument_id()].init().clone()))
    }

    fn remap_const_eq_constraints(
        positions: &[ArithmeticPos],
        constraints: &[(AtomArgumentSignature, ConstType)],
    ) -> Result<Vec<(AtomArgumentSignature, ConstType)>, PlanError> {
        constraints
            .iter()
            .map(|(sig, constant)| {
                let remapped = Self::remap_atom_signature(positions, sig)?;
                Ok((remapped, constant.clone()))
            })
            .collect()
    }

    fn remap_var_eq_constraints(
        positions: &[ArithmeticPos],
        constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
    ) -> Result<Vec<(AtomArgumentSignature, AtomArgumentSignature)>, PlanError> {
        constraints
            .iter()
            .map(|(left, right)| {
                Ok((
                    Self::remap_atom_signature(positions, left)?,
                    Self::remap_atom_signature(positions, right)?,
                ))
            })
            .collect()
    }

    /// Remap a key-value layout so every variable signature uses the given `atom_id`,
    /// preserving argument ids and constants.
    fn remap_atom_kv_layout(layout: &KeyValueLayout, atom_id: usize) -> KeyValueLayout {
        let remap = &|sig: &AtomArgumentSignature| {
            let atom_sig = AtomSignature::new(sig.is_positive(), atom_id);
            FactorPos::Var(AtomArgumentSignature::new(atom_sig, sig.argument_id()))
        };
        KeyValueLayout::new(
            layout.key().iter().map(|p| p.map_vars(remap)).collect(),
            layout.value().iter().map(|p| p.map_vars(remap)).collect(),
        )
    }

    fn remap_atom_signature(
        positions: &[ArithmeticPos],
        sig: &AtomArgumentSignature,
    ) -> Result<AtomArgumentSignature, PlanError> {
        let idx = sig.argument_id();
        let pos = positions.get(idx).ok_or_else(|| {
            PlanError::internal(format!(
                "remap_atom_signature: missing argument id {idx} in output layout ({} positions)",
                positions.len()
            ))
        })?;

        let signatures = pos.signatures();
        signatures.first().copied().copied().ok_or_else(|| {
            PlanError::internal(format!(
                "remap_atom_signature: no variable signature found for argument id {idx} during fusion"
            ))
        })
    }

    /// Rebuild the producer_consumer map and key-value layouts after fusion.
    fn rebuild_producer_consumer(
        &mut self,
        original_atom_fp: &HashSet<u64>,
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

    /// Collect distinct key-value layouts required by consumers of a given input fingerprint.
    /// Sorted by minimum consumer index.
    fn collect_consumer_layout_indices(
        &mut self,
        consumer_indices: &[usize],
        input_fp: u64,
    ) -> Result<Vec<ConsumerLayout>, PlanError> {
        // Map from (key indices, value indices) to consumer ids
        let mut layouts: BTreeMap<(Vec<usize>, Vec<usize>), Vec<usize>> = BTreeMap::new();
        let mut real_key_value_layout = None;

        // First pass: only join and antijoin contribute real key/value layout requirements.
        for &consumer_idx in consumer_indices {
            let join_inputs = match &self.transformation_infos[consumer_idx] {
                TransformationInfo::JoinToKV {
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    ..
                }
                | TransformationInfo::AntiJoinToKV {
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    ..
                } => Some((
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                )),
                _ => None,
            };

            if let Some((left_fp, right_fp, left_layout, right_layout)) = join_inputs {
                let matched_layout = if *left_fp == input_fp {
                    left_layout
                } else if *right_fp == input_fp {
                    right_layout
                } else {
                    return Err(PlanError::internal(format!(
                        "collect_consumer_layout_indices: consumer idx {consumer_idx} does not match input fp {input_fp:#018x} in join/antijoin layout"
                    )));
                };

                if real_key_value_layout.is_none() {
                    real_key_value_layout = Some(matched_layout.clone());
                }
                let (key_indices, value_indices) =
                    matched_layout.extract_argument_ids_from_layout();
                layouts
                    .entry((key_indices, value_indices))
                    .or_default()
                    .push(consumer_idx);
            }
        }

        // Second pass: KV-to-KV consumers inherit the join/antijoin layout requirement.
        // They don't define their own key/value split — they adopt the first join/antijoin's.
        for &consumer_idx in consumer_indices {
            // Only process KV-to-KV maps whose input matches this producer.
            if !matches!(
                &self.transformation_infos[consumer_idx],
                TransformationInfo::KVToKV { input_info_fp, .. } if *input_info_fp == input_fp
            ) {
                continue;
            }

            // The canonical layout comes from the first join/antijoin seen in pass 1.
            let layout = real_key_value_layout.clone().ok_or_else(|| {
                PlanError::internal(format!(
                    "collect_consumer_layout_indices: consumer idx {consumer_idx} missing join/antijoin layout for producer fp {input_fp:#018x}"
                ))
            })?;

            // Remap layout signatures to this consumer's atom id, then apply.
            let consumer_tx = &mut self.transformation_infos[consumer_idx];
            let atom_id = consumer_tx.input_kv_layout().0.extract_atom_id()?;
            consumer_tx.update_input_layout(Self::remap_atom_kv_layout(&layout, atom_id));

            // Group this consumer under the same (key, value) indices as the joins.
            let (key_indices, value_indices) = layouts.keys().next().cloned().ok_or_else(|| {
                PlanError::internal(format!(
                    "collect_consumer_layout_indices: consumer idx {consumer_idx} missing join/antijoin layout keys for producer fp {input_fp:#018x}"
                ))
            })?;
            layouts
                .entry((key_indices, value_indices))
                .or_default()
                .push(consumer_idx);
        }

        let mut consumer_collection: Vec<ConsumerLayout> = layouts
            .into_iter()
            .map(|((key_ids, value_ids), mut consumers)| {
                consumers.sort_unstable();
                (consumers[0], consumers, key_ids, value_ids)
            })
            .collect();
        consumer_collection.sort_by_key(|(first_consumer, _, _, _)| *first_consumer);
        Ok(consumer_collection)
    }

    /// Assign producer indices to consumer layout kinds.
    /// Ensures that each consumer layout kind is assigned at least one producer index
    /// that appears before its first consumer index.
    fn assign_layout_to_producer(
        tx_fp: u64,
        producer_indices: &[usize],
        consumer_layouts: &[ConsumerLayout],
    ) -> Result<Vec<LayoutAssignment>, PlanError> {
        // Check feasibility.
        if consumer_layouts.len() > producer_indices.len() {
            return Err(PlanError::internal(format!(
                "assign_layout_to_producer: {tx_fp:#018x} has {} consumer layout kinds but only {} producers available",
                consumer_layouts.len(),
                producer_indices.len()
            )));
        }

        let mut available: VecDeque<_> = producer_indices.iter().copied().collect();
        available.make_contiguous().sort_unstable();

        let mut assignments = Vec::with_capacity(consumer_layouts.len());

        for (first_consumer, consumers, key_ids, value_ids) in consumer_layouts {
            // Feasibility check above guarantees at least one producer candidate.
            let producer_idx = available.pop_front().ok_or_else(|| {
                PlanError::internal(
                    "assign_layout_to_producer: no available producer despite feasibility check",
                )
            })?;

            if producer_idx >= *first_consumer {
                return Err(PlanError::internal(format!(
                    "assign_layout_to_producer: no producer index found before consumer idx {first_consumer}"
                )));
            }

            assignments.push((
                vec![producer_idx],
                consumers.clone(),
                key_ids.clone(),
                value_ids.clone(),
            ));
        }

        // If there are any remaining available producers, assign them to the first consumer layout kind.
        // Randomly assign also works, for simplify code we just push to the first one.
        if !available.is_empty() {
            match assignments.first_mut() {
                Some((producer_ids, _, _, _)) => {
                    producer_ids.extend(available);
                    producer_ids.sort_unstable();
                }
                None => {
                    return Err(PlanError::internal(
                        "assign_layout_to_producer: no consumer layout kinds to receive extra producers",
                    ));
                }
            }
        }

        Ok(assignments)
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;

    /// A filter whose input is an EDB atom must survive fuse — the EDB
    /// guard at fuse.rs:84 blocks fusion into something that has no
    /// upstream producer. A broken guard would error out or silently
    /// drop the filter.
    #[test]
    fn fuse_map_skips_edb_input() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, 5).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        let before = planner.transformation_infos().len();
        assert!(
            before >= 1,
            "prepare must emit at least the const_eq filter"
        );

        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        let after = planner.transformation_infos().len();
        assert_eq!(
            before, after,
            "EDB-input filter must not be fused into its (absent) producer"
        );
    }

    /// fuse.rs:79 explicitly skips `is_sip_projection == true`. If that
    /// guard were removed, SIP's project→semijoin pair would collapse
    /// into the wrong producer and SIP semantics would silently break.
    ///
    /// Rule shape avoids positive-subset relations among atoms so that
    /// `prepare`'s `apply_positive_semijoin` doesn't consume the SIP
    /// opportunities before SIP runs.
    #[test]
    fn fuse_map_preserves_sip_projection() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .decl C(a: int32, b: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .decl Out(x: int32, w: int32, z: int32)\n\
            .output Out\n\
            Out(x, w, z) :- A(x, w), B(x, y), C(y, z).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner.apply_sip(&mut catalog).expect("sip");
        while !catalog.is_planned() {
            planner.core(&mut catalog, (0, 1)).expect("core");
        }

        let sip_before = planner
            .transformation_infos()
            .iter()
            .filter(|t| t.is_sip_projection())
            .count();
        assert!(sip_before > 0, "SIP must produce projections to test");

        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        let sip_after = planner
            .transformation_infos()
            .iter()
            .filter(|t| t.is_sip_projection())
            .count();
        assert_eq!(
            sip_before, sip_after,
            "fuse must preserve every SIP projection"
        );
    }
}
