//! Fuse pass over a rule's transformation infos: merge map steps into
//! their producers and push key/value layout requirements upstream, so
//! the pipeline reaches materialization without redundant hops.
//!
//! Both passes assume the orderings earlier phases established:
//!
//! 1. Base filters apply before any further operations.
//! 2. Comparisons apply before any semijoins.

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::collections::VecDeque;

use flowlog_parser::Constant;
use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::ComparisonExprPos;
use crate::catalog::FactorPos;
use crate::catalog::KvPredicates;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

/// Consumer input ports grouped by the (key indices, value indices) layout
/// they demand of a shared producer. A port is (consumer index, `is_left`);
/// see [`TransformationInfo::set_input_fp`] for the `is_left` convention.
type LayoutDemands = BTreeMap<(Vec<usize>, Vec<usize>), Vec<(usize, bool)>>;
/// Ordered consumer input ports alongside their key/value index selections.
/// (minimum consumer id, consumer ports, key indices, value indices)
type ConsumerLayout = (usize, Vec<(usize, bool)>, Vec<usize>, Vec<usize>);
/// Assigned producer indices with their consumer ports and key/value index selections.
/// (assigned producer ids, consumer ports, key indices, value indices)
type LayoutAssignment = (Vec<usize>, Vec<(usize, bool)>, Vec<usize>, Vec<usize>);

// =========================================================================
// Fusion
// =========================================================================
impl RulePlanner {
    /// Runs both fusion passes, then settles the fingerprints their
    /// rewiring deferred.
    pub(crate) fn fuse(&mut self, original_atom_fp: &HashSet<u64>) -> Result<(), PlanError> {
        trace!(
            "Transformation infos before fusion:\n{}",
            self.transformation_infos_dump()
        );
        self.fuse_map(original_atom_fp)?;
        self.fuse_kv_layout(original_atom_fp)?;
        // Input rewiring inside the passes above defers fingerprint
        // refreshes (an eager refresh would invalidate the fp-keyed maps
        // mid-pass); settle them now so fingerprints are content-current
        // whenever control is outside fuse.
        self.refresh_fps();
        self.rebuild_producer_consumer(original_atom_fp)?;
        trace!(
            "Transformation infos after fusion:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }
}

impl RulePlanner {
    /// Merges every fusable map into its producer(s): the producer takes
    /// over the map's output layout, predicates, and name, the map's
    /// consumers re-point to the producer, and the map is removed.
    ///
    /// Maps reading an EDB (no producer to merge into) and SIP
    /// projections (the project/semijoin pair must stay intact) are left
    /// in place.
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

            // Fusing a SIP projection would collapse SIP's
            // project/semijoin pair into the wrong producer.
            if *is_sip_projection {
                continue;
            }

            // An EDB input has no producer transformation to merge into.
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
                let producer_tx = &self.transformation_infos[input_producer_index];
                if producer_tx.is_neg_join() && !predicates.compare_exprs.is_empty() {
                    // Comparisons always apply before neg joins (module
                    // ordering rule 2), so a map with comparisons can
                    // never sit downstream of a neg join.
                    return Err(PlanError::internal(
                        "fuse_map: impossible fusion of map with neg join producer",
                    ));
                }

                trace!(
                    "[fuse_map] fuse at idx {}: input {:#018x} -> output {:#018x}; producer idx {}",
                    index, input_fp, output_fp, input_producer_index
                );

                trace!(
                    "[fuse_map]   -> keys: {:?}, values: {:?}",
                    out_kv_layout.key(),
                    out_kv_layout.value()
                );

                input_producer_output_fp = self.apply_fused_layout_filters_cmps(
                    input_producer_index,
                    &out_kv_layout,
                    &predicates,
                    fused_map_name.clone(),
                )?;
            }

            let output_consumer_indices = self.consumer_indices(output_fp)?;

            // Update all consumers to point to the producer's new output.
            let mut patched: HashSet<usize> = HashSet::new();
            for &output_consumer_index in &output_consumer_indices {
                if patched.insert(output_consumer_index) {
                    let consumer_tx = &mut self.transformation_infos[output_consumer_index];
                    consumer_tx.update_input_fp(input_producer_output_fp, &output_fp);
                }

                self.insert_consumer(
                    original_atom_fp,
                    input_producer_output_fp,
                    output_consumer_index,
                )?;
                trace!(
                    "[fuse_map]   -> updated consumer idx {} to input {:#018x}",
                    output_consumer_index, input_producer_output_fp
                );
                // Consumer input layouts stay as they are: each is updated
                // when its own iteration processes it as a join producer.
            }

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

        // Removals shifted indices and fusion changed fingerprints; the
        // map must be re-derived before anyone consults it.
        self.rebuild_producer_consumer(original_atom_fp)?;
        Ok(())
    }

    /// Pushes each consumer's required key/value split upstream, so
    /// producers emit arrangements keyed the way their consumers read
    /// them.
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
            // Clone out of the map; the loop body mutates `self`.
            let Some((producer_indices, consumers)) = self.producer_consumer.get(&tx_fp).cloned()
            else {
                // No producer: an original atom, nothing to re-key.
                continue;
            };

            if consumers.is_empty() {
                // No consumers: a final output, no layout demand on it.
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
                let mut new_output_fp = 0u64;
                for producer_idx in producers {
                    new_output_fp = {
                        let producer_tx = &mut self.transformation_infos[producer_idx];
                        producer_tx.refactor_output_key_value_layout(&key_indices, &value_indices);
                        producer_tx.refresh_output_fp();
                        producer_tx.output_info_fp()
                    };
                }

                // Rewire each consumer port to the new fingerprint. Ports,
                // not fingerprint matching: after the producer refresh both
                // sides of a self-join may already hold equal fps, and only
                // the recorded side belongs to this layout assignment.
                for (consumer_idx, is_left) in consumers {
                    self.transformation_infos[consumer_idx].set_input_fp(is_left, new_output_fp);
                }
            }
        }

        // Producer fingerprints changed; the map must be re-derived
        // before anyone consults it.
        self.rebuild_producer_consumer(original_atom_fp)?;
        Ok(())
    }
}

// --- Small helpers ---
impl RulePlanner {
    /// Rebuild the fused map's output layout over the producer's positions,
    /// update the producer's layout and comparisons, then return the new
    /// output fingerprint.
    #[inline]
    fn apply_fused_layout_filters_cmps(
        &mut self,
        producer_idx: usize,
        fused_out_layout: &KeyValueLayout,
        predicates: &KvPredicates,
        fused_map_output_name: String,
    ) -> Result<u64, PlanError> {
        let all_positions = self.collect_output_positions(producer_idx);
        let new_out_kv_layout = Self::transfer_layout(&all_positions, fused_out_layout)?;

        let remapped_const_eq =
            Self::remap_const_eq_constraints(&all_positions, &predicates.const_eq)?;
        let remapped_var_eq = Self::remap_var_eq_constraints(&all_positions, &predicates.var_eq)?;
        let remapped_cmps = Self::remap_comparisons(&all_positions, &predicates.compare_exprs)?;

        // The producer now semantically emits what the fused map used to
        // emit, so its output_name inherits the map's.
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
            producer_tx.update_output_name(fused_map_output_name);
            producer_tx.refresh_output_fp();
        }

        let new_fp = self.transformation_infos[producer_idx].output_info_fp();
        self.insert_producer(new_fp, producer_idx);
        Ok(new_fp)
    }

    /// Collects a producer's output positions, keys then values.
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

    /// Rebuilds a fused map's output layout over its producer's output
    /// positions.
    #[inline]
    fn transfer_layout(
        positions: &[ArithmeticPos],
        layout: &KeyValueLayout,
    ) -> Result<KeyValueLayout, PlanError> {
        // A plain column selects the producer position wholesale (it may
        // itself be computed); a computed position instead substitutes the
        // producer's factors for its variables.
        let transfer = |pos: &ArithmeticPos| -> Result<ArithmeticPos, PlanError> {
            match pos.plain_var() {
                Some(sig) => {
                    let id = sig.argument_id();
                    positions.get(id).cloned().ok_or_else(|| {
                        PlanError::internal(format!(
                            "transfer_layout: missing argument id {id} in producer output ({} positions)",
                            positions.len()
                        ))
                    })
                }
                None => Self::remap_arithmetic(positions, pos),
            }
        };
        Ok(KeyValueLayout::new(
            layout
                .key()
                .iter()
                .map(transfer)
                .collect::<Result<Vec<_>, _>>()?,
            layout
                .value()
                .iter()
                .map(transfer)
                .collect::<Result<Vec<_>, _>>()?,
        ))
    }

    /// Remaps a fused map's comparison expressions onto the producer's
    /// output positions.
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

    /// Remaps an arithmetic expression by resolving each of its variable
    /// signatures through `positions`.
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

    /// Remaps a fused map's constant-equality constraints onto the
    /// producer's output positions.
    fn remap_const_eq_constraints(
        positions: &[ArithmeticPos],
        constraints: &[(AtomArgumentSignature, Constant)],
    ) -> Result<Vec<(AtomArgumentSignature, Constant)>, PlanError> {
        constraints
            .iter()
            .map(|(sig, constant)| {
                let remapped = Self::remap_atom_signature(positions, sig)?;
                Ok((remapped, constant.clone()))
            })
            .collect()
    }

    /// Remaps a fused map's variable-equality constraints onto the
    /// producer's output positions.
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

    /// Resolves an atom-argument signature to the first signature of its
    /// position in the producer's output.
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

    /// Re-derives `producer_consumer` from the current infos: producers
    /// from output fingerprints, consumers from input fingerprints, one
    /// consumer entry per input port.
    fn rebuild_producer_consumer(
        &mut self,
        original_atom_fp: &HashSet<u64>,
    ) -> Result<(), PlanError> {
        self.producer_consumer.clear();

        let count = self.transformation_infos.len();
        trace!(
            "[rebuild_producer_consumer] rebuilding for {} transformations",
            count
        );

        // Producers first: insert_consumer requires its producer entry.
        for index in 0..count {
            let output_fp = self.transformation_infos[index].output_info_fp();
            self.insert_producer(output_fp, index);
            trace!(
                "[rebuild_producer_consumer] producer: idx {} -> fp {:#018x}",
                index, output_fp
            );
        }

        for index in 0..count {
            let (left_fp, right_fp_opt) = self.transformation_infos[index].input_info_fp();
            for input_fp in [Some(left_fp), right_fp_opt].into_iter().flatten() {
                self.insert_consumer(original_atom_fp, input_fp, index)?;
            }
        }

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

    /// Collects the distinct key/value layouts the consumers of `input_fp`
    /// demand, each with the ports demanding it, ordered by minimum
    /// consumer index.
    fn collect_consumer_layout_indices(
        &mut self,
        consumer_indices: &[usize],
        input_fp: u64,
    ) -> Result<Vec<ConsumerLayout>, PlanError> {
        let mut layouts: LayoutDemands = BTreeMap::new();
        let mut real_key_value_layout = None;

        // First pass: only join and antijoin contribute real key/value layout requirements.
        // Check both sides independently: one collection can feed both sides
        // of a self-join, and each side may demand a different layout.
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
                let matched_sides = [
                    (*left_fp == input_fp).then_some((true, left_layout)),
                    (*right_fp == input_fp).then_some((false, right_layout)),
                ];
                if matched_sides.iter().all(Option::is_none) {
                    return Err(PlanError::internal(format!(
                        "collect_consumer_layout_indices: consumer idx {consumer_idx} does not match input fp {input_fp:#018x} in join/antijoin layout"
                    )));
                }

                for (side, matched_layout) in matched_sides.into_iter().flatten() {
                    if real_key_value_layout.is_none() {
                        real_key_value_layout = Some(matched_layout.clone());
                    }
                    let (key_indices, value_indices) =
                        matched_layout.extract_argument_ids_from_layout();
                    layouts
                        .entry((key_indices, value_indices))
                        .or_default()
                        .push((consumer_idx, side));
                }
            }
        }

        // Second pass: KV-to-KV consumers define no key/value split of
        // their own; they adopt the first join/antijoin's.
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
                .push((consumer_idx, true));
        }

        let mut consumer_collection: Vec<ConsumerLayout> = layouts
            .into_iter()
            .map(|((key_ids, value_ids), mut consumers)| {
                consumers.sort_unstable();
                (consumers[0].0, consumers, key_ids, value_ids)
            })
            .collect();
        consumer_collection.sort_by_key(|(first_consumer, ..)| *first_consumer);
        Ok(consumer_collection)
    }

    /// Assigns producer indices to consumer layout kinds, giving each kind
    /// at least one producer that appears before its first consumer.
    fn assign_layout_to_producer(
        tx_fp: u64,
        producer_indices: &[usize],
        consumer_layouts: &[ConsumerLayout],
    ) -> Result<Vec<LayoutAssignment>, PlanError> {
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

        // Leftover producers all land on the first layout kind; any
        // assignment would do, this is the simplest.
        if !available.is_empty() {
            match assignments.first_mut() {
                Some((producer_ids, ..)) => {
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
    use crate::planner::TransformationInfo;

    /// A filter whose input is an EDB atom must survive fuse: there is no
    /// upstream producer to merge it into. A broken guard would error out
    /// or silently drop the filter.
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

    /// After equi-join fusion, fuse must key each side's arrangement on the
    /// *materialized* computed expression (`x + 1` / `y + 2`); collapsing it
    /// to the base column would join on the wrong value.
    #[test]
    fn fuse_keys_arrangement_on_materialized_equijoin_column() {
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
        planner.core(&mut catalog, (0, 1)).expect("core");
        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");

        let computed_keys = planner
            .transformation_infos()
            .iter()
            .filter(|t| matches!(t, TransformationInfo::KVToKV { .. }))
            .filter(|t| {
                t.output_kv_layout()
                    .key()
                    .iter()
                    .any(|p| p.plain_var().is_none())
            })
            .count();
        assert_eq!(
            computed_keys, 2,
            "each side keys on its computed expression"
        );
    }

    /// `fuse_map` explicitly skips SIP projections. If that guard were
    /// removed, SIP's project/semijoin pair would collapse into the wrong
    /// producer and SIP semantics would silently break.
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
