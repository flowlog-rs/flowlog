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
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos, FactorPos};
use parser::ConstType;

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
    pub fn fuse(&mut self, original_atom_fp: &HashSet<u64>) {
        trace!(
            "Transformation infos before fusion\n {:?}",
            self.transformation_infos,
        );
        self.fuse_map(original_atom_fp);
        self.fuse_kv_layout(original_atom_fp);
    }
}

impl RulePlanner {
    /// Fuse map transformation infos.
    ///
    /// Map transformations that directly consume the output of other transformations
    /// (and are not neg joins) can be fused into their producers.
    fn fuse_map(&mut self, original_atom_fp: &HashSet<u64>) {
        let mut fused_map_indices = Vec::new();

        // Iterate in reverse order so consumers are processed before their producers.
        for index in (0..self.transformation_infos.len()).rev() {
            let Some(TransformationInfo::KVToKV {
                input_info_fp,
                output_info_fp,
                output_kv_layout,
                compare_exprs_pos,
                const_eq_constraints,
                var_eq_constraints,
                ..
            }) = self.transformation_infos.get(index)
            else {
                continue;
            };

            if original_atom_fp.contains(input_info_fp) {
                trace!(
                    "[fuse_map] skip at idx {}: input is original atom {:#018x}",
                    index,
                    *input_info_fp
                );
                continue;
            }

            let input_fp = *input_info_fp;
            let output_fp = *output_info_fp;
            let out_kv_layout = output_kv_layout.clone();
            let cmp_exprs = compare_exprs_pos.clone();
            let const_eq_constraints = const_eq_constraints.clone();
            let var_eq_constraints = var_eq_constraints.clone();

            let input_producer_indices = self.producer_indices(input_fp);
            let mut input_producer_output_fp = 0u64;
            for &input_producer_index in &input_producer_indices {
                // Short-lived borrow to check if producer is a neg join
                let producer_tx = &self.transformation_infos[input_producer_index];
                if producer_tx.is_neg_join() && !cmp_exprs.is_empty() {
                    // We always apply possible comparisons before neg joins, so it is impossible
                    // to fuse a map with a neg join producer if there are any comparisons.
                    panic!(
                        "Planner error: [fuse_map] impossible fusion of map with neg join producer"
                    );
                }

                trace!(
                    "[fuse_map] fuse at idx {}: input {:#018x} -> output {:#018x}; producer idx {}",
                    index,
                    input_fp,
                    output_fp,
                    input_producer_index
                );

                // Extract output key/value argument ids from ArithmeticPos expressions
                let (key_argument_ids, value_argument_ids) =
                    out_kv_layout.extract_argument_ids_from_layout();
                trace!(
                    "[fuse_map]   -> key ids: {:?}, value ids: {:?}",
                    key_argument_ids,
                    value_argument_ids
                );

                // Apply fused layout + comparisons to producer, and get new output fp
                input_producer_output_fp = self.apply_fused_layout_filters_cmps(
                    input_producer_index,
                    &key_argument_ids,
                    &value_argument_ids,
                    &cmp_exprs,
                    &const_eq_constraints,
                    &var_eq_constraints,
                );
            }

            let output_consumer_indices = self.consumer_indices(output_fp);

            // Update all consumers to point to the producer's new output
            for &output_consumer_index in &output_consumer_indices {
                let consumer_tx = &mut self.transformation_infos[output_consumer_index];
                consumer_tx.update_input_fake_info_fp(input_producer_output_fp, &output_fp);

                // Update the producer-consumer mapping
                self.insert_consumer(
                    original_atom_fp,
                    input_producer_output_fp,
                    output_consumer_index,
                );
                trace!(
                    "[fuse_map]   -> updated consumer idx {} to input {:#018x}",
                    output_consumer_index,
                    input_producer_output_fp
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
            "Transformation infos after map fusion\n {:?}",
            self.transformation_infos,
        );

        // After removing fused maps, rebuild the producer-consumer
        self.rebuild_producer_consumer(original_atom_fp);
    }

    /// Fuse correct key-value layout requirements from downstream transformation infos
    /// to upstream transformations.
    fn fuse_kv_layout(&mut self, original_atom_fp: &HashSet<u64>) {
        let tx_fps: HashSet<u64> = self
            .transformation_infos
            .iter()
            .map(|tx| tx.output_info_fp())
            .collect();

        for &tx_fp in &tx_fps {
            // Copy out the producer index and current consumers (if any), then mutate
            let Some((producer_indices, consumers)) = self
                .producer_consumer
                .get(&tx_fp)
                .map(|(p, c)| (p.clone(), c.clone()))
            else {
                // No producer found - likely an original atom; ignore
                continue;
            };

            if consumers.is_empty() {
                // No consumers - likely a final output; ignore
                continue;
            }

            let consumer_layouts = self.collect_consumer_layout_indices(&consumers, tx_fp);
            let producer_consumer_assignments =
                Self::assign_layout_to_producer(&producer_indices, &consumer_layouts);

            for (producers, consumers, key_indices, value_indices) in producer_consumer_assignments
            {
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
        self.rebuild_producer_consumer(original_atom_fp);
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
        cmp_exprs: &[ComparisonExprPos],
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
    ) -> u64 {
        // Build the new output layout by selecting positions from the current producer output
        let all_positions = self.collect_output_positions(producer_idx);
        let new_out_kv_layout = self.generate_layout_from_argument_ids(
            &all_positions,
            key_argument_ids,
            value_argument_ids,
        );

        let remapped_const_eq =
            Self::remap_const_eq_constraints(&all_positions, const_eq_constraints);
        let remapped_var_eq = Self::remap_var_eq_constraints(&all_positions, var_eq_constraints);
        let remapped_cmps = Self::remap_comparisons(&all_positions, cmp_exprs);

        // Update producer output layout and comparisons
        {
            let producer_tx = &mut self.transformation_infos[producer_idx];
            producer_tx.update_output_key_value_layout(new_out_kv_layout);
            if !const_eq_constraints.is_empty() || !var_eq_constraints.is_empty() {
                producer_tx
                    .update_const_eq_and_var_eq_constraints(remapped_const_eq, remapped_var_eq);
            }
            if !cmp_exprs.is_empty() {
                producer_tx.update_comparisons(remapped_cmps);
            }
            producer_tx.update_output_fake_sig();
        }

        // Return the new output fingerprint
        let new_fp = self.transformation_infos[producer_idx].output_info_fp();
        self.insert_producer(new_fp, producer_idx);
        new_fp
    }

    // Collect all output positions (keys + values) from an upstream transformation.
    #[inline]
    fn collect_output_positions(&self, producer_idx: usize) -> Vec<ArithmeticPos> {
        self.transformation_infos[producer_idx]
            .output_kv_layout()
            .key()
            .iter()
            .chain(
                self.transformation_infos[producer_idx]
                    .output_kv_layout()
                    .value()
                    .iter(),
            )
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
    ) -> KeyValueLayout {
        let new_key: Vec<_> = key_ids
            .iter()
            .map(|id| {
                positions.get(*id).cloned().unwrap_or_else(|| {
                    panic!(
                        "Planner error: missing key argument id {} in output layout ({} positions)",
                        id,
                        positions.len()
                    )
                })
            })
            .collect();
        let new_value: Vec<_> = value_ids
            .iter()
            .map(|id| {
                positions.get(*id).cloned().unwrap_or_else(|| {
                    panic!(
                        "Planner error: missing value argument id {} in output layout ({} positions)",
                        id,
                        positions.len()
                    )
                })
            })
            .collect();
        KeyValueLayout::new(new_key, new_value)
    }

    /// Remap comparison expressions by converting each variable signature to the
    /// corresponding ArithmeticPos from the provided positions and rebuilding.
    fn remap_comparisons(
        positions: &[ArithmeticPos],
        cmps: &[ComparisonExprPos],
    ) -> Vec<ComparisonExprPos> {
        cmps.iter()
            .map(|c| {
                let remap_factor = |factor: &FactorPos| -> FactorPos {
                    match factor {
                        FactorPos::Var(sig) => {
                            let id = sig.argument_id();
                            positions
                                .get(id)
                                .unwrap_or_else(|| {
                                    panic!("Planner error: missing argument id {} in positions", id)
                                })
                                .init()
                                .clone()
                        }
                        FactorPos::Const(c) => FactorPos::Const(c.clone()),
                    }
                };

                let remap_expr = |expr: &ArithmeticPos| -> ArithmeticPos {
                    let init = remap_factor(expr.init());
                    let rest = expr
                        .rest()
                        .iter()
                        .map(|(op, f)| (op.clone(), remap_factor(f)))
                        .collect();
                    ArithmeticPos::new(init, rest)
                };

                let left = remap_expr(c.left());
                let right = remap_expr(c.right());
                ComparisonExprPos::from_parts(left, c.operator().clone(), right)
            })
            .collect()
    }

    fn remap_const_eq_constraints(
        positions: &[ArithmeticPos],
        constraints: &[(AtomArgumentSignature, ConstType)],
    ) -> Vec<(AtomArgumentSignature, ConstType)> {
        constraints
            .iter()
            .map(|(sig, constant)| {
                let remapped = Self::remap_atom_signature(positions, sig);
                (remapped, constant.clone())
            })
            .collect()
    }

    fn remap_var_eq_constraints(
        positions: &[ArithmeticPos],
        constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
    ) -> Vec<(AtomArgumentSignature, AtomArgumentSignature)> {
        constraints
            .iter()
            .map(|(left, right)| {
                (
                    Self::remap_atom_signature(positions, left),
                    Self::remap_atom_signature(positions, right),
                )
            })
            .collect()
    }

    fn remap_atom_signature(
        positions: &[ArithmeticPos],
        sig: &AtomArgumentSignature,
    ) -> AtomArgumentSignature {
        let idx = sig.argument_id();
        let pos = positions.get(idx).unwrap_or_else(|| {
            panic!(
                "Planner error: missing argument id {} in output layout ({} positions)",
                idx,
                positions.len()
            )
        });

        let signatures = pos.signatures();
        signatures.first().copied().copied().unwrap_or_else(|| {
            panic!(
                "Planner error: no variable signature found for argument id {} during fusion",
                idx
            )
        })
    }

    /// Rebuild the producer_consumer map and key-value layouts after fusion.
    fn rebuild_producer_consumer(&mut self, original_atom_fp: &HashSet<u64>) {
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
                index,
                output_fp
            );
        }

        // Second pass: register all consumers for each input fingerprint
        for index in 0..count {
            let (left_fp, right_fp_opt) = {
                let tx = &self.transformation_infos[index];
                let (left_fp, right_fp_opt) = tx.input_info_fp();
                (left_fp, right_fp_opt)
            };

            for input_fp in [Some(left_fp), right_fp_opt].into_iter().flatten() {
                self.insert_consumer(original_atom_fp, input_fp, index);
            }
        }

        // Detailed mapping summary
        for (fp, (prod_idx, consumers)) in &self.producer_consumer {
            trace!(
                "[rebuild_producer_consumer] mapping: fp {:#018x} -> producer {:?}, consumers {:?}",
                fp,
                prod_idx,
                consumers
            );
        }

        trace!(
            "[rebuild_producer_consumer] done: {} producers  entries",
            self.producer_consumer.len(),
        );
    }

    /// Collect distinct key-value layouts required by consumers of a given input fingerprint.
    /// Sorted by minimum consumer index.
    fn collect_consumer_layout_indices(
        &self,
        consumer_indices: &[usize],
        input_fp: u64,
    ) -> Vec<ConsumerLayout> {
        // Map from (key indices, value indices) to consumer ids
        let mut layouts: BTreeMap<(Vec<usize>, Vec<usize>), Vec<usize>> = BTreeMap::new();

        for &consumer_idx in consumer_indices {
            let key_value_layout = match &self.transformation_infos[consumer_idx] {
                TransformationInfo::KVToKV {
                    input_info_fp,
                    input_kv_layout,
                    ..
                } if *input_info_fp == input_fp => {
                    Some(input_kv_layout.extract_argument_ids_from_layout())
                }
                TransformationInfo::JoinToKV {
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    ..
                } => {
                    if *left_input_info_fp == input_fp {
                        Some(left_input_kv_layout.extract_argument_ids_from_layout())
                    } else if *right_input_info_fp == input_fp {
                        Some(right_input_kv_layout.extract_argument_ids_from_layout())
                    } else {
                        None
                    }
                }
                TransformationInfo::AntiJoinToKV {
                    left_input_info_fp,
                    right_input_info_fp,
                    left_input_kv_layout,
                    right_input_kv_layout,
                    ..
                } => {
                    if *left_input_info_fp == input_fp {
                        Some(left_input_kv_layout.extract_argument_ids_from_layout())
                    } else if *right_input_info_fp == input_fp {
                        Some(right_input_kv_layout.extract_argument_ids_from_layout())
                    } else {
                        None
                    }
                }
                _ => None,
            };

            let (key_indices, value_indices) = key_value_layout.unwrap_or_else(|| {
                panic!(
                    "Planner error: consumer idx {} missing layout for producer fp {:#018x}",
                    consumer_idx, input_fp
                )
            });

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
        consumer_collection
    }

    /// Assign producer indices to consumer layout kinds.
    /// Ensures that each consumer layout kind is assigned at least one producer index
    /// that appears before its first consumer index.
    fn assign_layout_to_producer(
        producer_indices: &[usize],
        consumer_layouts: &[ConsumerLayout],
    ) -> Vec<LayoutAssignment> {
        // Check feasibility.
        if consumer_layouts.len() > producer_indices.len() {
            panic!(
                "Planner error: {} consumer layout kinds but only {} producers available",
                consumer_layouts.len(),
                producer_indices.len()
            );
        }

        let mut available: VecDeque<_> = producer_indices.iter().copied().collect();
        available.make_contiguous().sort_unstable();

        let mut assignments = Vec::with_capacity(consumer_layouts.len());

        for (first_consumer, consumers, key_ids, value_ids) in consumer_layouts {
            // We already check that there is at least one producer candidate, so just unwrap.
            let producer_idx = available.pop_front().unwrap();

            if producer_idx >= *first_consumer {
                panic!(
                    "Planner error: no producer index found before consumer idx {}",
                    first_consumer
                );
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
            if let Some((producer_ids, _, _, _)) = assignments.first_mut() {
                producer_ids.extend(available);
                producer_ids.sort_unstable();
            } else {
                panic!("Planner error: no consumer layout kinds to receive extra producers");
            }
        }

        assignments
    }
}
