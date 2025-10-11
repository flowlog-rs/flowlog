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
//! 2. Always perform possible comparisons before projection.
//!
//! Not following these rules might introduce subtle bugs.

use std::collections::HashSet;
use tracing::trace;

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, ComparisonExprPos, FactorPos};

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
            let (input_fp, output_fp, out_kv_layout, cmp_exprs) =
                match self.transformation_infos.get(index) {
                    Some(TransformationInfo::KVToKV {
                        input_info_fp,
                        output_info_fp,
                        output_kv_layout,
                        compare_exprs_pos,
                        ..
                    }) => {
                        // Skip if the input is an original atom (cannot fuse)
                        if original_atom_fp.contains(input_info_fp) {
                            trace!(
                                "[fuse_map] skip at idx {}: input is original atom {:#018x}",
                                index,
                                *input_info_fp
                            );
                            continue;
                        }
                        (
                            *input_info_fp,
                            *output_info_fp,
                            output_kv_layout.clone(),
                            compare_exprs_pos.clone(),
                        )
                    }
                    _ => continue,
                };

            let input_producer_index = self.producer_index(input_fp);

            // Short-lived borrow to check if producer is a neg join
            let producer_tx = &self.transformation_infos[input_producer_index];
            if producer_tx.is_neg_join() && !cmp_exprs.is_empty() {
                // We always apply possible comparisons before neg joins, so it is impossible
                // to fuse a map with a neg join producer if there are any comparisons.
                panic!("Planner error: [fuse_map] impossible fusion of map with neg join producer");
            }

            let output_consumer_indices = self.consumer_indices(output_fp);

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
            let input_producer_output_fp = self.apply_fused_layout_and_comparisons(
                input_producer_index,
                &key_argument_ids,
                &value_argument_ids,
                &cmp_exprs,
            );

            // Update all consumers to point to the producer's new output
            for output_consumer_index in output_consumer_indices {
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

        // After removing fused maps, rebuild the producer-consumer and kv-layout caches
        self.rebuild_producer_consumer_and_kv_layouts(original_atom_fp);
    }

    /// Fuse correct key-value layout requirements from downstream transformation infos
    /// to upstream transformations.
    fn fuse_kv_layout(&mut self, original_atom_fp: &HashSet<u64>) {
        for (tx_fp, key_split) in self.kv_layouts.iter() {
            // Copy out the producer index and current consumers (if any), then mutate
            let Some((producer_idx, consumers)) = self
                .producer_consumer
                .get(tx_fp)
                .map(|(p, c)| (*p, c.clone()))
            else {
                // No producer found - likely an original atom; ignore
                continue;
            };

            // Update producer layout and fingerprint
            let new_output_fp = {
                let producer_tx = &mut self.transformation_infos[producer_idx];
                producer_tx.refactor_output_key_value_layout(*key_split);
                producer_tx.update_output_fake_sig();
                producer_tx.output_info_fp()
            };

            // Update consumers to use new fingerprint
            if let Some(consumers) = consumers {
                for consumer_idx in consumers {
                    self.transformation_infos[consumer_idx]
                        .update_input_fake_info_fp(new_output_fp, tx_fp);
                }
            }
        }

        // After updating kv-layouts, rebuild the producer-consumer and kv-layout caches
        self.rebuild_producer_consumer_and_kv_layouts(original_atom_fp);
    }
}

// -----------------------------
// Small helpers (private)
// -----------------------------
impl RulePlanner {
    /// Build a new output layout from argument ids, update the producer's layout and comparisons,
    /// then return the new output fingerprint.
    #[inline]
    fn apply_fused_layout_and_comparisons(
        &mut self,
        producer_idx: usize,
        key_argument_ids: &[usize],
        value_argument_ids: &[usize],
        cmp_exprs: &[ComparisonExprPos],
    ) -> u64 {
        // Build the new output layout by selecting positions from the current producer output
        let all_positions = self.collect_output_positions(producer_idx);
        let new_out_kv_layout = self.generate_layout_from_argument_ids(
            &all_positions,
            key_argument_ids,
            value_argument_ids,
        );

        // Update producer output layout and comparisons
        {
            let producer_tx = &mut self.transformation_infos[producer_idx];
            producer_tx.update_output_key_value_layout(new_out_kv_layout);
            if !cmp_exprs.is_empty() {
                let remapped_cmps = Self::remap_comparisons(&all_positions, cmp_exprs);
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

    /// Rebuild the producer_consumer map and key-value layouts after fusion.
    fn rebuild_producer_consumer_and_kv_layouts(&mut self, original_atom_fp: &HashSet<u64>) {
        // Clear caches
        self.producer_consumer.clear();
        self.kv_layouts.clear();

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
            let (left_fp, right_fp_opt, key_split_opt) = {
                let tx = &self.transformation_infos[index];
                let (left_fp, right_fp_opt) = tx.input_info_fp();
                let key_split_opt = if tx.is_general_join() {
                    let (left_kv_layout, ..) = tx.input_kv_layout();
                    Some(left_kv_layout.key().len())
                } else {
                    None
                };
                (left_fp, right_fp_opt, key_split_opt)
            };

            for input_fp in [Some(left_fp), right_fp_opt].into_iter().flatten() {
                self.insert_consumer(original_atom_fp, input_fp, index);
                if let Some(split) = key_split_opt {
                    self.kv_layouts.insert(input_fp, split);
                }
            }
        }

        // Detailed mapping summary
        for (fp, (prod_idx, consumers)) in &self.producer_consumer {
            trace!(
                "[rebuild_producer_consumer] mapping: fp {:#018x} -> producer {}, consumers {:?}",
                fp,
                prod_idx,
                consumers.as_ref().map(|v| &v[..])
            );
        }
        for (fp, split) in &self.kv_layouts {
            trace!(
                "[rebuild_producer_consumer] kv_layout: fp {:#018x} -> key_split {}",
                fp,
                split
            );
        }

        trace!(
            "[rebuild_producer_consumer] done: {} producers, {} kv_layout entries",
            self.producer_consumer.len(),
            self.kv_layouts.len()
        );
    }
}
