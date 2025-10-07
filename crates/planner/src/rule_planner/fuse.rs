use catalog::ArithmeticPos;
use std::collections::HashSet;
use tracing::trace;

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};

/// Here is some rule we should always follow,
/// 1. We always do filter first, so it is impossible to fuse any filters map
/// 2. We assume comparisons are always done before project unused arguments
impl RulePlanner {
    // -----------------------------
    // Public entry
    // -----------------------------
    /// Plan a single rule, producing a list of transformation infos.
    pub fn fuse(&mut self, original_atom_fp: &HashSet<u64>) {
        trace!(
            "transformations before fusing maps\n {:?}",
            self.transformation_infos,
        );
        self.fuse_map(original_atom_fp);
        self.fuse_kv_layout(original_atom_fp);
    }

    /// Fuse map transformation info.
    pub fn fuse_map(&mut self, original_atom_fp: &HashSet<u64>) {
        let mut fused_map_indices = Vec::new();

        // Iterate in reverse order to get indices
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

            let input_producer_index = self.producer_index_or_panic(input_fp);

            if self.transformation_infos[input_producer_index].is_neg_join() {
                // Skip if the input producer is a neg join (cannot fuse)
                trace!(
                    "[fuse_map] skip at idx {}: input producer {} is neg join",
                    index,
                    input_producer_index
                );
                continue;
            }

            let output_consumer_indices = self.consumer_indices(output_fp);

            // Update the input producer
            trace!(
                "[fuse_map] fuse at idx {}: input {:#018x} -> output {:#018x}; producer idx {}",
                index,
                input_fp,
                output_fp,
                input_producer_index
            );
            // Extract output key/value argument ids from ArithmeticPos expressions
            let key_argument_ids: Vec<usize> = out_kv_layout
                .key()
                .iter()
                .flat_map(|pos| pos.signatures())
                .map(|sig| sig.argument_id())
                .collect();
            let value_argument_ids: Vec<usize> = out_kv_layout
                .value()
                .iter()
                .flat_map(|pos| pos.signatures())
                .map(|sig| sig.argument_id())
                .collect();
            trace!(
                "[fuse_map]   -> key ids: {:?}, value ids: {:?}",
                key_argument_ids,
                value_argument_ids
            );

            // Build the new output layout by selecting positions from the current producer output
            let all_positions = self.collect_output_positions_of(input_producer_index);
            let new_out_kv_layout = self.layout_from_argument_ids(
                &all_positions,
                &key_argument_ids,
                &value_argument_ids,
            );

            // Update producer output layout
            self.transformation_infos[input_producer_index]
                .update_output_key_value_layout(new_out_kv_layout);

            // Rebuild comparison expressions using the new output layout mapping
            let remapped_cmps = Self::remap_comparisons(&all_positions, &cmp_exprs);
            self.transformation_infos[input_producer_index].update_comparisons(remapped_cmps);
            self.transformation_infos[input_producer_index].update_output_fake_sig();

            let input_producer_output_fp =
                self.transformation_infos[input_producer_index].output_info_fp();

            self.insert_producer(input_producer_output_fp, input_producer_index);

            // Update all consumers to point to the producer's new output
            for output_consumer_index in output_consumer_indices {
                self.transformation_infos[output_consumer_index]
                    .update_input_fake_info_fp(input_producer_output_fp, &output_fp);
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
                // Note: we do not need to update the input key-value layout of consumers here,
                // because they will be updated when they are processed as join producers in later iterations.
                // We iterate in reverse order, so the consumers will only be joins or output.
            }

            trace!("\n{:?}", self.transformation_infos);

            // Update the producer_consumer map
            fused_map_indices.push(index);
        }

        for index in fused_map_indices {
            self.transformation_infos.remove(index);
        }

        trace!(
            "transformations after fusing maps\n {:?}",
            self.transformation_infos,
        );

        // After removing fused maps, rebuild the producer-consumer and kv-layout caches
        self.rebuild_producer_consumer(original_atom_fp);
    }

    /// Fuse correct key-value layout requirements from downstream transformationinfos
    /// to upstream transformations.
    fn fuse_kv_layout(&mut self, original_atom_fp: &HashSet<u64>) {
        // Take a snapshot of kv_layout requirements to avoid borrowing self during mutation
        let layout_reqs: Vec<(u64, usize)> = self
            .kv_layouts
            .iter()
            .map(|(fp, split)| (*fp, *split))
            .collect();

        for (tx_fp, key_split) in layout_reqs {
            // Copy out the producer index and current consumers (if any), then mutate
            if let Some((producer_idx, consumers_opt)) = self
                .producer_consumer
                .get(&tx_fp)
                .map(|(p, c)| (*p, c.clone()))
            {
                // Update producer layout + fingerprint
                let new_output_fp = {
                    let producer_tx = &mut self.transformation_infos[producer_idx];
                    producer_tx.refactor_output_key_value_layout(key_split);
                    producer_tx.update_output_fake_sig();
                    producer_tx.output_info_fp()
                };

                // Update consumers to use new fingerprint
                if let Some(consumers) = consumers_opt {
                    for consumer_idx in consumers {
                        self.transformation_infos[consumer_idx]
                            .update_input_fake_info_fp(new_output_fp, &tx_fp);
                    }
                }
            } else {
                // No producer found - likely an original atom or inconsistent map; ignore
            }
        }

        // After updating kv-layouts, rebuild the producer-consumer and kv-layout caches
        self.rebuild_producer_consumer(original_atom_fp);
    }

    /// Rebuild the producer_consumer map after map fusing.
    fn rebuild_producer_consumer(&mut self, original_atom_fp: &HashSet<u64>) {
        // Clear caches
        self.producer_consumer.clear();
        self.kv_layouts.clear();

        trace!(
            "[rebuild_producer_consumer] rebuilding for {} transformations",
            self.transformation_infos.len()
        );

        // First pass: register all producers and kv-layouts using only local copies
        for index in 0..self.transformation_infos.len() {
            let output_fp = self.transformation_infos[index].output_info_fp();

            // Register producer
            self.insert_producer(output_fp, index);

            trace!(
                "[rebuild_producer_consumer] producer: idx {} -> fp {:#018x}",
                index,
                output_fp
            );
        }

        // Second pass: register all consumers for each input fingerprint
        for index in 0..self.transformation_infos.len() {
            let input_fps: Vec<u64> = {
                let tx = &self.transformation_infos[index];
                let (left, right_opt) = tx.input_info_fp();
                let mut v = vec![left];
                if let Some(r) = right_opt {
                    v.push(r);
                }
                v
            };

            let input_kv_layouts_offsets: Option<usize> = {
                let tx = &self.transformation_infos[index];
                if tx.is_join() {
                    let (left_kv_layout, ..) = tx.input_kv_layout();
                    Some(left_kv_layout.key().len())
                } else {
                    None
                }
            };

            for input_fp in input_fps {
                self.insert_consumer(original_atom_fp, input_fp, index);
                if let Some(offset) = input_kv_layouts_offsets {
                    self.kv_layouts.insert(input_fp, offset);
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

// -----------------------------
// Small helpers (private)
// -----------------------------
impl RulePlanner {
    #[inline]
    fn producer_index_or_panic(&self, fp: u64) -> usize {
        self.producer_consumer
            .get(&fp)
            .map(|(idx, _)| *idx)
            .unwrap_or_else(|| {
                panic!(
                    "Producer not found for transformation fingerprint: {:#018x}",
                    fp
                )
            })
    }

    #[inline]
    fn consumer_indices(&self, fp: u64) -> Vec<usize> {
        self.producer_consumer
            .get(&fp)
            .and_then(|(_, consumers)| consumers.clone())
            .unwrap_or_default()
    }

    #[inline]
    fn collect_output_positions_of(&self, producer_idx: usize) -> Vec<ArithmeticPos> {
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

    #[inline]
    fn layout_from_argument_ids(
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
                        "[fuse_map] missing key argument id {} in output layout ({} positions)",
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
                        "[fuse_map] missing value argument id {} in output layout ({} positions)",
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
        cmps: &[catalog::ComparisonExprPos],
    ) -> Vec<catalog::ComparisonExprPos> {
        cmps.iter()
            .map(|c| {
                let remap_expr = |expr: &ArithmeticPos| -> ArithmeticPos {
                    // Only support variable-only expressions during fusing; otherwise keep as-is
                    if expr.is_var() {
                        let sig = expr
                            .signatures()
                            .into_iter()
                            .next()
                            .expect("var expression should have one signature");
                        let id = sig.argument_id();
                        positions
                            .get(id)
                            .cloned()
                            .unwrap_or_else(|| panic!("missing argument id {} in positions", id))
                    } else {
                        expr.clone()
                    }
                };
                let left = remap_expr(c.left());
                let right = remap_expr(c.right());
                catalog::ComparisonExprPos::from_parts(left, c.operator().clone(), right)
            })
            .collect()
    }
}
