use std::collections::HashSet;
use tracing::{trace, warn};

use super::RulePlanner;
use crate::TransformationInfo;

/// Here is some rule we should always follow,
/// 1. We always do filter first, so it is impossible to fuse any filters map
/// 2. We assume comparisons are always done before project unused arguments

impl RulePlanner {
    /// Plan a single rule, producing a list of transformation infos.
    pub fn fuse(&mut self, original_atom_fp: &HashSet<u64>) {
        self.fuse_map(original_atom_fp);
        self.fuse_kv_layout(original_atom_fp);
    }

    /// Fuse map transformation info.
    pub fn fuse_map(&mut self, original_atom_fp: &HashSet<u64>) {
        let mut fused_map_indices = Vec::new();

        // Iterate in reverse order to get indices
        for index in (0..self.transformation_infos.len()).rev() {
            let (input_fp, output_fp, out_kv_layout, cmp) =
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

            let input_producer_index = self
                .producer_consumer
                .get(&input_fp)
                .unwrap_or_else(|| {
                    panic!(
                        "Producer not found for transformation fingerprint: {:#018x}",
                        input_fp
                    )
                })
                .0;

            if self.transformation_infos[input_producer_index].is_neg_join() {
                // Skip if the input producer is a neg join (cannot fuse)
                trace!(
                    "[fuse_map] skip at idx {}: input producer {} is neg join",
                    index,
                    input_producer_index
                );
                continue;
            }

            let output_consumer_indices = self
                .producer_consumer
                .get(&output_fp)
                .and_then(|(_, consumers)| consumers.clone())
                .unwrap_or_else(|| {
                    warn!(
                        "Consumer not found for transformation fingerprint: {:#018x}",
                        output_fp
                    );
                    Vec::new()
                });

            // Update the input producer
            trace!(
                "[fuse_map] fuse at idx {}: input {:#018x} -> output {:#018x}; producer idx {}",
                index,
                input_fp,
                output_fp,
                input_producer_index
            );
            self.transformation_infos[input_producer_index]
                .update_output_key_value_layout(out_kv_layout);
            self.transformation_infos[input_producer_index].update_comparisons(cmp);
            self.transformation_infos[input_producer_index].update_output_fake_sig();

            let input_producer_output_fp =
                self.transformation_infos[input_producer_index].output_info_fp();

            // Update all consumers to point to the producer's new output
            for output_consumer_index in output_consumer_indices {
                self.transformation_infos[output_consumer_index]
                    .update_input_fake_info_fp(input_producer_output_fp, &output_fp);
                trace!(
                    "[fuse_map]   -> updated consumer idx {} to input {:#018x}",
                    output_consumer_index,
                    input_producer_output_fp
                );
                // Note: we do not need to update the input key-value layout of consumers here,
                // because they will be updated when they are processed as join producers in later iterations.
                // We iterate in reverse order, so the consumers will only be joins or output.
            }

            // Update the producer_consumer map

            fused_map_indices.push(index);
        }

        for index in fused_map_indices {
            self.transformation_infos.remove(index);
        }

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
