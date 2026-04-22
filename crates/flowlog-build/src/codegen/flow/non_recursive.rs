//! Non-recursive flow codegen — two entry points per stratum:
//!
//! - **Core flows.** The stratum's non-recursive transformations; a
//!   recursive stratum can also carry these when the planner factors
//!   non-recursive work out of a fixpoint.
//! - **Post flows.** Final output processing after core flows: union the
//!   heads producing each IDB, dedup, and apply aggregation. Emitted only
//!   for non-recursive strata.

use std::collections::{HashMap, HashSet};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use tracing::trace;

use crate::parser::AggregationOperator;
use crate::planner::StratumPlanner;
use crate::profiler::{with_profiler, Profiler};

use crate::codegen::aggregation::{
    aggregation_avg_optimize, aggregation_count_optimize, aggregation_max_optimize,
    aggregation_merge_kv, aggregation_min_optimize, aggregation_reduce_stmt, aggregation_row_chop,
    aggregation_sum_optimize,
};
use crate::codegen::CodegenError;
use crate::codegen::CodeGen;

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================
impl CodeGen {
    /// Emit the stratum's non-recursive transformation pipelines, along with
    /// the per-stratum arrangement cache populated while building them.
    pub(crate) fn gen_non_recursive_core_flows(
        &mut self,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<(Vec<TokenStream>, HashMap<u64, Ident>), CodegenError> {
        let mut flows = Vec::new();
        // Stratum-scoped cache of arrangements; emit arrange_by_key just before first use.
        let mut non_recursive_arranged_map: HashMap<u64, Ident> = HashMap::new();

        for transformation in stratum.non_recursive_transformations() {
            let global_fp_to_ident = self.global_fp_to_ident.clone();
            flows.push(self.gen_transformation(
                &global_fp_to_ident,
                transformation,
                &mut non_recursive_arranged_map,
                stratum,
                profiler,
            )?);
        }

        trace!("Generated static flows:\n{}\n", quote! { #(#flows)* });
        Ok((flows, non_recursive_arranged_map))
    }

    /// Emit per-IDB post-processing: union the contributing heads, dedup,
    /// and apply aggregation (fast-path semiring for `DatalogBatch`, generic
    /// `reduce_core` otherwise).
    pub(crate) fn gen_non_recursive_post_flows(
        &mut self,
        calculated_output_fps: &HashSet<u64>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();
        let dedup_stats = self.dedup_nonrecursive();

        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            let output = self.find_global_ident(*idb_fp);
            let outs: Vec<Ident> = head_fps
                .iter()
                .map(|fp| format_ident!("t_{}", fp))
                .collect();

            // Union the per-head collections left-to-right.
            let head = outs[0].clone();
            let tail = &outs[1..];
            let mut expr: TokenStream = quote! { #head.clone() };
            for t in tail {
                expr = quote! { #expr.concat(#t.clone()) };
            }

            // Output already produced in an earlier stratum: fold the new
            // heads into the existing collection before deduping.
            let mut block = if calculated_output_fps.contains(idb_fp) {
                with_profiler(profiler, |profiler| {
                    profiler.concat_dedup_operator(
                        output.to_string(),
                        outs.iter().map(|id| id.to_string()).collect(),
                        output.to_string(),
                        outs.len() as u32,
                        false,
                    );
                });
                quote! {
                    let #output = #output
                        .concat(#expr)
                        #dedup_stats;
                }
            } else {
                with_profiler(profiler, |profiler| {
                    profiler.concat_dedup_operator(
                        output.to_string(),
                        outs.iter().map(|id| id.to_string()).collect(),
                        output.to_string(),
                        outs.len() as u32 - 1,
                        false,
                    );
                });
                quote! {
                    let #output = #expr
                        #dedup_stats;
                }
            };

            if let Some((agg_op, agg_pos, agg_arity)) = stratum.idb_to_aggregation_map().get(idb_fp)
            {
                let agg_type = self.agg_column_type(*idb_fp, *agg_pos)?;

                // `DatalogBatch`-only fast path: compute the aggregation with
                // a monoid via `threshold_semigroup`, skipping the second
                // arrangement that `reduce_core` would introduce.
                if self.config.is_datalog_batch() {
                    self.features.mark_as_collection();
                    self.features.mark_agg_semiring(*agg_op, agg_type);
                    self.features.mark_threshold_total();
                    self.features.mark_timely_map();
                    let pipeline = match agg_op {
                        AggregationOperator::Min => {
                            aggregation_min_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Max => {
                            aggregation_max_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Sum => {
                            aggregation_sum_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Count => {
                            aggregation_count_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                        AggregationOperator::Avg => {
                            aggregation_avg_optimize(*agg_arity, *agg_pos, agg_type)
                        }
                    };
                    block = quote! {
                        #block
                        let #output = #output
                            #pipeline;
                    };

                    with_profiler(profiler, |profiler| {
                        profiler.opt_aggregate_operator(
                            output.to_string(),
                            output.to_string(),
                            output.to_string(),
                        );
                    });
                } else {
                    self.features.mark_aggregation();
                    let row_chop = aggregation_row_chop(*agg_arity, *agg_pos);
                    let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
                    let reduce_stmt =
                        aggregation_reduce_stmt(self.config.is_incremental(), agg_op, agg_type)?;
                    block = quote! {
                        #block
                        let #output = #output
                            .map(#row_chop)
                            .arrange_by_key()
                            #reduce_stmt
                            .as_collection(#merge_kv);
                    };

                    with_profiler(profiler, |profiler| {
                        profiler.general_aggregate_operator(
                            output.to_string(),
                            output.to_string(),
                            output.to_string(),
                        );
                    });
                }
            }

            flows.push(block);
        }

        trace!(
            "Generated post-processing flows:\n{}\n",
            quote! { #(#flows)* }
        );
        Ok(flows)
    }
}
