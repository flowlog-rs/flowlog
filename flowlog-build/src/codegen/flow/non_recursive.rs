//! Non-recursive flow codegen — two entry points per stratum:
//!
//! - **Core flows.** The stratum's non-recursive transformations; a
//!   recursive stratum can also carry these when the planner factors
//!   non-recursive work out of a fixpoint.
//! - **Post flows.** Final output processing after core flows: union the
//!   heads producing each IDB, dedup, and apply aggregation. Emitted only
//!   for non-recursive strata.

use std::collections::HashSet;
use std::mem;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use tracing::trace;

use crate::planner::StratumPlanner;
use flowlog_parser::AggregationOperator;
use flowlog_profiler::{Profiler, with_profiler};

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::aggregation::{
    aggregation_avg_optimize, aggregation_count_optimize, aggregation_max_optimize,
    aggregation_merge_kv, aggregation_min_optimize, aggregation_reduce_stmt, aggregation_row_chop,
    aggregation_sum_optimize,
};

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================
impl CodeGen {
    /// Emit the stratum's non-recursive transformation pipelines into the
    /// program-wide outer-scope arrangement cache (`self.outer_arranged`).
    pub(crate) fn gen_non_recursive_core_flows(
        &mut self,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut flows = Vec::new();
        let global_fp_to_ident = self.global_fp_to_ident.clone();
        let mut outer_arranged = mem::take(&mut self.outer_arranged);

        for transformation in stratum.non_recursive_transformations() {
            flows.push(self.gen_transformation(
                &global_fp_to_ident,
                transformation,
                &mut outer_arranged,
                stratum,
                profiler,
            )?);
        }

        self.outer_arranged = outer_arranged;

        trace!("Generated static flows:\n{}\n", quote! { #(#flows)* });
        Ok(flows)
    }

    /// Emit per-IDB post-processing: union the contributing heads, dedup,
    /// and apply aggregation (fast-path semiring for `DatalogBatch`, generic
    /// `reduce_core` otherwise).
    pub(crate) fn gen_non_recursive_post_flows(
        &mut self,
        bound_fps: &HashSet<u64>,
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
            let head = &outs[0];
            let mut expr: TokenStream = quote! { #head.clone() };
            for t in &outs[1..] {
                expr = quote! { #expr.concat(#t.clone()) };
            }

            // Fold into the existing binding rather than shadowing it.
            let already_bound = bound_fps.contains(idb_fp);
            let (concat_expr, concat_count) = if already_bound {
                (quote! { #output.concat(#expr) }, outs.len() as u32)
            } else {
                (expr, outs.len() as u32 - 1)
            };

            with_profiler(profiler, |profiler| {
                profiler.concat_dedup_operator(
                    self.display_name(*idb_fp),
                    outs.iter().map(|id| id.to_string()).collect(),
                    output.to_string(),
                    concat_count,
                    false,
                );
            });

            let mut block = quote! {
                let #output = #concat_expr
                    #dedup_stats;
            };

            if let Some((agg_op, agg_pos, agg_arity)) = stratum.idb_to_aggregation_map().get(idb_fp)
            {
                let agg_type = self.agg_column_type(*idb_fp, *agg_pos)?;

                // `DatalogBatch`-only fast path: compute the aggregation with
                // a monoid via `threshold_semigroup`, skipping the second
                // arrangement that `reduce_core` would introduce.
                if self.config.is_datalog_batch() {
                    self.features.mark_as_collection();
                    self.features.mark_agg_semiring(*agg_op, agg_type.clone());
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
                            self.display_name(*idb_fp),
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
                            self.display_name(*idb_fp),
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
