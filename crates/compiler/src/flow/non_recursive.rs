//! Non-recursive flow generation for FlowLog compiler.
//!
//! This module handles the generation of non-recursive differential dataflow
//! pipelines based on the planned transformations.
//! - Core flow means the main non-recursive transformations within a stratum.
//!   It is worth notice that due to factoring optimization, even recursive stratum
//!   may contain non-recursive core flows.
//! - Post flow means the final output processing after core flows, such as
//!   unioning multiple IDBs into an output relation and applying aggregation.
//!   Post flows are only generated for non-recursive strata.

use std::collections::{HashMap, HashSet};

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote};
use tracing::trace;

use crate::aggregation::{
    aggregation_merge_kv, aggregation_min_optimize, aggregation_reduce, aggregation_row_chop,
};
use crate::Compiler;

use common::ExecutionMode;
use parser::AggregationOperator;
use planner::{StratumPlanner, Transformation};
use profiler::{with_profiler, Profiler};

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================
impl Compiler {
    /// Generate the non-recursive core differential dataflow pipelines, returning the assembled
    /// statements along with the arrangement map populated while building them.
    pub(crate) fn gen_non_recursive_core_flows(
        &mut self,
        transformations: &[Transformation],
        profiler: &mut Option<Profiler>,
    ) -> (Vec<TokenStream>, HashMap<u64, Ident>) {
        let mut flows = Vec::new();
        // Stratum-scoped cache of arrangements; emit arrange_by_key just before first use.
        let mut non_recursive_arranged_map: HashMap<u64, Ident> = HashMap::new();

        for transformation in transformations {
            let global_fp_to_ident = self.global_fp_to_ident.clone();
            flows.push(self.gen_transformation(
                &global_fp_to_ident,
                transformation,
                &mut non_recursive_arranged_map,
                profiler,
            ));
        }

        trace!("Generated static flows:\n{}\n", quote! { #(#flows)* });
        (flows, non_recursive_arranged_map)
    }

    /// Generate non recursive post-processing flows
    pub(crate) fn gen_non_recursive_post_flows(
        &mut self,
        calculated_output_fps: &HashSet<u64>,
        stratum: &StratumPlanner,
        profiler: &mut Option<Profiler>,
    ) -> Vec<TokenStream> {
        let mut flows = Vec::new();
        let dedup_stats = self.dedup_collection();

        for (output_fp, idb_fps) in stratum.output_to_idb_map() {
            // Rule outputs
            let output = self.find_global_ident(*output_fp);
            let mut outs: Vec<Ident> = Vec::new();

            for idb_fp in idb_fps {
                outs.push(format_ident!("t_{}", idb_fp));
            }

            // Build union expression from IDB sources.
            let head = outs[0].clone();
            let tail = &outs[1..];
            let mut expr: TokenStream = quote! { #head };
            for t in tail {
                expr = quote! { #expr.concat(&#t) };
            }

            // If this output was already computed in a previous stratum, union the previous
            // collection with the newly produced tuples before applying distinct.
            let mut block = if calculated_output_fps.contains(output_fp) {
                // Profiler: concat for repeated output (optional)
                with_profiler(profiler, |profiler| {
                    profiler.concat_operator(
                        output.to_string(),
                        outs.iter().map(|id| id.to_string()).collect(),
                        output.to_string(),
                        outs.len() as u32,
                    );
                });
                quote! {
                    let #output = #output
                        .concat(&#expr)
                        #dedup_stats;
                }
            } else {
                // Profiler: concat for new output (optional)
                with_profiler(profiler, |profiler| {
                    profiler.concat_operator(
                        output.to_string(),
                        outs.iter().map(|id| id.to_string()).collect(),
                        output.to_string(),
                        outs.len() as u32 - 1,
                    );
                });
                quote! {
                    let #output = #expr
                        #dedup_stats;
                }
            };

            // Aggregation logic (optional)
            if let Some((agg_op, agg_pos, agg_arity)) =
                stratum.output_to_aggregation_map().get(output_fp)
            {
                self.imports.mark_as_collection();
                self.imports.mark_semiring_one();

                // Min semiring fast path: replace reduce_core with threshold_semigroup
                // using the Min semigroup, avoiding a second arrangement.
                if matches!(agg_op, AggregationOperator::Min)
                    && matches!(self.config.mode(), ExecutionMode::Batch)
                {
                    self.imports.mark_min_semiring();
                    self.imports.mark_threshold_total();
                    self.imports.mark_timely_map();
                    let min_pipeline = aggregation_min_optimize(*agg_arity, *agg_pos);
                    block = quote! {
                        #block
                        let #output = #output
                            #min_pipeline;
                    };
                } else {
                    self.imports.mark_aggregation();
                    let row_chop = aggregation_row_chop(*agg_arity, *agg_pos);
                    let reduce_logic = aggregation_reduce(agg_op);
                    let merge_kv = aggregation_merge_kv(*agg_arity, *agg_pos);
                    block = quote! {
                        #block
                        let #output = #output
                            .map(#row_chop)
                            .reduce_core::<_,ValBuilder<_,_,_,_>,ValSpine<_,_,_,_>>(
                                "aggregation",
                                #reduce_logic
                            )
                            .as_collection(#merge_kv);
                    };
                }

                // Profiler: aggregation operator (optional)
                with_profiler(profiler, |profiler| {
                    profiler.aggregate_operator(
                        output.to_string(),
                        output.to_string(),
                        output.to_string(),
                    );
                });
            }
            flows.push(block);
        }

        trace!(
            "Generated post-processing flows:\n{}\n",
            quote! { #(#flows)* }
        );
        flows
    }
}
