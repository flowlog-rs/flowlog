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

use crate::aggregation::{aggregation_merge_kv, aggregation_reduce, aggregation_row_chop};
use crate::Compiler;

use planner::{StratumPlanner, Transformation};

// =========================================================================
// Non-Recursive Flow Generation
// =========================================================================
impl Compiler {
    /// Generate the non-recursive core differential dataflow pipelines, returning the assembled
    /// statements along with the arrangement map populated while building them.
    pub(crate) fn gen_non_recursive_core_flows(
        &mut self,
        transformations: &[Transformation],
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
    ) -> Vec<TokenStream> {
        let mut flows = Vec::new();
        let dedup_stats = self.dedup_collection();

        for (output_fp, idb_fps) in stratum.output_to_idb_map() {
            let output = self.find_global_ident(*output_fp);
            let mut outs: Vec<Ident> = Vec::new();

            for idb_fp in idb_fps {
                outs.push(format_ident!("t_{}", idb_fp));
            }

            let head = outs[0].clone();
            let tail = &outs[1..];
            let mut expr: TokenStream = quote! { #head };
            for t in tail {
                expr = quote! { #expr.concat(&#t) };
            }

            // If this output was already computed in a previous stratum, union the previous
            // collection with the newly produced tuples before applying distinct.
            let mut block = if calculated_output_fps.contains(output_fp) {
                quote! {
                    let #output = #output
                        .concat(&#expr)
                        #dedup_stats;
                }
            } else {
                quote! {
                    let #output = #expr
                        #dedup_stats;
                }
            };

            // Aggregation logic
            if let Some((agg_op, agg_pos, agg_arity)) =
                stratum.output_to_aggregation_map().get(output_fp)
            {
                self.imports.mark_aggregation();
                self.imports.mark_as_collection();
                self.imports.mark_semiring_one();
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
            flows.push(block);
        }

        trace!(
            "Generated post-processing flows:\n{}\n",
            quote! { #(#flows)* }
        );
        flows
    }
}
