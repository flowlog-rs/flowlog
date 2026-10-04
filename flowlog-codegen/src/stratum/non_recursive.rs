//! A non-recursive stratum: its prelude, then each relation's head step,
//! the union of its rule heads with its earlier binding, outside any loop.

use std::collections::HashSet;

use flowlog_planner::planner::StratumPlanner;
use flowlog_planner::planner::aggregate_mutability;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;

impl Codegen {
    /// Returns a non-recursive `stratum`: its prelude, then the head step
    /// of every relation it derives, each rebinding the relation's global
    /// binding. Records each relation's weight. `bound_fps` holds the
    /// relations an input or an earlier stratum has already bound.
    pub(super) fn gen_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        bound_fps: &HashSet<u64>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let prelude = self.gen_prelude(stratum, plan_graph)?;
        let mut heads = Vec::new();
        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            let name = self.display_name(*idb_fp);
            let binding = self.find_global_ident(*idb_fp);
            // Fold into the existing binding rather than shadowing it. A
            // relation whose earlier binding holds a static input, or a
            // static partial result from an earlier stratum, keeps that
            // binding's weight until the union lifts it.
            let earlier = bound_fps.contains(idb_fp).then(|| binding.clone());
            let (union, union_mutability) =
                self.gen_union_dedup(*idb_fp, head_fps, earlier, &binding, false, plan_graph)?;
            // An aggregated relation holds the answers, not the rows the
            // union collects, and the answers can be more mutable.
            let (aggregate, mutability) = match stratum.idb_to_aggregation_map().get(idb_fp) {
                Some(aggregation) => (
                    self.gen_aggregate(
                        *idb_fp,
                        *aggregation,
                        &binding,
                        &binding,
                        union_mutability,
                        plan_graph,
                    )?,
                    aggregate_mutability(union_mutability),
                ),
                None => (quote! {}, union_mutability),
            };
            // The stratifier assigns the relation the same value on its own;
            // the two derivations agree by Lemma 4 of
            // `docs/design/mutability.md`.
            debug_assert_eq!(
                stratum.mutability(*idb_fp),
                Some(mutability),
                "`{name}` binds at a weight the stratifier does not give it",
            );
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            heads.push(quote! { #union #aggregate });
        }
        Ok(quote! { #(#prelude)* #(#heads)* })
    }
}
