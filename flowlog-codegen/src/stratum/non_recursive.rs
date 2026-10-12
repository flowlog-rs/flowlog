//! A non-recursive stratum: its prelude, then each relation's head step,
//! the union of its rule heads with its earlier binding, outside any loop.

use std::collections::HashSet;

use flowlog_parser::Mutability;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
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
            // Each part, the earlier binding then the heads, at the weight
            // recorded for it: a head is a step's output, or the relation a
            // rule reads as it is, bound globally outside a loop.
            let parts: Vec<(Ident, Mutability)> = earlier
                .into_iter()
                .map(|binding| Ok((binding, self.mutability(*idb_fp)?)))
                .chain(
                    head_fps
                        .iter()
                        .map(|fp| Ok((self.find_global_ident(*fp), self.mutability(*fp)?))),
                )
                .collect::<Result<_, CodegenError>>()?;
            let (union, mutability) =
                self.gen_union_dedup(*idb_fp, &parts, &binding, false, plan_graph)?;
            // The stratifier assigns the relation the same value on its own;
            // the two derivations agree by Lemma 4 of
            // `docs/design/mutability.md`.
            debug_assert_eq!(
                stratum.mutability(*idb_fp),
                Some(mutability),
                "`{name}` binds at a weight the stratifier does not give it",
            );
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            heads.push(union);
        }
        Ok(quote! { #(#prelude)* #(#heads)* })
    }
}
