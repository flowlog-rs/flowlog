//! A non-recursive stratum's heads: each relation's union of its rule heads
//! with its earlier binding, outside any loop. The stratum's rule bodies are
//! its prelude.

use std::collections::HashSet;

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;

use crate::CodeGen;
use crate::CodegenError;

impl CodeGen {
    /// Returns the head step of every relation a non-recursive `stratum`
    /// derives, each rebinding the relation's global binding, and records
    /// each relation's weight. `bound_fps` holds the relations an input or an
    /// earlier stratum has already bound.
    pub(super) fn gen_non_recursive(
        &mut self,
        stratum: &StratumPlanner,
        bound_fps: &HashSet<u64>,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut heads = Vec::new();
        for (idb_fp, head_fps) in stratum.idb_to_heads_map() {
            // Fold into the existing binding rather than shadowing it. A
            // relation whose earlier binding holds a static input, or a
            // static partial result from an earlier stratum, keeps that
            // binding's weight until the union lifts it.
            let earlier = bound_fps
                .contains(idb_fp)
                .then(|| self.find_global_ident(*idb_fp));
            let (code, _, mutability) =
                self.gen_head(stratum, *idb_fp, head_fps, earlier, false, plan_graph)?;
            self.global_fp_to_mutability.insert(*idb_fp, mutability);
            heads.push(code);
        }
        Ok(heads)
    }
}
