//! Bundles per-pass fragments into [`CodeParts`].

use std::collections::HashSet;

use flowlog_common::ExecutionMode;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::idb_buffers::InspectorCodegen;
use crate::codegen::profile::render_profile_ops_const;

/// Token-stream fragments and rendered source files produced by
/// [`CodeGen::generate`]. All fields are `pub` so consumers can
/// pick the subset they need.
pub struct CodeParts {
    // -- dataflow graph --
    /// `let (handle, collection) = scope.new_collection::<_, Diff>();` per EDB.
    pub edb_decls: Vec<TokenStream>,
    /// LHS pattern for destructuring the dataflow return value.
    pub handle_binding: TokenStream,
    /// Expression returned from `worker.dataflow(|scope| { ... })`.
    pub dataflow_return: TokenStream,
    /// Per-stratum transformation rules.
    pub flows: Vec<TokenStream>,

    // -- output pipeline --
    /// Shared output buffer declarations (one `Arc<Mutex<Vec>>` per output relation).
    pub output_bufs: Vec<TokenStream>,
    /// Clones of shared buffers moved into the worker closure.
    pub output_buf_clones: Vec<TokenStream>,
    /// Per-worker local buffer declarations (`Rc<RefCell<Vec>>`).
    pub local_bufs: Vec<TokenStream>,
    /// `inspect()` calls that push into local buffers.
    pub inspectors: Vec<TokenStream>,
    /// Drain local → shared buffer at end of each epoch.
    pub flush: Vec<TokenStream>,
    /// `.printsize` size cell decls (`Arc<Mutex<i32>>`) before `timely::execute`.
    pub size_cell_decls: Vec<TokenStream>,
    /// Size cell clones moved into the worker closure.
    pub size_cell_clones: Vec<TokenStream>,

    // -- profiling — all fields below are empty when `--profile` is off.
    /// Struct definitions for profiling (emitted at file level).
    pub profile_structs: TokenStream,
    /// `const __FLOWLOG_OPS_JSON: &str = "..."` — static plan graph baked
    /// in; worker 0 writes it to `<stem>_log/ops.json` at startup.
    pub profile_ops: TokenStream,
    /// Logger registration code (emitted inside worker closure).
    pub profile_init: TokenStream,
    /// Metrics write-out for the current mode; empty without profiling.
    pub metrics_write: TokenStream,
    /// Step loop for the current mode; flushes metrics periodically when
    /// profiled.
    pub step_loop: TokenStream,

    /// Type aliases and constants for the `(Data, Diff, Time)` triple.
    pub type_declarations: TokenStream,
}

impl CodeGen {
    /// Run all code-generation passes and collect the resulting fragments
    /// into an [`CodeParts`].
    pub(crate) fn collect_parts(
        &mut self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<CodeParts, CodegenError> {
        // Record entering the main dataflow scope when profiling is on
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });

        // Static sections.
        let edb_decls = self.gen_edb_decls(plan_graph);
        let (handle_binding, dataflow_return) = self.gen_handle_binding();
        let profile_structs = self.gen_metrics_struct();
        let profile_init = self.gen_metrics_init();

        // Relations whose outer ident is already bound — by `gen_edb_decls`
        // (seeded here) or by a prior stratum. Without EDB seeding, a rule
        // for an EDB relation would shadow the EDB binding and drop its tuples.
        let mut flows: Vec<TokenStream> = Vec::new();
        let mut bound_fps: HashSet<u64> = self.program.edb_fingerprints();

        for (idx, stratum) in strata.iter().enumerate() {
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.update_stratum_block(idx);
            });

            let core_flows = self.gen_non_recursive_core_flows(stratum, plan_graph)?;
            flows.extend(core_flows);

            if stratum.is_recursive() {
                let outer_snapshot = self.outer_arranged.clone();
                flows.push(self.gen_recursive_block(&outer_snapshot, stratum, plan_graph)?);
            } else {
                flows.extend(self.gen_non_recursive_post_flows(&bound_fps, stratum, plan_graph)?);
            }

            bound_fps.extend(stratum.output_relations());
        }

        // Output inspectors.
        let InspectorCodegen {
            buf_declarations: output_bufs,
            buf_clones: output_buf_clones,
            local_decls: local_bufs,
            inspect_stmts: inspectors,
            flush_stmts: flush,
            size_cell_decls,
            size_cell_clones,
        } = self.collect_inspectors(plan_graph);

        let (metrics_write, step_loop) = match self.config.mode() {
            ExecutionMode::Inc => (
                self.gen_metrics_write_incremental(),
                self.gen_incremental_step_loop(),
            ),
            ExecutionMode::Batch => (self.gen_metrics_write_batch(), self.gen_batch_step_loop()),
        };

        // Rendered after the codegen loop so the plan graph is fully
        // populated. Empty when profile is off.
        let profile_ops = render_profile_ops_const(plan_graph.as_ref())?;

        let type_declarations = self.gen_type_declarations();

        Ok(CodeParts {
            edb_decls,
            handle_binding,
            dataflow_return,
            flows,
            output_bufs,
            output_buf_clones,
            local_bufs,
            inspectors,
            flush,
            size_cell_decls,
            size_cell_clones,
            profile_structs,
            profile_ops,
            profile_init,
            metrics_write,
            step_loop,
            type_declarations,
        })
    }
}
