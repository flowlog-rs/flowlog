//! [`Skeleton`]: a generated engine's fragments, grouped by where each goes
//! in the frontend's `main` or engine, and the order codegen fills them in.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::CodeGen;
use crate::CodegenError;
use crate::io::output::Outputs;
use crate::profile::render_profile_ops_const;

// =============================================================================
// Skeleton
// =============================================================================

/// A generated engine's fragments, each ready to splice where its field
/// says in the frontend's own `main` or engine.
///
/// The fragments name what the frontend must have in scope where it splices
/// them: `worker` and its `index` inside the worker closure, `Ts` from
/// `declarations`, and in an incremental engine `ProbeHandle` for `dataflow`
/// and the epoch's `time_stamp` for `step_loop` and `metrics_write`.
pub struct Skeleton {
    /// File-level declarations: `type Ts`, and the profiler's structs and ops
    /// const.
    pub declarations: TokenStream,
    /// Before `timely::execute`: one shared buffer per output.
    pub output_buffers: TokenStream,
    /// Among the worker closure's captures: each output buffer's clone.
    pub output_buffer_clones: TokenStream,
    /// At the top of the worker: profiler setup and worker-local producers.
    pub worker_init: TokenStream,
    /// The whole `let <handles> = worker.dataflow(...)` statement: the
    /// inputs, the strata, the outputs, and an incremental engine's `probe`.
    pub dataflow: TokenStream,
    /// Once the inputs of an epoch, or of the run, are in: steps the worker
    /// until the work is done, flushing profiler metrics periodically when
    /// profiled.
    pub step_loop: TokenStream,
    /// After the step loop: writes the profiler's metrics; empty without
    /// profiling.
    pub metrics_write: TokenStream,
    /// After the step loop: flushes each worker's buffered outputs to their
    /// shared buffers.
    pub flush: TokenStream,
}

impl CodeGen {
    /// Runs every code-generation pass and lays the fragments out as a
    /// [`Skeleton`].
    pub(crate) fn gen_skeleton(
        &mut self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Skeleton, CodegenError> {
        // Every operator below is addressed inside the main dataflow scope.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });

        let inputs = self.gen_edb_decls(plan_graph);
        let handles = self.gen_handles();
        let profile_structs = self.gen_metrics_struct();
        let profile_init = self.gen_metrics_init();
        let strata = self.gen_strata(strata, plan_graph)?;
        let Outputs {
            output_buffers,
            output_buffer_clones,
            local_producers,
            inspectors,
            flush,
        } = self.gen_outputs(plan_graph)?;

        let (metrics_write, step_loop) = if self.program.is_incremental() {
            (
                self.gen_metrics_write_incremental(),
                self.gen_incremental_step_loop(),
            )
        } else {
            (self.gen_metrics_write_batch(), self.gen_batch_step_loop())
        };

        // Rendered after every pass above so the plan graph is fully
        // populated. Empty when profile is off.
        let profile_ops = render_profile_ops_const(plan_graph.as_ref())?;
        let outer_time = self.outer_time_type();

        // An incremental engine probes every output to tell when an epoch's
        // outputs are complete; a batch engine runs to completion instead.
        let probe = self
            .program
            .is_incremental()
            .then(|| quote! { let mut probe = ProbeHandle::new(); });

        Ok(Skeleton {
            declarations: quote! {
                #outer_time
                #profile_structs
                #profile_ops
            },
            output_buffers: quote! { #(#output_buffers)* },
            output_buffer_clones: quote! { #(#output_buffer_clones)* },
            worker_init: quote! {
                #profile_init
                #(#local_producers)*
            },
            dataflow: quote! {
                let #handles =
                    worker.dataflow::<Ts, _, _>(|scope| {
                        #(#inputs)*
                        #(#strata)*
                        #probe
                        #(#inspectors)*
                        #handles
                    });
            },
            step_loop,
            metrics_write,
            flush: quote! { #(#flush)* },
        })
    }
}
