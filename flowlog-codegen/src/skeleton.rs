//! [`Skeleton`]: a generated engine's fragments, grouped by where each goes
//! in the frontend's `main` or engine, and the order codegen fills them in.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::input::Input;
use crate::io::output::Output;
use crate::profiling::Profiling;
use crate::tuple_tokens;

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
    /// Before `timely::execute`: one shared emitter per output, which the
    /// host reads the output from once the workers are done.
    pub emitters: TokenStream,
    /// Among the worker closure's captures: a handle to each emitter, so
    /// the host keeps its own.
    pub emitter_captures: TokenStream,
    /// At the top of the worker: profiler setup and worker-local emitters.
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
    /// After the step loop: publishes each worker-local emitter's buffered
    /// rows to its emitter.
    pub publish: TokenStream,
}

impl Codegen {
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

        let Input {
            declarations: input_declarations,
            handles,
        } = self.gen_input(plan_graph);
        let strata = self.gen_strata(strata, plan_graph)?;
        let Output {
            emitters,
            emitter_captures,
            local_emitters,
            inspectors,
            publishes,
        } = self.gen_output(plan_graph)?;

        // After every other pass, so the plan graph it bakes in is complete.
        let Profiling {
            declarations: profiling_declarations,
            collectors,
            periodic_flush,
            metrics_write,
        } = self.gen_profiling(plan_graph.as_ref())?;
        let outer_time = self.outer_time_tokens();

        // An incremental engine probes every output to tell when an epoch's
        // outputs are complete, and returns the probe beside the handles; a
        // batch engine runs to completion instead.
        let incremental = self.program.is_incremental();
        let (probe, step_loop) = if incremental {
            (
                Some(quote! { let probe = ProbeHandle::new(); }),
                quote! {
                    while probe.less_than(&time_stamp) {
                        worker.step();
                        #periodic_flush
                    }
                },
            )
        } else {
            (None, quote! { while worker.step() { #periodic_flush } })
        };
        // The dataflow closure returns the tuple its caller binds, both
        // spelled the same.
        let returned = tuple_tokens(
            handles
                .iter()
                .map(|handle| quote! { #handle })
                .chain(incremental.then(|| quote! { probe })),
        );

        Ok(Skeleton {
            declarations: quote! {
                #outer_time
                #profiling_declarations
            },
            emitters: quote! { #(#emitters)* },
            emitter_captures: quote! { #(#emitter_captures)* },
            worker_init: quote! {
                #collectors
                #(#local_emitters)*
            },
            dataflow: quote! {
                let #returned =
                    worker.dataflow::<Ts, _, _>(|scope| {
                        #input_declarations
                        #(#strata)*
                        #probe
                        #(#inspectors)*
                        #returned
                    });
            },
            step_loop,
            metrics_write,
            publish: quote! { #(#publishes)* },
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;

    use crate::test_harness::codegen;

    /// Append edges, their closure, a count over them, and a negation under
    /// an append filter.
    const APPEND_PROGRAM: &str = "
        .decl Edge(src: int32, dst: int32) append
        .input Edge
        .decl Block(node: int32) append
        .input Block
        .decl Reach(src: int32, dst: int32)
        Reach(s, d) :- Edge(s, d).
        Reach(s, d) :- Reach(s, m), Edge(m, d).
        .output Reach
        .decl Size(n: int32)
        Size(count(d)) :- Edge(_, d).
        .output Size
        .decl Open(src: int32, dst: int32)
        Open(x, y) :- Edge(x, y), !Block(x).
        .output Open
    ";

    /// An append program's dataflow arranges through the runtime's arrange
    /// by key and by self, reduces append rows through the append reduce,
    /// and loops at the lexicographic time with the feedback variable's
    /// weight spelled out.
    #[test]
    fn an_append_program_dispatches_on_its_weight() {
        let mut codegen = codegen(APPEND_PROGRAM);
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut None)
            .expect("generates")
            .dataflow
            .to_string();
        for expected in [
            ":: flowlog_runtime :: operators :: flowlog_arrange (",
            ":: flowlog_runtime :: operators :: flowlog_arrange_self (",
            ":: flowlog_runtime :: operators :: flowlog_reduce_append (",
            ":: flowlog_runtime :: time :: LexLoop",
            "Vec < (_ , _ , :: flowlog_runtime :: diff :: Append)",
        ] {
            assert!(dataflow.contains(expected), "{expected}\n{dataflow}");
        }
        assert!(!dataflow.contains("arrange_by_key ()"), "{dataflow}");
        assert!(!dataflow.contains("arrange_by_self ()"), "{dataflow}");
    }
}
