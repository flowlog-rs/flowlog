//! [`Skeleton`]: a generated engine's fragments, grouped by where each goes
//! in the frontend's `main` or engine, and the order codegen fills them in.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::output::Output;
use crate::profiling::Profiling;

// =============================================================================
// Skeleton
// =============================================================================

/// A generated engine's fragments, each ready to splice where its field
/// says in the frontend's own `main` or engine.
///
/// The fragments name what the frontend must have in scope where it splices
/// them: `worker` inside the worker closure, `Ts` from `declarations`, and
/// in an incremental engine `ProbeHandle` for `dataflow` and the epoch's
/// `time_stamp` for `step_loop` and `metrics_write`.
pub struct Skeleton {
    /// File-level declarations: `type Ts` when there is a `dataflow`, and the
    /// profiler's plan graph const.
    pub declarations: TokenStream,
    /// Before `timely::execute`: one shared emitter per output, which the
    /// host reads the output from once the workers are done.
    pub emitters: TokenStream,
    /// Among the worker closure's captures: a handle to each emitter, so
    /// the host keeps its own.
    pub emitter_captures: TokenStream,
    /// At the top of the worker: the profiler's install and worker-local
    /// emitters.
    pub worker_init: TokenStream,
    /// The whole `let <handles> = worker.dataflow(...)` statement: the
    /// inputs, the strata, the outputs, and an incremental engine's probes.
    /// Its own scope, at `time::Once`, computes every static collection
    /// once; an incremental engine nests the rest in a scope at
    /// `time::Epoch`, which static collections enter. Each scope inspects
    /// its own outputs: `once_probe` probes the static ones, and `probe`
    /// the rest. Empty when the program has none of them to build.
    pub dataflow: TokenStream,
    /// Once the inputs of an epoch, or of the run, are in: steps the worker
    /// until the work is done, flushing profiler metrics periodically when
    /// profiled. An incremental engine's work includes its static outputs,
    /// which complete only once every static input is closed: the frontend
    /// closes them before its first step loop.
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
        let dataflow = self.gen_once_scope(strata, plan_graph)?;
        let Output {
            emitters,
            emitter_captures,
            local_emitters,
            publishes,
        } = self.gen_output();

        // After every other pass, so the plan graph it bakes in is complete.
        let Profiling {
            declarations: profiling_declarations,
            install,
            periodic_flush,
            metrics_write,
        } = self.gen_profiling(plan_graph.as_ref())?;

        // An incremental engine steps until both scopes' outputs are
        // complete: `probe` tells for the `Epoch` scope's, and `once_probe`
        // for the `Once` scope's, which the first epoch completes. A batch
        // engine runs to completion instead.
        let step_loop = if self.program.is_incremental() {
            quote! {
                while probe.less_than(&time_stamp) || !once_probe.done() {
                    worker.step();
                    #periodic_flush
                }
            }
        } else {
            quote! { while worker.step() { #periodic_flush } }
        };
        // Only a dataflow names `Ts`.
        let outer_time = (!dataflow.is_empty()).then(|| self.outer_time_tokens());

        Ok(Skeleton {
            declarations: quote! {
                #outer_time
                #profiling_declarations
            },
            emitters: quote! { #(#emitters)* },
            emitter_captures: quote! { #(#emitter_captures)* },
            worker_init: quote! {
                #install
                #(#local_emitters)*
            },
            dataflow,
            step_loop,
            metrics_write,
            publish: quote! { #(#publishes)* },
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use proc_macro2::TokenStream;
    use quote::quote;
    use rstest::rstest;

    use crate::test_harness::codegen;

    /// Append edges, their closure, and a negation under an append filter.
    const APPEND_PROGRAM: &str = "
        .decl Edge(src: int32, dst: int32) append
        .input Edge
        .decl Block(node: int32) append
        .input Block
        .decl Reach(src: int32, dst: int32)
        Reach(s, d) :- Edge(s, d).
        Reach(s, d) :- Reach(s, m), Edge(m, d).
        .output Reach
        .decl Open(src: int32, dst: int32)
        Open(x, y) :- Edge(x, y), !Block(x).
        .output Open
    ";

    /// Arrangements must know the feedback weight before the loop's final
    /// assignment constrains it, so they can choose the append operators.
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
            ":: flowlog_runtime :: operators :: arrange (",
            ":: flowlog_runtime :: operators :: arrange_self (",
            ":: flowlog_runtime :: time :: LexLoop",
            "Vec < ((i32 , i32) , _ , :: flowlog_runtime :: diff :: Append)",
        ] {
            assert!(dataflow.contains(expected), "{expected}\n{dataflow}");
        }
        assert!(!dataflow.contains("arrange_by_key ()"), "{dataflow}");
        assert!(!dataflow.contains("arrange_by_self ()"), "{dataflow}");
    }

    /// A program whose every output was pruned has nothing to build, so it
    /// gets no dataflow, nor the `Ts` only a dataflow names. A program with
    /// no input but a derived output still builds one.
    // Cases: program, builds, declarations.
    #[rstest]
    #[case::derived_output(
        ".decl A(x: int32)\nA(x) :- A(x).\n.output A\n",
        true,
        quote! { type Ts = ::flowlog_runtime::time::Once; },
    )]
    #[case::pruned_output(".decl A(x: int32)\n.output A\n", false, quote! {})]
    fn only_a_program_with_something_to_build_has_a_dataflow(
        #[case] source: &str,
        #[case] builds: bool,
        #[case] declarations: TokenStream,
    ) {
        let mut codegen = codegen(source);
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let skeleton = codegen.generate(&planner, &mut None).expect("generates");
        assert_eq!(
            !skeleton.dataflow.is_empty(),
            builds,
            "{}",
            skeleton.dataflow
        );
        assert_eq!(skeleton.declarations.to_string(), declarations.to_string());
    }
}
