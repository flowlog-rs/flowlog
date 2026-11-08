//! [`Skeleton`]: a generated engine's fragments, grouped by where each goes
//! in the frontend's `main` or engine, and the order codegen fills them in.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::input::Input;
use crate::io::output::Output;
use crate::profiling::Profiling;
use crate::tuple_tokens;
use crate::ty::time::Clock;

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
    /// profiler's structs and ops const.
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
    /// Its own scope, at `time::Once`, computes every static collection
    /// once; an incremental engine nests the rest in a scope at
    /// `time::Epoch`, which static collections enter. Empty when the program
    /// has none of them to build.
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

        // The dataflow's own scope, at `time::Once`, computes the static
        // part once, whatever the engine: the static inputs, and every
        // collection that reads only static ones.
        let Input {
            declarations: once_inputs,
            handles: once_handles,
        } = self.gen_input(Clock::Once, plan_graph);
        // The unit binds after the static inputs, so its profiler node comes
        // last in the input block and no input's address moves, and before
        // the strata, which read it.
        let unit = self.gen_unit(strata, plan_graph);
        let once_strata = self.gen_strata(strata, Clock::Once, plan_graph)?;

        // An incremental engine nests the part that can change in a scope at
        // `time::Epoch`, after the static part it reads: the inputs that can
        // change, and every collection that reads one. Every operator of
        // that part is addressed inside the nested scope.
        let incremental = self.program.is_incremental();
        let (epoch_inputs, epoch_handles, epoch_strata) = if incremental {
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.enter_scope();
            });
            let Input {
                declarations,
                handles,
            } = self.gen_input(Clock::Epoch, plan_graph);
            let strata = self.gen_strata(strata, Clock::Epoch, plan_graph)?;
            (declarations, handles, strata)
        } else {
            (TokenStream::new(), Vec::new(), Vec::new())
        };
        let Output {
            emitters,
            emitter_captures,
            local_emitters,
            enters,
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

        // An incremental engine probes every output to tell when an epoch's
        // outputs are complete, and returns the probe beside the handles; a
        // batch engine runs to completion instead.
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
        // Each scope's closure returns the tuple its caller binds, both
        // spelled the same: the dataflow every handle and the probe, the
        // nested scope its own handles and the probe.
        let probe_handle = incremental.then(|| format_ident!("probe"));
        let returned = tuple_tokens(
            once_handles
                .iter()
                .chain(&epoch_handles)
                .chain(&probe_handle)
                .map(|handle| quote! { #handle }),
        );

        // The outputs are inspected where the probe and the emitters' time
        // are: in the dataflow's own scope in a batch engine, in the nested
        // one in an incremental engine.
        let outputs = quote! {
            #probe
            #(#enters)*
            #(#inspectors)*
        };
        let rest = if incremental {
            let nested = tuple_tokens(
                epoch_handles
                    .iter()
                    .chain(&probe_handle)
                    .map(|handle| quote! { #handle }),
            );
            let epoch = Clock::Epoch.time_tokens();
            quote! {
                let #nested = scope.scoped::<#epoch, _, _>("Incremental", |scope| {
                    #epoch_inputs
                    #(#epoch_strata)*
                    #outputs
                    #nested
                });
            }
        } else {
            outputs
        };
        // One import serves the inputs of both scopes: the nested scope's
        // closure is inside the dataflow's.
        let input_trait = self.program.has_inputs().then(|| {
            quote! { use ::flowlog_runtime::differential_dataflow::input::Input; }
        });
        // A program whose every output was pruned has nothing to build: its
        // dataflow would never read `scope`, and nothing would name `Ts`.
        let once = Clock::Once.time_tokens();
        let dataflow = (!self.program.is_empty()).then(|| {
            quote! {
                let #returned =
                    worker.dataflow::<#once, _, _>(|scope| {
                        #input_trait
                        #once_inputs
                        #unit
                        #(#once_strata)*
                        #rest
                        #returned
                    });
            }
        });
        let outer_time = dataflow.is_some().then(|| self.outer_time_tokens());

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
            dataflow: dataflow.unwrap_or_default(),
            step_loop,
            metrics_write,
            publish: quote! { #(#publishes)* },
        })
    }
}

// =============================================================================
// Scopes
// =============================================================================

impl Codegen {
    /// Returns `let b = b.enter(scope);` when a step at `clock` is the first
    /// to read the static binding `b` of collection `fp` in an incremental
    /// engine's `Epoch` scope; nothing otherwise. Once entered, `b` shadows
    /// the static binding for every later step there. An entered
    /// arrangement shares the static one's trace, so the static part is
    /// computed and arranged once, however many epochs read it.
    ///
    /// Returns an internal error for a static step that reads a collection
    /// that can change: a step is static only when every input is (Lemma 4
    /// of `docs/design/mutability.md`).
    pub(crate) fn gen_epoch_enter(
        &mut self,
        binding: &Ident,
        fp: u64,
        clock: Clock,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Option<TokenStream>, CodegenError> {
        match (Clock::of(self.mutability(fp)?), clock) {
            (Clock::Once, Clock::Epoch) => {
                if !self.epoch_entered.insert(binding.clone()) {
                    return Ok(None);
                }
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.enter_operator(binding.to_string(), binding.to_string());
                });
                Ok(Some(quote! { let #binding = #binding.enter(scope); }))
            }
            (Clock::Once, Clock::Once) | (Clock::Epoch, Clock::Epoch) => Ok(None),
            (Clock::Epoch, Clock::Once) => Err(CodegenError::internal(format!(
                "a static step reads `{}` (fingerprint 0x{fp:016x}), which can change",
                self.display_name(fp),
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_profiler::PlanGraph;
    use proc_macro2::TokenStream;
    use quote::format_ident;
    use quote::quote;
    use rstest::rstest;

    use crate::Codegen;
    use crate::CodegenError;
    use crate::test_harness::codegen;
    use crate::ty::time::Clock;

    /// Static links and their closure, which two rules join with mutable
    /// edges.
    const MIXED_PROGRAM: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Path(x: int32, y: int32)
        Path(x, y) :- Link(x, y).
        Path(x, z) :- Path(x, y), Link(y, z).
        .output Path
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Hop(x: int32, z: int32)
        Hop(x, z) :- Edge(x, y), Path(y, z).
        .output Hop
        .decl Back(x: int32, z: int32)
        Back(x, z) :- Edge(y, x), Path(y, z).
        .output Back
    ";

    /// A static input and one that can change.
    const TWO_INPUTS: &str = "
        .decl S(x: int32)
        .input S
        .decl M(x: int32) mutable
        .input M
        .decl R(x: int32)
        R(x) :- S(x), M(x).
        .output R
    ";

    /// An incremental engine computes its static part once, in the
    /// dataflow's own scope: the links, and their closure, which loops at
    /// `OnceLoop`. It nests the part that can change in a scope at `Epoch`,
    /// which each static binding read there enters once: the closure's
    /// arrangement, though both rules join it, and the closure, which its
    /// output inspects.
    #[test]
    fn an_incremental_engine_computes_its_static_part_once() {
        let mut codegen = codegen(MIXED_PROGRAM);
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut None)
            .expect("generates")
            .dataflow
            .to_string();
        let (once, epoch) = dataflow
            .split_once("\"Incremental\"")
            .expect("an incremental engine nests a scope");
        let path = quote! { let rel_1_path = rel_1_path.enter(scope); }.to_string();

        assert!(once.contains("diff :: Static >"), "{once}");
        assert!(once.contains("time :: OnceLoop"), "{once}");
        assert!(!once.contains("diff :: Mutable"), "{once}");
        assert!(epoch.contains("diff :: Mutable >"), "{epoch}");
        assert!(!epoch.contains("time :: OnceLoop"), "{epoch}");
        assert_eq!(epoch.matches(". enter (scope)").count(), 2, "{epoch}");
        assert!(epoch.contains(&path), "{epoch}");
    }

    /// Returns the fingerprint of the input relation spelled `name`.
    fn input_fp(codegen: &Codegen, name: &str) -> u64 {
        codegen
            .program
            .edbs()
            .into_iter()
            .find(|relation| relation.raw_name() == name)
            .expect("an input of the program")
            .fingerprint()
    }

    /// A step at `Epoch` enters a static binding the first time it reads
    /// it, and later steps read the entered one; a step reads a binding at
    /// its own clock as it is.
    // Cases: input, clock, what two reads emit.
    #[rstest]
    #[case::static_at_epoch("S", Clock::Epoch, ["let b = b . enter (scope) ;", ""])]
    #[case::static_at_once("S", Clock::Once, ["", ""])]
    #[case::mutable_at_epoch("M", Clock::Epoch, ["", ""])]
    fn an_epoch_step_enters_a_static_binding_once(
        #[case] input: &str,
        #[case] clock: Clock,
        #[case] expected: [&str; 2],
    ) {
        let mut codegen = codegen(TWO_INPUTS);
        let fp = input_fp(&codegen, input);
        let binding = format_ident!("b");
        let reads = [(); 2].map(|()| {
            codegen
                .gen_epoch_enter(&binding, fp, clock, &mut None)
                .expect("a read the clock admits")
                .unwrap_or_default()
                .to_string()
        });
        assert_eq!(reads, expected);
    }

    /// A static step reads only static collections, so one that reads a
    /// collection that can change is a planner bug.
    #[test]
    fn a_static_step_that_reads_a_changing_collection_is_an_internal_error() {
        let mut codegen = codegen(TWO_INPUTS);
        let fp = input_fp(&codegen, "M");
        let error = codegen
            .gen_epoch_enter(&format_ident!("b"), fp, Clock::Once, &mut None)
            .expect_err("no static step reads `M`");
        assert!(matches!(error, CodegenError::Internal(_)));
    }

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
            ":: flowlog_runtime :: operators :: flowlog_arrange (",
            ":: flowlog_runtime :: operators :: flowlog_arrange_self (",
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

    /// A body without rows reads the unit, which the dataflow binds after
    /// its inputs, if it has any, and the profiler records as the input
    /// block's last node, so every operator after it keeps its address. A
    /// dataflow whose bodies all read rows binds no unit.
    // Cases: program, input block.
    #[rstest]
    #[case::negation(
        ".decl On()\n.input On\n.decl M(x: int32)\nM(0) :- !On().\n.output M\n",
        &["On: input", "On: input dedup", "@unit: input"],
    )]
    #[case::no_input(
        ".decl M(x: int32)\nM(0) :- \"a\" < \"b\".\n.output M\n",
        &["@unit: input"],
    )]
    #[case::rows(
        ".decl On()\n.input On\n.decl S(x: int32)\n.input S\n\
         .decl M(x: int32)\nM(x) :- S(x), !On().\n.output M\n",
        &["On: input", "On: input dedup", "S: input", "S: input dedup"],
    )]
    fn only_a_body_without_rows_binds_the_unit(#[case] source: &str, #[case] input_block: &[&str]) {
        let mut codegen = codegen(source);
        let mut plan_graph = Some(PlanGraph::new(false));
        let planner =
            ProgramPlanner::from_program(&codegen.program, &mut plan_graph).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut plan_graph)
            .expect("generates")
            .dataflow
            .to_string();
        let binding = quote! { let unit = ::flowlog_runtime::operators::flowlog_unit(scope); };
        let nodes: Vec<&str> = plan_graph
            .as_ref()
            .expect("recorded")
            .nodes()
            .iter()
            .filter(|node| node.block.to_string() == "input")
            .map(|node| node.name.as_str())
            .collect();

        assert_eq!(
            dataflow.contains(&binding.to_string()),
            input_block.contains(&"@unit: input"),
            "{dataflow}"
        );
        assert_eq!(nodes, input_block);
    }
}
