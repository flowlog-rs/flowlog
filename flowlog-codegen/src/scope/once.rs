//! The dataflow's own scope, at `time::Once`: the static inputs, the unit,
//! the static part of every stratum, the `Epoch` scope when the engine is
//! incremental, then the static outputs' inspectors.

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::input::Input;
use crate::tuple_tokens;
use crate::ty::time::Clock;

impl Codegen {
    /// Returns the dataflow, whose own scope is at `time::Once`, as the
    /// statement `let (<handles>, <probes>) = worker.dataflow(..);`. The
    /// scope holds the static part, computed once whatever the engine: the
    /// static inputs, the unit, the static part of every stratum of
    /// `strata`, and the static outputs' inspectors. An incremental engine
    /// nests its `Epoch` scope between the strata and the inspectors, and
    /// returns `probe` and `once_probe` after the handles.
    ///
    /// Returns nothing for a program whose every output was pruned: its
    /// dataflow would never read `scope`.
    pub(crate) fn gen_once_scope(
        &mut self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        // Every operator below is addressed inside the main dataflow scope.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });
        let Input {
            declarations: inputs,
            handles,
        } = self.gen_input(Clock::Once, plan_graph);
        // The unit binds after the static inputs, so its profiler node comes
        // last in the input block and no input's address moves, and before
        // the strata, which read it.
        let unit = self.gen_unit(strata, plan_graph);
        let strata_flows = self.gen_static_strata(strata, plan_graph)?;
        let incremental = self.program.is_incremental();
        let (epoch_scope, epoch_handles) = if incremental {
            self.gen_epoch_scope(strata, plan_graph)?
        } else {
            (TokenStream::new(), Vec::new())
        };
        // A static output is inspected where it is computed, so its updates
        // stay at `()` and never wake the `Epoch` scope. Last, as only then
        // is every relation's weight final: one whose rules span strata can
        // be static in an early one.
        let inspectors = self.gen_inspectors(Clock::Once, plan_graph)?;
        if self.program.is_empty() {
            return Ok(TokenStream::new());
        }

        // The probe names its time, as the scope may have no output to
        // infer it from.
        let once = Clock::Once.time_tokens();
        let once_probe = incremental.then(|| {
            quote! { let once_probe = ProbeHandle::<#once>::new(); }
        });
        // The closure returns the tuple its caller binds, both spelled the
        // same: every handle, then an incremental engine's probes.
        let probes = incremental.then(|| [format_ident!("probe"), format_ident!("once_probe")]);
        let returned = tuple_tokens(
            handles
                .iter()
                .chain(&epoch_handles)
                .chain(probes.iter().flatten())
                .map(|handle| quote! { #handle }),
        );
        // One import serves the inputs of both scopes: the `Epoch` scope's
        // closure is inside this one.
        let input_trait = self.program.has_inputs().then(|| {
            quote! { use ::flowlog_runtime::differential_dataflow::input::Input; }
        });
        Ok(quote! {
            let #returned =
                worker.dataflow::<#once, _, _>(|scope| {
                    #input_trait
                    #inputs
                    #unit
                    #(#strata_flows)*
                    #epoch_scope
                    #once_probe
                    #(#inspectors)*
                    #returned
                });
        })
    }
}

#[cfg(test)]
mod tests {
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_profiler::PlanGraph;
    use quote::quote;
    use rstest::rstest;

    use crate::test_harness::codegen;

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
        let binding = quote! { let unit = ::flowlog_runtime::operators::unit(scope); };
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
