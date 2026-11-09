//! Profiling codegen: the calls a profiled engine makes into
//! `flowlog_runtime::profile`.
//!
//! The runtime's `Profiler` records each worker's metrics and writes its
//! tables. The engine installs one per worker, lets it flush once per
//! step, and ends each transaction's tables. Only the plan graph is baked
//! into the binary, as the `ops.json` worker 0 writes out.
//!
//! [`Codegen::gen_profiling`] returns the fragments as a [`Profiling`],
//! one per [`Skeleton`](crate::Skeleton) field they go into.

use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::error::CodegenError;

/// The profiler's fragments, grouped by where the
/// [`Skeleton`](crate::Skeleton) places them. Without profiling every
/// fragment is empty.
#[derive(Debug)]
pub(crate) struct Profiling {
    /// Among the skeleton's `declarations`: the plan graph const.
    pub declarations: TokenStream,
    /// Part of the skeleton's `worker_init`: binds the worker's `profiler`,
    /// which must be installed before the dataflow is built.
    pub install: TokenStream,
    /// Inside the skeleton's `step_loop`, once per step: the profiler's
    /// periodic flush.
    pub periodic_flush: TokenStream,
    /// After the step loop: the transaction's final tables.
    pub metrics_write: TokenStream,
}

impl Codegen {
    /// Returns the profiler's fragments for this engine. `plan_graph` is
    /// baked in as it stands, so every pass that records into it has run.
    ///
    /// Errors when profiling is on but no plan graph was recorded: the
    /// install would then name a const the declarations never define.
    pub(crate) fn gen_profiling(
        &self,
        plan_graph: Option<&PlanGraph>,
    ) -> Result<Profiling, CodegenError> {
        if !self.config.profiling_enabled() {
            return Ok(Profiling {
                declarations: quote! {},
                install: quote! {},
                periodic_flush: quote! {},
                metrics_write: quote! {},
            });
        }

        let plan_graph = plan_graph.ok_or_else(|| {
            CodegenError::internal("profiling is on but no plan graph was recorded")
        })?;
        let log_dir = self.profile_log_dir();
        let flush_interval = match self.config.metrics_flush_interval_ms() {
            0 => quote! { None },
            ms => quote! { Some(std::time::Duration::from_millis(#ms)) },
        };
        // A batch run's tables are its single transaction's; an incremental
        // engine's belong to the transaction just advanced past.
        let transaction = if self.program.is_incremental() {
            quote! { time_stamp - 1 }
        } else {
            quote! { 0 }
        };

        Ok(Profiling {
            declarations: gen_ops_const(plan_graph)?,
            install: quote! {
                let mut profiler = ::flowlog_runtime::profile::Profiler::install(
                    worker,
                    std::path::Path::new(#log_dir),
                    __FLOWLOG_OPS_JSON,
                    #flush_interval,
                );
            },
            periodic_flush: quote! { profiler.flush_if_due(#transaction); },
            metrics_write: quote! { profiler.end_transaction(#transaction); },
        })
    }

    /// Profiling output directory, `<stem>_log` (stem disambiguates programs
    /// sharing a process).
    #[must_use]
    fn profile_log_dir(&self) -> String {
        format!("{}_log", self.config.program_name())
    }
}

/// Renders the recorded plan graph as the `const &str` worker 0 writes out
/// as `ops.json`. Errors only if the plan graph fails to serialize, which a
/// well-formed graph never does: an internal error, not a user mistake.
fn gen_ops_const(plan_graph: &PlanGraph) -> Result<TokenStream, CodegenError> {
    let json = plan_graph
        .to_json_string()
        .map_err(|e| CodegenError::internal(format!("plan graph failed to serialize: {e}")))?;
    Ok(quote! {
        const __FLOWLOG_OPS_JSON: &str = #json;
    })
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    const BATCH: &str = ".decl Edge(a: int32, b: int32)\n.input Edge(IO=\"file\")\n.output Edge\n";
    const INCREMENTAL: &str = ".decl Edge(a: int32, b: int32) mutable\n\
                               .input Edge(IO=\"command\")\n.output Edge\n";

    /// Returns a plan graph with one operator recorded.
    fn plan_graph() -> PlanGraph {
        let mut graph = PlanGraph::new(/* incremental */ false);
        graph.map_join_operator("n".into(), vec![], "a".into(), 1);
        graph
    }

    /// Returns a profiled code generator over `source` that flushes every
    /// `interval_ms` milliseconds.
    fn profiled(source: &str, interval_ms: u64) -> Codegen {
        let mut cg = codegen(source);
        cg.config.profile = true;
        cg.config.metrics_flush_interval_ms = interval_ms;
        cg
    }

    #[test]
    fn without_profiling_every_fragment_is_empty() {
        let profiling = codegen(BATCH)
            .gen_profiling(None)
            .expect("nothing to serialize");
        assert!(profiling.declarations.is_empty());
        assert!(profiling.install.is_empty());
        assert!(profiling.periodic_flush.is_empty());
        assert!(profiling.metrics_write.is_empty());
    }

    #[test]
    fn recorded_plan_graph_is_baked_into_the_declarations() {
        let profiling = profiled(BATCH, 0)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        let declarations = profiling.declarations.to_string();
        assert!(declarations.contains("__FLOWLOG_OPS_JSON"));
        assert!(declarations.contains("nodes"));
    }

    /// The install passes the plan graph const unconditionally, so
    /// profiling without a recorded graph is a codegen bug, not a quiet
    /// omission.
    #[test]
    fn profiling_without_a_plan_graph_is_an_internal_error() {
        let error = profiled(BATCH, 0)
            .gen_profiling(None)
            .expect_err("no plan graph to bake");
        assert!(matches!(error, CodegenError::Internal(_)));
    }

    /// The install hands the profiler the program's log directory and its
    /// flush interval, where `0` means no periodic flush.
    // Cases: interval_ms, the install's interval argument.
    #[rstest]
    #[case::never(0, quote! { None })]
    #[case::every_five_seconds(
        5000,
        quote! { Some(std::time::Duration::from_millis(5000u64)) },
    )]
    fn install_passes_the_log_dir_and_flush_interval(
        #[case] interval_ms: u64,
        #[case] flush_interval: TokenStream,
    ) {
        let profiling = profiled(BATCH, interval_ms)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        // The default config has no program path, hence the stem.
        let expected = quote! {
            let mut profiler = ::flowlog_runtime::profile::Profiler::install(
                worker,
                std::path::Path::new("unknown_program_log"),
                __FLOWLOG_OPS_JSON,
                #flush_interval,
            );
        };
        assert_eq!(profiling.install.to_string(), expected.to_string());
    }

    /// The flush and the final write both name the transaction the tables
    /// belong to: a batch run's single `0`, an incremental epoch's
    /// `time_stamp - 1`.
    // Cases: source, transaction.
    #[rstest]
    #[case::batch(BATCH, quote! { 0 })]
    #[case::incremental(INCREMENTAL, quote! { time_stamp - 1 })]
    fn tables_are_named_after_the_closing_transaction(
        #[case] source: &str,
        #[case] transaction: TokenStream,
    ) {
        let profiling = profiled(source, 0)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        assert_eq!(
            [
                profiling.periodic_flush.to_string(),
                profiling.metrics_write.to_string(),
            ],
            [
                quote! { profiler.flush_if_due(#transaction); }.to_string(),
                quote! { profiler.end_transaction(#transaction); }.to_string(),
            ]
        );
    }
}
