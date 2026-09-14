//! Output inspection and publication through runtime emitters.
//!
//! Generated inspections retain dataflow operators and probes. Runtime
//! workers buffer updates and publish at the engine's completion boundary.

use flowlog_common::ExecutionMode;
use flowlog_parser::DataType;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;

/// Output fragments grouped by their insertion point in the engine.
#[derive(Default)]
pub(crate) struct InspectorCodegen {
    pub buf_declarations: Vec<TokenStream>, // before timely::execute
    pub buf_clones: Vec<TokenStream>,       // closure capture
    pub local_decls: Vec<TokenStream>,      // worker body, before dataflow
    pub inspect_stmts: Vec<TokenStream>,    // inside dataflow
    pub flush_stmts: Vec<TokenStream>,      // before barrier (all workers)
}

impl CodeGen {
    /// Connects output and count inspections to per-relation emitters.
    pub(crate) fn collect_inspectors(
        &mut self,
        plan_graph: &mut Option<PlanGraph>,
    ) -> InspectorCodegen {
        let mut cg = InspectorCodegen::default();
        with_plan_graph(plan_graph, |p| p.update_inspect_block());

        for idb in self.program.idbs() {
            let collection = self.find_global_ident(idb.fingerprint());
            let emitter = format_ident!("buf_{}", idb.name());
            let marker = format_ident!("Rel{}", idb.name());
            cg.buf_declarations.push(quote! {
                let #emitter = ::flowlog_runtime::io::output::Emitter::<#marker, Ts>::new();
            });
            cg.buf_clones
                .push(quote! { let #emitter = #emitter.clone(); });

            // Profiler edges use generated bindings; labels preserve the
            // relation's spelling in the source program.
            with_plan_graph(plan_graph, |p| {
                if idb.printsize() {
                    p.inspect_size_operator(collection.to_string(), idb.raw_name().to_string());
                }
                if idb.has_output() {
                    if self.config.output_to_stdout() {
                        p.inspect_content_terminal_operator(
                            collection.to_string(),
                            idb.raw_name().to_string(),
                        );
                    } else {
                        p.inspect_content_file_operator(
                            collection.to_string(),
                            idb.raw_name().to_string(),
                        );
                    }
                }
            });

            if idb.printsize() {
                cg.inspect_stmts.push(self.gen_size_inspector(
                    &collection,
                    &emitter,
                    idb.raw_name(),
                ));
            }

            // Nested float fields need OrderedFloat just like scalar columns.
            if idb
                .data_type()
                .iter()
                .any(|dt| dt.any_scalar(&DataType::is_float))
            {
                self.features.mark_ordered_float();
            }

            if idb.has_output() {
                let worker = format_ident!("local_{}", idb.name());
                cg.local_decls
                    .push(quote! { let #worker = #emitter.worker(); });
                cg.inspect_stmts
                    .push(self.gen_row_inspector(&collection, &worker));
                cg.flush_stmts.push(quote! { #worker.publish(); });
            }
        }

        cg
    }

    /// Counts distinct rows and records the consolidated count delta.
    fn gen_size_inspector(&self, collection: &Ident, emitter: &Ident, name: &str) -> TokenStream {
        // Set-normalized weights become the epoch's count delta. Batch
        // presence weights must first be lifted into numeric weights.
        let op_name = format!("{name}: inspect size");
        let (counted, probe) = match self.config.mode() {
            ExecutionMode::Batch => (
                quote! {
                    ::flowlog_runtime::operators::flowlog_map(
                        ::flowlog_runtime::operators::flowlog_dedup(#collection.clone()),
                        #op_name,
                        move |_, t, _| std::iter::once(((), t, 1_i32)),
                    )
                },
                quote! {},
            ),
            ExecutionMode::Inc => (
                quote! { ::flowlog_runtime::operators::flowlog_dedup(#collection.clone()) },
                quote! { .probe_with(&mut probe) },
            ),
        };

        quote! {{
            let #emitter = #emitter.clone();
            #counted
                .map(|_| ())
                .consolidate()
                .inspect(move |(_data, time, size)| {
                    #emitter.record_size(time, *size);
                })
                #probe;
        }}
    }

    /// Connects updates to a worker-local producer at the engine's timestamp.
    fn gen_row_inspector(&self, collection: &Ident, worker: &Ident) -> TokenStream {
        let inspect = match self.config.mode() {
            ExecutionMode::Batch => quote! {
                #collection.inspect(move |(data, time, _)| {
                    #worker.record(data, time, 1_i32);
                });
            },
            ExecutionMode::Inc => quote! {
                #collection
                    .consolidate()
                    .inspect(move |(data, time, diff)| {
                        #worker.record(data, time, *diff);
                    })
                    .probe_with(&mut probe);
            },
        };
        quote! {{
            let #worker = #worker.clone();
            #inspect
        }}
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case(ExecutionMode::Batch)]
    #[case(ExecutionMode::Inc)]
    fn count_only_output_has_no_worker_row_buffer(#[case] mode: ExecutionMode) {
        let mut config = Config {
            mode,
            ..Config::default()
        };
        let program = flowlog_parser::parse(
            concat!(
                env!("CARGO_MANIFEST_DIR"),
                "/../tests/fixtures/batch/printsize/program.dl"
            ),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("printsize fixture");
        let mut codegen = CodeGen::new(config, program);
        codegen.make_global_ident_map();
        let inspectors = codegen.collect_inspectors(&mut None);

        assert_eq!(inspectors.buf_declarations.len(), 2);
        assert_eq!(
            inspectors
                .local_decls
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            [quote! { let local_both = buf_both.worker(); }.to_string()],
        );
        assert_eq!(
            inspectors
                .flush_stmts
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            [quote! { local_both.publish(); }.to_string()],
        );
    }
}
