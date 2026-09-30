//! Outputs: every IDB's emitter, and the inspectors that feed it: row
//! changes for `.output`, the row count for `.printsize`. Workers buffer
//! updates and publish them at the engine's completion boundary. Every
//! emitted change is an `i32`, whatever the relation's weight: a static
//! relation's presence emits as one insertion.

use flowlog_parser::DataType;
use flowlog_parser::Mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::CodegenError;
use crate::codegen::output_emitter_ident;
use crate::codegen::relation_marker_ident;

/// The outputs' fragments, one entry per output in each, grouped by where
/// the [`Skeleton`](crate::codegen::Skeleton) field of the same name places
/// them.
#[derive(Default)]
pub(crate) struct Outputs {
    pub output_buffers: Vec<TokenStream>,
    pub output_buffer_clones: Vec<TokenStream>,
    /// Worker-local producers, part of the skeleton's `worker_init`.
    pub local_producers: Vec<TokenStream>,
    /// Inspectors, part of the skeleton's `dataflow`.
    pub inspectors: Vec<TokenStream>,
    pub flush: Vec<TokenStream>,
}

impl CodeGen {
    /// Returns every IDB's output fragments: its emitter, a size inspector
    /// for `.printsize`, and for `.output` a row inspector, the worker-local
    /// producer it records into, and that producer's flush. Also marks
    /// ordered floats when an IDB's columns hold a float.
    pub(crate) fn gen_outputs(
        &mut self,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Outputs, CodegenError> {
        let mut outputs = Outputs::default();
        with_plan_graph(plan_graph, |p| p.update_inspect_block());

        for idb in self.program.idbs() {
            let collection = self.find_global_ident(idb.fingerprint());
            let mutability = self.mutability(idb.fingerprint())?;
            let emitter = output_emitter_ident(idb.name());
            let marker = relation_marker_ident(idb.name());
            // Profiler edges use generated bindings; labels keep the
            // relation's spelling in the source program.
            let label = idb.raw_name().to_string();
            outputs.output_buffers.push(quote! {
                let #emitter = ::flowlog_runtime::io::output::Emitter::<#marker, Ts>::new();
            });
            outputs
                .output_buffer_clones
                .push(quote! { let #emitter = #emitter.clone(); });

            // A nested float field needs `OrderedFloat` as a scalar one does.
            if idb
                .data_type()
                .iter()
                .any(|dt| dt.any_scalar(&DataType::is_float))
            {
                self.features.mark_ordered_float();
            }

            if idb.printsize() {
                with_plan_graph(plan_graph, |p| {
                    p.inspect_size_operator(collection.to_string(), label.clone(), mutability);
                });
                outputs.inspectors.push(self.gen_size_inspector(
                    &collection,
                    &emitter,
                    idb.raw_name(),
                    mutability,
                ));
            }

            if idb.has_output() {
                with_plan_graph(plan_graph, |p| {
                    if self.config.output_to_stdout() {
                        p.inspect_content_terminal_operator(
                            collection.to_string(),
                            label.clone(),
                            mutability,
                        );
                    } else {
                        p.inspect_content_file_operator(
                            collection.to_string(),
                            label.clone(),
                            mutability,
                        );
                    }
                });
                let worker = format_ident!("local_{}", idb.name());
                outputs
                    .local_producers
                    .push(quote! { let #worker = #emitter.worker(); });
                outputs
                    .inspectors
                    .push(self.gen_row_inspector(&collection, &worker, mutability));
                outputs.flush.push(quote! { #worker.publish(); });
            }
        }

        Ok(outputs)
    }

    /// Returns the inspector that records, each epoch, the change in
    /// `collection`'s distinct-row count into `emitter`.
    fn gen_size_inspector(
        &self,
        collection: &Ident,
        emitter: &Ident,
        name: &str,
        mutability: Mutability,
    ) -> TokenStream {
        // After the dedup each row weighs one, so the rows' summed weights are
        // the count's change. A presence weight is lifted to a signed count
        // first, since presence cannot be summed into a count.
        let op_name = format!("{name}: inspect size");
        let deduped = quote! { ::flowlog_runtime::operators::flowlog_dedup(#collection.clone()) };
        let counted = match mutability {
            Mutability::Static => quote! {
                ::flowlog_runtime::operators::flowlog_lift(#deduped, #op_name)
            },
            Mutability::Mutable => deduped,
        };
        let probe = self.probe();

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

    /// Returns the inspector that records each of `collection`'s row changes,
    /// at its time, into the worker-local producer `worker`.
    fn gen_row_inspector(
        &self,
        collection: &Ident,
        worker: &Ident,
        mutability: Mutability,
    ) -> TokenStream {
        // A static relation is deduped once, at its minimum time, so each
        // row arrives as one presence and needs no consolidation.
        let inspected = match mutability {
            Mutability::Static => quote! {
                #collection.inspect(move |(data, time, _)| {
                    #worker.record(data, time, 1_i32);
                })
            },
            Mutability::Mutable => quote! {
                #collection
                    .consolidate()
                    .inspect(move |(data, time, diff)| {
                        #worker.record(data, time, *diff);
                    })
            },
        };
        let probe = self.probe();
        quote! {{
            let #worker = #worker.clone();
            #inspected #probe;
        }}
    }

    /// Emits the probe an incremental engine attaches to every output, so
    /// it can tell when an epoch's outputs are complete; nothing in a batch
    /// engine, which runs to completion instead.
    fn probe(&self) -> TokenStream {
        if self.program.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        }
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use rstest::rstest;

    use super::*;

    /// A code generator over `source`, with every relation's mutability taken
    /// from its declaration, as the strata would record it.
    fn codegen(source: &str) -> CodeGen {
        let mut file = tempfile::NamedTempFile::new().expect("tempfile");
        writeln!(file, "{source}").expect("write");
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut SourceMap::default(),
            &mut config,
        )
        .expect("program parses");
        let mut codegen = CodeGen::new(config, program);
        codegen.make_global_ident_map();
        codegen.global_fp_to_mutability = codegen
            .program
            .edbs()
            .into_iter()
            .chain(codegen.program.idbs())
            .map(|rel| (rel.fingerprint(), rel.input_mutability()))
            .collect();
        codegen
    }

    fn strings(tokens: &[TokenStream]) -> Vec<String> {
        tokens.iter().map(ToString::to_string).collect()
    }

    /// Every IDB gets an emitter, but only an `.output` one gets a row buffer
    /// and a flush, in either engine.
    #[rstest]
    #[case::batch("")]
    #[case::incremental(" mutable")]
    fn only_an_output_relation_buffers_rows(#[case] mutability: &str) {
        let mut codegen = codegen(&format!(
            ".decl Data(a: int32){mutability}\n.input Data\n\
             .decl Both(a: int32)\nBoth(a) :- Data(a).\n.output Both\n.printsize Both\n\
             .decl Count(a: int32)\nCount(a) :- Data(a).\n.printsize Count\n"
        ));
        let outputs = codegen.gen_outputs(&mut None).expect("outputs");
        assert_eq!(outputs.output_buffers.len(), 2);
        assert_eq!(
            strings(&outputs.local_producers),
            [quote! { let local_both = buf_both.worker(); }.to_string()]
        );
        assert_eq!(
            strings(&outputs.flush),
            [quote! { local_both.publish(); }.to_string()]
        );
    }

    /// A static relation's rows arrive once each, so they record as one
    /// insertion unconsolidated; a mutable one's changes consolidate first.
    /// Only an incremental engine probes.
    // Cases: input mutability, row inspector.
    #[rstest]
    #[case::static_batch(
        "",
        quote! {{
            let local_r = local_r.clone();
            r.inspect(move |(data, time, _)| {
                local_r.record(data, time, 1_i32);
            });
        }}
    )]
    #[case::mutable(
        " mutable",
        quote! {{
            let local_r = local_r.clone();
            r.consolidate()
                .inspect(move |(data, time, diff)| {
                    local_r.record(data, time, *diff);
                })
                .probe_with(&mut probe);
        }}
    )]
    fn a_row_inspector_records_each_change(
        #[case] mutability: &str,
        #[case] expected: TokenStream,
    ) {
        let codegen = codegen(&format!(".decl R(a: int32){mutability}\n.input R\n"));
        let input_mutability = codegen.program.edbs()[0].input_mutability();
        let tokens = codegen.gen_row_inspector(
            &format_ident!("r"),
            &format_ident!("local_r"),
            input_mutability,
        );
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    /// A presence cannot sum into a count, so a static relation lifts to a
    /// signed count before counting.
    #[test]
    fn a_static_size_inspector_lifts_presence_to_a_count() {
        let codegen = codegen(".decl R(a: int32)\n.input R\n");
        let tokens = codegen.gen_size_inspector(
            &format_ident!("r"),
            &format_ident!("buf_r"),
            "R",
            Mutability::Static,
        );
        let expected = quote! {{
            let buf_r = buf_r.clone();
            ::flowlog_runtime::operators::flowlog_lift(
                ::flowlog_runtime::operators::flowlog_dedup(r.clone()),
                "R: inspect size"
            )
            .map(|_| ())
            .consolidate()
            .inspect(move |(_data, time, size)| {
                buf_r.record_size(time, *size);
            });
        }};
        assert_eq!(tokens.to_string(), expected.to_string());
    }
}
