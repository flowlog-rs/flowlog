//! Output: every IDB's emitter, and the inspectors that feed it: row
//! changes for `.output`, the row count for `.printsize`. Workers buffer
//! updates and publish them at the engine's completion boundary. Every
//! emitted change is an `i32`, whatever the relation's weight: a presence,
//! static or append, emits as one insertion.

use flowlog_parser::Mutability;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::local_emitter_ident;
use crate::output_emitter_ident;
use crate::relation_marker_ident;

/// The outputs' fragments, one entry per output in each, grouped by where
/// the [`Skeleton`](crate::Skeleton) places them.
#[derive(Default)]
pub(crate) struct Output {
    /// The skeleton's `emitters`: each output's shared emitter.
    pub emitters: Vec<TokenStream>,
    /// The skeleton's `emitter_captures`: the worker closure's handle to
    /// each emitter.
    pub emitter_captures: Vec<TokenStream>,
    /// Part of the skeleton's `worker_init`: each `.output` relation's
    /// worker-local emitter.
    pub local_emitters: Vec<TokenStream>,
    /// Part of the skeleton's `dataflow`, before the inspectors: each static
    /// output entering an incremental engine's `Epoch` scope, where the
    /// probe and the emitters' time are.
    pub enters: Vec<TokenStream>,
    /// Part of the skeleton's `dataflow`: the inspectors recording into the
    /// emitters.
    pub inspectors: Vec<TokenStream>,
    /// The skeleton's `publish`: each worker-local emitter's publish into
    /// its emitter.
    pub publishes: Vec<TokenStream>,
}

impl Codegen {
    /// Returns every IDB's output fragments: its emitter, a size inspector
    /// for `.printsize`, and for `.output` a row inspector, the worker-local
    /// emitter it records into, and that emitter's publish; in an
    /// incremental engine, each static IDB enters the `Epoch` scope first.
    /// Also marks ordered floats when an IDB's columns hold a float.
    pub(crate) fn gen_output(
        &mut self,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Output, CodegenError> {
        let mut output = Output::default();
        with_plan_graph(plan_graph, |p| p.update_inspect_block());

        let clock = self.engine_clock();
        let outputs: Vec<u64> = self
            .program
            .idbs()
            .into_iter()
            .map(|idb| idb.fingerprint())
            .collect();
        for fp in outputs {
            let collection = self.find_global_ident(fp);
            output
                .enters
                .extend(self.gen_epoch_enter(&collection, fp, clock, plan_graph)?);
        }

        for idb in self.program.idbs() {
            let collection = self.find_global_ident(idb.fingerprint());
            let mutability = self.mutability(idb.fingerprint())?;
            let emitter = output_emitter_ident(idb.name());
            let marker = relation_marker_ident(idb.name());
            // Profiler edges use generated bindings; labels keep the
            // relation's spelling in the source program.
            let label = idb.raw_name().to_string();
            output.emitters.push(quote! {
                let #emitter = ::flowlog_runtime::io::output::Emitter::<#marker, Ts>::new();
            });
            output
                .emitter_captures
                .push(quote! { let #emitter = #emitter.clone(); });

            if idb.printsize() {
                with_plan_graph(plan_graph, |p| {
                    p.inspect_size_operator(collection.to_string(), label.clone());
                });
                output
                    .inspectors
                    .push(self.gen_size_inspector(&collection, &emitter, mutability));
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
                let local = local_emitter_ident(idb.name());
                output
                    .local_emitters
                    .push(quote! { let #local = #emitter.worker(); });
                output
                    .inspectors
                    .push(self.gen_row_inspector(&collection, &local, mutability));
                output.publishes.push(quote! { #local.publish(); });
            }
        }

        Ok(output)
    }

    /// Returns the inspector that records, each epoch, the change in
    /// `collection`'s distinct-row count into `emitter`.
    fn gen_size_inspector(
        &self,
        collection: &Ident,
        emitter: &Ident,
        mutability: Mutability,
    ) -> TokenStream {
        // Every relation's dedup already leaves a set: a presence relation
        // announces each row once, and a mutable one's row changes sum to
        // its membership. A batch thus changes the count by its length, or
        // by its weights' sum for a mutable relation. A batch is one
        // message, far below `i32::MAX` updates, so its length fits.
        let count = match mutability {
            Mutability::Static | Mutability::Append => quote! { updates.len() as i32 },
            Mutability::Mutable => quote! { updates.iter().map(|(_, _, diff)| diff).sum() },
        };
        let probe = self.gen_probe();

        // Each worker counts the batches it receives in place and adds each
        // count under one lock. Mapping the rows to `()` and consolidating
        // would build and sort a new batch for each one, and `consolidate`
        // would also send every row to one worker. Every engine reads the
        // size after all workers finish the epoch, so all counts are in by
        // then.
        quote! {{
            let #emitter = #emitter.clone();
            #collection
                .clone()
                .inspect_batch(move |_, updates| {
                    #emitter.record_size(#count);
                })
                #probe;
        }}
    }

    /// Returns the inspector that records each of `collection`'s row changes,
    /// at its time, into the worker-local emitter `local`.
    fn gen_row_inspector(
        &self,
        collection: &Ident,
        local: &Ident,
        mutability: Mutability,
    ) -> TokenStream {
        // A presence relation's dedup announces each row once, so a row
        // arrives as one presence and needs no consolidation.
        let inspected = match mutability {
            Mutability::Static | Mutability::Append => quote! {
                #collection.inspect(move |(data, time, _)| {
                    #local.record(data, time, 1_i32);
                })
            },
            Mutability::Mutable => quote! {
                #collection
                    .consolidate()
                    .inspect(move |(data, time, diff)| {
                        #local.record(data, time, *diff);
                    })
            },
        };
        let probe = self.gen_probe();
        quote! {{
            let #local = #local.clone();
            #inspected #probe;
        }}
    }

    /// Emits the probe an incremental engine attaches to every output, so
    /// it can tell when an epoch's output are complete; nothing in a batch
    /// engine, which runs to completion instead.
    fn gen_probe(&self) -> TokenStream {
        if self.program.is_incremental() {
            quote! { .probe_with(&probe) }
        } else {
            quote! {}
        }
    }
}

#[cfg(test)]
mod tests {
    use quote::format_ident;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;
    use crate::test_harness::strings;

    /// Every IDB gets an emitter, but only an `.output` one gets a
    /// worker-local emitter and a publish, in either engine.
    #[rstest]
    #[case::batch("")]
    #[case::incremental(" mutable")]
    fn only_an_output_relation_gets_a_local_emitter(#[case] mutability: &str) {
        let mut codegen = codegen(&format!(
            ".decl Data(a: int32){mutability}\n.input Data\n\
             .decl Both(a: int32)\nBoth(a) :- Data(a).\n.output Both\n.printsize Both\n\
             .decl Count(a: int32)\nCount(a) :- Data(a).\n.printsize Count\n"
        ));
        let output = codegen.gen_output(&mut None).expect("output");
        assert_eq!(output.emitters.len(), 2);
        // `Both` gets a size and a row inspector, `Count` a size inspector.
        assert_eq!(output.inspectors.len(), 3);
        assert_eq!(
            strings(&output.local_emitters),
            [quote! { let local_emitter_both = emitter_both.worker(); }.to_string()]
        );
        assert_eq!(
            strings(&output.publishes),
            [quote! { local_emitter_both.publish(); }.to_string()]
        );
    }

    /// A presence relation's rows arrive once each, so they record as one
    /// insertion unconsolidated; a mutable one's changes consolidate first.
    /// Only an incremental engine probes.
    // Cases: input mutability, row inspector.
    #[rstest]
    #[case::static_batch(
        "",
        quote! {{
            let local_emitter_r = local_emitter_r.clone();
            r.inspect(move |(data, time, _)| {
                local_emitter_r.record(data, time, 1_i32);
            });
        }}
    )]
    #[case::append(
        " append",
        quote! {{
            let local_emitter_r = local_emitter_r.clone();
            r.inspect(move |(data, time, _)| {
                local_emitter_r.record(data, time, 1_i32);
            })
            .probe_with(&probe);
        }}
    )]
    #[case::mutable(
        " mutable",
        quote! {{
            let local_emitter_r = local_emitter_r.clone();
            r.consolidate()
                .inspect(move |(data, time, diff)| {
                    local_emitter_r.record(data, time, *diff);
                })
                .probe_with(&probe);
        }}
    )]
    fn a_row_inspector_records_each_change(
        #[case] mutability: &str,
        #[case] expected: TokenStream,
    ) {
        let codegen = codegen(&format!(
            ".decl R(a: int32){mutability}\n.input R\n.output R\n"
        ));
        let input_mutability = codegen.program.edbs()[0].input_mutability();
        let tokens = codegen.gen_row_inspector(
            &format_ident!("r"),
            &format_ident!("local_emitter_r"),
            input_mutability,
        );
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    /// A presence relation's batch counts its rows; a mutable one's sums
    /// their weights. Only an incremental engine probes.
    // Cases: input mutability, size inspector.
    #[rstest]
    #[case::static_batch(
        "",
        quote! {{
            let emitter_r = emitter_r.clone();
            r.clone()
                .inspect_batch(move |_, updates| {
                    emitter_r.record_size(updates.len() as i32);
                });
        }}
    )]
    #[case::append(
        " append",
        quote! {{
            let emitter_r = emitter_r.clone();
            r.clone()
                .inspect_batch(move |_, updates| {
                    emitter_r.record_size(updates.len() as i32);
                })
                .probe_with(&probe);
        }}
    )]
    #[case::mutable(
        " mutable",
        quote! {{
            let emitter_r = emitter_r.clone();
            r.clone()
                .inspect_batch(move |_, updates| {
                    emitter_r.record_size(updates.iter().map(|(_, _, diff)| diff).sum());
                })
                .probe_with(&probe);
        }}
    )]
    fn a_size_inspector_counts_at_the_relations_weight(
        #[case] mutability: &str,
        #[case] expected: TokenStream,
    ) {
        let codegen = codegen(&format!(
            ".decl R(a: int32){mutability}\n.input R\n.output R\n"
        ));
        let input_mutability = codegen.program.edbs()[0].input_mutability();
        let tokens = codegen.gen_size_inspector(
            &format_ident!("r"),
            &format_ident!("emitter_r"),
            input_mutability,
        );
        assert_eq!(tokens.to_string(), expected.to_string());
    }
}
