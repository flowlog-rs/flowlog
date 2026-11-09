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
use crate::ty::time::Clock;

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
    /// The skeleton's `publish`: each worker-local emitter's publish into
    /// its emitter.
    pub publishes: Vec<TokenStream>,
}

impl Codegen {
    /// Returns every IDB's output fragments: its emitter, and for `.output`
    /// the worker-local emitter its row inspector records into, and that
    /// emitter's publish. [`Self::gen_inspectors`] returns the inspectors.
    pub(crate) fn gen_output(&self) -> Output {
        let mut output = Output::default();
        for idb in self.program.idbs() {
            let emitter = output_emitter_ident(idb.name());
            let marker = relation_marker_ident(idb.name());
            output.emitters.push(quote! {
                let #emitter = ::flowlog_runtime::io::output::Emitter::<#marker, Ts>::new();
            });
            output
                .emitter_captures
                .push(quote! { let #emitter = #emitter.clone(); });

            if idb.has_output() {
                let local = local_emitter_ident(idb.name());
                output
                    .local_emitters
                    .push(quote! { let #local = #emitter.worker(); });
                output.publishes.push(quote! { #local.publish(); });
            }
        }
        output
    }

    /// Returns the inspectors of every IDB at `clock`, for the scope at
    /// `clock` to hold: a size inspector for `.printsize`, and a row
    /// inspector for `.output`. Each scope inspects the outputs it
    /// computes, so a static output's updates never enter an incremental
    /// engine's `Epoch` scope, which would wake on each batch of them.
    pub(crate) fn gen_inspectors(
        &self,
        clock: Clock,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let mut inspectors = Vec::new();
        with_plan_graph(plan_graph, |p| p.update_inspect_block());

        for idb in self.program.idbs() {
            let mutability = self.mutability(idb.fingerprint())?;
            if Clock::of(mutability) != clock {
                continue;
            }
            let collection = self.find_global_ident(idb.fingerprint());
            // Profiler edges use generated bindings; labels keep the
            // relation's spelling in the source program.
            let label = idb.raw_name().to_string();

            if idb.printsize() {
                with_plan_graph(plan_graph, |p| {
                    p.inspect_size_operator(collection.to_string(), label.clone(), mutability);
                });
                inspectors.push(self.gen_size_inspector(
                    &collection,
                    &output_emitter_ident(idb.name()),
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
                inspectors.push(self.gen_row_inspector(
                    &collection,
                    &local_emitter_ident(idb.name()),
                    mutability,
                ));
            }
        }

        Ok(inspectors)
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
        // Every relation's dedup already leaves a set: a presence relation
        // announces each row once, and a mutable one's row changes sum to
        // its membership. Counting therefore needs no second dedup, only a
        // signed weight, which a presence must first lift to.
        let op_name = format!("{name}: inspect size");
        let counted = match mutability {
            Mutability::Static | Mutability::Append => quote! {
                ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                    #collection.clone(), #op_name,
                )
            },
            Mutability::Mutable => quote! { #collection.clone() },
        };
        let probe = self.gen_probe(mutability);

        // Each worker sums its own rows; the emitter adds the partial sums,
        // one locked add per batch. `consolidate` would exchange every row
        // to the worker that owns `()` and sort them there just to count
        // them. Every engine reads the size after all workers finish the
        // epoch, so all partial sums are in by then.
        quote! {{
            let #emitter = #emitter.clone();
            #counted
                .map(|_| ())
                .consolidate_stream()
                .inspect(move |(_data, _time, size)| {
                    #emitter.record_size(*size);
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
        // arrives as one presence and needs no consolidation. A static row
        // is inspected where it is computed, at `()`, and records at the
        // engine's first time, the time it would enter at.
        let inspected = match mutability {
            Mutability::Static => quote! {
                #collection.inspect(move |(data, _time, _)| {
                    #local.record(
                        data,
                        &::flowlog_runtime::timely::progress::Timestamp::minimum(),
                        1_i32,
                    );
                })
            },
            Mutability::Append => quote! {
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
        let probe = self.gen_probe(mutability);
        quote! {{
            let #local = #local.clone();
            #inspected #probe;
        }}
    }

    /// Emits the probe an incremental engine attaches to an output with
    /// `mutability`, so it can tell when the output is complete: a static
    /// output's is `once_probe`, in the dataflow's own scope, and any
    /// other's is `probe`, in the nested one. Nothing in a batch engine,
    /// which runs to completion instead.
    fn gen_probe(&self, mutability: Mutability) -> TokenStream {
        if !self.program.is_incremental() {
            return quote! {};
        }
        match Clock::of(mutability) {
            Clock::Once => quote! { .probe_with(&once_probe) },
            Clock::Epoch => quote! { .probe_with(&probe) },
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
        let codegen = codegen(&format!(
            ".decl Data(a: int32){mutability}\n.input Data\n\
             .decl Both(a: int32)\nBoth(a) :- Data(a).\n.output Both\n.printsize Both\n\
             .decl Count(a: int32)\nCount(a) :- Data(a).\n.printsize Count\n"
        ));
        let output = codegen.gen_output();
        assert_eq!(output.emitters.len(), 2);
        assert_eq!(
            strings(&output.local_emitters),
            [quote! { let local_emitter_both = emitter_both.worker(); }.to_string()]
        );
        assert_eq!(
            strings(&output.publishes),
            [quote! { local_emitter_both.publish(); }.to_string()]
        );
    }

    /// Each scope inspects the outputs at its clock, so a static output's
    /// updates never enter the `Epoch` scope: `once_probe` probes them in
    /// the dataflow's own scope, and `probe` the rest in the nested one.
    #[test]
    fn each_scope_inspects_the_outputs_at_its_clock() {
        let codegen = codegen(
            ".decl S(a: int32)\n.input S\n.output S\n.printsize S\n\
             .decl M(a: int32) mutable\n.input M\n.printsize M\n",
        );
        // `S` gets a size and a row inspector, `M` a size inspector.
        for (clock, count, probe) in [
            (Clock::Once, 2, quote! { .probe_with(&once_probe) }),
            (Clock::Epoch, 1, quote! { .probe_with(&probe) }),
        ] {
            let inspectors = strings(
                codegen
                    .gen_inspectors(clock, &mut None)
                    .expect("inspectors"),
            );
            assert_eq!(inspectors.len(), count, "{inspectors:?}");
            let probe = probe.to_string();
            assert!(
                inspectors
                    .iter()
                    .all(|inspector| inspector.contains(&probe)),
                "{inspectors:?}"
            );
        }
    }

    /// A presence relation's rows arrive once each, so they record as one
    /// insertion unconsolidated, a static one's at the engine's first time;
    /// a mutable one's changes consolidate first. Only an incremental
    /// engine probes.
    // Cases: input mutability, row inspector.
    #[rstest]
    #[case::static_batch(
        "",
        quote! {{
            let local_emitter_r = local_emitter_r.clone();
            r.inspect(move |(data, _time, _)| {
                local_emitter_r.record(
                    data,
                    &::flowlog_runtime::timely::progress::Timestamp::minimum(),
                    1_i32,
                );
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

    /// A presence cannot sum into a count, so a static or append relation
    /// lifts to a signed count before counting; a mutable one's counts sum
    /// as they are. Only an incremental engine probes.
    // Cases: input mutability, size inspector.
    #[rstest]
    #[case::static_batch(
        "",
        quote! {{
            let emitter_r = emitter_r.clone();
            ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                r.clone(), "R: inspect size",
            )
            .map(|_| ())
            .consolidate_stream()
            .inspect(move |(_data, _time, size)| {
                emitter_r.record_size(*size);
            });
        }}
    )]
    #[case::append(
        " append",
        quote! {{
            let emitter_r = emitter_r.clone();
            ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                r.clone(), "R: inspect size",
            )
            .map(|_| ())
            .consolidate_stream()
            .inspect(move |(_data, _time, size)| {
                emitter_r.record_size(*size);
            })
            .probe_with(&probe);
        }}
    )]
    #[case::mutable(
        " mutable",
        quote! {{
            let emitter_r = emitter_r.clone();
            r.clone()
                .map(|_| ())
                .consolidate_stream()
                .inspect(move |(_data, _time, size)| {
                    emitter_r.record_size(*size);
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
            "R",
            input_mutability,
        );
        assert_eq!(tokens.to_string(), expected.to_string());
    }
}
