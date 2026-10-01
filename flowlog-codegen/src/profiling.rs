//! Profiling codegen: the observation side of the metrics pipeline.
//!
//! A profiled binary dumps raw observations under `<stem>_log/`:
//! `ops.json` (static plan graph, worker 0, startup) and `metrics/`, an
//! `operators_worker_*`/`channels_worker_*` table pair per worker per
//! transaction. The generated code derives nothing;
//! `flowlog_profiler::metrics` owns the schema and derives tuple flow on
//! read, so change writer and reader together.
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
    /// Among the skeleton's `declarations`: the metric structs, the address
    /// formatter, and the plan graph const.
    pub declarations: TokenStream,
    /// Part of the skeleton's `worker_init`: the maps the metrics land in
    /// and the timely logger filling them, with worker 0's `ops.json` write
    /// and the flush timer.
    pub collectors: TokenStream,
    /// Inside the skeleton's `step_loop`, once per step: re-dumps the
    /// in-flight metrics tables when the flush interval has elapsed, so a
    /// long or interrupted run still leaves the latest snapshot on disk.
    /// Empty with a zero interval.
    pub periodic_flush: TokenStream,
    /// The run's, or the epoch's, final metrics tables. An incremental
    /// engine then zeroes its counters so the next epoch's tables hold
    /// deltas, not running totals.
    pub metrics_write: TokenStream,
}

impl Codegen {
    /// Returns the profiler's fragments for this engine. `plan_graph` is
    /// baked in as it stands, so every pass that records into it has run.
    ///
    /// Errors when profiling is on but no plan graph was recorded: the
    /// fragments would then write a const the declarations never define.
    pub(crate) fn gen_profiling(
        &self,
        plan_graph: Option<&PlanGraph>,
    ) -> Result<Profiling, CodegenError> {
        if !self.config.profiling_enabled() {
            return Ok(Profiling {
                declarations: quote! {},
                collectors: quote! {},
                periodic_flush: quote! {},
                metrics_write: quote! {},
            });
        }

        let incremental = self.program.is_incremental();
        // A batch run's tables are its single transaction's; an incremental
        // engine's belong to the transaction just advanced past.
        let epoch = if incremental {
            quote! { time_stamp - 1 }
        } else {
            quote! { 0 }
        };

        let plan_graph = plan_graph.ok_or_else(|| {
            CodegenError::internal("profiling is on but no plan graph was recorded")
        })?;
        let structs = gen_structs();
        let ops_const = gen_ops_const(plan_graph)?;
        let write_tables = self.gen_write_tables(&epoch);
        let reset = incremental.then(|| {
            quote! {
                // Zero each dimension's contents in place (not back to
                // `None`), so an operator idle this round still reads `0`,
                // not `n/a`.
                for (_id, m) in metrics.borrow_mut().iter_mut() {
                    if let Some(t) = m.time.as_mut() {
                        *t = TimeStats::default();
                    }
                }
                chan_send.borrow_mut().clear();
                chan_recv.borrow_mut().clear();
            }
        });
        Ok(Profiling {
            declarations: quote! { #structs #ops_const },
            collectors: self.gen_collectors(),
            periodic_flush: self.gen_periodic_flush(&write_tables),
            metrics_write: quote! { #write_tables #reset },
        })
    }

    /// Profiling output directory, `<stem>_log` (stem disambiguates programs
    /// sharing a process).
    fn profile_log_dir(&self) -> String {
        format!("{}_log", self.config.program_name())
    }

    /// Emits the maps the write fragments read and the timely logger
    /// filling them, with worker 0's `ops.json` write and the timer the
    /// periodic flush reads (`worker` in scope).
    fn gen_collectors(&self) -> TokenStream {
        let log_dir = self.profile_log_dir();
        let ops_path = format!("{log_dir}/ops.json");
        let interval = self.config.metrics_flush_interval_ms();
        let flush_timer = (interval > 0).then(|| {
            quote! {
                let mut __flowlog_last_flush = std::time::Instant::now();
                let __flowlog_flush_interval = std::time::Duration::from_millis(#interval);
            }
        });

        quote! {
            // Per-operator metrics, keyed by operator id (worker-local).
            let metrics: Rc<RefCell<HashMap<usize, OpMetrics>>> =
                Rc::new(RefCell::new(HashMap::new()));
            // Channel topology: id -> (scope_addr, source idx, source port,
            // target idx, target port, ships-batches). The flag marks
            // channels whose payload is arrangement batches; their message
            // counts are batch handles, not tuples.
            let chan_info: Rc<RefCell<HashMap<usize, (Vec<usize>, usize, usize, usize, usize, bool)>>> =
                Rc::new(RefCell::new(HashMap::new()));
            // Per-channel record volume by direction. Each lands on the
            // operator's own worker (correct for >1).
            let chan_send: Rc<RefCell<HashMap<usize, i64>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let chan_recv: Rc<RefCell<HashMap<usize, i64>>> =
                Rc::new(RefCell::new(HashMap::new()));

            let metrics_log = Rc::clone(&metrics);
            let chan_info_log = Rc::clone(&chan_info);
            let chan_send_log = Rc::clone(&chan_send);
            let chan_recv_log = Rc::clone(&chan_recv);

            // Worker 0 plants the static plan graph beside the runtime logs.
            // Best-effort: a write failure here shouldn't take down the dataflow.
            if worker.index() == 0 {
                let _ = std::fs::create_dir_all(#log_dir);
                // Clear a previous run's metrics: the reader globs the
                // whole directory, so leftovers would merge into this run.
                let _ = std::fs::remove_dir_all(concat!(#log_dir, "/metrics"));
                let _ = std::fs::write(#ops_path, __FLOWLOG_OPS_JSON);
            }

            // Timely stream: identity, time, and channel volume. Profiling
            // is an observation side channel: without a registry it degrades
            // to no metrics, never to a dead dataflow.
            match worker.log_register() {
                Some(mut log_registry) => {
                    log_registry.insert::<TimelyEventBuilder, _>("timely", move |_batch_time, data| {
                        let Some(data) = data else { return; };
                        for (ts, event) in data.iter() {
                            match event {
                                TimelyEvent::Operates(op) => {
                                    let mut map = metrics_log.borrow_mut();
                                    let e = map.entry(op.id).or_default();
                                    e.name = op.name.to_string();
                                    e.addr = op.addr.clone();
                                }
                                TimelyEvent::Schedule(sched) => {
                                    let mut map = metrics_log.borrow_mut();
                                    let t = map
                                        .entry(sched.id)
                                        .or_default()
                                        .time
                                        .get_or_insert_with(Default::default);
                                    match sched.start_stop {
                                        StartStop::Start => {
                                            t.current_start = Some(*ts);
                                        }
                                        StartStop::Stop => {
                                            if let Some(st) = t.current_start.take() {
                                                let delta = ts
                                                    .checked_sub(st)
                                                    .unwrap_or(Duration::ZERO);
                                                t.total_active += delta;
                                                t.activations += 1;
                                            }
                                        }
                                    }
                                }
                                // source/target are scope-local indices; the full
                                // operator addr is `scope_addr ++ [index]`.
                                TimelyEvent::Channels(c) => {
                                    // FlowLog row types never mention "Batch", so
                                    // the data-type name identifies batch-shipping
                                    // (arranged) channels exactly.
                                    let ships_batches = c.typ.contains("Batch");
                                    chan_info_log.borrow_mut().insert(
                                        c.id,
                                        (
                                            c.scope_addr.clone(),
                                            c.source.0,
                                            c.source.1,
                                            c.target.0,
                                            c.target.1,
                                            ships_batches,
                                        ),
                                    );
                                }
                                TimelyEvent::Messages(m) => {
                                    let map = if m.is_send {
                                        &chan_send_log
                                    } else {
                                        &chan_recv_log
                                    };
                                    *map.borrow_mut().entry(m.channel).or_default() +=
                                        m.record_count;
                                }
                                TimelyEvent::PushProgress(_)
                                | TimelyEvent::Shutdown(_)
                                | TimelyEvent::CommChannels(_)
                                | TimelyEvent::Park(_)
                                | TimelyEvent::Text(_) => {}
                            }
                        }
                    });
                }
                None => {
                    eprintln!("flowlog profiling: log registry unavailable, metrics disabled");
                }
            }

            #flush_timer
        }
    }

    /// Emits `write` guarded by the flush timer `gen_collectors` set up:
    /// it runs once `metrics_flush_interval_ms` milliseconds have passed
    /// since the last flush. Empty with a zero interval.
    fn gen_periodic_flush(&self, write: &TokenStream) -> TokenStream {
        if self.config.metrics_flush_interval_ms() == 0 {
            return quote! {};
        }

        quote! {
            if __flowlog_last_flush.elapsed() >= __flowlog_flush_interval {
                #write
                __flowlog_last_flush = std::time::Instant::now();
            }
        }
    }

    /// Emits the dump of the counters as they stand into transaction
    /// `epoch`'s table pair (`index` in scope), leaving them intact: a
    /// periodic flush overwrites the same pair without splitting its delta
    /// across writes. The dump is best-effort end-to-end: profiling never
    /// aborts the profiled run, so a failed create degrades to a warning
    /// and the reader's missing-file handling.
    fn gen_write_tables(&self, epoch: &TokenStream) -> TokenStream {
        let dir = format!("{}/metrics", self.profile_log_dir());
        let ops_fmt = format!("{dir}/operators_worker_t{{}}_{{}}.log");
        let chans_fmt = format!("{dir}/channels_worker_t{{}}_{{}}.log");
        let ops_header = "{:<20} {:<6} {:<11} {}";
        let chans_header = "{:<20} {:<5} {:<9} {:<5} {:<9} {:<6} {:<12} {}";
        quote! {
            {
                let dump = || -> std::io::Result<()> {
                    std::fs::create_dir_all(#dir)?;

                    // Operator table, sorted by numeric address for stable output.
                    let map = metrics.borrow();
                    let mut rows: Vec<&OpMetrics> = map.values().collect();
                    rows.sort_by(|a, b| a.addr.cmp(&b.addr));

                    // Periodic flushes can be interrupted mid-write, so route the
                    // table through an atomic write.
                    ::flowlog_runtime::io::write_atomic(format!(#ops_fmt, #epoch, index), |w| {
                        writeln!(w, #ops_header, "addr", "acts", "active_ms", "name")?;
                        for m in &rows {
                            // Non-applicable dimensions print `n/a`.
                            let (acts, active_ms) = m.time.as_ref().map_or_else(
                                || ("n/a".to_string(), "n/a".to_string()),
                                |t| (
                                    t.activations.to_string(),
                                    format!("{:.3}", t.total_active.as_secs_f64() * 1000.0),
                                ),
                            );
                            writeln!(w, #ops_header, fmt_addr(&m.addr), acts, active_ms, m.name)?;
                        }
                        if rows.is_empty() {
                            writeln!(w, "(no operators recorded)")?;
                        }
                        Ok(())
                    })?;

                    // Channel table, sorted by topology for stable output.
                    let info = chan_info.borrow();
                    let sends = chan_send.borrow();
                    let recvs = chan_recv.borrow();
                    let mut chans: Vec<_> = info
                        .iter()
                        .map(|(id, (scope, src, src_port, tgt, tgt_port, batch))| {
                            (
                                scope,
                                src,
                                src_port,
                                tgt,
                                tgt_port,
                                u8::from(*batch),
                                sends.get(id).copied().unwrap_or(0),
                                recvs.get(id).copied().unwrap_or(0),
                            )
                        })
                        .collect();
                    chans.sort();

                    ::flowlog_runtime::io::write_atomic(format!(#chans_fmt, #epoch, index), |w| {
                        writeln!(
                            w,
                            #chans_header,
                            "scope", "src", "src_port", "tgt", "tgt_port", "batch", "sent", "recvd"
                        )?;
                        for (scope, src, src_port, tgt, tgt_port, batch, sent, recvd) in chans {
                            writeln!(
                                w,
                                #chans_header,
                                fmt_addr(scope), src, src_port, tgt, tgt_port, batch, sent, recvd
                            )?;
                        }
                        Ok(())
                    })
                };
                if let Err(e) = dump() {
                    eprintln!("flowlog profiling: metrics dump into {} failed: {e}", #dir);
                }
            }
        }
    }
}

// =============================================================================
// Module-scope fragments
// =============================================================================

/// Emits the metric structs and the address formatter; the other
/// fragments reference them unqualified.
fn gen_structs() -> TokenStream {
    quote! {
        /// Scheduling stats (from `TimelyEvent::Schedule`).
        #[derive(Clone, Debug, Default)]
        struct TimeStats {
            /// Total time the operator spent scheduled on this worker.
            total_active: Duration,
            /// Number of times the operator was scheduled (Stop events).
            activations: u64,
            /// Timestamp of the last Start event, used to compute deltas.
            current_start: Option<Duration>,
        }

        /// Per-operator metrics. `time` is `None` until the first event
        /// (a dimension that doesn't apply writes `n/a`).
        #[derive(Clone, Debug, Default)]
        struct OpMetrics {
            /// Operator name and address path (from `TimelyEvent::Operates`).
            name: String,
            addr: Vec<usize>,
            time: Option<TimeStats>,
        }

        /// The wire form of an operator or scope address, as the reader's
        /// `Addr` parser expects it.
        fn fmt_addr(addr: &[usize]) -> String {
            let cells: Vec<String> = addr.iter().map(|x| x.to_string()).collect();
            format!("[{}]", cells.join(", "))
        }
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

    const BATCH: &str = ".decl Edge(a: int32, b: int32)\n.input Edge(IO=\"file\")\n";
    const INCREMENTAL: &str =
        ".decl Edge(a: int32, b: int32) mutable\n.input Edge(IO=\"command\")\n";

    /// Returns a plan graph with one operator recorded.
    fn plan_graph() -> PlanGraph {
        let mut graph = PlanGraph::new(false);
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
        assert!(profiling.collectors.is_empty());
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

    /// The worker init writes the plan graph const unconditionally, so
    /// profiling without a recorded graph is a codegen bug, not a quiet
    /// omission.
    #[test]
    fn profiling_without_a_plan_graph_is_an_internal_error() {
        let error = profiled(BATCH, 0)
            .gen_profiling(None)
            .expect_err("no plan graph to bake");
        assert!(matches!(error, CodegenError::Internal(_)));
    }

    /// Each engine names a table pair after the transaction it closes: a
    /// batch run's single `t0`, an incremental epoch's `time_stamp - 1`.
    // Cases: source, path arguments.
    #[rstest]
    #[case(BATCH, quote! { 0, index })]
    #[case(INCREMENTAL, quote! { time_stamp - 1, index })]
    fn tables_are_named_after_the_closing_transaction(
        #[case] source: &str,
        #[case] path_args: TokenStream,
    ) {
        let profiling = profiled(source, 0)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        // The default config has no program path, hence the stem.
        let path = "unknown_program_log/metrics/operators_worker_t{}_{}.log";
        let expected = quote! { format!(#path, #path_args) }.to_string();
        assert!(profiling.metrics_write.to_string().contains(&expected));
    }

    /// Only an incremental engine's write resets the counters, so each
    /// epoch's tables hold deltas; a batch run's final tables need none.
    // Cases: source, resets.
    #[rstest]
    #[case(BATCH, false)]
    #[case(INCREMENTAL, true)]
    fn only_the_incremental_write_resets_the_counters(#[case] source: &str, #[case] resets: bool) {
        let profiling = profiled(source, 0)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        let write = profiling.metrics_write.to_string();
        assert!(write.contains("write_atomic"));
        assert_eq!(write.contains("TimeStats :: default"), resets);
    }

    /// The timer and the flush that reads it come and go together.
    // Cases: interval_ms, flushes.
    #[rstest]
    #[case(0, false)]
    #[case(5000, true)]
    fn periodic_flush_is_emitted_only_with_a_nonzero_interval(
        #[case] interval_ms: u64,
        #[case] flushes: bool,
    ) {
        let profiling = profiled(BATCH, interval_ms)
            .gen_profiling(Some(&plan_graph()))
            .expect("serializes");
        let collectors = profiling.collectors.to_string();
        let periodic_flush = profiling.periodic_flush.to_string();
        assert_eq!(collectors.contains("__flowlog_last_flush"), flushes);
        assert_eq!(periodic_flush.contains("write_atomic"), flushes);
        assert_eq!(periodic_flush.is_empty(), !flushes);
    }
}
