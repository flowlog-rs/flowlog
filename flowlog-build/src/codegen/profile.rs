//! Profiling codegen: the observation side of the metrics pipeline.
//!
//! A profiled binary dumps raw observations under `<stem>_log/`:
//! `ops.json` (static plan graph, worker 0, startup) and `metrics/`, an
//! `operators_worker_*`/`channels_worker_*` table pair per worker per
//! transaction. The generated code derives nothing;
//! `flowlog_profiler::metrics` owns the schema and derives tuple flow on
//! read, so change writer and reader together.
//!
//! Fragment assembly: [`CodeGen::gen_metrics_struct`] at module scope,
//! [`CodeGen::gen_metrics_init`] in the worker closure, one write
//! fragment per mode at its flush points. All empty without profiling.

use flowlog_profiler::PlanGraph;
use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;
use crate::codegen::error::CodegenError;

impl CodeGen {
    /// Profiling output directory, `<stem>_log` (stem disambiguates programs
    /// sharing a process).
    fn profile_log_dir(&self) -> String {
        format!("{}_log", self.config.program_name())
    }

    /// Emits the metric structs and address formatter at module scope;
    /// the other fragments reference them unqualified.
    pub(crate) fn gen_metrics_struct(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

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

    /// Emits collection setup for the top of the worker closure (`worker`
    /// in scope): the observation maps the write fragments read, the
    /// timely logger feeding them, and worker 0's `ops.json` write.
    pub(crate) fn gen_metrics_init(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let log_dir = self.profile_log_dir();
        let ops_path = format!("{log_dir}/ops.json");

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

            let metrics_timely = Rc::clone(&metrics);
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
                                    let mut map = metrics_timely.borrow_mut();
                                    let e = map.entry(op.id).or_default();
                                    e.name = op.name.to_string();
                                    e.addr = op.addr.clone();
                                }
                                TimelyEvent::Schedule(sched) => {
                                    let mut map = metrics_timely.borrow_mut();
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
        }
    }

    /// Emits the batch write-out for after the dataflow drains (`index`
    /// in scope): one `t0` table pair per worker.
    pub(crate) fn gen_metrics_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/metrics", self.profile_log_dir());
        let ops_fmt = format!("{dir}/operators_worker_t0_{{}}.log");
        let chans_fmt = format!("{dir}/channels_worker_t0_{{}}.log");
        gen_metrics_write_core(
            &dir,
            quote! { format!(#ops_fmt, index) },
            quote! { format!(#chans_fmt, index) },
        )
    }

    /// Emits the incremental write-out for the commit path (`time_stamp`
    /// and `index` in scope): one table pair for the just-committed
    /// transaction, then a counter reset; snapshots are deltas, not
    /// running totals.
    pub(crate) fn gen_metrics_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/metrics", self.profile_log_dir());
        let ops_fmt = format!("{dir}/operators_worker_t{{}}_{{}}.log");
        let chans_fmt = format!("{dir}/channels_worker_t{{}}_{{}}.log");
        let write = gen_metrics_write_core(
            &dir,
            quote! { format!(#ops_fmt, time_stamp - 1, index) },
            quote! { format!(#chans_fmt, time_stamp - 1, index) },
        );

        quote! {
            #write

            // Zero each dimension's contents in place (not back to `None`), so
            // an operator idle this round still reads `0`, not `n/a`.
            for (_id, m) in metrics.borrow_mut().iter_mut() {
                if let Some(t) = m.time.as_mut() {
                    *t = TimeStats::default();
                }
            }
            chan_send.borrow_mut().clear();
            chan_recv.borrow_mut().clear();
        }
    }
}

// =============================================================================
// Plan-graph const and shared write-out
// =============================================================================

/// Renders the recorded plan graph as a `const &str` baked into the
/// generated module. Errors only if the plan graph fails to serialize,
/// which a well-formed graph never does: an internal error, not a user
/// mistake.
///
/// A `None` plan graph renders an empty token stream so non-profile builds
/// carry no dead const.
pub(crate) fn render_profile_ops_const(
    plan_graph: Option<&PlanGraph>,
) -> Result<TokenStream, CodegenError> {
    let Some(plan_graph) = plan_graph else {
        return Ok(quote! {});
    };
    let json = plan_graph
        .to_json_string()
        .map_err(|e| CodegenError::internal(format!("plan graph failed to serialize: {e}")))?;
    Ok(quote! {
        const __FLOWLOG_OPS_JSON: &str = #json;
    })
}

/// Emits the operator and channel table write-out shared by batch and
/// incremental modes. The dump is best-effort end-to-end: profiling never
/// aborts the profiled run, so a failed create degrades to a warning and
/// the reader's missing-file handling.
fn gen_metrics_write_core(
    dir: &str,
    ops_path_expr: TokenStream,
    chans_path_expr: TokenStream,
) -> TokenStream {
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

                let mut w = BufWriter::new(File::create(#ops_path_expr)?);
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
                w.flush()?;

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

                let mut w = BufWriter::new(File::create(#chans_path_expr)?);
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
                w.flush()
            };
            if let Err(e) = dump() {
                eprintln!("flowlog profiling: metrics dump into {} failed: {e}", #dir);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::ExecutionMode;

    use super::*;

    /// A `None` plan graph must render nothing, so non-profile builds
    /// carry no dead const.
    #[test]
    fn none_plan_graph_renders_no_tokens() {
        let ts = render_profile_ops_const(None).expect("None cannot fail");
        assert!(ts.is_empty());
    }

    /// A recorded plan graph bakes the ops const with its JSON payload.
    #[test]
    fn recorded_plan_graph_renders_the_ops_const() {
        let mut graph = PlanGraph::new(ExecutionMode::DatalogBatch);
        graph.map_join_operator("n".into(), vec![], "a".into(), 1);
        let ts = render_profile_ops_const(Some(&graph)).expect("serializes");
        let rendered = ts.to_string();
        assert!(rendered.contains("__FLOWLOG_OPS_JSON"));
        assert!(rendered.contains("nodes"));
    }
}
