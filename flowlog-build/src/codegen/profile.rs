//! Profiling codegen: emits an optional per-operator metrics table, folding
//! three sources keyed by timely operator id / address:
//!
//! - **Time** (timely `Schedule`): active time + activations.
//! - **Flow** (timely `Channels`+`Messages`): tuples in/out — makes a join's
//!   intermediate-result size visible, which the differential stream can't
//!   (a join owns no arrangement).
//! - **Arrangement** (differential `Batch`/`Merge`/`Drop`/`Batcher`): trace
//!   residency, compaction churn, batcher bytes.
//!
//! Both loggers fold into one `HashMap<operator_id, OpMetrics>`. A `Messages`
//! event carries only a channel id, so flow is buffered and resolved to
//! operator addresses at write time. Cells that don't apply print `n/a`, not `0`.
//!
//! Output (per worker, cwd-relative, namespaced by program stem):
//! - `<stem>_log/ops.json` — static plan graph, baked in as a `const &str` and
//!   written by worker 0 at startup.
//! - `<stem>_log/metrics/metrics_worker_t{t}_{index}.log` — `t`=0 batch, txn
//!   time incremental.

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;
use crate::profiler::Profiler;

impl CodeGen {
    /// Profiler output directory, `<stem>_log` (stem disambiguates programs
    /// sharing a process).
    fn profile_log_dir(&self) -> String {
        format!("{}_log", self.config.program_name())
    }

    /// Emits the per-operator metrics structs (profiling builds only).
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

            /// Arrangement stats (from `DifferentialEvent`).
            #[derive(Clone, Debug, Default)]
            struct ArrangeStats {
                /// Total records entering the arrangement (summed over batches).
                batch_total_len: usize,
                /// Number of completed merge (compaction) events.
                merge_completes: u64,
                /// Sum of input lengths across merge completions.
                merge_input_total: usize,
                /// Sum of output lengths across merge completions.
                merge_output_total: usize,
                /// Total records freed across drops.
                drop_total_len: usize,
                /// Running batcher bytes used / allocated (net of deltas), kept
                /// only to derive the peaks.
                batcher_size: isize,
                batcher_capacity: isize,
                /// Peak batcher bytes used / allocated — the byte-level signal
                /// (the running values return to ~0 once batchers flush).
                batcher_size_peak: isize,
                batcher_capacity_peak: isize,
            }

            /// Tuple flow, resolved from channels at write time. Each direction
            /// is `None` when the operator has no edge there (a source has no
            /// `tup_in`, a sink no `tup_out`).
            #[derive(Clone, Debug, Default)]
            struct FlowStats {
                tup_in: Option<i64>,
                tup_out: Option<i64>,
            }

            /// Per-operator metrics. `time`/`arrange` are `None` until the first
            /// event (a dimension that doesn't apply writes `n/a`); `flow` is
            /// per-direction.
            #[derive(Clone, Debug, Default)]
            struct OpMetrics {
                /// Operator name and address path (from `TimelyEvent::Operates`).
                name: String,
                addr: Vec<usize>,
                time: Option<TimeStats>,
                arrange: Option<ArrangeStats>,
                flow: FlowStats,
            }
        }
    }

    /// Emits the two logger registrations (timely + differential) that fold
    /// into the shared metrics map, plus worker 0's `ops.json` write.
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
            // Channel topology: id -> (scope_addr, source idx, target idx).
            let chan_info: Rc<RefCell<HashMap<usize, (Vec<usize>, usize, usize)>>> =
                Rc::new(RefCell::new(HashMap::new()));
            // Per-channel volume by direction: sends → tup_out, receives →
            // tup_in. Each lands on the operator's own worker (correct for >1).
            let chan_send: Rc<RefCell<HashMap<usize, i64>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let chan_recv: Rc<RefCell<HashMap<usize, i64>>> =
                Rc::new(RefCell::new(HashMap::new()));

            let metrics_timely = Rc::clone(&metrics);
            let chan_info_log = Rc::clone(&chan_info);
            let chan_send_log = Rc::clone(&chan_send);
            let chan_recv_log = Rc::clone(&chan_recv);

            // Worker 0 plants the static plan graph beside the runtime logs.
            // Best-effort — a write failure here shouldn't take down the dataflow.
            if worker.index() == 0 {
                let _ = std::fs::create_dir_all(#log_dir);
                let _ = std::fs::write(#ops_path, __FLOWLOG_OPS_JSON);
            }

            // Timely stream: identity, time, and flow.
            worker
                .log_register()
                .expect("failed to get log_register")
                .insert::<TimelyEventBuilder, _>("timely", move |_batch_time, data| {
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
                                chan_info_log.borrow_mut().insert(
                                    c.id,
                                    (c.scope_addr.clone(), c.source.0, c.target.0),
                                );
                            }
                            // Accumulate send/receive volume per channel.
                            TimelyEvent::Messages(m) => {
                                let map = if m.is_send {
                                    &chan_send_log
                                } else {
                                    &chan_recv_log
                                };
                                *map.borrow_mut().entry(m.channel).or_default() +=
                                    m.record_count;
                            }
                            _ => {}
                        }
                    }
                });

            // Differential stream: arrangement residency + compaction.
            let metrics_diff = Rc::clone(&metrics);
            worker
                .log_register()
                .expect("failed to get log_register")
                .insert::<DifferentialEventBuilder, _>(
                    "differential/arrange",
                    move |_batch_time, data| {
                        let Some(data) = data else { return; };
                        for (_ts, event) in data.iter() {
                            match event {
                                DifferentialEvent::Batch(b) => {
                                    let mut map = metrics_diff.borrow_mut();
                                    let a = map.entry(b.operator).or_default()
                                        .arrange.get_or_insert_with(Default::default);
                                    a.batch_total_len += b.length;
                                }
                                DifferentialEvent::Merge(m) => {
                                    if let Some(complete_len) = m.complete {
                                        let mut map = metrics_diff.borrow_mut();
                                        let a = map.entry(m.operator).or_default()
                                            .arrange.get_or_insert_with(Default::default);
                                        a.merge_completes += 1;
                                        a.merge_input_total += m.length1 + m.length2;
                                        a.merge_output_total += complete_len;
                                    }
                                    // ignore merge-start (no size info)
                                }
                                DifferentialEvent::Drop(d) => {
                                    let mut map = metrics_diff.borrow_mut();
                                    let a = map.entry(d.operator).or_default()
                                        .arrange.get_or_insert_with(Default::default);
                                    a.drop_total_len += d.length;
                                }
                                DifferentialEvent::Batcher(b) => {
                                    let mut map = metrics_diff.borrow_mut();
                                    let a = map.entry(b.operator).or_default()
                                        .arrange.get_or_insert_with(Default::default);
                                    a.batcher_size += b.size_diff;
                                    a.batcher_capacity += b.capacity_diff;
                                    a.batcher_size_peak = a.batcher_size_peak.max(a.batcher_size);
                                    a.batcher_capacity_peak =
                                        a.batcher_capacity_peak.max(a.batcher_capacity);
                                }
                                _ => {} // MergeShortfall, TraceShare: not tracked
                            }
                        }
                    },
                );
        }
    }

    /// Emits the unified metrics write-out for **batch** mode.
    ///
    /// Writes one file per worker: `<stem>_log/metrics/metrics_worker_t0_{index}.log`.
    pub(crate) fn gen_metrics_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/metrics", self.profile_log_dir());
        let path_fmt = format!("{dir}/metrics_worker_t0_{{}}.log");
        gen_metrics_write_core(&dir, quote! { format!(#path_fmt, index) })
    }

    /// Emits the **incremental** write-out: one file per worker per committed
    /// transaction, then resets per-transaction counters (keeping identity and
    /// topology).
    pub(crate) fn gen_metrics_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/metrics", self.profile_log_dir());
        let path_fmt = format!("{dir}/metrics_worker_t{{}}_{{}}.log");
        let write =
            gen_metrics_write_core(&dir, quote! { format!(#path_fmt, time_stamp - 1, index) });

        quote! {
            #write

            // Zero each dimension's contents in place (not back to `None`), so
            // an operator idle this round still reads `0`, not `n/a`.
            for (_id, m) in metrics.borrow_mut().iter_mut() {
                if let Some(t) = m.time.as_mut() {
                    *t = TimeStats::default();
                }
                if let Some(a) = m.arrange.as_mut() {
                    *a = ArrangeStats::default();
                }
            }
            chan_send.borrow_mut().clear();
            chan_recv.borrow_mut().clear();
        }
    }
}

// =================================================================
// Private helper functions (not tied to CodeGen state)
// =================================================================

/// Render the static plan-graph profiler as a `const &str` baked into the
/// generated module. Worker 0 writes it to `<stem>_log/ops.json` on startup
/// (see [`CodeGen::gen_metrics_init`]).
///
/// `None` profiler → empty token stream so non-profile builds carry no dead const.
pub(crate) fn render_profile_ops_const(profiler: Option<&Profiler>) -> TokenStream {
    let Some(profiler) = profiler else {
        return quote! {};
    };
    let json = profiler.to_json_string();
    quote! {
        const __FLOWLOG_OPS_JSON: &str = #json;
    }
}

/// Resolve flow, then write one row per operator. A block so borrows drop.
fn gen_metrics_write_core(dir: &str, file_path_expr: TokenStream) -> TokenStream {
    let create_msg = format!("failed to create {dir} directory");
    let header =
        "{:<20} {:<6} {:<11} {:<10} {:<10} {:<10} {:<7} {:<11} {:<11} {:<10} {:<12} {:<12} {}";
    quote! {
        {
            // Resolve buffered channel volume into per-operator flow (a
            // `Messages` event only knows its channel, so it's joined against
            // the topology here, not accumulated live like time/arrange).
            {
                let info = chan_info.borrow();
                let sends = chan_send.borrow();
                let recvs = chan_recv.borrow();

                // Operator address of a channel endpoint, `scope_addr ++ [idx]`;
                // `None` for index 0 — the scope boundary, not a leaf operator
                // (its I/O is on the parent-scope edges).
                let endpoint_addr = |scope_addr: &Vec<usize>, idx: usize| -> Option<Vec<usize>> {
                    (idx != 0).then(|| {
                        let mut a = scope_addr.clone();
                        a.push(idx);
                        a
                    })
                };
                let mut out_by_addr: HashMap<Vec<usize>, i64> = HashMap::new();
                let mut in_by_addr: HashMap<Vec<usize>, i64> = HashMap::new();

                // `or_insert(0)` seeds connected endpoints (zero flow → `0`,
                // missing edge → `n/a`), then add this channel's volume.
                for (chan, (scope_addr, src, tgt)) in info.iter() {
                    if let Some(a) = endpoint_addr(scope_addr, *src) {
                        *out_by_addr.entry(a).or_insert(0) +=
                            sends.get(chan).copied().unwrap_or(0);
                    }
                    if let Some(a) = endpoint_addr(scope_addr, *tgt) {
                        *in_by_addr.entry(a).or_insert(0) +=
                            recvs.get(chan).copied().unwrap_or(0);
                    }
                }
                for m in metrics.borrow_mut().values_mut() {
                    m.flow.tup_out = out_by_addr.get(&m.addr).copied();
                    m.flow.tup_in = in_by_addr.get(&m.addr).copied();
                }
            }

            // Sort by numeric address for stable output.
            let map = metrics.borrow();
            let mut rows: Vec<&OpMetrics> = map.values().collect();
            rows.sort_by(|a, b| a.addr.cmp(&b.addr));

            std::fs::create_dir_all(#dir).expect(#create_msg);
            let f = File::create(#file_path_expr)
                .expect("failed to create metrics log file");
            let mut w = BufWriter::new(f);

            writeln!(
                w,
                #header,
                "addr", "acts", "active_ms", "tup_in", "tup_out",
                "arr_in", "merges", "merge_in", "merge_out", "dropped",
                "bat_bytes", "bat_cap", "name"
            ).ok();

            // Non-applicable dimensions print `n/a`.
            let na = || "n/a".to_string();
            for m in &rows {
                let (acts, active_ms) = m.time.as_ref().map_or_else(
                    || (na(), na()),
                    |t| (
                        t.activations.to_string(),
                        format!("{:.3}", t.total_active.as_secs_f64() * 1000.0),
                    ),
                );
                let tup_in = m.flow.tup_in.map_or_else(na, |v| v.to_string());
                let tup_out = m.flow.tup_out.map_or_else(na, |v| v.to_string());
                let (arr_in, merges, merge_in, merge_out, dropped, bat_bytes, bat_cap) =
                    m.arrange.as_ref().map_or_else(
                        || (na(), na(), na(), na(), na(), na(), na()),
                        |a| (
                            a.batch_total_len.to_string(),
                            a.merge_completes.to_string(),
                            a.merge_input_total.to_string(),
                            a.merge_output_total.to_string(),
                            a.drop_total_len.to_string(),
                            a.batcher_size_peak.to_string(),
                            a.batcher_capacity_peak.to_string(),
                        ),
                    );
                writeln!(
                    w,
                    #header,
                    format!("{:?}", m.addr), acts, active_ms, tup_in, tup_out,
                    arr_in, merges, merge_in, merge_out, dropped, bat_bytes, bat_cap, m.name
                ).ok();
            }

            if rows.is_empty() {
                writeln!(w, "(no operators recorded)").ok();
            }
            w.flush().ok();
        }
    }
}
