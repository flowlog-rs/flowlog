//! Profiling code generation for FlowLog.
//!
//! This module emits optional profiling support into the generated source.
//!
//! **Time profiling** (timely dataflow level):
//! Registers a timely logger to aggregate per-operator active time and
//! activation count on each worker.
//!
//! **Memory profiling** (differential dataflow level):
//! Registers a differential dataflow arrangement logger to track batch,
//! merge, drop, and batcher events per operator (arrangement memory usage).
//!
//! Output layout — all paths are cwd-relative at runtime. The directory
//! is `<stem>_log_<YYYYMMDD_HHMMSS>`, timestamped at compile time (see
//! `Config::profile_log_dir`) so successive compiles write to distinct
//! folders instead of clobbering one another:
//!
//! - `<dir>/ops.json`                                  (static plan graph)
//! - `<dir>/time/time_worker_t0_{index}.log`           (batch)
//! - `<dir>/time/time_worker_t{time_stamp}_{index}.log`   (incremental)
//! - `<dir>/memory/memory_worker_t0_{index}.log`       (batch)
//! - `<dir>/memory/memory_worker_t{time_stamp}_{index}.log`  (incremental)
//!
//! `ops.json` is baked into the generated source as a `const &str` and
//! written on engine startup by worker 0, so it lands in the same folder
//! as the runtime logs without any compile-time disk write.

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::CodeGen;
use crate::profiler::Profiler;

impl CodeGen {
    // =================================================================
    // Time profiling (timely dataflow level)
    // =================================================================

    /// Generates the per-operator time profiling data structure.
    ///
    /// This is emitted into the generated `main.rs` only when profiling is enabled.
    /// The struct stores worker-local aggregate timing and activation statistics per operator.
    pub(crate) fn gen_time_profile_struct(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            /// Worker-local aggregate profiling stats for a timely operator.
            #[derive(Clone, Debug, Default)]
            struct OpStats {
                /// Human-readable operator name (captured from `TimelyEvent::Operates`).
                name: String,
                /// Debug-printed operator address path (e.g. `[0, 8, 4]`).
                addr: String,
                /// Total time the operator spent scheduled/running on this worker.
                total_active: Duration,
                /// Number of times the operator was scheduled (Stop events counted).
                activations: u64,
                /// Timestamp of the last Start event (if any), used to compute deltas.
                current_start: Option<Duration>,
            }
        }
    }

    /// Generates timely logging registration code for time profiling.
    ///
    /// The generated code registers a timely logger under the `"timely"` stream and aggregates:
    /// - `Operates` events (operator name + address)
    /// - `Schedule` events (Start/Stop pairs → total active time + activation count)
    ///
    /// Worker 0 also drops `<stem>_log/ops.json` (the static plan graph baked
    /// in as `__FLOWLOG_OPS_JSON`) so the visualizer finds it next to the
    /// runtime logs.
    ///
    /// Notes:
    /// - This is worker-local aggregation: each worker maintains its own `HashMap<operator_id, OpStats>`.
    /// - Timely's log callback may deliver multiple events per batch; we fold them into aggregates.
    pub(crate) fn gen_time_profile_init(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let log_dir = self.config.profile_log_dir();
        let ops_path = format!("{log_dir}/ops.json");

        quote! {
            // Per-operator aggregate stats, keyed by operator id (worker-local).
            let op_stats: Rc<RefCell<HashMap<usize, OpStats>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let op_stats_in_log = Rc::clone(&op_stats);

            // Worker 0 plants the static plan graph beside the runtime logs.
            // Best-effort — a write failure here shouldn't take down the dataflow.
            if worker.index() == 0 {
                let _ = std::fs::create_dir_all(#log_dir);
                let _ = std::fs::write(#ops_path, __FLOWLOG_OPS_JSON);
            }

            // Register a logger that receives timely events and folds them into `op_stats`.
            worker
                .log_register()
                .expect("failed to get log_register")
                .insert::<TimelyEventBuilder, _>("timely", move |_batch_time, data| {
                    let Some(data) = data else {
                        // Flush marker: we don't write per-event logs; we only aggregate.
                        return;
                    };

                    for (ts, event) in data.iter() {
                        match event {
                            // Operator metadata: capture name and address.
                            TimelyEvent::Operates(op) => {
                                let mut map = op_stats_in_log.borrow_mut();
                                let entry = map.entry(op.id).or_default();
                                entry.name = op.name.to_string();
                                entry.addr = format!("{:?}", op.addr);
                            }

                            // Scheduling activity: Start/Stop pairs determine "active time".
                            TimelyEvent::Schedule(sched) => {
                                let mut map = op_stats_in_log.borrow_mut();
                                let entry = map.entry(sched.id).or_default();

                                match sched.start_stop {
                                    StartStop::Start => {
                                        // Record the start timestamp (overwrites if nested/duplicated).
                                        entry.current_start = Some(*ts);
                                    }
                                    StartStop::Stop => {
                                        // Accumulate duration if we saw a corresponding Start.
                                        if let Some(st) = entry.current_start.take() {
                                            let delta = ts
                                                .checked_sub(st)
                                                .unwrap_or(Duration::ZERO);
                                            entry.total_active += delta;
                                            entry.activations += 1;
                                        }
                                    }
                                }
                            }

                            _ => {}
                        }
                    }
                });
        }
    }

    /// Force-drain timely's log registry into our `op_stats`/`dd_stats`
    /// callbacks. DD/timely events sit in per-thread `LogPusher`s until
    /// timely schedules a logger drain, which is opportunistic and gets
    /// starved when workers are busy with heavy ops. Without an explicit
    /// drain before a profile write, the snapshot is stale.
    ///
    /// Returns an empty stream when profiling is off.
    pub(crate) fn gen_log_register_flush(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }
        quote! {
            if let Some(mut __reg) = worker.log_register() {
                __reg.flush();
            }
        }
    }

    /// Emit the batch-mode `while worker.step()` loop. With `--profile` and
    /// `--profile-flush-secs` both set, the loop body overwrites the
    /// `<stem>_log/{time,memory}/*.log` files every N seconds so the
    /// latest snapshot is on disk for the next compile to read.
    pub(crate) fn gen_step_loop_batch(
        &self,
        time_write: &TokenStream,
        memory_write: &TokenStream,
    ) -> TokenStream {
        let Some(period_secs) = self
            .config
            .profile_flush_secs()
            .filter(|_| self.config.profiling_enabled())
        else {
            return quote! { while worker.step() {} };
        };

        let drain = self.gen_log_register_flush();
        quote! {
            let mut __profile_last_flush = std::time::Instant::now();
            let __profile_flush_period =
                std::time::Duration::from_secs(#period_secs);
            while worker.step() {
                if __profile_last_flush.elapsed() >= __profile_flush_period {
                    #drain
                    #time_write
                    #memory_write
                    __profile_last_flush = std::time::Instant::now();
                }
            }
        }
    }

    /// Emits time profiling write-out logic for **batch** mode.
    ///
    /// Writes one file per worker under `<stem>_log/time/`:
    /// `<stem>_log/time/time_worker_t0_{index}.log`
    pub(crate) fn gen_time_profile_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/time", self.config.profile_log_dir());
        let path_fmt = format!("{dir}/time_worker_t0_{{}}.log");
        gen_time_profile_write_core(&dir, quote! { format!(#path_fmt, index) })
    }

    /// Emits time profiling write-out logic for **incremental** mode.
    ///
    /// Writes one file per worker per committed transaction time:
    /// `<stem>_log/time/time_worker_t{time_stamp}_{index}.log`
    pub(crate) fn gen_time_profile_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/time", self.config.profile_log_dir());
        let path_fmt = format!("{dir}/time_worker_t{{}}_{{}}.log");
        let write =
            gen_time_profile_write_core(&dir, quote! { format!(#path_fmt, time_stamp - 1, index) });

        // Reset timing counters after each write so stats are per-transaction,
        // but keep operator metadata (name, addr) for the next round.
        // Note: { #write } scopes the op_stats.borrow() inside gen_time_profile_write_core
        // so it drops before the borrow_mut() below. The memory path doesn't need this
        // because gen_memory_profile_write_core already wraps its body in a block.
        quote! {
            { #write }

            for (_id, st) in op_stats.borrow_mut().iter_mut() {
                st.total_active = Duration::ZERO;
                st.activations = 0;
                st.current_start = None;
            }
        }
    }

    // =================================================================
    // Memory profiling (differential dataflow arrangement level)
    // =================================================================

    /// Generates the per-operator memory profiling data structure.
    ///
    /// Emitted alongside `OpStats` when profiling is enabled.
    pub(crate) fn gen_memory_profile_struct(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            /// Per-operator differential-dataflow arrangement memory statistics.
            #[derive(Clone, Debug, Default)]
            struct DdArrangeStats {
                /// Number of Batch events received.
                batch_count: u64,
                /// Total records entering the arrangement (summed across batches).
                batch_total_len: usize,
                /// Number of completed merge (compaction) events.
                merge_completes: u64,
                /// Sum of input lengths across merge completions.
                merge_input_total: usize,
                /// Sum of output lengths across merge completions.
                merge_output_total: usize,
                /// Total records freed across drop events.
                drop_total_len: usize,
                /// Batcher size delta (bytes) – positive = allocated, negative = freed.
                batcher_size: isize,
            }
        }
    }

    /// Generates DD arrangement logging registration code for memory profiling.
    ///
    /// The generated code registers a logger under the `"differential/arrange"` stream
    /// and aggregates Batch, Merge (complete), Drop, and Batcher events per operator.
    pub(crate) fn gen_memory_profile_init(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            // Per-operator DD arrangement stats, keyed by operator id (worker-local).
            let dd_stats: Rc<RefCell<HashMap<usize, DdArrangeStats>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let dd_stats_in_log = Rc::clone(&dd_stats);

            // Register a logger for differential dataflow arrangement events.
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
                                    let mut map = dd_stats_in_log.borrow_mut();
                                    let e = map.entry(b.operator).or_default();
                                    e.batch_count += 1;
                                    e.batch_total_len += b.length;
                                }
                                DifferentialEvent::Merge(m) => {
                                    if let Some(complete_len) = m.complete {
                                        let mut map = dd_stats_in_log.borrow_mut();
                                        let e = map.entry(m.operator).or_default();
                                        e.merge_completes += 1;
                                        e.merge_input_total += m.length1 + m.length2;
                                        e.merge_output_total += complete_len;
                                    }
                                    // ignore merge-start (no size info)
                                }
                                DifferentialEvent::Drop(d) => {
                                    let mut map = dd_stats_in_log.borrow_mut();
                                    let e = map.entry(d.operator).or_default();
                                    e.drop_total_len += d.length;
                                }
                                DifferentialEvent::Batcher(b) => {
                                    let mut map = dd_stats_in_log.borrow_mut();
                                    let e = map.entry(b.operator).or_default();
                                    e.batcher_size += b.size_diff;
                                }
                                _ => {} // MergeShortfall, TraceShare: not memory-related
                            }
                        }
                    },
                );
        }
    }

    /// Emits memory profiling write-out logic for **batch** mode.
    ///
    /// Writes `<stem>_log/memory/memory_worker_t0_{index}.log` per worker.
    pub(crate) fn gen_memory_profile_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/memory", self.config.profile_log_dir());
        let path_fmt = format!("{dir}/memory_worker_t0_{{}}.log");
        gen_memory_profile_write_core(&dir, quote! { format!(#path_fmt, index) })
    }

    /// Emits memory profiling write-out logic for **incremental** mode.
    ///
    /// Writes `<stem>_log/memory/memory_worker_t{time_stamp}_{index}.log` per worker per txn.
    pub(crate) fn gen_memory_profile_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        let dir = format!("{}/memory", self.config.profile_log_dir());
        let path_fmt = format!("{dir}/memory_worker_t{{}}_{{}}.log");
        let write = gen_memory_profile_write_core(
            &dir,
            quote! { format!(#path_fmt, time_stamp - 1, index) },
        );

        // Reset memory counters after each write so stats are per-transaction.
        quote! {
            #write

            for (_id, st) in dd_stats.borrow_mut().iter_mut() {
                *st = DdArrangeStats::default();
            }
        }
    }
}

// =================================================================
// Private helper functions (not tied to CodeGen state)
// =================================================================

/// Render the static plan-graph profiler as a `const &str` baked into the
/// generated module. Worker 0 writes it to `<stem>_log/ops.json` on
/// startup (see [`CodeGen::gen_time_profile_init`]).
///
/// `None` profiler → empty token stream so non-profile builds carry no
/// dead const.
pub(crate) fn render_profile_ops_const(profiler: Option<&Profiler>) -> TokenStream {
    let Some(profiler) = profiler else {
        return quote! {};
    };
    let json = profiler.to_json_string();
    quote! {
        const __FLOWLOG_OPS_JSON: &str = #json;
    }
}

/// Shared implementation for writing time profiling stats to a file.
fn gen_time_profile_write_core(dir: &str, file_path_expr: TokenStream) -> TokenStream {
    let create_msg = format!("failed to create {dir} directory");
    quote! {
        // Snapshot + sort for deterministic output.
        let map = op_stats.borrow();
        let mut rows: Vec<(usize, OpStats)> =
            map.iter().map(|(id, st)| (*id, st.clone())).collect();
        rows.sort_by_key(|(id, _st)| *id);

        std::fs::create_dir_all(#dir).expect(#create_msg);

        let stats_file = File::create(#file_path_expr)
            .expect("failed to create operator stats log file");
        let mut stats_writer = BufWriter::new(stats_file);

        // Header row.
        writeln!(
            stats_writer,
            "{:<20} {:<12} {:<16} {}",
            "addr", "activations", "total_active_ms", "name"
        )
        .ok();

        // Data rows.
        for (_id, st) in rows {
            let total_ms = st.total_active.as_secs_f64() * 1000.0;
            writeln!(
                stats_writer,
                "{:<20} {:<12} {:<16.3} {}",
                st.addr, st.activations, total_ms, st.name
            )
            .ok();
        }

        stats_writer.flush().ok();
    }
}

/// Shared implementation for writing memory (arrangement) stats to a file.
///
/// One row per arrangement the dataflow touched. The differential logger
/// fires only for arrangement operators, so `dd_stats`' key set *is* the
/// set of arrangements — `op_stats` is consulted only to resolve each id
/// to its name and address (a differential event carries just the id).
///
/// Each row carries `batch_count` (number of batch events) and
/// `batcher_bytes` (arrangement memory) alongside throughput. A large
/// `batch_count` against a small `batched_in` is the signature of a
/// stalling plan — many tiny or empty batches — which a throughput-only
/// view cannot see.
fn gen_memory_profile_write_core(dir: &str, file_path_expr: TokenStream) -> TokenStream {
    let create_msg = format!("failed to create {dir} directory");
    quote! {
        // --- DD arrangement stats write-out ---
        {
            let op_map = op_stats.borrow();
            let dd_map = dd_stats.borrow();

            // One row per arrangement (`dd_map`'s keys); `op_map` resolves
            // each id to its address and name.
            let mut rows: Vec<(Vec<usize>, String, String, DdArrangeStats)> = dd_map
                .iter()
                .map(|(id, st)| {
                    let (addr, name) = op_map
                        .get(id)
                        .map(|o| (o.addr.clone(), o.name.clone()))
                        .unwrap_or_else(|| (format!("[id={}]", id), "<unknown>".to_string()));
                    // Parse "[0, 8, 9]" -> vec![0, 8, 9] for numeric sort.
                    let nums: Vec<usize> = addr
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    (nums, addr, name, st.clone())
                })
                .collect();
            // Sort numerically by address components.
            rows.sort_by(|a, b| a.0.cmp(&b.0));

            std::fs::create_dir_all(#dir).expect(#create_msg);

            let dd_file = File::create(#file_path_expr)
                .expect("failed to create DD arrange stats log file");
            let mut w = BufWriter::new(dd_file);

            // Table header
            writeln!(
                w,
                "{:<20} {:<14} {:<10} {:<14} {:<14} {:<14} {:<14} {:<16} {}",
                "addr", "batched_in", "merges", "merge_in", "merge_out",
                "dropped", "batch_count", "batcher_bytes", "name"
            ).ok();

            for (_nums, addr, name, st) in &rows {
                writeln!(
                    w,
                    "{:<20} {:<14} {:<10} {:<14} {:<14} {:<14} {:<14} {:<16} {}",
                    addr,
                    st.batch_total_len,
                    st.merge_completes,
                    st.merge_input_total,
                    st.merge_output_total,
                    st.drop_total_len,
                    st.batch_count,
                    st.batcher_size,
                    name
                ).ok();
            }

            w.flush().ok();
        }
    }
}
