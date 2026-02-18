//! Profiling code generation for FlowLog.
//!
//! This module emits optional profiling support into the generated `main.rs`.
//!
//! **Time profiling** (timely dataflow level):
//! Registers a timely logger to aggregate per-operator active time and
//! activation count on each worker.
//!
//! **Memory profiling** (differential dataflow level):
//! Registers a differential dataflow arrangement logger to track batch,
//! merge, drop, and batcher events per operator (arrangement memory usage).
//!
//! Generated artifacts (time profiling):
//! - Batch mode: `log/time_worker_{index}.log`
//! - Incremental mode: `log/time_worker_t{time_stamp}_{index}.log`
//!
//! Generated artifacts (memory profiling):
//! - Batch mode: `log/memory_worker_{index}.log`
//! - Incremental mode: `log/memory_worker_t{time_stamp}_{index}.log`

use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
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
    /// Notes:
    /// - This is worker-local aggregation: each worker maintains its own `HashMap<operator_id, OpStats>`.
    /// - Timely's log callback may deliver multiple events per batch; we fold them into aggregates.
    pub(crate) fn gen_time_profile_init(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            // Per-operator aggregate stats, keyed by operator id (worker-local).
            let op_stats: Rc<RefCell<HashMap<usize, OpStats>>> =
                Rc::new(RefCell::new(HashMap::new()));
            let op_stats_in_log = Rc::clone(&op_stats);

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

    /// Emits time profiling write-out logic for **batch** mode.
    ///
    /// Writes one file per worker under `log/`:
    /// `log/time_worker_{index}.log`
    pub(crate) fn gen_time_profile_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        gen_time_profile_write_core(quote! {
            format!("log/time_worker_{}.log", index)
        })
    }

    /// Emits time profiling write-out logic for **incremental** mode.
    ///
    /// Writes one file per worker per committed transaction time:
    /// `log/time_worker_t{time_stamp}_{index}.log`
    pub(crate) fn gen_time_profile_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        gen_time_profile_write_core(quote! {
            format!("log/time_worker_t{}_{}.log", time_stamp, index)
        })
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
                /// Total number of records entering the arrangement (summed across batches).
                batch_total_len: usize,
                /// Number of completed merge (compaction) events.
                merge_completes: u64,
                /// Sum of input lengths across merge completions.
                merge_input_total: usize,
                /// Sum of output lengths across merge completions.
                merge_output_total: usize,
                /// Number of Drop events (records freed).
                drop_count: u64,
                /// Total number of records freed across drops.
                drop_total_len: usize,
                /// Batcher size delta (bytes) – positive = allocated, negative = freed.
                batcher_size: isize,
                /// Batcher capacity delta (bytes).
                batcher_capacity: isize,
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
                                    e.drop_count += 1;
                                    e.drop_total_len += d.length;
                                }
                                DifferentialEvent::Batcher(b) => {
                                    let mut map = dd_stats_in_log.borrow_mut();
                                    let e = map.entry(b.operator).or_default();
                                    e.batcher_size += b.size_diff;
                                    e.batcher_capacity += b.capacity_diff;
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
    /// Writes `log/memory_worker_{index}.log` per worker.
    pub(crate) fn gen_memory_profile_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        gen_memory_profile_write_core(quote! {
            format!("log/memory_worker_{}.log", index)
        })
    }

    /// Emits memory profiling write-out logic for **incremental** mode.
    ///
    /// Writes `log/memory_worker_t{time_stamp}_{index}.log` per worker per txn.
    pub(crate) fn gen_memory_profile_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        gen_memory_profile_write_core(quote! {
            format!("log/memory_worker_t{}_{}.log", time_stamp, index)
        })
    }
}

// =================================================================
// Private helper functions (not tied to Compiler state)
// =================================================================

/// Shared implementation for writing time profiling stats to a file.
fn gen_time_profile_write_core(file_path_expr: TokenStream) -> TokenStream {
    quote! {
        // Snapshot + sort for deterministic output.
        let map = op_stats.borrow();
        let mut rows: Vec<(usize, OpStats)> =
            map.iter().map(|(id, st)| (*id, st.clone())).collect();
        rows.sort_by_key(|(id, _st)| *id);

        std::fs::create_dir_all("log").expect("failed to create log directory");

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

/// Shared implementation for writing memory profiling stats to a file.
fn gen_memory_profile_write_core(file_path_expr: TokenStream) -> TokenStream {
    quote! {
        // --- DD arrangement stats write-out ---
        {
            let op_map = op_stats.borrow();
            let dd_map = dd_stats.borrow();

            // Build rows with (addr_nums, addr_string, name, stats)
            let mut rows: Vec<(Vec<usize>, String, String, DdArrangeStats)> = dd_map
                .iter()
                .map(|(id, st)| {
                    let (addr, name) = op_map
                        .get(id)
                        .map(|o| (o.addr.clone(), o.name.clone()))
                        .unwrap_or_else(|| (
                            format!("[id={}]", id),
                            "<unknown>".to_string(),
                        ));
                    // Parse "[0, 8, 9]" -> vec![0, 8, 9] for numeric sort
                    let nums: Vec<usize> = addr
                        .trim_matches(|c| c == '[' || c == ']')
                        .split(',')
                        .filter_map(|s| s.trim().parse().ok())
                        .collect();
                    (nums, addr, name, st.clone())
                })
                .collect();
            // Sort numerically by address components
            rows.sort_by(|a, b| a.0.cmp(&b.0));

            std::fs::create_dir_all("log").expect("failed to create log directory");

            let dd_file = File::create(#file_path_expr)
                .expect("failed to create DD arrange stats log file");
            let mut w = BufWriter::new(dd_file);

            // Table header
            writeln!(
                w,
                "{:<20} {:<14} {:<10} {:<14} {:<14} {:<14} {}",
                "addr", "batched_in", "merges", "merge_in", "merge_out", "dropped", "name"
            ).ok();

            for (_nums, addr, name, st) in &rows {
                writeln!(
                    w,
                    "{:<20} {:<14} {:<10} {:<14} {:<14} {:<14} {}",
                    addr,
                    st.batch_total_len,
                    st.merge_completes,
                    st.merge_input_total,
                    st.merge_output_total,
                    st.drop_total_len,
                    name
                ).ok();
            }

            if rows.is_empty() {
                writeln!(w, "(no differential arrangement events recorded)").ok();
            }

            w.flush().ok();
        }
    }
}
