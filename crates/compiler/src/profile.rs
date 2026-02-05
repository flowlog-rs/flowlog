//! Profiling code generation for FlowLog.
//!
//! This module emits optional profiling support into the generated `main.rs`.
//! When profiling is enabled, the generated program registers a timely logger
//! to aggregate per-operator activity (active time + activation count) on each
//! worker, and writes a compact summary table to disk.
//!
//! Generated artifacts:
//! - Batch mode: `log/operator_stats_worker_{index}.log`
//! - Incremental mode: `operator_stats_worker_t{time_stamp}_{index}.log`

use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Generates the per-operator profiling data structure.
    ///
    /// This is emitted into the generated `main.rs` only when profiling is enabled.
    /// The struct stores worker-local aggregate timing and activation statistics per operator.
    pub(crate) fn gen_profile_struct(&self) -> TokenStream {
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

    /// Generates timely logging registration code for profiling.
    ///
    /// The generated code registers a timely logger under the `"timely"` stream and aggregates:
    /// - `Operates` events (operator name + address)
    /// - `Schedule` events (Start/Stop pairs → total active time + activation count)
    ///
    /// Notes:
    /// - This is worker-local aggregation: each worker maintains its own `HashMap<operator_id, OpStats>`.
    /// - Timely’s log callback may deliver multiple events per batch; we fold them into aggregates.
    pub(crate) fn gen_profile_init(&self) -> TokenStream {
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

    /// Emits profiling write-out logic for **batch** mode.
    ///
    /// Writes one file per worker under `log/`:
    /// `log/operator_stats_worker_{index}.log`
    ///
    /// The file is a stable, tabular summary sorted by operator id.
    pub(crate) fn gen_profile_write_batch(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            // Snapshot + sort for deterministic output.
            let map = op_stats.borrow();
            let mut rows: Vec<(usize, OpStats)> =
                map.iter().map(|(id, st)| (*id, st.clone())).collect();
            rows.sort_by_key(|(id, _st)| *id);

            // Ensure log directory exists (batch mode writes under `log/`).
            std::fs::create_dir_all("log").expect("failed to create log directory");

            let stats_file = File::create(format!("log/operator_stats_worker_{}.log", index))
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

    /// Emits profiling write-out logic for **incremental** mode.
    ///
    /// Writes one file per worker per committed transaction time:
    /// `operator_stats_worker_t{time_stamp}_{index}.log`
    pub(crate) fn gen_profile_write_incremental(&self) -> TokenStream {
        if !self.config.profiling_enabled() {
            return quote! {};
        }

        quote! {
            // Snapshot + sort for deterministic output.
            let map = op_stats.borrow();
            let mut rows: Vec<(usize, OpStats)> =
                map.iter().map(|(id, st)| (*id, st.clone())).collect();
            rows.sort_by_key(|(id, _st)| *id);

            // Ensure log directory exists (batch mode writes under `log/`).
            std::fs::create_dir_all("log").expect("failed to create log directory");

            let stats_file = File::create(format!(
                "log/operator_stats_worker_t{}_{}.log",
                time_stamp, index
            ))
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
}
