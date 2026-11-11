//! Profiling: operator and channel metrics, heap accounting, and log tables.
//!
//! A [`Profiler`] records its worker's timely log and writes what it saw,
//! with the worker's heap counters from [`ProfilingAlloc`], under the run's log
//! directory, one table pair per transaction `t` and worker `w`:
//!
//! ```text
//! ops.json                                the plan graph, from worker 0
//! metrics/operators_worker_t{t}_{w}.log   time and heap per operator
//! metrics/channels_worker_t{t}_{w}.log    records per channel
//! ```
//!
//! The tables hold raw observations only. `flowlog_profiler::metrics` owns
//! their schema and derives everything else on read, so change the writer
//! and the reader together.

use std::cell::RefCell;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::Duration;
use std::time::Instant;

use timely::logging::TimelyEventBuilder;
use timely::worker::Worker;

use crate::io::write_atomic;
use crate::time::Epoch;

mod memory;
mod metrics;

pub use memory::ProfilingAlloc;
#[cfg(test)]
pub(crate) use metrics::ships_batches;

use self::metrics::Heap;
use self::metrics::Metrics;

// =============================================================================
// Profiler
// =============================================================================

/// Records one worker's operator and channel metrics, and writes them out
/// one table pair per transaction.
#[derive(Debug)]
pub struct Profiler {
    worker: usize,
    metrics_dir: PathBuf,
    // Shared with the logger, which timely's log registry owns: the
    // registry takes a `'static` closure, so it cannot borrow from here.
    metrics: Rc<RefCell<Metrics>>,
    /// Whether [`memory`] accepted the worker. When it did not, the
    /// worker's counters are a previous run's.
    memory_registered: bool,
    flush_interval: Option<Duration>,
    last_flush: Instant,
}

impl Profiler {
    /// Registers `worker` for heap accounting and installs a logger that
    /// records its timely events. Install it before building the dataflow:
    /// timely logs each operator and channel once, as it builds them.
    ///
    /// Worker 0 also writes `ops_json`, the plan graph, to
    /// `log_dir/ops.json`, and clears the tables a previous run left in
    /// `log_dir/metrics`.
    ///
    /// With a `flush_interval`, [`flush_if_due`](Self::flush_if_due)
    /// rewrites the current tables at most that often; with `None`, only
    /// [`end_transaction`](Self::end_transaction) writes them.
    ///
    /// Profiling is best effort: registration and table-write failures warn
    /// without stopping execution.
    #[must_use]
    pub fn install(
        worker: &Worker,
        log_dir: &Path,
        ops_json: &str,
        flush_interval: Option<Duration>,
    ) -> Self {
        let index = worker.index();
        let memory_registered = match memory::register_worker(index) {
            Ok(()) => true,
            Err(error) => {
                eprintln!("flowlog profiling: {error}; memory unmeasured");
                false
            }
        };

        let metrics_dir = log_dir.join("metrics");
        if index == 0 {
            // Best effort: a failure surfaces as the tables fail to write.
            // The reader globs the whole metrics directory, so a previous
            // run's tables left there would merge into this run's.
            let _ = fs::create_dir_all(log_dir);
            let _ = fs::remove_dir_all(&metrics_dir);
            let _ = fs::write(log_dir.join("ops.json"), ops_json);
        }

        let metrics = Rc::new(RefCell::new(Metrics::default()));
        match worker.log_register() {
            Some(mut registry) => {
                let shared = Rc::clone(&metrics);
                registry.insert::<TimelyEventBuilder, _>("timely", move |_, events| {
                    let Some(events) = events else { return };
                    let mut metrics = shared.borrow_mut();
                    for (time, event) in events.iter() {
                        metrics.record(*time, event);
                    }
                });
            }
            None => eprintln!("flowlog profiling: log registry unavailable, metrics disabled"),
        }

        Self {
            worker: index,
            metrics_dir,
            metrics,
            memory_registered,
            flush_interval,
            last_flush: Instant::now(),
        }
    }

    /// Rewrites transaction `transaction`'s tables with the counts so far
    /// once the flush interval has passed since the last flush, so a long
    /// or interrupted run still leaves recent metrics on disk. The counts
    /// carry on into the transaction's final tables.
    pub fn flush_if_due(&mut self, transaction: Epoch) {
        let due = self
            .flush_interval
            .is_some_and(|interval| self.last_flush.elapsed() >= interval);
        if due {
            self.write(transaction);
            self.last_flush = Instant::now();
        }
    }

    /// Writes transaction `transaction`'s final tables, then zeroes the time
    /// and channel traffic counters. Heap accounting continues across
    /// transactions. A batch run is the single transaction `0`.
    pub fn end_transaction(&mut self, transaction: Epoch) {
        self.write(transaction);
        self.metrics.borrow_mut().reset();
    }

    fn write(&self, transaction: Epoch) {
        if let Err(error) = self.write_tables(transaction) {
            eprintln!(
                "flowlog profiling: metrics dump into {} failed: {error}",
                self.metrics_dir.display()
            );
        }
    }

    fn write_tables(&self, transaction: Epoch) -> io::Result<()> {
        fs::create_dir_all(&self.metrics_dir)?;
        let heap = if self.memory_registered {
            Heap::read(self.worker)
        } else {
            None
        };
        let metrics = self.metrics.borrow();
        let path = |table: &str| {
            let name = format!("{table}_worker_t{transaction}_{}.log", self.worker);
            self.metrics_dir.join(name)
        };
        // A periodic flush can be cut off mid-write, so each table is
        // replaced whole.
        write_atomic(path("operators"), |out| {
            metrics.write_operators(out, heap.as_ref())
        })?;
        write_atomic(path("channels"), |out| metrics.write_channels(out))
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use timely::dataflow::InputHandleVec;
    use timely::dataflow::ProbeHandle;
    use timely::dataflow::operators::Inspect;
    use timely::dataflow::operators::Probe;
    use timely::dataflow::operators::vec::Input;

    use super::*;

    // These workers use the system allocator, so heap columns read `n/a`.

    /// Builds `input -> inspect -> probe` on `worker`.
    fn pipeline(worker: &mut Worker) -> (InputHandleVec<Epoch, u32>, ProbeHandle<Epoch>) {
        worker.dataflow(|scope| {
            let (input, stream) = scope.new_input();
            let (probe, _) = stream.inspect(|_| {}).probe();
            (input, probe)
        })
    }

    /// Sends `records` records at `input`'s time, closes that time, and
    /// steps `worker` until the records have crossed the dataflow.
    fn send(
        worker: &mut Worker,
        input: &mut InputHandleVec<Epoch, u32>,
        probe: &ProbeHandle<Epoch>,
        records: u32,
    ) {
        for record in 0..records {
            input.send(record);
        }
        input.advance_to(input.time() + 1);
        while probe.less_than(input.time()) {
            worker.step();
        }
    }

    /// Returns worker 0's `table` for `transaction`, under `log_dir`.
    fn read_table(log_dir: &Path, table: &str, transaction: Epoch) -> String {
        let path = log_dir.join(format!("metrics/{table}_worker_t{transaction}_0.log"));
        fs::read_to_string(path).expect("the profiler wrote the table")
    }

    #[test]
    fn install_writes_the_plan_graph_into_a_new_log_dir() {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let log_dir = dir.path().join("program_log");
        let install_dir = log_dir.clone();
        timely::execute_directly(move |worker| {
            let _profiler = Profiler::install(worker, &install_dir, r#"{"nodes":[]}"#, None);
        });
        let ops = fs::read_to_string(log_dir.join("ops.json")).expect("worker 0 wrote it");
        assert_eq!(ops, r#"{"nodes":[]}"#);
    }

    /// The reader merges every table in the metrics directory, so a
    /// previous run's must not survive into this one.
    #[test]
    fn install_clears_a_previous_runs_tables() {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let old_table = dir.path().join("metrics/operators_worker_t0_0.log");
        fs::create_dir_all(dir.path().join("metrics")).expect("creates the old metrics");
        fs::write(&old_table, "a previous run's table").expect("writes the old table");
        let log_dir = dir.path().to_path_buf();
        timely::execute_directly(move |worker| {
            let _profiler = Profiler::install(worker, &log_dir, "{}", None);
        });
        assert!(!old_table.exists());
    }

    /// Each transaction's tables count only its own records: the second
    /// reads 2, not the 5 sent across both.
    #[test]
    fn each_transaction_reports_only_its_own_traffic() {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let log_dir = dir.path().to_path_buf();
        timely::execute_directly(move |worker| {
            let mut profiler = Profiler::install(worker, &log_dir, "{}", None);
            let (mut input, probe) = pipeline(worker);
            send(worker, &mut input, &probe, 3);
            profiler.end_transaction(0);
            send(worker, &mut input, &probe, 2);
            profiler.end_transaction(1);
        });
        assert_eq!(
            read_table(dir.path(), "channels", 0),
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  1     0         2     0         0      3            3
[0]                  2     0         3     0         0      3            3
"
        );
        assert_eq!(
            read_table(dir.path(), "channels", 1),
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  1     0         2     0         0      2            2
[0]                  2     0         3     0         0      2            2
"
        );
    }

    #[test]
    fn a_failed_write_does_not_stop_the_next_transactions_metrics() {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let log_dir = dir.path().join("blocked");
        fs::write(&log_dir, "not a directory").expect("blocks the log directory");
        let run_dir = log_dir.clone();
        timely::execute_directly(move |worker| {
            let mut profiler = Profiler::install(worker, &run_dir, "{}", None);
            let (mut input, probe) = pipeline(worker);
            send(worker, &mut input, &probe, 3);
            profiler.end_transaction(0);

            fs::remove_file(&run_dir).expect("unblocks the log directory");
            send(worker, &mut input, &probe, 2);
            profiler.end_transaction(1);
        });
        assert!(!log_dir.join("metrics/channels_worker_t0_0.log").exists());
        assert_eq!(
            read_table(&log_dir, "channels", 1),
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  1     0         2     0         0      2            2
[0]                  2     0         3     0         0      2            2
"
        );
    }

    /// A periodic flush writes the counts so far and zeroes nothing, so
    /// the transaction's final tables count all of its records.
    #[test]
    fn a_flush_keeps_the_counts_for_the_final_tables() {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let log_dir = dir.path().to_path_buf();
        let flushed = timely::execute_directly(move |worker| {
            let mut profiler = Profiler::install(worker, &log_dir, "{}", Some(Duration::ZERO));
            let (mut input, probe) = pipeline(worker);
            send(worker, &mut input, &probe, 3);
            profiler.flush_if_due(0);
            let flushed = read_table(&log_dir, "channels", 0);
            send(worker, &mut input, &probe, 2);
            profiler.end_transaction(0);
            flushed
        });
        assert_eq!(
            flushed,
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  1     0         2     0         0      3            3
[0]                  2     0         3     0         0      3            3
"
        );
        assert_eq!(
            read_table(dir.path(), "channels", 0),
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  1     0         2     0         0      5            5
[0]                  2     0         3     0         0      5            5
"
        );
    }

    /// A periodic flush writes only once its interval has passed since
    /// install, and never without one.
    // Cases: flush interval, whether the first flush writes.
    #[rstest]
    #[case::no_interval(None, false)]
    #[case::interval_passed(Some(Duration::ZERO), true)]
    #[case::interval_pending(Some(Duration::from_secs(3600)), false)]
    fn a_flush_writes_once_the_interval_passes(
        #[case] flush_interval: Option<Duration>,
        #[case] writes: bool,
    ) {
        let dir = tempfile::tempdir().expect("creates a temp dir");
        let log_dir = dir.path().to_path_buf();
        timely::execute_directly(move |worker| {
            let mut profiler = Profiler::install(worker, &log_dir, "{}", flush_interval);
            profiler.flush_if_due(0);
        });
        let table = dir.path().join("metrics/operators_worker_t0_0.log");
        assert_eq!(table.exists(), writes);
    }
}
