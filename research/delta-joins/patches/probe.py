"""Inject an arrangement-size probe into a generated FlowLog main.rs.

With FLOWLOG_PROBE=1 the binary tracks, from differential's Batch/Merge/Drop log events, how many
update records every arrangement's trace holds (summed over workers). At exit it prints the peak
total, the peak held in batchers, the peak rows held inside lookup operators, and the largest
arrangements (PROBE_TOP of them, default 16). Without the variable it registers nothing and costs
nothing.
"""

import sys
from pathlib import Path

from _edit import insert_after

BATCH_DONE = 'println!("{:?}:\\tDataflow executed", timer.elapsed());'
# Incremental mains print no "executed" line; they report once every worker has joined.
WORKERS_JOINED = "let messages = failure.messages();"
PROBE_MODULE = r'''
mod arrangement_probe {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    use ::flowlog_runtime::differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
    use ::flowlog_runtime::timely::logging::{TimelyEvent, TimelyEventBuilder};

    #[derive(Default)]
    struct State {
        names: HashMap<usize, String>,
        live: HashMap<usize, i64>,
        peak: HashMap<usize, i64>,
        total: i64,
        peak_total: i64,
        at_peak: HashMap<usize, i64>,
        batcher_total: i64,
        peak_batcher_total: i64,
    }

    fn state() -> &'static Mutex<State> {
        static STATE: OnceLock<Mutex<State>> = OnceLock::new();
        STATE.get_or_init(Default::default)
    }

    fn enabled() -> bool {
        std::env::var("FLOWLOG_PROBE").map_or(false, |v| v == "1")
    }

    pub fn install(worker: &mut ::flowlog_runtime::timely::worker::Worker) {
        if !enabled() {
            return;
        }
        let first = worker.index() == 0;
        let Some(mut registry) = worker.log_register() else { return };
        if first {
            registry.insert::<TimelyEventBuilder, _>("timely", |_, data| {
                if let Some(data) = data {
                    let mut s = state().lock().unwrap();
                    for (_, event) in data.iter() {
                        if let TimelyEvent::Operates(op) = event {
                            s.names.insert(op.id, op.name.clone());
                        }
                    }
                }
            });
        }
        registry.insert::<DifferentialEventBuilder, _>("differential/arrange", |_, data| {
            let Some(data) = data else { return };
            let mut s = state().lock().unwrap();
            for (_, event) in data.iter() {
                let (op, delta) = match event {
                    DifferentialEvent::Batch(b) => (b.operator, b.length as i64),
                    DifferentialEvent::Merge(m) => match m.complete {
                        Some(c) => (m.operator, c as i64 - m.length1 as i64 - m.length2 as i64),
                        None => continue,
                    },
                    DifferentialEvent::Drop(d) => (d.operator, -(d.length as i64)),
                    DifferentialEvent::Batcher(b) => {
                        s.batcher_total += b.records_diff as i64;
                        s.peak_batcher_total = s.peak_batcher_total.max(s.batcher_total);
                        continue;
                    }
                    _ => continue,
                };
                let live = {
                    let live = s.live.entry(op).or_default();
                    *live += delta;
                    *live
                };
                let peak = s.peak.entry(op).or_default();
                *peak = (*peak).max(live);
                s.total += delta;
                if s.total > s.peak_total {
                    s.peak_total = s.total;
                    s.at_peak = s.live.clone();
                }
            }
        });
    }

    /// Counts records flowing through a collection, summed over workers; flushes on drop.
    #[allow(dead_code)]
    pub struct Counter {
        local: u64,
        global: &'static std::sync::atomic::AtomicU64,
    }

    #[allow(dead_code)]
    impl Counter {
        pub fn new(label: &'static str) -> Self {
            let mut counters = counters().lock().unwrap();
            let global = match counters.iter().find(|(l, _)| *l == label) {
                Some((_, g)) => *g,
                None => {
                    let g: &'static std::sync::atomic::AtomicU64 = Box::leak(Box::default());
                    counters.push((label, g));
                    g
                }
            };
            Counter { local: 0, global }
        }
        #[inline]
        pub fn bump(&mut self) {
            self.local += 1;
            if self.local == 1 << 16 {
                self.global.fetch_add(self.local, std::sync::atomic::Ordering::Relaxed);
                self.local = 0;
            }
        }
    }

    impl Drop for Counter {
        fn drop(&mut self) {
            self.global.fetch_add(self.local, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn counters() -> &'static Mutex<Vec<(&'static str, &'static std::sync::atomic::AtomicU64)>> {
        static COUNTERS: OnceLock<Mutex<Vec<(&'static str, &'static std::sync::atomic::AtomicU64)>>> = OnceLock::new();
        COUNTERS.get_or_init(Default::default)
    }

    pub fn report() {
        for (label, count) in counters().lock().unwrap().iter() {
            println!("[count] {}={}", label, count.load(std::sync::atomic::Ordering::Relaxed));
        }
        if !enabled() {
            return;
        }
        let s = state().lock().unwrap();
        println!("[probe] peak_total_trace_records={} peak_batcher_records={}", s.peak_total, s.peak_batcher_total);
        let held = ::flowlog_runtime::operators::lookup_rows_held_peak();
        println!("[probe] peak_lookup_rows_held={} stashed={} ready={}", held.total, held.stashed, held.ready);
        let mut ops: Vec<(i64, usize)> = s.peak.iter().map(|(op, peak)| (*peak, *op)).collect();
        ops.sort_unstable_by(|a, b| b.cmp(a));
        let top = std::env::var("PROBE_TOP").ok().and_then(|n| n.parse().ok()).unwrap_or(16);
        for (peak, op) in ops.iter().take(top) {
            let name = s.names.get(op).map(String::as_str).unwrap_or("?");
            let at = s.at_peak.get(op).copied().unwrap_or(0);
            println!("[probe] op={:<5} peak={:<12} at_total_peak={:<12} {}", op, peak, at, name);
        }
    }
}
'''

main_rs = Path(sys.argv[1])
code = main_rs.read_text()
code = insert_after(code, "let index = worker.index();", "\n                    arrangement_probe::install(worker);")
report_at = BATCH_DONE if BATCH_DONE in code else WORKERS_JOINED
code = insert_after(code, report_at, "\n    arrangement_probe::report();")
main_rs.write_text(code + PROBE_MODULE)
