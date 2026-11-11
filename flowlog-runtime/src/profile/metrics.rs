//! Worker-local event counters and their operator and channel tables.
//! Heap snapshots add allocator counters to the operator rows.

use std::collections::HashMap;
use std::fmt::Display;
use std::io;
use std::io::Write;
use std::time::Duration;

use timely::logging::StartStop;
use timely::logging::TimelyEvent;
use timely::progress::Source;
use timely::progress::Target;

use super::memory;
use super::memory::MAX_OPERATORS;
use super::memory::OperatorMemory;
use super::memory::Owner;

// =============================================================================
// Metrics
// =============================================================================

/// What a worker's timely log reported, keyed by timely's worker-local
/// operator and channel identifiers.
#[derive(Debug, Default)]
pub(super) struct Metrics {
    operators: HashMap<usize, Operator>,
    channels: HashMap<usize, Channel>,
}

impl Metrics {
    /// Adds one event, logged `time` after the worker started.
    pub(super) fn record(&mut self, time: Duration, event: &TimelyEvent) {
        match event {
            TimelyEvent::Operates(operates) => {
                let operator = self.operators.entry(operates.id).or_default();
                operator.name.clone_from(&operates.name);
                operator.addr.clone_from(&operates.addr);
            }
            TimelyEvent::Schedule(scheduled) => {
                let schedule = self
                    .operators
                    .entry(scheduled.id)
                    .or_default()
                    .schedule
                    .get_or_insert_default();
                match scheduled.start_stop {
                    StartStop::Start => schedule.started = Some(time),
                    StartStop::Stop => {
                        if let Some(started) = schedule.started.take() {
                            schedule.active += time.saturating_sub(started);
                            schedule.activations += 1;
                        }
                    }
                }
            }
            TimelyEvent::Channels(connected) => {
                self.channels.insert(
                    connected.id,
                    Channel {
                        scope: connected.scope_addr.clone(),
                        source: Source::new(connected.source.0, connected.source.1),
                        target: Target::new(connected.target.0, connected.target.1),
                        ships_batches: ships_batches(&connected.typ),
                        sent: 0,
                        received: 0,
                    },
                );
            }
            // Timely logs a channel as it connects it, before any message
            // can cross it, so a message on an unknown channel crossed one
            // connected before the profiler was installed.
            TimelyEvent::Messages(messages) => {
                if let Some(channel) = self.channels.get_mut(&messages.channel) {
                    let count = if messages.is_send {
                        &mut channel.sent
                    } else {
                        &mut channel.received
                    };
                    *count += messages.record_count;
                }
            }
            TimelyEvent::PushProgress(_)
            | TimelyEvent::Shutdown(_)
            | TimelyEvent::CommChannels(_)
            | TimelyEvent::Park(_)
            | TimelyEvent::Text(_) => {}
        }
    }

    /// Zeroes the counts and keeps the dataflow's shape, so an operator
    /// idle in the next transaction reads `0`, not `n/a`.
    pub(super) fn reset(&mut self) {
        for operator in self.operators.values_mut() {
            if let Some(schedule) = &mut operator.schedule {
                *schedule = Schedule::default();
            }
        }
        for channel in self.channels.values_mut() {
            channel.sent = 0;
            channel.received = 0;
        }
    }

    /// Writes the operator table, in address order. Heap columns read `n/a`
    /// without `heap`, and time columns for an operator never scheduled.
    pub(super) fn write_operators(
        &self,
        out: &mut dyn Write,
        heap: Option<&Heap>,
    ) -> io::Result<()> {
        operator_row(
            out,
            [
                &"addr",
                &"acts",
                &"active_ms",
                &"alloc_bytes",
                &"live_bytes",
                &"peak_bytes",
                &"name",
            ],
        )?;
        if let Some(outside) = heap.and_then(|heap| heap.outside) {
            operator_row(
                out,
                [
                    &"[]",
                    &"n/a",
                    &"n/a",
                    &outside.allocated,
                    &outside.live,
                    &outside.peak,
                    &"(outside operators)",
                ],
            )?;
        }
        let mut operators: Vec<_> = self.operators.iter().collect();
        operators.sort_by_key(|(_, operator)| &operator.addr);
        for (&id, operator) in operators {
            let [acts, active_ms] = match &operator.schedule {
                Some(schedule) => [
                    schedule.activations.to_string(),
                    format!("{:.3}", schedule.active.as_secs_f64() * 1000.0),
                ],
                None => ["n/a"; 2].map(str::to_owned),
            };
            let [allocated, live, peak] = match heap.and_then(|heap| heap.operator(id)) {
                Some(bytes) => {
                    [bytes.allocated, bytes.live, bytes.peak].map(|count| count.to_string())
                }
                None => ["n/a"; 3].map(str::to_owned),
            };
            operator_row(
                out,
                [
                    &fmt_addr(&operator.addr),
                    &acts,
                    &active_ms,
                    &allocated,
                    &live,
                    &peak,
                    &operator.name,
                ],
            )?;
        }
        if self.operators.is_empty() {
            writeln!(out, "(no operators recorded)")?;
        }
        Ok(())
    }

    /// Writes the channel table, in order of scope, then source, then
    /// target.
    pub(super) fn write_channels(&self, out: &mut dyn Write) -> io::Result<()> {
        channel_row(
            out,
            [
                &"scope",
                &"src",
                &"src_port",
                &"tgt",
                &"tgt_port",
                &"batch",
                &"sent",
                &"recvd",
            ],
        )?;
        let mut channels: Vec<_> = self.channels.values().collect();
        channels.sort_by_key(|channel| (&channel.scope, channel.source, channel.target));
        for channel in channels {
            channel_row(
                out,
                [
                    &fmt_addr(&channel.scope),
                    &channel.source.node,
                    &channel.source.port,
                    &channel.target.node,
                    &channel.target.port,
                    &u8::from(channel.ships_batches),
                    &channel.sent,
                    &channel.received,
                ],
            )?;
        }
        Ok(())
    }
}

/// Returns `true` if a channel of the type timely logs as `typ` carries
/// arrangement batches.
#[must_use]
pub(crate) fn ships_batches(typ: &str) -> bool {
    // FlowLog's row types never mention `Batch`; every batch type does.
    typ.contains("Batch")
}

fn operator_row(out: &mut dyn Write, cells: [&dyn Display; 7]) -> io::Result<()> {
    let [addr, acts, active_ms, allocated, live, peak, name] = cells;
    writeln!(
        out,
        "{addr:<20} {acts:<6} {active_ms:<11} {allocated:<12} {live:<12} {peak:<12} {name}"
    )
}

fn channel_row(out: &mut dyn Write, cells: [&dyn Display; 8]) -> io::Result<()> {
    let [
        scope,
        source,
        source_port,
        target,
        target_port,
        batch,
        sent,
        received,
    ] = cells;
    writeln!(
        out,
        "{scope:<20} {source:<5} {source_port:<9} {target:<5} {target_port:<9} {batch:<6} \
         {sent:<12} {received}"
    )
}

/// Formats an address the way the reader parses it: `[0, 4, 2]`.
#[must_use]
fn fmt_addr(addr: &[usize]) -> String {
    let parts: Vec<String> = addr.iter().map(usize::to_string).collect();
    format!("[{}]", parts.join(", "))
}

// =============================================================================
// Recorded events
// =============================================================================

#[derive(Debug, Default)]
struct Operator {
    name: String,
    /// The operator's scope's address, then its index in that scope.
    addr: Vec<usize>,
    /// `None` until the operator is first scheduled.
    schedule: Option<Schedule>,
}

/// An operator's scheduling since the counts were last zeroed.
#[derive(Debug, Default)]
struct Schedule {
    activations: u64,
    active: Duration,
    /// When the activation under way started.
    started: Option<Duration>,
}

#[derive(Debug)]
struct Channel {
    /// The address of the scope the channel runs in; its endpoints are
    /// operator indices in that scope.
    scope: Vec<usize>,
    source: Source,
    target: Target,
    /// Whether the channel carries arrangement batches, whose records are
    /// batches, not tuples.
    ships_batches: bool,
    /// Records this worker pushed into the channel.
    sent: i64,
    /// Records this worker took out of the channel.
    received: i64,
}

// =============================================================================
// Heap
// =============================================================================

/// One worker's heap counters, by owner.
#[derive(Debug)]
pub(super) struct Heap {
    operators: HashMap<usize, OperatorMemory>,
    outside: Option<OperatorMemory>,
}

impl Heap {
    /// Reads `worker`'s counters, or `None` if it has none. A profiler
    /// allocates on its worker as it installs, so a worker under
    /// [`ProfilingAlloc`](memory::ProfilingAlloc) always has some; none
    /// means the host did not install it.
    #[must_use]
    pub(super) fn read(worker: usize) -> Option<Self> {
        let entries = memory::operator_memory(worker);
        if entries.is_empty() {
            return None;
        }
        let mut heap = Self {
            operators: HashMap::new(),
            outside: None,
        };
        for entry in entries {
            match entry.owner {
                Owner::Operator(id) => {
                    heap.operators.insert(id, entry);
                }
                Owner::Outside => heap.outside = Some(entry),
                // The operator table has no address to report it under.
                Owner::Overflow => eprintln!(
                    "flowlog profiling: worker {worker} has operators beyond the memory \
                     accounting limit; their {} peak bytes are unreported",
                    entry.peak
                ),
            }
        }
        Some(heap)
    }

    /// Operator `id`'s counters, zero if it never allocated, or `None` if
    /// its identifier is beyond what [`memory`] tracks.
    #[must_use]
    fn operator(&self, id: usize) -> Option<OperatorMemory> {
        (id < MAX_OPERATORS).then(|| {
            self.operators.get(&id).copied().unwrap_or(OperatorMemory {
                owner: Owner::Operator(id),
                allocated: 0,
                live: 0,
                peak: 0,
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;
    use timely::logging::ChannelsEvent;
    use timely::logging::MessagesEvent;
    use timely::logging::OperatesEvent;
    use timely::logging::ScheduleEvent;

    use super::*;

    // Timely's log cannot be scripted from outside, so feed events at
    // chosen times directly into the recorder.

    /// Returns the metrics recorded from `events`, each logged the given
    /// number of microseconds after the worker started.
    fn recorded(events: &[(u64, TimelyEvent)]) -> Metrics {
        let mut metrics = Metrics::default();
        for (micros, event) in events {
            metrics.record(Duration::from_micros(*micros), event);
        }
        metrics
    }

    fn operates(id: usize, addr: &[usize], name: &str) -> TimelyEvent {
        TimelyEvent::Operates(OperatesEvent {
            id,
            addr: addr.to_vec(),
            name: name.to_owned(),
        })
    }

    fn scheduled(id: usize, start_stop: StartStop) -> TimelyEvent {
        TimelyEvent::Schedule(ScheduleEvent { id, start_stop })
    }

    fn connects(
        id: usize,
        scope: &[usize],
        source: (usize, usize),
        target: (usize, usize),
        typ: &str,
    ) -> TimelyEvent {
        TimelyEvent::Channels(ChannelsEvent {
            id,
            scope_addr: scope.to_vec(),
            source,
            target,
            typ: typ.to_owned(),
        })
    }

    fn messages(channel: usize, is_send: bool, record_count: i64) -> TimelyEvent {
        TimelyEvent::Messages(MessagesEvent {
            is_send,
            channel,
            source: 0,
            target: 0,
            seq_no: 0,
            record_count,
        })
    }

    fn operator_table(metrics: &Metrics, heap: Option<&Heap>) -> String {
        let mut out = Vec::new();
        metrics
            .write_operators(&mut out, heap)
            .expect("writes to memory");
        String::from_utf8(out).expect("the table is UTF-8")
    }

    /// An operator accrues an activation and its time once the activation
    /// stops. A stop without a start began before the profiler was
    /// installed; a start without a stop is still under way.
    // Cases: schedule events as (microseconds, start or stop), then
    // (activations, active time), or `None` if never scheduled.
    #[rstest]
    #[case::one(vec![(1000, StartStop::Start), (3500, StartStop::Stop)], Some((1, 2500)))]
    #[case::two(
        vec![
            (1000, StartStop::Start),
            (3500, StartStop::Stop),
            (4000, StartStop::Start),
            (4500, StartStop::Stop),
        ],
        Some((2, 3000)),
    )]
    #[case::stop_without_start(vec![(3500, StartStop::Stop)], Some((0, 0)))]
    #[case::under_way(vec![(1000, StartStop::Start)], Some((0, 0)))]
    #[case::never_scheduled(vec![], None)]
    fn only_finished_activations_count(
        #[case] schedule: Vec<(u64, StartStop)>,
        #[case] expected: Option<(u64, u64)>,
    ) {
        let mut metrics = recorded(&[(0, operates(1, &[0, 1], "Map"))]);
        for (micros, start_stop) in schedule {
            metrics.record(Duration::from_micros(micros), &scheduled(1, start_stop));
        }
        let expected = expected.map(|(acts, micros)| (acts, Duration::from_micros(micros)));
        let schedule = metrics.operators[&1].schedule.as_ref();
        assert_eq!(
            schedule.map(|schedule| (schedule.activations, schedule.active)),
            expected
        );
    }

    /// A channel counts the records this worker sent into it apart from
    /// those it took out. A message on a channel connected before the
    /// profiler was installed has no row to count on.
    #[test]
    fn messages_count_on_their_channel_by_direction() {
        let metrics = recorded(&[
            (0, connects(7, &[0], (1, 0), (2, 0), "Vec<u32>")),
            (0, messages(7, /* is_send */ true, 3)),
            (0, messages(7, /* is_send */ true, 2)),
            (0, messages(7, /* is_send */ false, 4)),
            (0, messages(9, /* is_send */ true, 8)),
        ]);
        let counts: Vec<_> = metrics
            .channels
            .iter()
            .map(|(&id, channel)| (id, channel.sent, channel.received))
            .collect();
        assert_eq!(counts, [(7, 5, 4)]);
    }

    /// Zeroing keeps the dataflow's shape: an operator scheduled before
    /// reads `0`, not `n/a`, and a channel keeps its row.
    #[test]
    fn reset_zeroes_the_counts_and_keeps_the_dataflow() {
        let mut metrics = recorded(&[
            (0, operates(1, &[0, 1], "Map")),
            (0, operates(2, &[0, 2], "Filter")),
            (1000, scheduled(1, StartStop::Start)),
            (3500, scheduled(1, StartStop::Stop)),
            (0, connects(7, &[0], (1, 0), (2, 0), "Vec<u32>")),
            (0, messages(7, /* is_send */ true, 3)),
        ]);
        metrics.reset();
        let schedules: Vec<_> = [1, 2]
            .iter()
            .map(|id| {
                let schedule = metrics.operators[id].schedule.as_ref();
                schedule.map(|schedule| (schedule.activations, schedule.active))
            })
            .collect();
        assert_eq!(schedules, [Some((0, Duration::ZERO)), None]);
        let channel = &metrics.channels[&7];
        assert_eq!((channel.sent, channel.received), (0, 0));
    }

    /// Operator rows follow numeric address order, which neither the
    /// operators' identifiers nor their addresses' text agree with.
    #[test]
    fn operator_rows_follow_numeric_address_order() {
        let metrics = recorded(&[
            (0, operates(1, &[0, 10], "Join")),
            (0, operates(2, &[0], "Dataflow")),
            (0, operates(3, &[0, 2], "Map")),
        ]);
        let table = operator_table(&metrics, None);
        let addrs: Vec<_> = table
            .lines()
            .skip(1)
            .map(|row| row[..20].trim_end())
            .collect();
        assert_eq!(addrs, ["[0]", "[0, 2]", "[0, 10]"]);
    }

    /// A column reads `n/a` only where nothing was measured: time for an
    /// operator never scheduled, and heap bytes without a heap or for an
    /// operator beyond what `memory` tracks. An operator that never
    /// allocated reads `0`, after the bytes allocated outside operators.
    #[test]
    fn operator_table_reads_n_a_only_where_nothing_was_measured() {
        let metrics = recorded(&[
            (0, operates(1, &[0], "Dataflow")),
            (0, operates(2, &[0, 1], "Input")),
            (0, operates(MAX_OPERATORS, &[0, 2], "Untracked")),
            (1000, scheduled(2, StartStop::Start)),
            (3500, scheduled(2, StartStop::Stop)),
        ]);
        let bytes = |owner, allocated, live, peak| OperatorMemory {
            owner,
            allocated,
            live,
            peak,
        };
        let heap = Heap {
            operators: HashMap::from([(2, bytes(Owner::Operator(2), 4096, 1024, 2048))]),
            outside: Some(bytes(Owner::Outside, 512, 256, 384)),
        };

        assert_eq!(
            operator_table(&metrics, None),
            "\
addr                 acts   active_ms   alloc_bytes  live_bytes   peak_bytes   name
[0]                  n/a    n/a         n/a          n/a          n/a          Dataflow
[0, 1]               1      2.500       n/a          n/a          n/a          Input
[0, 2]               n/a    n/a         n/a          n/a          n/a          Untracked
"
        );
        assert_eq!(
            operator_table(&metrics, Some(&heap)),
            "\
addr                 acts   active_ms   alloc_bytes  live_bytes   peak_bytes   name
[]                   n/a    n/a         512          256          384          (outside operators)
[0]                  n/a    n/a         0            0            0            Dataflow
[0, 1]               1      2.500       4096         1024         2048         Input
[0, 2]               n/a    n/a         n/a          n/a          n/a          Untracked
"
        );
    }

    #[test]
    fn an_operator_table_without_operators_says_so() {
        assert_eq!(
            operator_table(&Metrics::default(), None),
            "\
addr                 acts   active_ms   alloc_bytes  live_bytes   peak_bytes   name
(no operators recorded)
"
        );
    }

    /// Channel rows follow scope, then source, then target, and flag the
    /// channels whose type ships arrangement batches.
    #[test]
    fn channel_table_lists_channels_by_scope_then_endpoints() {
        let metrics = recorded(&[
            (0, connects(1, &[0, 4], (0, 0), (1, 0), "Vec<u32>")),
            (0, connects(2, &[0], (2, 0), (3, 1), "Vec<(u32, u32)>")),
            (
                0,
                connects(3, &[0], (2, 0), (3, 0), "Vec<Rc<OrdValBatch<..>>>"),
            ),
            (0, messages(2, /* is_send */ true, 5)),
            (0, messages(2, /* is_send */ false, 5)),
            (0, messages(3, /* is_send */ true, 1)),
        ]);
        let mut out = Vec::new();
        metrics.write_channels(&mut out).expect("writes to memory");
        assert_eq!(
            String::from_utf8(out).expect("the table is UTF-8"),
            "\
scope                src   src_port  tgt   tgt_port  batch  sent         recvd
[0]                  2     0         3     0         1      1            0
[0]                  2     0         3     1         0      5            5
[0, 4]               0     0         1     0         0      0            0
"
        );
    }
}
