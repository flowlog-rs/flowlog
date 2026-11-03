//! Times one lookup step on synthetic data three ways: FlowLog's lookup,
//! differential-dogs3's half joins as Materialize calls them, and a binary
//! join, so the lookup's cost can be set against the implementations it
//! follows.
//!
//! ```text
//! lookup-bench ENGINE SCENARIO [WORKERS]
//! ```
//!
//! Every engine extends each prefix `(k, p)` by each value `v` that `k` has
//! in a relation `A(k, v)` arranged by key, and emits `(p, v)`:
//!
//! - `flowlog`: `flowlog_path`, `flowlog_lookup` with `Version::New` and
//!   `flowlog_land`, as a generated plan calls them.
//! - `flowlog-noland`: the same without `flowlog_land`, a map that moves
//!   nothing on a totally ordered clock.
//! - `hj1`, `hj2`: differential-dogs3 0.25.1's `half_join` and
//!   `half_join2`, as Materialize calls them: comparing times with `<=` and
//!   yielding after a million matches.
//! - `join`: arranges the prefixes by key and joins the two arrangements,
//!   as a binary plan does.
//!
//! Scenarios:
//!
//! - `int`: `u32` keys. `A` holds 8M pairs over 2M keys from time 0, and
//!   4M prefixes arrive at time 1.
//! - `str`: 30-byte `String` keys, as FlowLog generates for a string column
//!   without `--str-intern`, at a quarter of `int`'s sizes.
//! - `inc`: `int` with the prefixes spread over 100 epochs, each of which
//!   also adds 20K pairs to `A` for its own prefixes to see. A binary join
//!   would also join each epoch's pairs with the earlier prefixes, so `join`
//!   does not run it.
//!
//! Prints one line of `name=value` fields: the seconds to arrange `A` and to
//! answer the prefixes, the records emitted, their total weight and a
//! checksum every engine must agree on, and the process's peak resident
//! memory in MiB once `A` is arranged and once the prefixes are answered.
//! `scripts/lookup_bench.sh` runs every engine on every scenario and prints
//! the medians.

use std::cell::Cell;
use std::ops::Range;
use std::rc::Rc;
use std::time::Instant;

use differential_dataflow::AsCollection;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use differential_dataflow::input::Input;
use differential_dataflow::operators::arrange::Arranged;
use differential_dataflow::operators::arrange::TraceAgent;
use differential_dataflow::trace::implementations::ValSpine;
use differential_dogs3::operators::half_join;
use differential_dogs3::operators::half_join2;
use flowlog_runtime::operators::Version;
use flowlog_runtime::operators::flowlog_land;
use flowlog_runtime::operators::flowlog_lookup;
use flowlog_runtime::operators::flowlog_map;
use flowlog_runtime::operators::flowlog_path;
use flowlog_runtime::time::Epoch;
use timely::container::CapacityContainerBuilder;
use timely::container::PushInto;
use timely::dataflow::ProbeHandle;
use timely::progress::Antichain;
use timely::worker::Worker;

const USAGE: &str = "usage: lookup-bench ENGINE SCENARIO [WORKERS]
  ENGINE:   flowlog | flowlog-noland | hj1 | hj2 | join
  SCENARIO: int | str | inc";

/// Times are epochs, as in an incremental FlowLog program.
type Time = Epoch;

/// `A(k, v)`, arranged by `k`.
type Relation<'scope, K> = Arranged<'scope, TraceAgent<ValSpine<K, u32, Time, isize>>>;

/// The prefixes `(k, p)`.
type Prefixes<'scope, K> = VecCollection<'scope, Time, (K, u32), isize>;

/// The matches after which a half join yields, as Materialize sets it in
/// `src/compute/src/render/join/delta_join.rs`.
const MATCHES_PER_YIELD: usize = 1_000_000;

/// Tags that keep the rows of `A` and of the prefixes apart.
const PAIRS: u64 = 1;
const PREFIXES: u64 = 2;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let (engine, scenario, workers) = match args.as_slice() {
        [engine, scenario] => (engine.as_str(), scenario.as_str(), "1"),
        [engine, scenario, workers] => (engine.as_str(), scenario.as_str(), workers.as_str()),
        _ => fail("expected two or three arguments"),
    };
    let chosen = Engine::parse(engine).unwrap_or_else(|| fail("unknown engine"));
    let shape = match scenario {
        "int" | "str" => Shape::STATIC,
        "inc" if chosen == Engine::Join => fail("`join` does not run `inc`"),
        "inc" => Shape::INCREMENTAL,
        _ => fail("unknown scenario"),
    };
    let strings = scenario == "str";
    let shape = if strings { shape.quarter() } else { shape };
    let peers: usize = workers
        .parse()
        .ok()
        .filter(|&peers| peers > 0)
        .unwrap_or_else(|| fail("WORKERS must be a positive whole number"));

    let guards = timely::execute(timely::Config::process(peers), move |worker| {
        if strings {
            run::<String>(worker, chosen, shape)
        } else {
            run::<u32>(worker, chosen, shape)
        }
    })
    .expect("timely starts its workers");
    let reports: Vec<Report> = guards
        .join()
        .into_iter()
        .map(|report| report.expect("every worker finishes"))
        .collect();

    let slowest = |seconds: fn(&Report) -> f64| reports.iter().map(seconds).fold(0.0, f64::max);
    let peak = |mib: fn(&Report) -> u64| reports.iter().map(mib).max().unwrap_or(0);
    println!(
        "engine={engine} scenario={scenario} workers={peers} arrange_s={:.3} answer_s={:.3} \
         records={} weight={} checksum={:016x} peak_arranged_mib={} peak_mib={}",
        slowest(|report| report.arrange_s),
        slowest(|report| report.answer_s),
        reports
            .iter()
            .map(|report| report.tally.records)
            .sum::<u64>(),
        reports
            .iter()
            .map(|report| report.tally.weight)
            .sum::<i64>(),
        reports
            .iter()
            .fold(0u64, |sum, report| sum.wrapping_add(report.tally.checksum)),
        peak(|report| report.peak_arranged_mib),
        peak(|report| report.peak_mib),
    );
}

/// Prints `problem` and the usage, and exits.
fn fail(problem: &str) -> ! {
    eprintln!("lookup-bench: {problem}\n{USAGE}");
    std::process::exit(2)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Engine {
    Flowlog,
    FlowlogNoLand,
    HalfJoin1,
    HalfJoin2,
    Join,
}

impl Engine {
    fn parse(name: &str) -> Option<Self> {
        Some(match name {
            "flowlog" => Engine::Flowlog,
            "flowlog-noland" => Engine::FlowlogNoLand,
            "hj1" => Engine::HalfJoin1,
            "hj2" => Engine::HalfJoin2,
            "join" => Engine::Join,
            _ => return None,
        })
    }
}

/// The sizes of a scenario.
#[derive(Debug, Clone, Copy)]
struct Shape {
    /// Pairs `A` holds from time 0.
    pairs: u64,
    /// Keys the pairs and the prefixes draw from.
    keys: u64,
    /// Prefixes in all, spread evenly over the epochs.
    prefixes: u64,
    /// Epochs the prefixes arrive in, the first at time 1.
    epochs: u32,
    /// Pairs each epoch adds to `A`.
    pairs_per_epoch: u64,
}

impl Shape {
    const STATIC: Shape = Shape {
        pairs: 8_000_000,
        keys: 2_000_000,
        prefixes: 4_000_000,
        epochs: 1,
        pairs_per_epoch: 0,
    };

    const INCREMENTAL: Shape = Shape {
        epochs: 100,
        pairs_per_epoch: 20_000,
        ..Shape::STATIC
    };

    fn quarter(self) -> Shape {
        Shape {
            pairs: self.pairs / 4,
            keys: self.keys / 4,
            prefixes: self.prefixes / 4,
            pairs_per_epoch: self.pairs_per_epoch / 4,
            ..self
        }
    }
}

/// A key type, built from a key id.
trait Key: ExchangeData + Hashable + Ord {
    fn of(id: u64) -> Self;
}

impl Key for u32 {
    fn of(id: u64) -> Self {
        u32::try_from(id).expect("key ids fit in u32")
    }
}

impl Key for String {
    /// A 30-byte name.
    fn of(id: u64) -> Self {
        format!("<pkg.Type{id:08}: void m()>/0")
    }
}

/// SplitMix64's finalizer: a well-mixed 64-bit value for each input.
fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

/// Row `index` of the relation tagged `relation`: a key among `keys` and a
/// payload.
fn row<K: Key>(relation: u64, index: u64, keys: u64) -> (K, u32) {
    let bits = mix(relation << 56 | index);
    (K::of((bits & 0xFFFF_FFFF) % keys), (bits >> 32) as u32)
}

/// The rows of `relation` in `indices` that this worker feeds.
fn share<K: Key>(relation: u64, indices: Range<u64>, keys: u64, worker: &Worker) -> Vec<(K, u32)> {
    let (index, peers) = (worker.index() as u64, worker.peers() as u64);
    indices
        .filter(|i| i % peers == index)
        .map(|i| row(relation, i, keys))
        .collect()
}

/// What an engine emitted on one worker.
#[derive(Debug, Clone, Copy, Default)]
struct Tally {
    records: u64,
    weight: i64,
    /// The sum of each record's hash times its weight, so that records
    /// split or merged differently still agree.
    checksum: u64,
}

/// A worker's measurements.
#[derive(Debug, Clone, Copy)]
struct Report {
    arrange_s: f64,
    answer_s: f64,
    tally: Tally,
    peak_arranged_mib: u64,
    peak_mib: u64,
}

/// The process's peak resident memory so far, in MiB.
fn peak_mib() -> u64 {
    let status = std::fs::read_to_string("/proc/self/status").expect("/proc/self/status");
    status
        .lines()
        .find_map(|line| line.strip_prefix("VmHWM:"))
        .and_then(|kib| kib.trim().trim_end_matches("kB").trim().parse::<u64>().ok())
        .expect("VmHWM in kB")
        / 1024
}

/// Arranges `A` at time 0, then feeds each epoch's prefixes and pairs and
/// waits for `engine` to answer them.
fn run<K: Key>(worker: &mut Worker, engine: Engine, shape: Shape) -> Report {
    let tally = Rc::new(Cell::new(Tally::default()));
    let probe = ProbeHandle::new();
    let (mut pairs, mut prefixes) = worker.dataflow::<Time, _, _>(|scope| {
        let (pairs, relation) = scope.new_collection::<(K, u32), isize>();
        let (prefixes, rows) = scope.new_collection::<(K, u32), isize>();
        build(engine, rows, relation.arrange_by_key(), &tally, &probe);
        (pairs, prefixes)
    });

    let started = Instant::now();
    for pair in share::<K>(PAIRS, 0..shape.pairs, shape.keys, worker) {
        pairs.update(pair, 1);
    }
    pairs.advance_to(1);
    prefixes.advance_to(1);
    pairs.flush();
    prefixes.flush();
    worker.step_while(|| probe.less_than(&1));
    let arrange_s = started.elapsed().as_secs_f64();
    let peak_arranged_mib = peak_mib();

    let rows_per_epoch = shape.prefixes / u64::from(shape.epochs);
    let batches: Vec<_> = (0..u64::from(shape.epochs))
        .map(|epoch| {
            let rows = epoch * rows_per_epoch..(epoch + 1) * rows_per_epoch;
            let added = shape.pairs + epoch * shape.pairs_per_epoch
                ..shape.pairs + (epoch + 1) * shape.pairs_per_epoch;
            (
                share::<K>(PREFIXES, rows, shape.keys, worker),
                share::<K>(PAIRS, added, shape.keys, worker),
            )
        })
        .collect();
    let started = Instant::now();
    for (epoch, (rows, added)) in (1..).zip(batches) {
        for row in rows {
            prefixes.update(row, 1);
        }
        for pair in added {
            pairs.update(pair, 1);
        }
        pairs.advance_to(epoch + 1);
        prefixes.advance_to(epoch + 1);
        pairs.flush();
        prefixes.flush();
        worker.step_while(|| probe.less_than(&(epoch + 1)));
    }
    let answer_s = started.elapsed().as_secs_f64();

    Report {
        arrange_s,
        answer_s,
        tally: tally.get(),
        peak_arranged_mib,
        peak_mib: peak_mib(),
    }
}

/// Builds `engine` over `rows` and `relation`, counting what it emits into
/// `tally` and its progress into `probe`.
fn build<'scope, K: Key>(
    engine: Engine,
    rows: Prefixes<'scope, K>,
    relation: Relation<'scope, K>,
    tally: &Rc<Cell<Tally>>,
    probe: &ProbeHandle<Time>,
) {
    let count = {
        let tally = Rc::clone(tally);
        move |out: &(u32, u32), lands: &Time, weight: isize| {
            let mut sum = tally.get();
            sum.records += 1;
            sum.weight += weight as i64;
            sum.checksum = sum
                .checksum
                .wrapping_add((out, lands).hashed().wrapping_mul(weight as u64));
            tally.set(sum);
        }
    };
    match engine {
        Engine::Flowlog | Engine::FlowlogNoLand => {
            let path = flowlog_lookup(
                flowlog_path(rows, "Path"),
                relation,
                "Lookup",
                Version::New,
                |(key, _): &(K, u32)| key.clone(),
                |&(_, payload): &(K, u32), &value: &u32| Some((payload, value)),
            );
            if engine == Engine::Flowlog {
                flowlog_land(path, "Land")
                    .inspect(move |(out, time, weight)| count(out, time, *weight))
                    .probe_with(probe);
            } else {
                path.inspect(move |((out, lands), _, weight)| count(out, lands, *weight))
                    .probe_with(probe);
            }
        }
        Engine::HalfJoin1 => {
            half_join::half_join_internal_unsafe::<_, _, _, _, _, _, _, _, Builder>(
                proposals(rows),
                relation,
                step_back,
                sees,
                |_, matches| matches > MATCHES_PER_YIELD,
                |session, _, &payload, &value, &time, &weight, matches| {
                    for (lands, matched) in matches.drain(..) {
                        session.give((((payload, value), lands), time, weight * matched));
                    }
                },
            )
            .as_collection()
            .inspect(move |((out, lands), _, weight)| count(out, lands, *weight))
            .probe_with(probe);
        }
        Engine::HalfJoin2 => {
            half_join2::half_join_internal_unsafe::<_, _, _, _, _, _, _, _, Builder>(
                proposals(rows),
                relation,
                step_back,
                sees,
                |_, matches| matches > MATCHES_PER_YIELD,
                |builder, _, &payload, &value, &time, &weight, matches| {
                    for (lands, matched) in matches.drain(..) {
                        builder.push_into((((payload, value), lands), time, weight * matched));
                    }
                },
            )
            .as_collection()
            .inspect(move |((out, lands), _, weight)| count(out, lands, *weight))
            .probe_with(probe);
        }
        Engine::Join => {
            rows.arrange_by_key()
                .join_core(relation, |_, &payload, &value| Some((payload, value)))
                .inspect(move |(out, time, weight)| count(out, time, *weight))
                .probe_with(probe);
        }
    }
}

/// The output builder Materialize gives a half join.
type Builder = CapacityContainerBuilder<Vec<(((u32, u32), Time), Time, isize)>>;

/// The rows as a half join takes them, `(k, p, t)`: each carries the time
/// its matches land at, its own to start with.
fn proposals<'scope, K: Key>(
    rows: Prefixes<'scope, K>,
) -> VecCollection<'scope, Time, (K, u32, Time), isize> {
    flowlog_map(rows, "Proposals", |(key, payload), time: Time, weight| {
        std::iter::once(((key, payload, time), time, weight))
    })
}

/// The compaction a half join allows while a row at `time` waits, as
/// Materialize sets it: one step back.
fn step_back(time: &Time, frontier: &mut Antichain<Time>) {
    frontier.insert(time.saturating_sub(1));
}

/// Whether a row at `row` sees an update at `update`, as `Version::New`
/// decides.
fn sees(update: &Time, row: &Time) -> bool {
    update <= row
}
