# flowlog-profiler

Profiling model for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate defines the plan graph FlowLog writes at compile time and the
reader that joins a profiled run's metrics back onto it. It is consumed by
[`flowlog-build`](https://crates.io/crates/flowlog-build) and FlowLog's
profiling tools; you typically don't depend on it directly.

## What it offers

- `PlanGraph` — the shared model: flowlog-build records it at compile
  time; every consumer deserializes it and reads its nodes and rules to
  interpret metrics.
- `metrics` — `metrics::read()` joins a run's logs onto a plan and returns
  per-transaction `Snapshot`s: per-node and per-operator measured facts
  keyed to plan ids, with no plan structure re-shipped.

The split is deliberate: the profiler exposes facts keyed to the plan; a
consumer holds the `PlanGraph` and joins by node id, deriving roots,
trees, and views itself.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
