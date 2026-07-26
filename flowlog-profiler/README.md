# flowlog-profiler

Profiling model for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler: the plan graph written at compile time, and the reader that joins a profiled run's metrics back onto it. Consumed by [`flowlog-build`](https://crates.io/crates/flowlog-build) and FlowLog's profiling tools; you typically don't depend on it directly.

## Layout

- `plan` — `PlanGraph`: nodes, rules, blocks, and the builder that records them at compile time.
- `metrics` — `metrics::read()` joins a run's logs onto a plan, yielding per-transaction `Snapshot`s.
- `addr` — operator addresses that key metrics to plan nodes.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
