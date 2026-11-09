# flowlog-runtime

Runtime support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate is consumed by code generated from [`flowlog-build`](https://crates.io/crates/flowlog-build).
You typically don't call into it directly.

## What it offers

- `RuntimeArgs` (optional `cli` feature) - executable argument parsing with
  directory defaults and Timely worker options.
- `io::Relation` - shared input and output relation declarations.
- `io::input` - one `Loader` per relation,
  fed from files, text puts, or typed host rows. The runtime owns worker
  partitioning and decoding for both generated binaries and libraries.
- `io::output` - `Emitter` collects worker results and emits text, typed
  snapshots, weighted deltas, or independent counts. Ordering and limits
  apply to text and snapshots; host deltas remain unfiltered.
- `error` - `RuntimeError` for input validation, ingestion, and SQLite output.
  Output text writers return `std::io::Result`.
- `profile` - `Profiler` records operator and channel metrics and writes
  profiling tables; `ProfilingAlloc` adds heap accounting by operator.
- `intern` — thread-safe string-interning pool.
- `txn` - transaction updates (`TxnOp`), the `Request`s an incremental
  driver makes of its workers, and `Broadcast`, which hands each request to
  every worker.
- `shell::Shell` (optional `shell` feature) - the interactive shell of
  incremental executables. It reads commands with line editing, history, and
  tab completion, and stages the updates they name into transactions.

## Usage

Add it alongside [`flowlog-build`](https://crates.io/crates/flowlog-build):

```toml
[dependencies]
flowlog-runtime = "0.6"

[build-dependencies]
flowlog-build = "0.7"
```

## License

MIT OR Apache-2.0, at your option — see [LICENSE-MIT](./LICENSE-MIT) and [LICENSE-APACHE](./LICENSE-APACHE).
