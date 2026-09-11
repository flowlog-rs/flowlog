# flowlog-runtime

Runtime support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate is consumed by code generated from [`flowlog-build`](https://crates.io/crates/flowlog-build).
You typically don't call into it directly.

## What it offers

- `RuntimeArgs` (optional `cli` feature) - executable argument parsing with
  directory defaults and Timely worker options.
- `io::input` — `Relation` declarations and one `Loader` per relation,
  fed from files, text puts, or typed host rows. The runtime owns worker
  partitioning and decoding for both generated binaries and libraries.
- `io::write_atomic` — atomic output-file replacement.
- `error` — `RuntimeError`, everything the runtime can fail at.
- `intern` — thread-safe string-interning pool.
- `sort` — `k_way_merge` and `topk` used by generated `ORDER BY` / `LIMIT`
  drain code.
- `txn` — transaction state types (`TxnOp`, `TxnAction`, `TxnState`)
  consumed by incremental-mode drivers to broadcast per-epoch commits.

## Usage

Add it alongside [`flowlog-build`](https://crates.io/crates/flowlog-build):

```toml
[dependencies]
flowlog-runtime = "0.3"

[build-dependencies]
flowlog-build = "0.4"
```

## License

Apache-2.0 — see [LICENSE](./LICENSE).
