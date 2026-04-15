# flowlog-runtime

Runtime support crate for [FlowLog](https://github.com/flowlog-rs/flowlog), a
Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow)
compiler.

This crate is consumed by code generated from [`flowlog-build`](https://crates.io/crates/flowlog-build).
You typically don't call into it directly.

## What it offers

- Re-exports of `timely`, `differential-dataflow`, `ordered-float`, `serde`,
  and `lasso` so that generated code only needs one dependency.
- `Relation` trait — implemented by each generated input struct.
- `io` — parallel-ingest helpers: `partition`, `byte_range_reader`, and
  first-column sharding.
- `intern` — thread-safe string-interning pool.
- `sort` — `k_way_merge` and `topk` used by generated `ORDER BY` / `LIMIT`
  drain code.

## Usage

Add it alongside [`flowlog-build`](https://crates.io/crates/flowlog-build):

```toml
[dependencies]
flowlog-runtime = "0.1"

[build-dependencies]
flowlog-build = "0.1"
```

## License

Apache-2.0 — see [LICENSE](./LICENSE).
