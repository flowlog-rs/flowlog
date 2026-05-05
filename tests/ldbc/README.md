# tests/ldbc/

This directory is the **future home for the LDBC SNB correctness slice**:
small/medium scale-factor inputs with known-good outputs that the engine
must reproduce byte-for-byte.

It is intentionally empty today.

## Why it's empty

Until the perf split, this directory held `ldbc.sh` + `config.txt`, a
runner that downloaded SF3 datasets, executed FlowLog and DuckDB
side-by-side, diffed the answers, and reported per-row timing. That
shape blends correctness and perf in one script — see `../../AGENTS.md`
for why the split matters. Per the spec:

- the **timing slice** of LDBC (scale-factor / throughput, the runner +
  programs + data) moves to the sibling `flowlog-bench` repo
  (`scripts/ldbc.sh`, `programs/ldbc/{flowlog,souffle}/`,
  `facts/ldbc/`).
- the **correctness slice** (small/medium SF + static known-good
  outputs) stays here, but needs to be re-authored in the same shape as
  `tests/oracle/` — i.e. one or more `config_*.txt` files paired with
  pre-baked expected outputs.

The pre-split runner is preserved in git history at the
`pre-bench-split` tag if anyone needs to compare past behaviour.

## What "fits here" once authored

A correctness slice for LDBC should look like:

- one or more small SF dataset(s) checked into / fetched from a stable
  cache (small enough that a CI run can byte-diff in seconds, not hours)
- known-good output files (the "what *should* this query return on this
  input?" oracle) — preferably authored once from a trusted reference,
  then committed
- a runner script that lives next to (or shares) `tests/oracle/`'s
  `run_compiler.sh` / `run_lib.sh` so both lowering paths are exercised
- exit code 0 on byte-diff agreement, non-zero on any divergence

Anything that *measures* (wall time, peak RSS, scaling curves) belongs
in `flowlog-bench`, not here.
