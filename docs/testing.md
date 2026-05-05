# Testing infrastructure

A 5-layer pipeline gated by one entry point. Each layer is cheaper than
the next; the cheap ones gate the expensive ones.

Per-suite details: [`tests/README.md`](../tests/README.md),
[`tools/sweep/README.md`](../tools/sweep/README.md).

## The 5 layers

| Layer | What it checks                                              | Time       |
|------:|-------------------------------------------------------------|------------|
| **L0** | `cargo test --release --workspace`                         | ~15 s warm |
| **L1** | ~95 small `.dl` fixtures, byte-diff vs `expected/`         | ~1 min     |
| **L2** | Real benchmark programs, byte-diff vs **Souffle** oracle   | ~2 min     |
| **L3** | Wall time + peak RSS vs interpreter and/or Souffle         | hours      |
| **L4** | LDBC SNB queries vs DuckDB *(opt-in)*                      | hours      |

L1 and L2 each run **twice**: once via the `flowlog-compiler` binary,
once via a synthesised crate that links the runtime as a library
(`unit_compiler` / `unit_lib`, `datalog_batch_compiler` / `_lib`). Both
must pass — they hit different code paths.

## One entry point

```bash
make smoke           # ~5 min — every layer, tiny subset
make sweep           # full regression sweep (hours)
make sweep-no-perf   # correctness only (skip L3)
make perf            # L3 in isolation
make test            # cargo test --release --workspace

# or directly:
bash tools/sweep/run_full_sweep.sh [flags]
```

The sweep runs L0 → L4 in order, captures per-step logs, and writes one
`diagnosis.txt` whose first line is `VERDICT: PASS / FAIL / PASS WITH N
FLAG(S)`.

## Sweep flags

| Flag                                | Purpose                                          |
|-------------------------------------|--------------------------------------------------|
| `--smoke`                           | Tiny subset per layer (~5 min).                  |
| `--skip-l3`                         | Correctness only — skip the long perf compare.   |
| `--include-ldbc`                    | Opt in to L4.                                    |
| `--keep-going`                      | Don't abort on first failure.                    |
| `--workers N`                       | Override thread budget (default `min(64, nproc)`). |
| `--baseline=interpreter[,souffle]`  | Pick L3 baselines (default `interpreter`).       |
| `--num-runs N`                      | L3 repetitions per pair (default 3, median kept). |

## Environment

| Var                       | Default          | Effect                                                                                  |
|---------------------------|------------------|-----------------------------------------------------------------------------------------|
| `WORKERS`                 | `min(64, nproc)` | Thread budget — applied **identically to every engine**. Fairness invariant.            |
| `L3_BASELINE`             | `interpreter`    | Comma-separated, any of `{interpreter, souffle}`. Same as `--baseline=`.                |
| `L3_NUM_RUNS`             | `3`              | Same as `--num-runs`.                                                                   |
| `FLOWLOG_KEEP_DATASETS`   | `0`              | If `1`, never `rm -rf` datasets after a run. Set by `source /datasets/env.sh`.          |
| `FLOWLOG_FORCE_CLEANUP`   | `0`              | Override the symlink cache guard. `KEEP=1` always wins.                                 |

`BASELINE` and `NUM_RUNS` (bare names) are accepted as back-compat
aliases for `L3_BASELINE` / `L3_NUM_RUNS`.

## L3 perf (`tools/benchmark/compare.sh`)

Pairs come from `tools/benchmark/config.txt`:

```text
graph_analysis/reach.dl=twitter [interp:skip] [souffle:skip]
graph_analysis/cc.dl=livejournal [souffle:skip]
```

Each pair runs under up to four engines:

- **Interpreter** — `vldb26-artifact` repo (sibling checkout).
- **Compiler**    — this repo, batch mode.
- **Lib mode**    — synthesised crate that links the runtime.
- **Souffle**     — canonical `.dl` from `tools/benchmark/souffle-programs/`.

Every run is wrapped in `/usr/bin/time -v` (peak RSS alongside wall time).
Output: `result/benchmark/comparison_results.csv` (26 columns).

Souffle compile recipe (FlowLog VLDB paper canonical form):

```bash
souffle -o <bin> -p /dev/null -j N -F <facts> <prog.dl>
```

`-o <bin>` (not `-c`) produces a parallel binary; `-j N` at compile time
tells Souffle to emit OpenMP pragmas. A build-time log line confirms
libgomp linkage.

## Outputs

Everything lands under `result/sweep/<UTC-timestamp>/`:

- `meta.txt`                 — git head, branch, env, start time.
- `00-cargo-build.log` … `40-ldbc.log` — per-step transcripts.
- `comparison_results.csv`   — copy of the L3 CSV (if L3 ran).
- **`diagnosis.txt`**        — verdict + per-step status table + perf
  roll-up + flagged anomalies (lib drift > 40 %, Souffle faster than
  compiler, compiler RSS > 2× interpreter, etc.). Also printed to stdout.

Exit `0` iff every layer that ran returned `0`.

## Recipes

```bash
# Quickest sanity check before pushing
make smoke

# Full correctness sweep, skip the long perf
bash tools/sweep/run_full_sweep.sh --skip-l3

# Targeted L3 on one pair
NUM_RUNS=3 bash tools/benchmark/compare.sh --fresh \
    --baseline=interpreter,souffle \
    <(echo 'graph_analysis/reach.dl=livejournal')

# Keep datasets between runs (skip HuggingFace re-downloads)
source /datasets/env.sh
```
