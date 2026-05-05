# FlowLog testing infrastructure — high level

> A crisp overview of the test/benchmark stack: layers, the single
> entry point, the flags you actually need, and the env-var contract.
> Per-suite implementation details live in [`tests/README.md`](../tests/README.md)
> and [`tools/sweep/README.md`](../tools/sweep/README.md).

## The 5 layers (cheap → expensive, each gates the next)

| Layer | What it checks                                                   | Tool                                                                                                | Time      |
|-------|------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|-----------|
| **L0** | Rust unit tests across the workspace                            | `cargo test --release --workspace`                                                                  | ~15 s warm |
| **L1** | ~95 small `.dl` fixtures, byte-diff vs `expected/`              | `tests/unit/unit_compiler.sh` + `unit_lib.sh`                                                       | ~1 min    |
| **L2** | Real benchmark programs, byte-diff vs **Souffle** reference (correctness oracle) | `tests/complex/datalog_batch_compiler.sh` + `_lib.sh`                                               | ~2 min    |
| **L3** | Wall time + peak RSS vs interpreter and/or Souffle              | `tools/benchmark/compare.sh`                                                                        | ~hours    |
| **L4** | LDBC SNB queries vs DuckDB (opt-in)                             | `tests/ldbc/ldbc.sh`                                                                                | ~hours    |

L1 and L2 each run in **two modes**:

- `unit_compiler` / `datalog_batch_compiler` — exercises the
  `flowlog-compiler` binary path.
- `unit_lib` / `datalog_batch_lib` — synthesises a small Rust crate that
  links the runtime as a library and calls `engine.run()`.

Both must pass — they hit different code paths.

## One entry point

```bash
bash tools/sweep/run_full_sweep.sh [flags]
# or via the Makefile:
make smoke           # ~5 min — every layer, tiny subset
make sweep           # full regression sweep (hours)
make sweep-no-perf   # correctness only (skip L3)
make perf            # L3 in isolation
make test            # cargo test --release --workspace
```

The sweep script runs L0 → L4 in order, captures per-step logs, and
writes one `diagnosis.txt` whose first line is a one-shot
`VERDICT: PASS / FAIL / PASS WITH N FLAG(S)`.

## Sweep flags (the only ones you need to remember)

| Flag                                   | Purpose                                                                  |
|----------------------------------------|--------------------------------------------------------------------------|
| `--smoke`                              | Tiny subset per layer — ~5 min total. For quick sanity.                  |
| `--skip-l3`                            | Skip the long perf compare. Correctness-only sweep.                      |
| `--include-ldbc`                       | Opt in to L4 (off by default).                                            |
| `--keep-going`                         | Don't abort on first failure; collect all evidence.                      |
| `--workers N`                          | Override thread budget (default `min(64, nproc)`).                        |
| `--baseline=interpreter[,souffle]`     | Pick L3 baselines. Default `interpreter`. Forwarded verbatim to compare.sh. |
| `--num-runs N`                         | Override L3 repetitions (default 5, median kept).                         |

## Environment variables (the contract)

| Var                       | Default            | Effect                                                                                                                                                                                  |
|---------------------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `WORKERS`                 | `min(64, nproc)`   | Thread budget — applied **identically to every engine** (interpreter `--workers`, compiler `-w`, lib mode env, Souffle `-j` at both compile AND run time). This is the fairness invariant. |
| `FLOWLOG_KEEP_DATASETS`   | `0`                | If `1`, never `rm -rf` datasets after a run. Set automatically by `source /datasets/env.sh` on the dev VM.                                                                              |
| `FLOWLOG_FORCE_CLEANUP`   | `0`                | Override the symlink-aware cache guard (only relevant if `facts/` is a symlink to a shared cache). `KEEP=1` always wins over `FORCE=1`.                                                |

The `cleanup_dataset` helper used by L2/L3/L4 honours these three
variables identically across all three layers (canonical
`CACHE_PATCH_v2` form).

## L3 specifics

`tools/benchmark/compare.sh` has its own per-pair config
(`tools/benchmark/config.txt`) listing `program=dataset` pairs and
optional skip tags:

```text
graph_analysis/reach.dl=twitter [interp:skip] [souffle:skip]
graph_analysis/sssp.dl=livejournal-sssp [interp:skip]
```

Each pair is timed under up to four engines:

- **Interpreter** (`vldb26-artifact` repo, expected as a sibling checkout).
- **Compiler** (this repo, batch mode).
- **Lib mode** (synthesised runner crate that links the runtime).
- **Souffle** (canonical `.dl` programs from `tools/benchmark/souffle-programs/`).

Every run is wrapped in `/usr/bin/time -v` so peak RSS is captured
alongside wall time. Output: `result/benchmark/comparison_results.csv`
(22 columns including `*_Load`, `*_Exec`, `*_Total`, `*_PeakRss_MB`,
`Souffle_vs_Compiler_Total`, `Crosscheck_Souffle`, etc.).

The Souffle compile recipe is the FlowLog VLDB paper canonical form:

```bash
souffle -o <bin> -p /dev/null -j N -F <facts> <prog.dl>
```

`-o <bin>` (NOT `-c`) is what produces a parallel binary; `-j N` at
compile time is what tells Souffle to emit OpenMP pragmas at codegen.
A build-time sanity log line counts the pragmas in the generated `.cpp`
so you can see whether the binary is actually parallel.

## Outputs

Everything lands under `result/sweep/<UTC-timestamp>/`:

- `meta.txt` — git head, branch, env, start time.
- `00-cargo-build.log` … `40-ldbc.log` — per-step transcripts.
- `comparison_results.csv` — copy of the L3 perf CSV (if L3 ran).
- **`diagnosis.txt`** — top-of-file verdict, per-step status table,
  perf CSV roll-up with min/max/sum aggregates, and a list of
  flagged anomalies (lib drift > 40 %, Souffle faster than compiler,
  compiler RSS > 2× interpreter, etc.). Also printed to stdout.

Exit code is `0` iff every layer that ran returned `0`.

## Quickstart recipes

```bash
# Quickest sanity check before pushing
make smoke

# Full correctness sweep, skip the long perf
bash tools/sweep/run_full_sweep.sh --skip-l3

# Targeted L3 perf compare on one program=dataset pair
NUM_RUNS=3 bash tools/benchmark/compare.sh --fresh \
    --baseline=interpreter,souffle <(echo 'graph_analysis/reach.dl=livejournal')

# Keep datasets between runs (avoid HuggingFace re-downloads)
source /datasets/env.sh    # exports FLOWLOG_KEEP_DATASETS=1
```
