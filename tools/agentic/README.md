# tools/agentic — FlowLog test/benchmark harness for agentic loops

A clean, layered regression harness designed for **closed-loop agents**
that propose code changes, run the full test stack, and decide whether
to accept or revert based on a single diagnosis report. Every layer is
also runnable on its own; only this directory adds the unified
entry point and the memory-tracking instrumentation.

## Quick start

```bash
# Full sweep (L0 → L1 → L2 → L3, ~hours, datasets cached if available)
bash tools/agentic/run_full_sweep.sh

# Smoke sweep (~20 min: 3 unit fixtures, 3 complex pairs, 2 perf pairs)
bash tools/agentic/run_full_sweep.sh --smoke

# Skip the long performance compare layer
bash tools/agentic/run_full_sweep.sh --skip-l3

# Don't stop on first failure (keep collecting evidence)
bash tools/agentic/run_full_sweep.sh --keep-going

# Include the optional LDBC SNB layer
bash tools/agentic/run_full_sweep.sh --include-ldbc
```

Output lands under `result/sweep/<timestamp>/`:

| File | Purpose |
|---|---|
| `meta.txt`               | git head, branch, env, start time |
| `00-cargo-build.log` … `40-ldbc.log` | per-step transcripts |
| `comparison_results.csv` | copy of the perf CSV (if L3 ran) |
| **`diagnosis.txt`**      | rolled-up final report (also tee'd to stdout) |

The diagnosis lists each step with status (OK/FAIL/SKIP), elapsed
seconds, and either the last log line (on success) or the first
matching `error|fail|panic` line (on failure). When L3 ran, it also
prints a tabular summary of `Compiler_Exec`, `Lib_Exec`,
`Compiler_PeakRss_MB`, `Lib_PeakRss_MB`, etc. plus min/max/sum
aggregates.

## The four layers

| Layer | Runner(s) | What it gates on | First-time cost on this VM |
|---|---|---|---|
| **L0** Cargo workspace | `cargo build --release --workspace`, `cargo test --release --workspace` | Compiler + every `#[test]` (~168) | ~1–2 min cold, <15 s incremental |
| **L1** Unit fixtures | `tests/unit/unit_compiler.sh` (binary), `tests/unit/unit_lib.sh` (library) | ~95 fixtures across 4 modes (`datalog-batch`, `datalog-inc`, `extend-batch`, `extend-inc`); golden-file diffs | ~10–15 min |
| **L2** Souffle oracle | `tests/complex/datalog_batch_compiler.sh`, `tests/complex/datalog_batch_lib.sh` | ~25 (program, dataset) pairs from `config_integer.txt` and `config_string.txt`; every `.output` relation diffed against a pre-computed Souffle reference | ~1.5–3 h |
| **L3** Perf compare  | `tools/benchmark/compare.sh` | ~30 pairs in `tools/benchmark/config.txt`; runs interpreter / compiler / lib NUM_RUNS=5; **records BOTH wall time AND peak RSS** in `result/benchmark/comparison_results.csv` | ~6–12 h |
| **L4** LDBC SNB (opt-in) | `tests/ldbc/ldbc.sh` | LDBC interactive queries vs DuckDB | ~5–15 min |

## Memory measurement

Both `tools/benchmark/bench_one.sh` and `tools/benchmark/compare.sh`
now wrap every timed run in `/usr/bin/time -v` and parse the
`Maximum resident set size (kbytes)` line. The median peak RSS over
NUM_RUNS is recorded alongside the median elapsed time.

* `bench_one.sh` emits TWO contract lines on stdout:
  ```
  elapsed_seconds <median> <min> <max> <runs> <workers>
  peak_rss_kb     <median> <min> <max> <runs> <workers>
  ```
  The original `elapsed_seconds` line is unchanged so any extractor
  that only reads it keeps working untouched.

* `compare.sh` adds four columns at the end of `comparison_results.csv`
  (after the original 14 columns):
  `Interp_PeakRss_MB`, `Compiler_PeakRss_MB`, `Lib_PeakRss_MB`,
  `Lib_vs_Compiler_Mem`. Existing CSV consumers that ignore unknown
  trailing columns work unchanged.

* `print_pair_summary` adds a `MEM` line per pair to the console
  output.

To gate an agentic perf loop on memory regressions in addition to
time, point the perf-gate's extractor at `peak_rss_kb` (e.g. in a
groomer/minimalist TOML, copy the existing `[gates.perf]` block and
set `extract_token = "peak_rss_kb"`, `max_regression_pct = 10.0`).

## Constraints

The agent **must**:

* Run *all* correctness gates (L0 + L1 + L2) on every iteration. L1
  exercises a different lowering path than L2 and the Souffle oracle
  catches benchmark-program regressions the small unit fixtures miss.
* Treat any non-zero exit from any runner as a hard stop (default;
  override with `--keep-going` only for diagnostic runs).
* Verify a claimed speedup by running L3 at least twice (or by
  inspecting median stability across the 5 in-script repetitions).

The agent **must not**:

* Modify fixtures under `tests/unit/`, the configs in `tests/complex/`
  or `tools/benchmark/`, the baseline interpreter
  (`../vldb26-artifact`), or the Souffle reference tarballs on
  HuggingFace, in order to shift numbers. These are part of the
  contract being measured.
* Disable or skip any fixture inside any correctness runner.
* Change the allocator (`mimalloc` is intentional — see the comment
  in `tools/benchmark/lib_runner.sh`); a different allocator changes
  both timing and RSS in non-comparable ways.

## Caching tip

`compare.sh` calls `cleanup_dataset` after each pair by default. In an
agentic loop where you re-run the same datasets, set:

```bash
export FLOWLOG_KEEP_DATASETS=1
```

(or `source /datasets/env.sh` on the dev VM). Disk requirements are
in the tens of GB if you retain `arabic`/`orkut`/`livejournal`.

## Files

| Path | Purpose |
|---|---|
| `tools/agentic/run_full_sweep.sh` | Unified entry point — runs L0–L3 (+ optional L4) and emits `diagnosis.txt`. |
| `tools/benchmark/bench_one.sh`    | Single-pair perf wrapper used by per-iter perf gates; emits `elapsed_seconds` + `peak_rss_kb`. |
| `tools/benchmark/compare.sh`      | Multi-pair perf comparison vs interpreter/lib; CSV + console table; **time + RSS**. |
| `tests/unit/unit_compiler.sh`     | L1 binary-mode runner (golden files). |
| `tests/unit/unit_lib.sh`          | L1 library-mode runner (golden files). |
| `tests/complex/datalog_batch_compiler.sh` | L2 binary-mode runner (Souffle oracle). |
| `tests/complex/datalog_batch_lib.sh`      | L2 library-mode runner (Souffle oracle). |
| `tests/complex/config_integer.txt` / `config_string.txt` | (program, dataset) pairs for L2. |
| `tools/benchmark/config.txt`      | (program, dataset) pairs for L3. |
| `tests/ldbc/ldbc.sh`              | L4 (opt-in). |

## Exit code

`run_full_sweep.sh` exits `0` iff every layer that ran returned `0`.
The diagnosis file and console output both clearly show whether the
overall sweep passed or failed; the per-step table makes it trivial
for a wrapping agent to find which layer(s) regressed.
