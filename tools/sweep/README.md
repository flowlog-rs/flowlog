# tools/sweep — unified test/benchmark runner

`run_full_sweep.sh` runs FlowLog's four-suite regression stack in dependency order and emits one diagnosis report. For the suites themselves, see [`tests/README.md`](../../tests/README.md).

## Invocation

```bash
bash tools/sweep/run_full_sweep.sh [flags]
```

Flags:

- `--smoke` — tiny subsets per suite (~5 min total).
- `--skip-l3` — skip the long perf compare.
- `--include-ldbc` — opt-in L4 (LDBC SNB).
- `--keep-going` — don't abort on first failure; collect all evidence.
- `--workers N` — override `WORKERS` (default: `min(64, nproc)`).
- `--baseline=interpreter[,souffle]` — pick L3 baselines (default `interpreter`).
- `--num-runs N` — override L3 timed runs per pair (default 3).

See [`docs/testing.md`](../../docs/testing.md) for the full env-var contract (`L3_BASELINE`, `L3_NUM_RUNS`, `WORKERS`, `FLOWLOG_KEEP_DATASETS`, …).

Convenience targets in the top-level `Makefile`: `make smoke`, `make sweep`, `make sweep-no-perf`, `make perf`.

## Output

Everything lands under `result/sweep/<UTC-timestamp>/`:

- `meta.txt` — git head, branch, env, start time.
- `00-cargo-build.log` … `40-ldbc.log` — per-step transcripts.
- `comparison_results.csv` — copy of the L3 perf CSV (if L3 ran).
- **`diagnosis.txt`** — rolled-up final report; also tee'd to stdout.

The diagnosis lists each step with status (OK / FAIL / SKIP), elapsed seconds, and either the last log line (on success) or the first matching `error|fail|panic` line (on failure). When L3 ran, it also prints a tabular summary of `Compiler_Exec`, `Lib_Exec`, `Compiler_PeakRss_MB`, `Lib_PeakRss_MB`, plus min/max/sum aggregates.

## Exit code

Returns `0` iff every suite that ran returned `0`. The `OVERALL` line in `diagnosis.txt` mirrors this for human readers.

## Design notes

- **Each sweep is a snapshot.** The L3 perf compare is always invoked with `--fresh`, so `result/benchmark/` is wiped before the run. This means a sweep can never "resume" a partially-completed L3 from a previous attempt — by design, every sweep produces a complete, self-contained CSV (or none at all). If you need resume semantics for ad-hoc benchmarking, drop `--fresh` and invoke `tools/benchmark/compare.sh` directly.

- **No per-pair wall-clock timeout (yet).** A regression that hangs a single L3 pair (e.g. infinite loop in compiled binary, runaway Souffle program) will stall the entire sweep until the OS or the user kills it. LDBC has its own `--timeout 300s`, but L0–L3 do not. Adding a per-pair timeout is a robustness improvement worth a separate, careful PR — see the rubber-duck notes in commit `1c25918` (rss_log preservation under SIGTERM, compile-step coverage, and partial-success quorum all need handling).
