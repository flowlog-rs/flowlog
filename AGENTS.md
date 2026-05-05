# flowlog — testing scope

This repo is the **correctness** surface for FlowLog. Performance work lives elsewhere.
Anything an agent does here should be in service of: build the engine, prove it answers
queries correctly on a known set of programs and inputs.

**Shape of the repo.** Two things live here: the engine source (`crates/`) and a
**library of test scripts** (`tests/` + minimal `tools/env*` plumbing). There is
no top-level orchestrator. Each script is independently runnable with a stable
exit code; composing them into a full pass is the caller's job (CI, agent
pipeline, dev workflow, or the bench repo). If you find yourself wanting to add
a "run everything" wrapper, that wrapper belongs in the *consumer*, not here.

## Repo split (read this first)

| Concern              | Lives in              | Notes                                        |
| -------------------- | --------------------- | -------------------------------------------- |
| Engine source        | `crates/`             | Rust workspace                               |
| Correctness tests    | `tests/`              | Fixtures, oracle outputs, LDBC, safety       |
| Test env / doctor    | `tools/env*`          | `env.sh`, `env.ps1`, `env_check.sh`          |
| Performance benches  | **separate repo**     | [`flowlog-rs/flowlog-bench`](https://github.com/flowlog-rs/flowlog-bench) (bootstrapped from `pre-bench-split`) |

**Functional vs. perf rule.** Several suites (LDBC is the obvious one) have both a
correctness flavor and a timing flavor. Always split them:

- the *functional* half — "did we get the right answer?" — stays here
- the *perf* half — "how fast / how much memory?" — moves to `flowlog-bench`

If a single file does both, separate them at split time. Don't import perf cases here
"because they're already in the dataset."

## What lives in `tests/`

- `tests/fixtures/`   — small Datalog programs + input CSVs used by oracle tests
- `tests/oracle/`     — expected outputs; the comparator decides pass/fail
- `tests/ldbc/`       — LDBC **correctness** cases only (small/medium scale-factor
                        inputs with known-good outputs). LDBC timing/scaling runs
                        live in `flowlog-bench`.
- `tests/safety/`     — cross-cutting invariants. Today: a regression test that
                        every implementation of `cleanup_dataset` honours the
                        symlink-guard contract, so `rm -rf` through the `facts/`
                        symlink can't nuke the shared dataset cache. Lives outside
                        `tests/oracle` / `tests/ldbc` because it exercises all of
                        them at once.
- `tests/lib/`        — shared bash/python helpers used only by tests

Entry point: `make test` (and `make oracle` for the oracle suite alone).

## What lives in `tools/` (in this repo)

Only environment plumbing that the test suite itself needs:

- `tools/env/env.sh`, `tools/env/env.ps1` — **one-time** machine setup
  (Linux/macOS and Windows). Run once on a fresh dev box / runner image;
  installs rustup, apt/brew/choco packages, then `cargo check`. Not meant to
  be called on every run.
- `tools/env_check.sh`                    — fast (<1 s) read-only health probe
  for per-session / per-checkout state (env vars, `facts/` symlink, dataset
  cache, tree cleanliness). Sole consumer: `make doctor`.

Anything that *measures* belongs in the bench repo, not here.

## What moved out → `flowlog-bench`

`flowlog-bench`'s job, from this repo's point of view, is exactly two things:
**grab the latest flowlog, run comparisons.** What it compares against (past
flowlog versions, Soufflé, DuckDB, anything else) is the bench repo's concern,
not ours. We publish a buildable flowlog; they consume it.

Two kinds of things leave with the split:

**(a) Goes to `flowlog-bench`** — the perf-measurement scripts and corpus:

- single-program timing runner (`bench_one.sh`)
- cross-engine compare (`compare.sh` — flowlog vs. Soufflé/interpreter at one version)
- A/B-version regression check (`perf_compare.sh` — flowlog@base vs. flowlog@head)
- Soufflé reference cache, plotting (`plot_speedup.py`)
- **Soufflé programs corpus** (`tools/benchmark/souffle-programs/`) — these
  exist only as cross-engine perf inputs; they have no role in correctness
  testing here.
- timing slice of LDBC (scale-factor / throughput).

**(b) Just deleted** — the sweep orchestrator (`tools/sweep/`):

`run_full_sweep.sh` and its companion Makefile targets (`make smoke`,
`make sweep`, `make sweep-no-perf`, `make perf`) are removed outright. They
exist today because correctness layers and a perf layer were entangled in
one stack; with that entanglement gone, the orchestrator has no reason to
live here. If `flowlog-bench` wants a perf-side sweep, it writes its own
that calls `cross_engine.sh` + `ldbc.sh`. If CI or an agent loop wants a
correctness sweep here, it chains `make test` + `make oracle` + `make
test-safety` itself — three lines, no bash glue needed.

`flowlog-bench` consumes flowlog as a buildable input (a git ref it can fetch
and build). It does not write back into this tree.

## Contract for agents

When operating in this repo:

1. **Do** add/modify code under `crates/`, `tests/`, `tools/env*`.
2. **Do** run `make doctor` first if env failures appear; surface the report verbatim.
3. **Do** use `make test` / `make oracle` as the success signal. A green oracle run is
   the definition of "correct."
4. **Do**, when adding an LDBC (or similar) case, ask: is this checking an *answer* or
   a *number*? Answers stay; numbers go to `flowlog-bench`.
5. **Don't** add timing assertions, perf gates, or benchmark scripts here. Open an
   issue against `flowlog-bench` instead.
6. **Don't** import datasets larger than the existing fixtures. Big inputs live in the
   bench repo.
7. **Don't** edit `tools/benchmark/` or `tools/sweep/` — `tools/benchmark/`
   leaves with the bench split, and `tools/sweep/` is being deleted outright
   (this repo doesn't ship an orchestrator). Treat both as read-only legacy.
8. **Don't** add a "run everything" wrapper here. If you need to chain
   suites, do it in your CI / agent / Makefile target on the *consumer* side.

## Pipeline hand-off

| Stage           | Repo             | Output                                    |
| --------------- | ---------------- | ----------------------------------------- |
| Build + correct | `flowlog`        | binary + `make test` green                |
| Perf measure    | `flowlog-bench`  | timing CSVs, speedup plots, regressions   |

A change is shippable when this repo's tests pass. Perf signoff is a separate gate
owned by `flowlog-bench`.

## The sibling repo: `flowlog-bench`

This section is the **handoff spec** for the bench split. It lives here (not
in the bench repo) so that whoever does the move knows what to take, what to
leave, and what shape to drop it into.

### What moves in (file-by-file)

Naming note for the move: the two existing "compare" scripts measure on
**orthogonal axes** but share a name that hides it. Rename at split time so
the axis is obvious from the filename:

| Old name                  | New name                     | Axis                                  |
| ------------------------- | ---------------------------- | ------------------------------------- |
| `compare.sh`              | `scripts/cross_engine.sh`    | flowlog vs. other engines, fixed ver. |
| `perf_compare.sh`         | `scripts/regression.sh`      | flowlog@A vs. flowlog@B, same engine  |
| `bench_one.sh`            | `scripts/bench_one.sh`       | shared primitive, unchanged           |

File-by-file:

| From (this repo)                        | To (`flowlog-bench`)             |
| --------------------------------------- | -------------------------------- |
| `tools/benchmark/bench_one.sh`          | `scripts/bench_one.sh`           |
| `tools/benchmark/compare.sh`            | `scripts/cross_engine.sh`        |
| `tools/benchmark/lib_runner.sh`         | `scripts/lib/runner.sh`          |
| `tools/benchmark/plot_speedup.py`       | `plotting/plot_speedup.py`       |
| `tools/benchmark/config.txt`            | `config/default.txt`             |
| `tools/benchmark/souffle-programs/`     | `programs/micro/souffle/`        |
| `tools/perf_compare.sh`                 | `scripts/regression.sh`          |
| timing slice of `tests/ldbc/` (programs) | `programs/ldbc/{flowlog,souffle}/` |
| timing slice of `tests/ldbc/` (data)     | `facts/ldbc/` (gitignored, fetched) |
| timing slice of `tests/ldbc/` (runner)   | `scripts/ldbc.sh`                 |
| perf-only helpers from `tests/lib/`     | `scripts/lib/`                   |
| `Makefile` targets `bench-one`, perf-* | `Makefile` (re-rooted)           |

Deleted, not moved: `tools/sweep/run_full_sweep.sh`, `tools/sweep/README.md`,
and the Makefile targets `smoke` / `sweep` / `sweep-no-perf` / `perf`. The
bench repo writes its own perf-side sweep if it wants one; this repo doesn't
ship an orchestrator at all.

What does **not** move: `crates/`, `tests/oracle/`, `tests/safety/`,
`tests/fixtures/`, the correctness slice of `tests/ldbc/`, `tools/env/*`,
`tools/env_check.sh`. Those are this repo's job.

### Proposed structure

```
flowlog-bench/
├── README.md              — purpose, quickstart
├── AGENTS.md              — agent contract (mirrors this file's discipline)
├── Makefile               — single source of entry points
├── tools/
│   ├── get_flowlog.sh     — fetch + build flowlog at a given ref. Honours
│   │                        FLOWLOG_REF=<sha|tag|branch> (default: main).
│   │                        Output: ./flowlog/<short_sha>/{src,target/release}
│   └── env/               — one-time machine install (souffle, duckdb, GNU
│                            time, rustup). Same philosophy as flowlog repo:
│                            run `env.sh` once on a fresh box, you're done.
│                            No env_check.sh — if a script needs deps, it
│                            fails loudly at the first call.
├── flowlog/               — gitignored; populated by get_flowlog.sh.
│                            Holds one or more <short_sha>/ build trees so
│                            regression runs can keep both base and head
│                            built side-by-side without rebuilding.
├── scripts/               — how to run one program / one comparison
│   ├── bench_one.sh       — primitive: one program × one dataset × one engine
│   ├── cross_engine.sh    — flowlog vs. {soufflé, interpreter, …} at one ref
│   ├── regression.sh      — flowlog@base vs. flowlog@head (A/B over commits)
│   ├── ldbc.sh            — LDBC timing / scaling
│   └── lib/               — shared bash helpers (logging, timing, parsing)
├── programs/              — programs only (in git). Grouped by
│   │                       **benchmark suite**, then by **dialect**.
│   ├── micro/             — single-program micro-benchmarks
│   │   ├── flowlog/       — cspa.dl, …  (flowlog dialect)
│   │   └── souffle/       — cspa.dl, …  (Soufflé equivalents, one-to-one)
│   └── ldbc/              — LDBC SNB suite (queries × scale-factors)
│       ├── flowlog/       — q1.dl, q2.dl, …
│       └── souffle/       — q1.dl, q2.dl, …
│   # future suites (tpc-h, custom) sit here as siblings.
├── facts/                 — gitignored; populated by an external data
│   │                       handler (the existing /datasets symlink +
│   │                       fetcher pattern). Layout mirrors `programs/`
│   │                       at the *suite* level (data is dialect-agnostic).
│   │                       Never committed; scripts only read.
│   ├── micro/
│   └── ldbc/
├── plotting/              — plot_speedup.py and any post-processing
├── config/                — default bench config (workers, timeout, baselines)
└── results/               — gitignored; CSVs, plots, raw timing land here
```

### Specifying which flowlog commit to bench

The bench repo treats flowlog as a **fetched, built input**, not a submodule.
Reasons: a perf repo routinely benches arbitrary commits (regression bisects,
A/B over a feature branch, comparing main against a release tag), and a
submodule's gitlink would force a bench-repo commit per attempt. A fetch
script is one arg.

Standard call shape:

```bash
# Bench main:
make cross-engine

# Bench a specific commit:
FLOWLOG_REF=abc1234 make cross-engine

# A/B over two commits (regression.sh fetches both):
FLOWLOG_BASE=abc1234 FLOWLOG_HEAD=def5678 make regression
```

Each cached build lives at `flowlog/<short_sha>/`, so re-running the same
ref is free. `get_flowlog.sh` is idempotent: if the ref is already built, it
no-ops.

### Design principles for the bench repo

1. **Flowlog is a fetched input, not a fork.** The bench repo never patches
   engine code; if it needs an engine change, that's a PR against this repo
   first. It pulls flowlog source via `tools/get_flowlog.sh` and builds it.
2. **Any commit is benchable, by design.** Every script accepts a flowlog
   ref (env var or flag). No bench-repo commit is required to switch which
   flowlog version you're measuring. Default is `main`; explicit refs win.
3. **Programs / facts / scripts / outputs are physically separated.**
   `programs/` is the rule corpus (in git, dialect-split). `facts/` is the
   dataset cache (gitignored, populated by the data handler). `scripts/` is
   code. `results/` is gitignored output. They never overlap; scripts only
   read from `programs/` + `facts/` and only write to `results/`.
4. **One Make target per task, no full-sweep orchestrator.** `make bench-one
   PROG=…`, `make cross-engine`, `make regression`, `make plot`. Each script
   already iterates over its (program × dataset) pairs internally; a wrapper
   that calls all three would just be glue. If a consumer wants "run
   everything," it chains the three targets itself — same script-library
   philosophy as the flowlog repo.
5. **Comparisons are pluggable.** Adding another engine (DuckDB, etc.) is a
   new file under `scripts/`, not a rewrite of `cross_engine.sh`.
6. **Reproducibility over cleverness.** Every per-run result directory under
   `results/` ships a `run_info.txt` sidecar that records the flowlog commit
   (resolved to a full SHA, not `main`), corpus revision (with a dirty flag),
   config file path + sha256, host, worker count, num-runs, runner-specific
   knobs (baselines, tolerances, …), and a UTC timestamp. The result CSV/TSV
   sits next to it so the pair is self-describing. Resume semantics are
   *enforced* against this manifest — `cross_engine.sh` hard-fails if any
   identity field changed since the existing CSV was started, telling the
   caller to either revert or `--fresh`. A run from a year ago must be
   reconstructable.
7. **Bench env is heavier than test env, and that's fine.** Soufflé, DuckDB,
   GNU time, larger dataset caches all live with the bench repo's
   `tools/env/`. The flowlog repo's env stays minimal.
