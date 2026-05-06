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

The bench split removed two kinds of things from this repo:

**(a) Perf-measurement scripts and corpus** — moved to `flowlog-bench`
(see its [`AGENTS.md`](https://github.com/flowlog-rs/flowlog-bench/blob/main/AGENTS.md#file-by-file-lineage-from-the-split)
for the full lineage). The Soufflé programs corpus, the LDBC timing
slice, and the per-program / cross-engine / regression runners all
moved together.

**(b) The sweep orchestrator** — `tools/sweep/run_full_sweep.sh` and
the Makefile targets `smoke` / `sweep` / `sweep-no-perf` / `perf` were
deleted outright, not moved. They existed because correctness layers
and a perf layer were entangled in one stack; with that entanglement
gone, the orchestrator has no reason to live here. If `flowlog-bench`
wants a perf-side sweep, it writes its own. If CI or an agent loop
wants a correctness sweep here, it chains `make test` + `make oracle`
+ `make test-safety` itself — three lines, no bash glue needed.

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
7. **Don't** add a "run everything" wrapper here. If you need to chain
   suites, do it in your CI / agent / Makefile target on the *consumer* side.

## Pipeline hand-off

| Stage           | Repo             | Output                                    |
| --------------- | ---------------- | ----------------------------------------- |
| Build + correct | `flowlog`        | binary + `make test` green                |
| Perf measure    | `flowlog-bench`  | timing CSVs, speedup plots, regressions   |

A change is shippable when this repo's tests pass. Perf signoff is a separate gate
owned by `flowlog-bench`.

## The sibling repo: `flowlog-bench`

The bench repo is its own source of truth — see
[`flowlog-rs/flowlog-bench/AGENTS.md`](https://github.com/flowlog-rs/flowlog-bench/blob/main/AGENTS.md)
for its design principles, repo layout, agent contract, and the
file-by-file lineage of what moved out of this repo at the
`pre-bench-split` tag.

From this repo's point of view its job is exactly: **grab the latest
flowlog, run comparisons.** What it compares against (past flowlog
versions, Soufflé, DuckDB) is the bench repo's concern, not ours. We
publish a buildable flowlog; they consume it.
