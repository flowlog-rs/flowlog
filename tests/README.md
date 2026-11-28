# Testing infrastructure

This folder is the **correctness** surface for FlowLog. Each suite is independently
runnable.

## What's here

| Path                        | What it does                                                  | Time       |
|-----------------------------|---------------------------------------------------------------|------------|
| `cargo nextest run --workspace` | Per-crate `#[test]`s (nextest); doctests via `cargo test --doc` | <15 s warm |
| `tests/fixtures/`           | ~140 hand-curated `.dl` programs, byte-diff vs `expected/`    | ~2 min     |
| `tests/epochs/`             | Random transcripts, **every epoch** vs batch recomputes       | ~3 min     |
| `tests/oracle/`             | Real benchmarks, byte-diff vs **Soufflé** reference outputs   | ~30 min    |
| `tests/lib/`                | Shared bash helpers (sourced by every runner)                 | —          |

The fixtures and the Soufflé oracle exercise **two lowering paths**. Compiler
mode builds the `flowlog-compiler` binary and compiles each program to a
standalone executable; library mode synthesises a small Rust crate that links
`flowlog-build` + `flowlog-runtime` and calls `engine.run()` directly. They
hit different code paths; both must pass. `fixtures/run.sh` runs both (or
one, with `-m compiler|lib`); `oracle/` ships them as `run_compiler.sh` and
`run_lib.sh`. The every-epoch oracle runs compiler mode only. Both modes
generate the same dataflow but feed it differently, the binary through its
shell and a library engine through its staging methods, so a run kept as a
fixture (see below) and the incremental fixtures check library mode.

Every fixture is one directory `tests/fixtures/<name>/`. A fixture is
incremental when its program declares an `append` or `mutable` input; it
then ships a `commands.txt` transaction transcript (`begin`, `insert <rel>
<tuple>`, `delete <rel> @<file>`, `insert <rel>` for a nullary relation,
`commit`, `quit`; the shell's `help` lists them all), and its name says so:
`txn_*` (transaction shell mechanics), `mixed_*` (static and mutable inputs
in one program), `append_*` (an append input), or `*_delta` (a batch feature
re-checked per epoch). Static fixtures use none of these forms. The runners
refuse a fixture whose name, `commands.txt`, and `.decl`s disagree.

SQLite I/O fixtures (`sqlite_*`) follow the same layout, with and without
`ord`. Their `sqlite_setup.sql` creates the input database. Each `expected/<table>`
file contains the expected JSON rows of that output table, compared without
row ordering. Empty files assert that the table exists and has no rows.
The compiler runner sources `fixtures/sqlite_helper.sh` for these steps.
The library runner skips them because library engines use host I/O.

## How to run

Unit and integration tests run under [cargo-nextest](https://nexte.st)
(`cargo install cargo-nextest --locked`); doctests run separately via
`cargo test --doc`, since nextest does not execute them.

```bash
# Unit + integration tests (nextest) + doctests
make test

# Fixtures: all ~140 programs through both modes; -j N for N workers
bash tests/fixtures/run.sh -j 8
bash tests/fixtures/run.sh -m lib recursive_tc_delta   # one fixture, one mode

# Fixtures compiled with -P: each run's profile must match its plan
bash tests/fixtures/run.sh -m compiler --profile -j 8

# Every-epoch oracle: every case on the seeds CI runs; -j N cases at once
python3 tests/epochs/run.py -j 8
python3 tests/epochs/run.py --seeds 100 hybrid   # search further, one case

# Soufflé oracle, both lowering paths by default
make oracle CONFIG=tests/oracle/config.txt
make oracle CONFIG=tests/oracle/config.txt MODE=lib

# Forward runner flags through ARGS
make oracle CONFIG=tests/oracle/config.txt \
            ARGS="--keep-datasets --workers $(nproc) \
                  --souffle-ref-cache /datasets/souffle_ref_tarballs"
```

## Fixture runner caches

`fixtures/run.sh` honors `CARGO_TARGET_DIR` and keeps everything under
`<target>/e2e/`. `-j N` starts N workers that pull (mode, fixture) tasks from
one queue; each worker owns a slot, `slot-<i>/`, with a Cargo target
directory per mode (`compiler/cargo`, `lib/cargo`). The runtime and its
dependencies build once per slot and mode, and every later task there
compiles only its own crate. Slots persist between runs (about 300 MB per
mode), so a warm run is much faster than the first; a run removes the slots
beyond its `-j`. `rm -rf <target>/e2e` resets all of it.

## Profiler check

The profiler predicts which timely operators each plan node becomes
(`flowlog-profiler/src/plan/steps.rs`). A timely or differential bump, or a
codegen change, can shift those addresses.

- `run.sh --profile` compiles each fixture with `-P`, runs it, and reads the
  profile back with `flowlog-visualizer`, which fails when the run's
  operator log and the plan disagree.
- Each fixture shard in [`ci.yml`](../.github/workflows/ci.yml) runs the
  ordinary compiler and library checks, then runs its compiler fixtures
  again with `--profile`. The required `End-to-end fixtures` check passes
  only when both steps succeed, including in the merge queue.

## Every-epoch oracle

A fixture pins the epochs of one hand-written transcript; `tests/epochs/`
draws many random transcripts and computes what every epoch should hold
from batch runs. Each case is a program in `cases/` that mixes static,
append, and mutable inputs, in and out of recursion, plus an entry in
`cases.py` that draws its data. For each seed, `run.py`:

1. draws the files the program loads and a transcript of transactions:
   shuffled inserts and deletes, some of which cancel within their
   transaction;
2. compiles the program and its batch twin, the same rules with every
   input static and read from a file;
3. runs the twin over the inputs as they stand after each epoch; the lines
   gained and lost between consecutive runs are that epoch's expected
   `<Rel>_t<k>.csv`;
4. runs the program on the transcript at several worker counts and
   compares every epoch file with the expected one, ignoring line order.

CI runs the default seeds and worker counts, in a job that the required
`End-to-end fixtures` check waits on, so a CI failure replays locally.
`--seeds N` searches further, `--seed S` replays one seed, `--workers`
picks the worker counts, and case names pick cases; `make epochs [J=<n>]
[ARGS=...]` wraps it all. The constants at the top of `run.py` set these
defaults and the transcript's shape. `-j N` checks N cases at once, each
building in its own slot under `<target>/epochs/`; slots persist between
runs, like the fixture slots.

A failed run prints its first differences and a replay command, and stays
in `<target>/epochs/failures/<case>-s<seed>-w<workers>/`, laid out as a
fixture with the run's own output in `actual/`. A run that hangs stops its
case, as the runs after it would most likely hang too. To keep a run as a
regression test, delete `actual/` and move the directory into
`tests/fixtures/` under an incremental fixture name, such as
`mixed_<what>`; it then runs in both lowering paths.

To add a case, write `cases/<name>.dl` and add a `<name>` entry to
`CASES`. Its `load` draws the rows of each input the program reads with
`.input R(IO="file", filename="R.csv", delimiter=",")`; its `update` draws
the row that one insert or delete names, for each append or mutable input,
read with `.input R(IO="command", delimiter=",")`. A nullary input's one
row is `()`: `row()` draws it and `fixed(())` loads it. Small domains make
updates collide, join, and retract one another. `run.py` refuses what it
cannot model: other directives, inline facts, a program with no append or
mutable input, and `load` or `update` keys that disagree with the program.

The expected outputs come from FlowLog's own batch engine rather than
Soufflé; the comment above `reference` in `run.py` says why, and what that
leaves unchecked.

## Oracle runner flags

Accepted by `tests/oracle/run_compiler.sh` and `run_lib.sh`:

| Flag | Default | Effect |
|---|---|---|
| `--keep-datasets` | off | Don't delete `<repo>/facts/<dataset>` after each pair. |
| `--workers <n>` | 64 | Worker thread count. |
| `--souffle-ref-cache <dir>` | — | If `<dir>/<ref>.tar.gz` exists, `cp` it instead of fetching from HuggingFace. |

> [!WARNING]
> **If `<repo>/facts/` is a symlink, pass `--keep-datasets`.** Without
> it the runner aborts on the first cleanup attempt — by design, to
> avoid `rm -rf`'ing through the link into a shared cache like
> `/datasets/facts`. Alternative: `rm <repo>/facts` to break the link
> before running.

## Caching behavior

`cleanup_dataset` (in `tests/oracle/common.sh`) runs after each pair:

| Condition | Action |
|---|---|
| `--keep-datasets` passed | Skip cleanup. |
| `<repo>/facts/` is a symlink | **Die.** Refuses to `rm -rf` through the symlink to avoid nuking a shared cache. |
| Otherwise | `rm -rf <repo>/facts/<dataset>`. |

To use a shared cache safely, pass `--keep-datasets`. To opt out of
the symlink layout, `rm <repo>/facts` to break the link, then run
without the flag.

The Soufflé reference cache is purely a download-vs-cp optimisation;
extracted refs and tarballs are always cleaned per-pair regardless.

## Why Soufflé as the oracle

Soufflé is an **independent Datalog engine** — that's the value. A
miscompilation that produces wrong tuples diverges from Soufflé at
the first relation byte; the diff is shown in the failure message.
The Soufflé *binary* is not required — only the pre-baked reference
outputs (CSV tarballs hosted at HuggingFace under
`NemoYuu/flowlog_benchmark`).
