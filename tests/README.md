# Testing infrastructure

This folder is the **correctness** surface for FlowLog. Each suite is independently
runnable.

## What's here

| Path                        | What it does                                                  | Time       |
|-----------------------------|---------------------------------------------------------------|------------|
| `cargo nextest run --workspace` | Per-crate `#[test]`s (nextest); doctests via `cargo test --doc` | <15 s warm |
| `tests/fixtures/`           | ~140 hand-curated `.dl` programs, byte-diff vs `expected/`    | ~2 min     |
| `tests/oracle/`             | Real benchmarks, byte-diff vs **Soufflé** reference outputs   | ~30 min    |
| `tests/lib/`                | Shared bash helpers (sourced by every runner)                 | —          |
| `tests/ldbc/` *(future)*    | LDBC SNB correctness — empty placeholder                      | —          |

Every suite exercises **two lowering paths**. Compiler mode builds the
`flowlog-compiler` binary and compiles each program to a standalone
executable; library mode synthesises a small Rust crate that links
`flowlog-build` + `flowlog-runtime` and calls `engine.run()` directly. They
hit different code paths; both must pass. `fixtures/run.sh` runs both (or
one, with `-m compiler|lib`); `oracle/` ships them as `run_compiler.sh` and
`run_lib.sh`.

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
