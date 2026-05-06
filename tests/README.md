# Testing infrastructure

This folder is the **correctness** surface for FlowLog. Each suite is independently
runnable.

## What's here

| Path                        | What it does                                                  | Time       |
|-----------------------------|---------------------------------------------------------------|------------|
| `cargo test --workspace`    | Per-crate `#[test]`s — the unit-test layer                    | <15 s warm |
| `tests/fixtures/`           | ~95 hand-curated `.dl` programs, byte-diff vs `expected/`     | ~2 min     |
| `tests/oracle/`             | Real benchmarks, byte-diff vs **Soufflé** reference outputs   | ~30 min    |
| `tests/lib/`                | Shared bash helpers (sourced by every runner)                 | —          |
| `tests/ldbc/` *(future)*    | LDBC SNB correctness — empty placeholder                      | —          |

Both `fixtures/` and `oracle/` ship **two runner scripts** —
`run_compiler.sh` and `run_lib.sh`. The compiler runner builds the
`flowlog-compiler` binary; the lib runner synthesises a small Rust
crate that links `flowlog-build` + `flowlog-runtime` and calls
`engine.run()` directly. They hit different code paths; both must
pass.

## How to run

```bash
# Unit tests
make test

# Fixtures (no flags, runs all ~95 programs)
bash tests/fixtures/run_compiler.sh
bash tests/fixtures/run_lib.sh

# Soufflé oracle, both lowering paths by default
make oracle CONFIG=tests/oracle/config_integer.txt
make oracle CONFIG=tests/oracle/config_string.txt MODE=lib

# Forward runner flags through ARGS
make oracle CONFIG=tests/oracle/config_integer.txt \
            ARGS="--keep-datasets --workers $(nproc) \
                  --souffle-ref-cache /datasets/souffle_ref_tarballs"
```

## Oracle runner flags

Accepted by `tests/oracle/run_compiler.sh` and `run_lib.sh`:

| Flag | Default | Effect |
|---|---|---|
| `--keep-datasets` | off | Don't delete `<repo>/facts/<dataset>` after each pair. |
| `--workers <n>` | 64 | Worker thread count. |
| `--souffle-ref-cache <dir>` | — | If `<dir>/<ref>.tar.gz` exists, `cp` it instead of fetching from HuggingFace. |
| `--sip` | off | Also test with the `--sip` optimization (binary) / `Builder::sip(true)` (lib). |

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
