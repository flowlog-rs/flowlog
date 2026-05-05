# Testing infrastructure — overview

This repo is the **correctness** surface for FlowLog. Performance and
benchmarking work lives in the sibling `flowlog-bench` repo — see
[`AGENTS.md`](../AGENTS.md) for the split rationale.

Per-suite details and recipes: [`tests/README.md`](../tests/README.md).

## The suites

| Suite                              | What it checks                                              | Time       |
|------------------------------------|-------------------------------------------------------------|------------|
| `cargo test --workspace`           | Per-crate `#[test]`s — the unit-test layer                  | ~15 s warm |
| `tests/safety/`                    | `cleanup_dataset` symlink-guard contract                    | <1 s       |
| `tests/fixtures/`                  | ~95 small `.dl` programs, byte-diff vs `expected/`          | ~2 min     |
| `tests/oracle/`                    | Real benchmark programs, byte-diff vs **Soufflé** reference | ~30 min    |
| `tests/ldbc/` *(future)*           | LDBC SNB correctness (small SF, known-good outputs) — TBD   | n/a today  |

Both fixture- and oracle-level suites run **twice**: once via the
`flowlog-compiler` binary, once via a synthesised crate that links the
runtime as a library (`tests/fixtures/run_compiler.sh` + `run_lib.sh`
for fixtures; `tests/oracle/run_compiler.sh` + `run_lib.sh` for the
oracle). Both must pass — they hit different code paths.

## Entry points

```bash
make build          # cargo build --release --workspace
make test           # cargo test  --release --workspace
make test-safety    # tests/safety/cleanup_dataset_test.sh
make doctor         # tools/env_check.sh — env health probe
make oracle CONFIG=tests/oracle/config_integer.txt [MODE=compiler|lib|both]
```

For fixtures, call the runners directly:

```bash
bash tests/fixtures/run_compiler.sh
bash tests/fixtures/run_lib.sh
```

There is **no** top-level "run everything" wrapper. Each script is
independently runnable with a stable exit code; composing them is the
caller's job — see [`AGENTS.md`](../AGENTS.md) for the rationale.

## Environment

| Var                         | Default          | Effect                                                      |
|-----------------------------|------------------|-------------------------------------------------------------|
| `WORKERS`                   | `min(64, nproc)` | Thread budget for the oracle runner.                        |
| `FLOWLOG_KEEP_DATASETS`     | unset            | If truthy (`1`/`yes`/`true`/`on`), never `rm -rf` datasets. |
| `FLOWLOG_FORCE_CLEANUP`     | unset            | Override the symlink cache guard. `KEEP=*` always wins.     |
| `FLOWLOG_SOUFFLE_REF_CACHE` | `/datasets/souffle_ref_tarballs` | Reuse cached Soufflé ref tarballs. |

`source /datasets/env.sh` on the standard dev VM sets the dataset
keep-flag and the Soufflé-ref cache for you.
