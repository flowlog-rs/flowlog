# Testing infrastructure

This repo is the **correctness** surface for FlowLog. Performance and
benchmarking work lives in the sibling `flowlog-bench` repo — see
[`AGENTS.md`](../AGENTS.md) for the split rationale and the
file-by-file handoff.

There is **no** top-level "run everything" wrapper. Each script is
independently runnable with a stable exit code; composing them into a
full pass is the caller's job (CI, agent pipeline, dev workflow). If
you need a single command, chain `make test`, `make oracle`, and
`make test-safety`.

Per-suite pointers: this file (per-suite contracts and recipes), and
`docs/testing.md` (one-page overview).

## Suites

| Suite                          | What it checks                                                | Time       |
|--------------------------------|---------------------------------------------------------------|------------|
| `cargo test --workspace`       | Per-crate `#[test]`s — the unit-test layer                    | ~15 s warm |
| `tests/safety/`                | `cleanup_dataset` symlink-guard contract (cross-cutting)      | <1 s       |
| `tests/fixtures/`              | ~95 small `.dl` programs, byte-diff vs `expected/`            | ~2 min     |
| `tests/oracle/`                | Real benchmark programs, byte-diff vs **Soufflé** reference   | ~30 min    |
| `tests/ldbc/` *(future)*       | LDBC SNB correctness (small SF, known-good outputs) — TBD     | n/a today  |

Each fixture- and oracle-level suite runs **twice**: once via the
`flowlog-compiler` binary, once via a synthesised crate that links the
runtime as a library (`tests/fixtures/run_compiler.sh` + `run_lib.sh`;
`tests/oracle/run_compiler.sh` + `run_lib.sh`). Both must pass — they
hit different code paths.

## Entry points

```bash
make build          # cargo build --release --workspace
make test           # cargo test  --release --workspace
make test-safety    # tests/safety/cleanup_dataset_test.sh   (<1s)
make doctor         # tools/env_check.sh — env health probe   (<1s)

# Soufflé oracle:
make oracle CONFIG=tests/oracle/config_integer.txt           # default MODE=both
make oracle CONFIG=tests/oracle/config_string.txt MODE=lib   # only lib lowering
```

For the fixtures, call the runners directly (no Make wrapper — they
take no arguments and the closer-to-the-script form is simpler):

```bash
bash tests/fixtures/run_compiler.sh
bash tests/fixtures/run_lib.sh
```

## Suite details

### Workspace unit tests &nbsp;(`cargo test --workspace`)

Compiles every crate and runs every Rust `#[test]`. The sanity gate.
If this fails the rest of the suite is meaningless — you have a broken
parser/typechecker/planner/codegen.

| | |
|---|---|
| **Time, warm** | `< 15 s` |
| **Time, cold** | `~ 2 min` |
| **Failure mode** | localised — points at one function or one rewrite |

### Cache-safety regression &nbsp;(`tests/safety/cleanup_dataset_test.sh`)

`tests/oracle/common.sh::cleanup_dataset` is the only place this repo
deletes from the dataset cache. The contract it must honour:

1. `FLOWLOG_KEEP_DATASETS` truthy → never delete.
2. `FACT_DIR` is a symlink → never delete unless `FLOWLOG_FORCE_CLEANUP=1`.
   (Protects the persistent `/datasets` cache from being `rm -rf`'d
   through a `facts → /datasets/facts` symlink.)
3. otherwise → delete the dataset's subdir.

The safety test exercises that contract in isolation (faux dirs in
`mktemp`, no real datasets touched). It runs in `<1 s` and is meant as a
pre-flight gate ahead of the oracle: a regression here would silently
delete tens of GB of cached datasets on the next oracle run.

The test lives outside `tests/oracle/` because it's a **cross-cutting
invariant**: any future implementation of `cleanup_dataset` (anywhere
in this repo or in `flowlog-bench`) is expected to honour the same
contract, and this test is what keeps them aligned.

### Fixture-level end-to-end &nbsp;(`tests/fixtures/`)

Runs ~95 small hand-curated `.dl` programs end-to-end and byte-diffs
the output against pinned `expected/` files. Each fixture is a
directory:

```
tests/fixtures/datalog-batch/<feature>/
├── program.dl                # the program under test
├── data/                     # optional: input CSVs
├── expected/                 # required: one file per .output relation
├── commands.txt              # optional: incremental transcript
└── runtime_flags             # optional: flags forwarded to the binary
```

Coverage spans all four execution modes:

| Subdirectory     | Mode                  | Exercises |
|------------------|-----------------------|-----------|
| `datalog-batch/` | Standard batch        | every aggregation, arithmetic, comparison, join, negation, recursion, type, and UDF feature |
| `datalog-inc/`   | Standard incremental  | `insert / delete / file_load / abort / multi_txn` deltas via `commands.txt` |
| `extend-batch/`  | Extended batch        | `loop` / `fixpoint` blocks under Extended semantics |
| `extend-inc/`    | Extended incremental  | reserved (no fixtures yet) |

> [!IMPORTANT]
> **Two runners exercise the same fixtures via two lowering paths — both must pass.**
> `run_compiler.sh` builds and runs the `flowlog-compiler` binary;
> `run_lib.sh` synthesises a small Rust runner crate that links
> `flowlog-build` (build script) + `flowlog-runtime` (engine) and calls
> `engine.run()` directly. Library mode hits a different code path than
> binary mode, so a regression that passes one and fails the other
> almost always points at the build-script ↔ binary-target boundary.

### Soufflé oracle &nbsp;(`tests/oracle/`)

For each `program=dataset` pair in `config_integer.txt` (and
`config_string.txt` for `--str-intern` programs), runs FlowLog against
the dataset, then byte-diffs **every `.output` relation** against the
corresponding pre-computed [Soufflé](https://souffle-lang.github.io/)
reference output (a tarball auto-fetched from HuggingFace and cached
locally under `tests/oracle/cache/`).

> [!TIP]
> Soufflé being an **independent Datalog engine** is the key — it makes
> this a real correctness oracle, not a tautology against FlowLog
> itself. A miscompilation that produces wrong tuples will diverge from
> Soufflé at the first relation byte; the diff is shown in the failure
> message. The Soufflé *binary* is not required — only the pre-baked
> reference outputs.

The 19 program-dataset pairs span three program families:

| Family               | Programs |
|----------------------|----------|
| Graph analysis       | `tc`, `sg`, `reach`, `cc`, `sssp`, `bipartite`, `dyck` |
| Knowledge reasoning  | `crdt`, `galen` |
| Program analysis     | `andersen`, `cspa`, `csda`, `batik`, `biojava`, `eclipse`, `xalan`, `cvc5`, `z3`, `zxing` |

A typical run cross-checks **~140 output relations and ~700 M tuples**.
Same dual binary/library runners as fixtures
(`tests/oracle/run_compiler.sh` / `tests/oracle/run_lib.sh`).

#### Soufflé reference cache

The oracle downloads pre-baked Soufflé `.csv` references from
HuggingFace and unpacks them into `/dev/shm/` per pair. On long-lived
dev VMs you can short-circuit the download by setting
`FLOWLOG_SOUFFLE_REF_CACHE=<dir>` — `download_souffle_ref` then `cp`s
from `<dir>/<program>_<dataset>.tar.gz` if the file exists, falling
back to a live download otherwise. `/datasets/env.sh` on the standard
dev VM sets this for you.

### LDBC SNB &nbsp;(`tests/ldbc/`) — *future*

The directory is intentionally empty today. The pre-split LDBC runner
was a runtime cross-validator (FlowLog vs DuckDB on SF3 with per-row
timing), which the spec classifies as the "timing slice" — that runner
moves to `flowlog-bench/scripts/ldbc.sh`. The "correctness slice"
(small/medium SF + static known-good outputs) is to be authored fresh
in the same shape as `tests/oracle/`. See
[`tests/ldbc/README.md`](ldbc/README.md) for what fits here.

## Caching

`tests/oracle/common.sh::cleanup_dataset` deletes each dataset after
use **unless** `FLOWLOG_KEEP_DATASETS` is set to a truthy value — `1`,
`yes`, `true`, `on` (any case) — or you `source /datasets/env.sh` on
the dev VM. The larger datasets total **tens of GB** (`arabic`,
`orkut`, `livejournal`, `cspa-postgresql`, …); keeping them avoids
HuggingFace re-downloads between iterations.

**Symlink safety.** When `FACT_DIR` is a symlink (the standard dev-VM
layout has the repo's `facts/ → /datasets/facts/`), cleanup is
**always** skipped — even with `FLOWLOG_KEEP_DATASETS` unset — so a
stray run can't `rm -rf` a 100+ GB cache through the symlink. To
override and actually delete through the symlink, set
`FLOWLOG_FORCE_CLEANUP=1`. The same contract is exercised by
[`tests/safety/cleanup_dataset_test.sh`](safety/cleanup_dataset_test.sh)
(`make test-safety`).

## Closed-loop integration

External tools (regression CI, agentic loops) drive FlowLog through
**two `make` targets** plus **two env vars**:

| `make` target | What it does | Stability contract |
|---|---|---|
| `make doctor` | Print an 8-section health report; exit 0 when env is ready, 1 on a blocking error. | The exit code; the ✗ / ! / ✓ glyphs are the public log surface. |
| `make oracle CONFIG=<file> [MODE=compiler\|lib\|both]` | Run every `<program=dataset>` line in `<config_file>` against the Soufflé oracle in either or both lowering paths; exit non-zero on any byte-diff mismatch. | Positional `CONFIG=`; `MODE=` defaults to `both`. Config-file basename starting with `config_string*` toggles `--str-intern`. Honours `WORKERS`, `RAYON_NUM_THREADS`, `FLOWLOG_KEEP_DATASETS`, `FLOWLOG_SOUFFLE_REF_CACHE` env. |

Two env vars round out the contract:

- `FLOWLOG_KEEP_DATASETS` (truthy): preserve cached datasets between iterations.
- `FLOWLOG_SOUFFLE_REF_CACHE=<dir>`: preserve the Soufflé reference tarball cache between iterations.

A typical agentic loop launches with all three set:

```bash
source /datasets/env.sh                          # FLOWLOG_KEEP_DATASETS=1, FLOWLOG_SOUFFLE_REF_CACHE=...
make doctor || exit 1                            # gate on a green env-check
make test-safety                                 # cache-safety contract
WORKERS=$(nproc) FLOWLOG_KEEP_DATASETS=1 \
    make oracle CONFIG=tests/oracle/config_integer.txt
```

For perf gates / benchmarking, use the `flowlog-bench` sibling repo's
`make cross-engine` / `make regression` instead — see `AGENTS.md`.
