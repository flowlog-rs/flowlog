<p align="center">
  <img src="https://raw.githubusercontent.com/flowlog-rs/flowlog/main/FlowLog.png" alt="FlowLog" width="320"/>
</p>

<h3 align="center">A composable Datalog engine that compiles programs into efficient, scalable Differential&nbsp;Dataflow executables.</h3>

<p align="center">
  <a href="#what-is-it">What</a> •
  <a href="#quick-start">Quick&nbsp;Start</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#cli">CLI</a> •
  <a href="#tests">Tests</a> •
  <a href="https://www.vldb.org/pvldb/vol19/p361-zhao.pdf">Paper</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/flowlog-build"><img src="https://img.shields.io/crates/v/flowlog-build.svg?label=flowlog-build" alt="flowlog-build on crates.io"/></a>
  <a href="https://docs.rs/flowlog-build"><img src="https://docs.rs/flowlog-build/badge.svg" alt="flowlog-build docs"/></a>
  <a href="https://crates.io/crates/flowlog-runtime"><img src="https://img.shields.io/crates/v/flowlog-runtime.svg?label=flowlog-runtime" alt="flowlog-runtime on crates.io"/></a>
  <a href="https://docs.rs/flowlog-runtime"><img src="https://docs.rs/flowlog-runtime/badge.svg" alt="flowlog-runtime docs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"/></a>
</p>

## What is it

You write Datalog (`.dl`); FlowLog compiles it into a **standalone Rust executable** on top of [Timely](https://github.com/TimelyDataflow/timely-dataflow) + [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow).

|              | Batch *(run once)*           | Incremental *(maintain)*       |
|--------------|------------------------------|--------------------------------|
| **Datalog**  | `datalog-batch` *(default)* ✅ | `datalog-inc` ✅                |
| **Extended** | `extend-batch` 🚧             | `extend-inc` 🚧                 |

✅ supported · 🚧 work-in-progress (Extended modes accept `loop { … }` / `fixpoint { … }` syntax for explicit recursion control; `extend-inc` has no test fixtures yet, and `--profile` panics under either Extended sub-mode).

## Quick start

```bash
bash tools/env/env.sh        # toolchain + helpers
cargo build --release        # builds target/release/flowlog-compiler

# canonical reachability example
mkdir -p reach
printf '1\n'        > reach/Source.csv
printf '1,2\n2,3\n' > reach/Arc.csv

target/release/flowlog-compiler example/graph_analysis/reach.dl \
    -F reach -o reach_bin -D -    # -D - prints to stderr
./reach_bin -w 4                  # 4 timely workers
```

The program ([`example/graph_analysis/reach.dl`](example/graph_analysis/reach.dl)):

```datalog
.decl Source(id: int32)
.input Source(IO="file", filename="Source.csv", delimiter=",")
.decl Arc(x: int32, y: int32)
.input Arc(IO="file", filename="Arc.csv", delimiter=",")

.decl Reach(id: int32)
Reach(y) :- Source(y).
Reach(y) :- Reach(x), Arc(x,y).
.printsize Reach
```

More examples and incremental usage: <https://www.flowlog-rs.com/>.

## Architecture

A `.dl` program flows through five sequential stages, with three support modules feeding into the planner and codegen.

```mermaid
flowchart LR
    src[".dl"] --> p[parser] --> t[typechecker] --> s[stratifier] --> pl[planner] --> cg[codegen] --> out[executable]
    cat[catalog]:::side -.-> pl
    opt[optimizer]:::side -.-> pl
    prof[profiler]:::side -.-> cg
    classDef side fill:#fff8e1,stroke:#a80,stroke-dasharray:3 3
```

### Pipeline stages

| Stage | What it does | Key types |
|---|---|---|
| **parser** | Pest grammar → typed AST. Resolves `.include` directives at the text level. | `Program`, `FlowLogRule`, `Segment`, `Lexeme` trait |
| **typechecker** | Rejects ill-typed programs and **pins** every polymorphic literal (`Int(_)`, `Float(_)`) to a concrete width. | `check_program`, `TypeCheckError` |
| **stratifier** | SCC-based scheduling. `loop` / `fixpoint` blocks become hard barriers, each producing one recursive stratum. | `Stratifier`, `StratifyError` |
| **planner** | Per-rule pipeline (`prepare → SIP* → core → fuse → post`) plus stratum-level dedup, recursive/non-recursive split, and aggregation metadata. | `StratumPlanner`, `RulePlanner`, `Transformation`, `Collection` |
| **codegen** | Emits Timely + DD operator chains as `CodeParts` token streams. | `CodeGen`, `CodeParts`, `Features` |

### Support modules

| Module | Role |
|---|---|
| **catalog** | Per-rule metadata (signatures, supersets, filters) + range-restriction check. Built per-rule **inside** the planner. |
| **optimizer** | EDB cardinalities + plan tree (today: left-deep, source order — cost-based ordering is future work). Consulted by the planner's core phase. |
| **profiler** | Operator-level trace; build-time predictions + run-time logs. Datalog modes only — panics under `extend-*`. |
| **common** | Source spans, diagnostics, `Config`, fingerprints. Used by every stage. |

### Two output paths

`CodeParts` is consumed by either of two frontends:

| Path | Where | Output |
|---|---|---|
| **Library mode** | `flowlog-build/src/build/` | `$OUT_DIR/<stem>.rs` for `include!()` from your crate. Driven by `flowlog_build::compile()` in your `build.rs`. |
| **Binary mode** | `flowlog-compiler/` | A scaffolded Cargo project + `cargo build --release` + a binary copied to `-o <PATH>`. Driven by the `flowlog-compiler` CLI. |

The runtime crate `flowlog-runtime` supplies what generated code calls into: thread-safe string interning, file-IO sharding, `k_way_merge` / `topk` for `ORDER BY` / `LIMIT` drains, and `Transaction` state types used by incremental drivers.

### Workspace layout

```
flowlog/
├── crates/
│   ├── flowlog-build/         compile pipeline (library); used from build.rs
│   │   └── src/{parser, typechecker, stratifier, catalog, optimizer,
│   │              planner, codegen, build, profiler, common}/
│   ├── flowlog-compiler/      standalone CLI binary
│   └── flowlog-runtime/       runtime helpers consumed by generated code
├── example/                   .dl programs (graph_analysis, knowledge_reasoning,
│                              ldbc_snb, program_analysis, extended)
└── tests/                     unit / complex / ldbc end-to-end suites
```

### Mode → stage interactions

`Config::mode` flows through to:

- **stratifier** rejects non-loop recursion under Extended mode (`StratifyError::RecursionOutsideLoop`);
- **codegen** picks `Diff = Present` for `datalog-batch` and `Diff = i32` everywhere else;
- **codegen** wraps recursive strata in `.iterate(...)` (batch) or `Variable`-scoped logic (incremental);
- **build** assemblers (`engine/batch.rs`, `engine/incremental.rs`) emit either a `DatalogBatchEngine` with a single `.run()` or a `DatalogIncrementalEngine` driven by `Transaction::commit()`.

A `u64` fingerprint threads through `catalog → planner → codegen` so the same logical collection is arranged once and shared across rules.

## CLI

```bash
flowlog-compiler <PROGRAM> [OPTIONS]
```

| Flag | What it does |
|---|---|
| `PROGRAM` | Path to a `.dl` file. `all` / `--all` iterates over `example/`. |
| `-F, --fact-dir <DIR>` | Prepended to each `.input` `filename=`. |
| `-o <PATH>` | Output executable path. Default: program stem (`reach.dl` → `./reach`). |
| `-D, --output-dir <DIR>` | Where to materialize `.output` relations. Pass `-` for stderr. |
| `--mode <MODE>` | `datalog-batch` *(default)* · `datalog-inc` · `extend-batch` · `extend-inc`. |
| `--sip` | Sideways Information Passing — push binding constraints into body atoms. |
| `--str-intern` | Intern string columns at load time for faster joins / lower memory. |
| `-I, --include-dir <DIR>` | Extra search directory for `.include` (repeatable). |
| `--udf-file <PATH>` | Rust source defining UDFs declared via `.extern fn`. |
| `--save-temps` | Keep the intermediate generated crate. |
| `-P, --profile` | Operator-level profiling. **Datalog modes only — panics under Extended.** |
| `-h, --help` | Print Clap-generated help. |

## Tests

| Suite | Coverage | Runner |
|---|---|---|
| `tests/unit/` — fast end-to-end fixtures | `datalog-batch`, `datalog-inc`, `extend-batch` | `unit_compiler.sh` (binary), `unit_lib.sh` (library) |
| `tests/complex/` — diff against [Souffle](https://souffle-lang.github.io/) reference (network on first run) | `datalog-batch` | `datalog_batch_compiler.sh`, `datalog_batch_lib.sh` |
| `tests/ldbc/` — LDBC SNB queries | `datalog-batch` | `ldbc.sh` |

```bash
bash tests/unit/unit_compiler.sh                # run every fixture (binary mode)
bash tests/unit/unit_lib.sh agg_avg agg_count   # run named fixtures (library mode)
bash tests/complex/datalog_batch_compiler.sh    # full Souffle correctness sweep
```

A fixture is a directory with `program.dl`, optional `data/` (CSVs), `expected/` (one file per `.output`), plus optional `commands.txt` / `runtime_flags`.

## Background reading

> **FlowLog: Efficient and Extensible Datalog via Incrementality** \
> Hangdong Zhao, Zhenghong Yu, Srinag Rao, Simon Frisk, Zhiwei Fan, Paraschos Koutris \
> VLDB 2026 — [pVLDB](https://www.vldb.org/pvldb/vol19/p361-zhao.pdf) · [artifacts](https://github.com/flowlog-rs/vldb26-artifact)

## Contributing

Issues and PRs welcome. Before submitting, run the unit suites:

```bash
bash tests/unit/unit_compiler.sh
bash tests/unit/unit_lib.sh
```
