<p align="center">
  <img src="https://raw.githubusercontent.com/flowlog-rs/flowlog/main/FlowLog.png" alt="FlowLog Logo" width="320"/>
</p>

<p align="center">
  <h3 align="center">A composable Datalog engine that compiles programs into efficient, scalable Differential Dataflow executables.</h3>
</p>

<p align="center">
  <a href="#tldr">TL;DR</a> •
  <a href="#quick-start">Quick Start</a> •
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

> **Status:** under active development; interfaces may change without notice.

## TL;DR

You write Datalog (`.dl`). FlowLog **compiles** it — through a parser, type checker, stratifier, optimizer, planner, and code generator — into a **standalone Rust executable** that runs on top of [Timely](https://github.com/TimelyDataflow/timely-dataflow) + [Differential Dataflow](https://github.com/TimelyDataflow/differential-dataflow). You get four execution modes out of the box:

|                | **Batch** (run-once) | **Incremental** (maintain) |
|----------------|----------------------|-----------------------------|
| **Datalog**    | `datalog-batch` *(default)* | `datalog-inc`               |
| **Extended**\* | `extend-batch`       | `extend-inc`                |

\* Extended adds explicit `loop { … }` / `fixpoint { … }` blocks for fine-grained control over recursion.

## Quick Start

```bash
# 1. install toolchain + helpers
bash tools/env.sh

# 2. build the workspace
cargo build --release

# 3. compile and run the canonical reachability example
mkdir -p reach
printf '1\n'             > reach/Source.csv
printf '1,2\n2,3\n'      > reach/Arc.csv

flowlog example/reach.dl -F reach -o reach_bin -D -   # compile
./reach_bin -w 4                                      # run on 4 workers
```

That's it. The Datalog program itself:

```datalog
.decl Source(id: number)
.input Source(IO="file", filename="Source.csv", delimiter=",")

.decl Arc(x: number, y: number)
.input Arc(IO="file", filename="Arc.csv", delimiter=",")

.decl Reach(id: number)
.printsize Reach

Reach(y) :- Source(y).
Reach(y) :- Reach(x), Arc(x, y).
```

For incremental mode, profiler usage, and richer examples see <https://www.flowlog-rs.com/>.

## Architecture

```mermaid
flowchart LR
    SRC[".dl source"] --> P[parser]
    P --> TC[typechecker]
    TC --> S[stratifier]
    S --> PL[planner]
    PL --> CG[codegen]
    CG --> EXE["Rust + DD<br/>executable"]

    CAT[catalog]:::side -.per-rule metadata.-> PL
    OPT[optimizer]:::side -.cost / join order.-> PL
    PROF[profiler]:::side -.optional trace.-> CG

    classDef side fill:#fff8e1,stroke:#a80,stroke-dasharray:3 3
```

The repository is a small Cargo workspace of three crates plus example programs and tests:

| Crate | Role |
|---|---|
| **`flowlog-build`** | The whole pipeline as a library — used from `build.rs` to bake a Datalog program into your Rust crate. Houses `parser`, `typechecker`, `catalog`, `stratifier`, `optimizer`, `planner`, `codegen`, and `profiler` as submodules. |
| **`flowlog-compiler`** | The standalone `flowlog` binary — calls into `flowlog-build`, then scaffolds and `cargo build`s a self-contained executable. |
| **`flowlog-runtime`** | Tiny runtime consumed by generated code: string interning, file IO sharding, sort/merge helpers, and incremental-transaction state. |

Each module under `flowlog-build/src/` has its own `README.md` describing purpose, design, and key types — start there when you need to understand or modify a stage.

## CLI

```bash
flowlog <PROGRAM> [OPTIONS]
```

| Flag | Required when… | What it does |
|---|---|---|
| `PROGRAM` | always | Path to a `.dl` file. Use `all` / `--all` to iterate over `example/`. |
| `-F, --fact-dir <DIR>` | `.input` uses relative filenames | Prepends `<DIR>` to each `filename=` parameter. |
| `-o <PATH>` | optional | Output executable path; defaults to the program stem (`reach.dl` → `./reach`). |
| `-D, --output-dir <DIR>` | any `.output` is used | Where to materialize output relations. Pass `-` to print tuples to stderr instead. |
| `--mode <MODE>` | optional | `datalog-batch` *(default)* \| `datalog-inc` \| `extend-batch` \| `extend-inc`. |
| `-P, --profile` | optional | Enable operator-level profiling (writes `log/` next to the executable). |
| `-h, --help` | — | Full Clap help with examples and env vars. |

## Tests

End-to-end tests live in `tests/`, organised by execution mode:

| Directory | Mode |
|---|---|
| `tests/datalog-batch/` | Standard batch Datalog *(default)* |
| `tests/datalog-inc/` | Incremental Datalog |
| `tests/extend-batch/` | Extended batch (explicit loops) |
| `tests/extend-inc/` | Extended incremental |

```bash
bash tests/run.sh                       # full suite
bash tests/run.sh loop_fixpoint negation # selected tests
```

Each test directory contains `program.dl`, optional `data/` (CSV facts), `expected/` (one file per `.output` relation), and an optional `commands.txt` (incremental transcripts) / `runtime_flags`.

## Background Reading

> **FlowLog: Efficient and Extensible Datalog via Incrementality**  \
> Hangdong Zhao, Zhenghong Yu, Srinag Rao, Simon Frisk, Zhiwei Fan, Paraschos Koutris  \
> VLDB 2026 (Boston) — [pVLDB](https://www.vldb.org/pvldb/vol19/p361-zhao.pdf) • [Artifacts](https://github.com/flowlog-rs/vldb26-artifact)

## Contributing

Issues and pull requests welcome. Before submitting, please run `cargo test` and `bash tests/run.sh` and confirm both pass on your change.
