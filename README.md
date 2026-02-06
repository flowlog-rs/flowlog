<p align="center">
  <img src="FlowLog.png" alt="FlowLog Logo" width="700"/>
</p>

<p align="center">
  <strong>Composable Datalog-to-Rust compiler for scalable Differential Dataflow programs.</strong>
</p>

<p align="center">
  <a href="#end-to-end-example">Quick Start</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#compiler-cli">Compiler CLI</a> •
  <a href="https://arxiv.org/pdf/2511.00865">FlowLog Paper</a>
</p>

**Status:** FlowLog is under active development; interfaces may change without notice.

## Architecture

```
crates/
├── common       # shared CLI/parsing utilities and fingerprint helpers
├── parser       # Pest grammar and AST for the FlowLog language
├── stratifier   # dependency graph + SCC-based scheduling of rules
├── catalog      # per-rule metadata (signatures, filters, comparisons)
├── optimizer    # heuristic plan trees consumed by the planner
├── planner      # lowers strata into transformation flows
└── compiler     # compile Timely/DD executables from planned strata
```

## Getting Started

### Prerequisites

```bash
$ bash tools/env.sh
```

The bootstrap script installs a stable Rust toolchain and a few helper utilities. At a minimum you need `rustup`, `cargo`, and a compiler capable of building Timely/Differential (Rust 1.80+ recommended).

### Build the Workspace

```bash
$ cargo build --release
```

## Compiler CLI

Use the compiler to lower a FlowLog program into a Timely/Differential Cargo project.

```bash
$ cargo run -p compiler -- <PROGRAM> [OPTIONS]
```

| Flag | Description | Required | Notes |
|------|-------------|----------|-------|
| `PROGRAM` | Path to a `.dl` file. Accepts `all` or `--all` to iterate over every program in `example/`. | Yes | Parsed relative to the workspace unless absolute. |
| `-F, --fact-dir <DIR>` | Directory containing input CSVs referenced by `.input` directives. | When `.input` uses relative filenames | Prepends `<DIR>` to each `filename=` parameter; omit to use paths embedded in the program. |
| `-o, --output <NAME>` | Override the generated Cargo package name. | No | Default derives from `<PROGRAM>`; project is written to `../<NAME>`. |
| `-D, --output-dir <DIR>` | Location for materializing `.output` relations. | Required when any relation uses `.output` | Pass `-` to print tuples to stderr instead of writing files. |
| `--mode <MODE>` | Choose execution semantics: `batch` (default) or `incremental`. | No | `batch` uses `Present`; `incremental` switches the diff type to `i32`. |
| `-h, --help` | Show full Clap help text. | No | Includes additional examples and environment variables. |

## End-to-End Example

The `example/reach.dl` program computes nodes reachable from a small seed set. Below is the same program for reference.

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

### 1. Generate the Executable

```bash
$ cargo run -p compiler -- example/reach.dl -F reach -o reach_flowlog -D -
```

Key flags:

- `-F reach` points the compiler at the directory holding `Source.csv` and `Arc.csv`.
- `-o reach_flowlog` names the generated Cargo project (written to `../reach_flowlog`).
- `-D -` prints IDB tuples and sizes to stderr; pass a directory path to materialize CSV output files instead.
- `--mode incremental` switches the diff type to `i32` (default is batch semantics).


### 2. Prepare a Tiny Dataset

```bash
$ cd reach_flowlog
$ mkdir -p reach
$ cat <<'EOF' > reach/Source.csv
  1
  EOF

$ cat <<'EOF' > reach/Arc.csv
  1,2
  2,3
  EOF
```

### 3. Build and Run the Generated Project

```bash
$ cargo run --release -- -w 4
```

## Regression Harness

The regression harness in `tools/check` automates dataset downloads, code generation, execution, and result verification against stored cardinalities.

```bash
$ bash tools/check/check.sh
```

- Programs and datasets are enumerated in `tools/check/config.txt`.
- Datasets are cached under `facts/` and cleaned up between runs.
- Logs and parsed relation sizes are written to `result/logs/` and `result/parsed/`.
- The script creates temporary Cargo projects alongside the repository (e.g., `../flowlog_reach_livejournal`) and removes them after verification.

## Language Overview

FlowLog accepts a Souffle-compatible Datalog dialect with modules for recursion, negation, arithmetic, and aggregations.

```datalog
.decl Edge(src: number, dst: number)
.input Edge(IO="file", filename="edge.csv")

.decl Triangle(x: number, y: number, z: number)
.printsize Triangle

.decl PathLen(src: number, dst: number, total: number)
.printsize PathLen

Triangle(x, y, z) :-
    Edge(x, y),
    Edge(y, z),
    Edge(z, x),
    x != y,
    y != z,
    z != x.

PathLen(src, dst, sum(len)) :-
    Edge(src, mid),
    Edge(mid, dst),
    len = 1.

IndirectOnly(x, z) :-
    Edge(x, y),
    Edge(y, z),
    !Edge(x, z).
```

Features at a glance:

- Stratified negation with automatic SCC detection.
- Arithmetic expressions in rule heads and bodies (left-associative evaluation).
- Aggregations (`count`, `sum`, `min`, `max`) with per-relation type consistency.
- CSV ingestion for integer or string relations, sharded across Timely workers on the first attribute.
- Optional `.output` directives to spill IDBs to disk and `.printsize` to record cardinalities.

## Current Limitations

- Aggregations must appear as the final argument of an IDB head, and every rule deriving that relation must agree on the operator. The grammar accepts `average/AVG`, but the pipeline will panic if it is used (support pending).
- Input ingestion presently assumes comma-delimited UTF-8 files even if a different delimiter is specified.
- Generated projects are placed one directory above the workspace (e.g., `../reach_flowlog`) by design; adjust paths or move them as needed.

## Background Reading

FlowLog builds on the FlowLog paper:

> **FlowLog: Efficient and Extensible Datalog via Incrementality**  \
> Hangdong Zhao, Zhenghong Yu, Srinag Rao, Simon Frisk, Zhiwei Fan, Paraschos Koutris  \
> VLDB 2026 (Boston) — [arXiv 2511.00865](https://arxiv.org/pdf/2511.00865)

## Contributing

Contributions and bug reports are welcome. Please open an issue or submit a pull request once you have reproduced the change with `cargo test` (and `tools/check/check.sh` when it is relevant).

## Acknowledgement
FlowLog succeeds [FlowLog-VLDB](https://github.com/flowlog-rs/FlowLog-VLDB); many thanks to [**Hangdong Zhao**](https://github.com/hdz284) for continued support throughout the transition.
