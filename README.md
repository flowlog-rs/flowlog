<p align="center">
  <img src="https://raw.githubusercontent.com/flowlog-rs/flowlog/main/FlowLog.png" alt="FlowLog Logo" width="320"/>
</p>

<p align="center">
  <h3 align="center">Composable Datalog engine that compiles programs into efficient and scalable Differential Dataflow executables.</h3>
</p>

<p align="center">
  <a href="#quick-start">Quick Start</a> •
  <a href="#architecture">Architecture</a> •
  <a href="#compiler-cli">Compiler CLI</a> •
  <a href="https://www.vldb.org/pvldb/vol19/p361-zhao.pdf">FlowLog Paper</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/flowlog-build"><img src="https://img.shields.io/crates/v/flowlog-build.svg?label=flowlog-build" alt="flowlog-build on crates.io"/></a>
  <a href="https://docs.rs/flowlog-build"><img src="https://docs.rs/flowlog-build/badge.svg" alt="flowlog-build docs"/></a>
  <a href="https://crates.io/crates/flowlog-runtime"><img src="https://img.shields.io/crates/v/flowlog-runtime.svg?label=flowlog-runtime" alt="flowlog-runtime on crates.io"/></a>
  <a href="https://docs.rs/flowlog-runtime"><img src="https://docs.rs/flowlog-runtime/badge.svg" alt="flowlog-runtime docs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"/></a>
</p>

> **Status** · Under active development; interfaces may change without notice.

## Quick Start

**1 — Install the toolchain.** One-time setup: installs a stable Rust toolchain (1.80+) and the required OS packages, then runs `cargo check --workspace` as a smoke test.

```bash
$ bash env/env.sh     # Linux / macOS
PS> .\env\env.ps1     # Windows (elevated PowerShell)
```

**2 — Build.** The compiler lands at `target/release/flowlog-compiler`.

```bash
$ cargo build --release
```

**3 — Run an example.** `example/graph_analysis/reach.dl` computes the nodes reachable from a seed set:

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

Create a tiny dataset, then compile and run it:

```bash
$ mkdir -p reach
$ printf '1\n'        > reach/Source.csv
$ printf '1,2\n2,3\n' > reach/Arc.csv

# Compile to a binary, then run it on 4 worker threads
$ target/release/flowlog-compiler example/graph_analysis/reach.dl -F reach -o reach_bin -D -
$ ./reach_bin -w 4
```

See [Compiler CLI](#compiler-cli) for every flag. Batch mode is shown here; for incremental mode and the profiler, see <https://www.flowlog-rs.com/>.

## Architecture

A `.dl` program compiles through five stages, with three side modules assisting the planner and codegen:

```text
.dl → parser → typechecker → stratifier → planner → codegen → executable
```

**Pipeline**

- **parser** — reads `.dl` into a typed AST, each node tagged with its source location.
- **typechecker** — resolves each literal's type (`1` → `int32`).
- **stratifier** — groups rules into strata (one per `loop` / `fixpoint`) so recursion runs in order.
- **planner** — lowers each rule to a Differential Dataflow plan, sharing common sub-plans to reuse arrangements.
- **codegen** — emits the plan as Timely + Differential Dataflow Rust.

**Side modules**

- **catalog** — supplies the planner with per-rule metadata (signatures, pushdown filters, range checks).
- **optimizer** — gives the planner cardinality-based join ordering and worst-case optimal joins (WIP).
- **profiler** — instruments codegen to collect runtime metrics from Timely / Differential Dataflow operators.

**Crates**

- **`flowlog-build`** — library; call from `build.rs` to compile `.dl` to Rust at build time.
- **`flowlog-compiler`** — CLI; compiles `.dl` into a standalone executable.
- **`flowlog-runtime`** — linked into the generated output (interning, IO, sort/merge, incremental-txn state); not a direct dependency.

## Compiler CLI

```bash
$ flowlog-compiler <PROGRAM> [OPTIONS]
```

`<PROGRAM>` is a path to a `.dl` file, or `all` / `--all` to compile every program in `example/`. Common options:

- `-F, --fact-dir <DIR>` — prepend `<DIR>` to relative `filename=` paths in `.input` directives.
- `-o <PATH>` — output executable path; defaults to the program stem (`reach.dl` → `./reach`).
- `-D, --output-dir <DIR>` — where to materialize `.output` relations; `-` prints tuples to stderr.
- `--mode <MODE>` — `datalog-batch` (default), `datalog-inc`, `extend-batch`, or `extend-inc` (extended modes WIP).
- `--sip` — sideways information passing: filter later body atoms by earlier bindings to shrink joins (off by default).
- `--str-intern` — intern string columns at load for faster joins and lower memory (off by default).
- `-P, --profile` — collect execution statistics (Datalog modes only).
- `-h, --help` — full help text.

## Testing

See [`tests/README.md`](tests/README.md) for per-suite contracts and recipes.

## Publication

> **FlowLog: Efficient and Extensible Datalog via Incrementality**  
> Hangdong Zhao, Zhenghong Yu, Srinag Rao, Simon Frisk, Zhiwei Fan, Paraschos Koutris  
> VLDB 2026, Boston

- **Paper** — [PVLDB Vol. 19](https://www.vldb.org/pvldb/vol19/p361-zhao.pdf)
- **Artifacts** — [flowlog-rs/vldb26-artifact](https://github.com/flowlog-rs/vldb26-artifact)

## Contributing

Issues and pull requests are welcome. PRs must pass CI before merge.
