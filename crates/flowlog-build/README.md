<p align="center">
  <img src="https://raw.githubusercontent.com/flowlog-rs/flowlog/main/FlowLog.png" alt="FlowLog Logo" width="320"/>
</p>

<p align="center">
  <h3 align="center">Build-script integration for FlowLog — compile Datalog (<code>.dl</code>) programs into Rust at <code>cargo build</code> time.</h3>
</p>

<p align="center">
  <a href="#setup">Setup</a> •
  <a href="#advanced-options">Advanced Options</a> •
  <a href="https://docs.rs/flowlog-build">API Docs</a> •
  <a href="https://github.com/flowlog-rs/flowlog">FlowLog Repo</a>
</p>

<p align="center">
  <a href="https://crates.io/crates/flowlog-build"><img src="https://img.shields.io/crates/v/flowlog-build.svg" alt="flowlog-build on crates.io"/></a>
  <a href="https://docs.rs/flowlog-build"><img src="https://docs.rs/flowlog-build/badge.svg" alt="flowlog-build docs"/></a>
  <a href="LICENSE"><img src="https://img.shields.io/badge/license-Apache--2.0-blue.svg" alt="License"/></a>
</p>

**Status:** FlowLog is under active development; interfaces may change without notice.

---

`flowlog-build` compiles a Datalog (`.dl`) program into a Rust module your
crate `include!`s from `build.rs`. It pairs with
[`flowlog-runtime`](https://crates.io/crates/flowlog-runtime), which supplies
the dataflow primitives the generated code depends on.

If you want a standalone Datalog-to-Rust CLI instead of a build-script
integration, see the `flowlog-compiler` binary in the
[FlowLog repo](https://github.com/flowlog-rs/flowlog).

## Setup

```toml
# Cargo.toml
[dependencies]
flowlog-runtime = "0.2"

[build-dependencies]
flowlog-build = "0.1"
```

```rust,no_run
// build.rs
fn main() -> std::io::Result<()> {
    flowlog_build::compile("policy.dl")
}
```

```rust,ignore
// src/lib.rs
pub mod policy {
    include!(concat!(env!("OUT_DIR"), "/policy.rs"));
}

use policy::DatalogBatchEngine;

let mut engine = DatalogBatchEngine::new(4); // 4 timely workers
engine.insert_edge(vec![(1, 2), (2, 3)]);
let results = engine.run();
```

The generated module exposes one `insert_<relation>` method per input
relation declared in the `.dl` program, plus a `run()` that returns all
output relations.

## Advanced Options

Use `Builder` when you need multiple programs, include directories, UDFs,
or non-default codegen flags:

```rust,no_run
use flowlog_build::Builder;

// build.rs
fn main() {
    if let Err(err) = Builder::default()
        .sip(true)              // sideways information passing
        .string_intern(true)    // intern string-typed columns at ingest
        .udf_file("src/udf.rs") // included as `mod udf` in generated code
        .compile(
            &["policy.dl", "auth.dl"],
            &["shared/includes"],
        )
    {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
```

See the [API docs](https://docs.rs/flowlog-build) for the full `Builder` surface.

## Error Rendering

The free `compile()` function renders any pipeline diagnostic against the
source map and surfaces it through `io::Error`, so `cargo build` shows a
source-annotated error. `Builder::compile` returns the structured
`BoxError` instead if you want to format it yourself.

## License

Apache-2.0 — see [LICENSE](./LICENSE).
