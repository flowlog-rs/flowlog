//! Shared pipeline configuration.

use std::path::Path;

/// Execution strategy for FlowLog workflows
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum ExecutionMode {
    /// Single-pass batch execution.
    /// Only tracks whether facts are present or absent,
    /// making it suitable for high-performance static execution.
    #[default]
    Batch,
    /// Incremental execution.
    /// Maintains state across updates, tracking how many times each fact
    /// is derived, supporting incremental view maintenance.
    Inc,
}

/// Shared pipeline configuration consumed by parse, plan, and codegen.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Path to the Datalog (.dl) program file.
    pub program: String,
    /// Execution modes.
    pub mode: ExecutionMode,
    /// Collect per-rule execution statistics (timing, tuple counts).
    pub profile: bool,
    /// Enable Sideways Information Passing.
    pub sip: bool,
    /// Intern string columns as compact integer keys at load time.
    pub str_intern: bool,
    /// Path to a Rust source file containing UDF implementations.
    pub udf_file: Option<String>,
    /// Extra search directories for `.include` directives.
    pub include_dirs: Vec<String>,
    /// Whether `.output` relations drain to stdout (`-D -`) rather than files.
    /// Derived by the CLI from `--output-dir`; always `false` in library mode.
    pub output_to_stdout: bool,
    /// When set, fact strings are interned serially rather than in parallel.
    /// Interning order, and therefore `ord(_)` values, is then deterministic
    /// across worker counts (`-w N` matches `-w 1`).
    pub serialize_load: bool,
    /// Milliseconds between periodic metric flushes while a profiled run is
    /// executing, so a long or interrupted run still leaves the latest
    /// snapshot on disk. `0` flushes only at each natural write point (batch:
    /// after the run drains; incremental: after each transaction commits). A
    /// non-zero interval additionally re-dumps the in-flight tables mid-run.
    pub metrics_flush_interval_ms: u64,
}

impl Config {
    /// Path to the Datalog (.dl) program file.
    pub fn program(&self) -> &str {
        &self.program
    }

    /// The program's file stem (e.g. `galen` for `galen.dl`), or
    /// `unknown_program` if the path has no usable stem.
    pub fn program_name(&self) -> String {
        program_stem(&self.program).to_string()
    }

    /// The configured execution mode.
    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Returns `true` when the mode maintains state across updates.
    pub fn is_incremental(&self) -> bool {
        self.mode == ExecutionMode::Inc
    }

    /// Returns `true` for the one-shot batch mode, the only mode carrying
    /// the `Present` difference; `Inc` carries `i32` to track multiplicity.
    pub fn is_batch(&self) -> bool {
        self.mode == ExecutionMode::Batch
    }

    /// Returns `true` when operator-level profiling is on.
    pub fn profiling_enabled(&self) -> bool {
        self.profile
    }

    /// Returns `true` when sideways information passing is on, filtering
    /// later body atoms by earlier bindings.
    pub fn sip_enabled(&self) -> bool {
        self.sip
    }

    /// Returns `true` when string columns are interned as compact integer
    /// keys at load time.
    pub fn str_intern_enabled(&self) -> bool {
        self.str_intern
    }

    /// Path to the user-supplied UDF implementation file, if any.
    pub fn udf_file(&self) -> Option<&str> {
        self.udf_file.as_deref()
    }

    /// Extra `.include` search directories.
    pub fn include_dirs(&self) -> Vec<&Path> {
        self.include_dirs.iter().map(Path::new).collect()
    }

    /// Returns `true` when `.output` relations drain to stdout rather than
    /// files.
    pub fn output_to_stdout(&self) -> bool {
        self.output_to_stdout
    }

    /// Returns `true` when fact-string interning must run serially to keep
    /// `ord(_)` deterministic.
    pub fn serialize_load(&self) -> bool {
        self.serialize_load
    }

    /// Milliseconds between periodic metric flushes; `0` disables them.
    pub fn metrics_flush_interval_ms(&self) -> u64 {
        self.metrics_flush_interval_ms
    }
}

/// File stem of a program path (e.g. `galen` for `path/to/galen.dl`), or
/// `unknown_program` if the path has no usable stem.
pub fn program_stem(program: &str) -> &str {
    Path::new(program)
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("unknown_program")
}
