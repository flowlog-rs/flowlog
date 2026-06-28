//! Shared pipeline configuration.

use std::path::Path;

/// Execution strategy for FlowLog workflows
#[derive(Clone, Copy, Debug, Eq, PartialEq, Default)]
#[cfg_attr(feature = "clap", derive(clap::ValueEnum))]
pub enum ExecutionMode {
    /// Datalog single-pass batch execution.
    /// Only tracks whether facts are present or absent,
    /// making it suitable for high-performance static execution.
    #[default]
    DatalogBatch,
    /// Datalog incremental execution.
    /// Maintains state across updates, tracking how many times each fact
    /// is derived, supporting incremental view maintenance.
    DatalogInc,
    /// Extended batch execution with explicit `loop` blocks.
    /// Recursion is only allowed inside `loop` blocks; any recursive
    /// dependency in plain rules is a hard error.
    ExtendBatch,
    /// Extended incremental execution with explicit `loop` blocks.
    /// Combines incremental view maintenance with explicit loop control.
    ExtendInc,
}

impl ExecutionMode {
    /// Map onto the profiler's local mode enum (the profiler crate is
    /// independent of `flowlog-build`, so it carries its own copy).
    pub fn profile_mode(self) -> flowlog_profiler::ProfileMode {
        use flowlog_profiler::ProfileMode;
        match self {
            Self::DatalogBatch => ProfileMode::DatalogBatch,
            Self::DatalogInc => ProfileMode::DatalogInc,
            Self::ExtendBatch => ProfileMode::ExtendBatch,
            Self::ExtendInc => ProfileMode::ExtendInc,
        }
    }

    pub(crate) fn is_incremental(self) -> bool {
        matches!(self, Self::DatalogInc | Self::ExtendInc)
    }

    pub(crate) fn is_batch(self) -> bool {
        matches!(self, Self::DatalogBatch | Self::ExtendBatch)
    }
}

/// Shared pipeline configuration consumed by parse → plan → codegen.
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

    /// Whether the mode maintains state incrementally across updates.
    pub fn is_incremental(&self) -> bool {
        self.mode.is_incremental()
    }

    /// Whether the mode is a one-shot batch mode.
    pub fn is_batch(&self) -> bool {
        self.mode.is_batch()
    }

    /// Whether the mode is `DatalogBatch`. This is the only mode that uses
    /// `Present` diff; all other modes use `i32` diff for multiplicity tracking.
    pub fn is_datalog_batch(&self) -> bool {
        self.mode == ExecutionMode::DatalogBatch
    }

    /// Whether Extended Datalog mode is enabled (loop blocks allowed,
    /// implicit recursion forbidden).
    pub fn is_extended(&self) -> bool {
        matches!(
            self.mode,
            ExecutionMode::ExtendBatch | ExecutionMode::ExtendInc
        )
    }

    /// Whether profiling instrumentation is enabled.
    pub fn profiling_enabled(&self) -> bool {
        if self.profile && self.is_extended() {
            unimplemented!("-P (profiling) is not yet supported with extended modes");
        }
        self.profile
    }

    /// Whether Sideways Information Passing (SIP) optimization is enabled.
    pub fn sip_enabled(&self) -> bool {
        self.sip
    }

    /// Whether string interning is enabled.
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

    /// Whether `.output` relations drain to stdout rather than files.
    pub fn output_to_stdout(&self) -> bool {
        self.output_to_stdout
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
