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
    /// Let an incremental engine own its single timely worker on the calling
    /// thread instead of a spawned one. Requested here; the build also has
    /// to satisfy [`Config::inlines_single_worker`].
    pub inline_single_worker: bool,
    /// Enable Sideways Information Passing.
    pub sip: bool,
    /// Intern string columns as compact integer keys at load time.
    pub str_intern: bool,
    /// Caller guarantee that every EDB update already is a set-correct net
    /// delta: a fact is inserted only while absent and retracted only while
    /// resident. Unchecked; breaking it leaves multiplicities above 1 in the
    /// derived relations.
    pub trusted_set_inputs: bool,
    /// Path to a Rust source file containing UDF implementations.
    pub udf_file: Option<String>,
    /// Extra search directories for `.include` directives.
    pub include_dirs: Vec<String>,
    /// Whether `.output` relations drain to stdout (`-D -`) rather than files.
    /// Derived by the CLI from `--output-dir`; always `false` in library mode.
    pub output_to_stdout: bool,
    /// When set, fact strings are interned serially rather than in parallel, so
    /// interning order — and therefore `ord(_)` values — is deterministic across
    /// worker counts (`-w N` matches `-w 1`).
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

    /// Returns `true` if the incremental engine is generated in its inline
    /// single-worker shape. Gated to [`ExecutionMode::DatalogInc`]: the
    /// extended incremental mode is not covered yet. Requesting it
    /// alongside profiling or in any other mode is rejected by the
    /// library-mode build rather than silently downgraded here.
    pub fn inlines_single_worker(&self) -> bool {
        self.inline_single_worker && self.mode == ExecutionMode::DatalogInc
    }

    /// Returns `true` if the EDB input clamp may be dropped: the caller
    /// promised set-correct net deltas and the mode is
    /// [`ExecutionMode::DatalogInc`], the one mode whose inputs are exactly
    /// those caller-supplied deltas. Every other mode also feeds itself
    /// (file loads, batch inserts) and keeps the clamp regardless.
    ///
    /// Answers the question per program, not per relation: a relation
    /// seeded with `.fact` rows is fed by the compiler too, so codegen
    /// keeps its clamp even when this returns `true`.
    pub fn skips_edb_normalization(&self) -> bool {
        self.trusted_set_inputs && self.mode == ExecutionMode::DatalogInc
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

    /// Whether fact-string interning must be serial for deterministic `ord(_)`.
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Every variant, so a new mode has to state its answers here rather
    /// than inherit them from a wildcard.
    const ALL_MODES: [ExecutionMode; 4] = [
        ExecutionMode::DatalogBatch,
        ExecutionMode::DatalogInc,
        ExecutionMode::ExtendBatch,
        ExecutionMode::ExtendInc,
    ];

    #[test]
    fn both_opt_in_options_default_to_off() {
        let config = Config::default();
        assert!(!config.inline_single_worker);
        assert!(!config.trusted_set_inputs);
        assert!(!config.inlines_single_worker());
        assert!(!config.skips_edb_normalization());
    }

    #[test]
    fn edb_normalization_is_kept_in_every_mode_unless_inputs_are_trusted() {
        for mode in ALL_MODES {
            let config = Config {
                mode,
                ..Config::default()
            };
            assert!(
                !config.skips_edb_normalization(),
                "{mode:?} must normalize EDB inputs by default"
            );
        }
    }

    #[test]
    fn only_datalog_inc_skips_edb_normalization_when_inputs_are_trusted() {
        for mode in ALL_MODES {
            let config = Config {
                mode,
                trusted_set_inputs: true,
                ..Config::default()
            };
            assert_eq!(
                config.skips_edb_normalization(),
                mode == ExecutionMode::DatalogInc,
                "{mode:?} skip decision under trusted inputs"
            );
        }
    }

    #[test]
    fn inline_single_worker_is_honored_when_asked_for() {
        let config = Config {
            mode: ExecutionMode::DatalogInc,
            inline_single_worker: true,
            ..Config::default()
        };
        assert!(config.inlines_single_worker());
    }

    /// The inline shape is only generated for the one incremental engine
    /// that carries it; every other mode ignores the request outright, and
    /// the library-mode build rejects it before reaching codegen.
    #[test]
    fn only_datalog_inc_inlines_its_single_worker() {
        for mode in ALL_MODES {
            let config = Config {
                mode,
                inline_single_worker: true,
                ..Config::default()
            };
            assert_eq!(
                config.inlines_single_worker(),
                mode == ExecutionMode::DatalogInc,
                "{mode:?} inline decision"
            );
        }
    }
}
