//! Shared pipeline configuration.

use std::fmt;
use std::path::Path;
use std::str::FromStr;

/// Shared pipeline configuration consumed by parse, plan, and codegen.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Path to the Datalog (.dl) program file.
    pub program: String,
    /// Collect per-rule execution statistics (timing, tuple counts).
    pub profile: bool,
    /// Intern string columns as compact integer keys at load time.
    pub str_intern: bool,
    /// How a recursive presence relation drops rows it derives again.
    pub recursive_dedup: DedupStrategy,
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

    /// Returns `true` when operator-level profiling is on.
    pub fn profiling_enabled(&self) -> bool {
        self.profile
    }

    /// Returns `true` when string columns are interned as compact integer
    /// keys at load time.
    pub fn str_intern_enabled(&self) -> bool {
        self.str_intern
    }

    /// How a recursive presence relation drops rows it derives again.
    pub fn recursive_dedup(&self) -> DedupStrategy {
        self.recursive_dedup
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

/// How a recursive presence relation drops the rows its rules derive again.
/// Everywhere else one method serves, and this choice has no effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DedupStrategy {
    /// Probe a hash table of the rows announced so far. Each repeat costs
    /// one probe, which pays off when rules derive rows many times; the
    /// table keeps free slots beside the rows.
    #[default]
    Hash,
    /// Sort every derived row into Differential Dataflow's threshold
    /// arrangement. Each repeat is sorted with the rest, and the rows are
    /// stored packed, which costs less memory when rows are rarely derived
    /// twice.
    Sort,
}

impl DedupStrategy {
    /// Every strategy, in the order a listing of them shows. A new variant
    /// belongs here too; `Display`'s match already forces its spelling.
    const ALL: [DedupStrategy; 2] = [DedupStrategy::Hash, DedupStrategy::Sort];
}

/// The spelling a user writes for the strategy, which [`FromStr`] parses
/// back.
impl fmt::Display for DedupStrategy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            DedupStrategy::Hash => "hash",
            DedupStrategy::Sort => "sort",
        })
    }
}

impl FromStr for DedupStrategy {
    type Err = String;

    fn from_str(spelling: &str) -> Result<Self, Self::Err> {
        DedupStrategy::ALL
            .into_iter()
            .find(|strategy| strategy.to_string() == spelling)
            .ok_or_else(|| {
                let expected: Vec<String> = DedupStrategy::ALL
                    .iter()
                    .map(|strategy| format!("`{strategy}`"))
                    .collect();
                format!(
                    "unknown dedup method `{spelling}`; expected {}",
                    expected.join(" or ")
                )
            })
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

    #[test]
    fn every_dedup_strategy_round_trips_through_its_spelling() {
        for strategy in DedupStrategy::ALL {
            assert_eq!(strategy.to_string().parse::<DedupStrategy>(), Ok(strategy));
        }
    }

    #[test]
    fn dedup_strategies_are_spelled_hash_and_sort() {
        assert_eq!(
            DedupStrategy::ALL.map(|strategy| strategy.to_string()),
            ["hash", "sort"]
        );
    }

    #[test]
    fn an_unknown_dedup_spelling_is_rejected() {
        assert_eq!(
            "tree".parse::<DedupStrategy>(),
            Err("unknown dedup method `tree`; expected `hash` or `sort`".to_string())
        );
    }
}
