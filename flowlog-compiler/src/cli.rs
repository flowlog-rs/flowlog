//! Command-line parsing and conversion into pipeline [`Config`] and build
//! [`CompileOptions`].

use clap::Parser;
use flowlog_common::Config;
use flowlog_common::DedupStrategy;

use crate::CompileOptions;

/// Command line arguments for the FlowLog compiler.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the Datalog (.dl) program file.
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Default input fact directory (otherwise the working directory).
    /// The executable's `-F` flag overrides it at runtime.
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Path for the generated Rust executable.
    #[arg(short = 'o', value_name = "PATH")]
    pub executable_path: Option<String>,

    /// Default output directory (otherwise the working directory).
    /// Use `-` for stdout. The executable's `-D` flag overrides it at runtime.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,

    /// Collect per-rule execution statistics (timing, tuple counts, ...).
    #[arg(long, short = 'P')]
    pub profile: bool,

    /// Milliseconds between periodic metric flushes while a profiled run
    /// executes, so a long or interrupted run still leaves the latest partial
    /// metrics on disk. `0` flushes only at each natural write point (end of a
    /// batch run, or after each incremental commit). Needs `--profile`.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub metrics_flush_interval_ms: u64,

    /// Intern string columns as compact integer keys at load time for faster
    /// joins, hashing, and lower memory usage. Recommended when the majority
    /// of join keys are string-typed.
    #[arg(long)]
    pub str_intern: bool,

    /// How a recursive relation drops the rows its rules derive again.
    /// `hash` probes a table of the rows seen so far, fastest when rules
    /// derive the same rows many times; `sort` sorts every derived row,
    /// which keeps less memory when rows are rarely derived twice.
    #[arg(long, value_name = "METHOD", default_value_t)]
    pub recursive_dedup: DedupStrategy,

    /// Path to a Rust source file containing UDF implementations.
    /// Functions declared with `.extern fn` in the Datalog
    /// program must be defined in this file.
    #[arg(long, value_name = "PATH")]
    pub udf_file: Option<String>,

    /// Keep the generated Rust project in DIR after compilation.
    /// Without this option, a hidden scratch project is removed on success.
    #[arg(short = 'B', long, value_name = "DIR")]
    pub build_dir: Option<String>,

    /// Store Cargo artifacts in DIR, shared across generated projects.
    /// Relative paths start at the compiler's working directory. Overrides
    /// CARGO_TARGET_DIR; when omitted, Cargo uses its environment and config.
    #[arg(short = 'T', long, value_name = "DIR")]
    pub target_dir: Option<String>,

    /// Type-check the generated crate with `cargo check` instead of building
    /// an executable. Faster; conflicts with `-o`.
    #[arg(long, conflicts_with = "executable_path")]
    pub check: bool,

    /// Extra search directory for `.include` directives. May be specified
    /// multiple times. Includes are resolved by trying the parent file's
    /// directory first, then each `-I` directory in order.
    #[arg(short = 'I', long = "include-dir", value_name = "DIR")]
    pub include_dirs: Vec<String>,
}

impl Cli {
    /// Project the CLI onto the shared pipeline [`Config`].
    pub fn to_config(&self) -> Config {
        Config {
            program: self.program.clone(),
            profile: self.profile,
            str_intern: self.str_intern,
            recursive_dedup: self.recursive_dedup,
            udf_file: self.udf_file.clone(),
            include_dirs: self.include_dirs.clone(),
            output_to_stdout: self.output_dir.as_deref() == Some("-"),
            serialize_load: false,
            metrics_flush_interval_ms: self.metrics_flush_interval_ms,
        }
    }

    /// Extracts build settings and applies the default executable path.
    pub fn to_compile_options(&self) -> CompileOptions {
        CompileOptions::new(
            &self.program,
            self.executable_path.clone(),
            self.output_dir.clone(),
            self.fact_dir.clone(),
            self.build_dir.clone(),
            self.target_dir.clone(),
            self.check,
        )
    }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Cases: arguments after the program, strategy.
    #[rstest]
    #[case(&[], DedupStrategy::Hash)]
    #[case(&["--recursive-dedup", "hash"], DedupStrategy::Hash)]
    #[case(&["--recursive-dedup", "sort"], DedupStrategy::Sort)]
    fn the_recursive_dedup_flag_reaches_the_pipeline_config(
        #[case] args: &[&str],
        #[case] expected: DedupStrategy,
    ) {
        let cli = Cli::try_parse_from(["flowlog", "program.dl"].iter().chain(args))
            .expect("arguments parse");
        assert_eq!(cli.to_config().recursive_dedup(), expected);
    }

    #[test]
    fn an_unknown_recursive_dedup_method_is_rejected() {
        let error = Cli::try_parse_from(["flowlog", "program.dl", "--recursive-dedup", "tree"])
            .expect_err("`tree` is not a dedup method");
        assert_eq!(error.kind(), clap::error::ErrorKind::ValueValidation);
    }
}
