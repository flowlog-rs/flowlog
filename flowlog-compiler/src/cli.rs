//! Command-line interface for the FlowLog compiler binary.
//!
//! `Cli` is the clap front-end. It owns every CLI/driver concern (output
//! paths, build directory, fact directory, `--save-temps`) and projects the
//! pipeline-relevant subset onto a shared [`flowlog_common::Config`] via
//! [`Cli::to_config`].

use clap::Parser;
use flowlog_common::Config;
use flowlog_common::ExecutionMode;

use crate::CompileOptions;

/// Command line arguments for the FlowLog compiler.
#[derive(Parser, Debug, Clone, Default)]
#[command(version, about, long_about = None)]
pub struct Cli {
    /// Path to the Datalog (.dl) program file.
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Directory containing input fact files.
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Path for the generated Rust executable.
    #[arg(short = 'o', value_name = "PATH")]
    pub executable_path: Option<String>,

    /// Directory for writing output relations. Use `-` for stdout.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,

    /// Execution strategy: `batch` (default) or `inc`.
    #[arg(long, value_enum, default_value = "batch", value_name = "MODE")]
    mode: ExecutionMode,

    /// Collect per-rule execution statistics (timing, tuple counts, ...).
    #[arg(long, short = 'P')]
    pub profile: bool,

    /// Milliseconds between periodic metric flushes while a profiled run
    /// executes, so a long or interrupted run still leaves the latest partial
    /// metrics on disk. `0` flushes only at each natural write point (end of a
    /// batch run, or after each incremental commit). Needs `--profile`.
    #[arg(long, value_name = "MS", default_value_t = 5000)]
    pub metrics_flush_interval_ms: u64,

    /// Enable Sideways Information Passing to propagate binding constraints
    /// from rule heads into body atoms, reducing intermediate results.
    #[arg(long)]
    pub sip: bool,

    /// Intern string columns as compact integer keys at load time for faster
    /// joins, hashing, and lower memory usage. Recommended when the majority
    /// of join keys are string-typed.
    #[arg(long)]
    pub str_intern: bool,

    /// Path to a Rust source file containing UDF implementations.
    /// Functions declared with `.extern fn` in the Datalog
    /// program must be defined in this file.
    #[arg(long, value_name = "PATH")]
    pub udf_file: Option<String>,

    /// Build the generated Rust crate in DIR and keep it afterwards.
    /// Reusing the same DIR skips dependency rebuilds, reuses incremental
    /// artifacts across recompiles, and leaves the generated sources
    /// readable. Without it, the crate is built in a hidden scratch
    /// directory that is removed after a successful build.
    #[arg(short = 'B', long, value_name = "DIR")]
    pub build_dir: Option<String>,

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
            mode: self.mode,
            profile: self.profile,
            sip: self.sip,
            str_intern: self.str_intern,
            udf_file: self.udf_file.clone(),
            include_dirs: self.include_dirs.clone(),
            output_to_stdout: self.output_dir.as_deref() == Some("-"),
            serialize_load: false,
            metrics_flush_interval_ms: self.metrics_flush_interval_ms,
        }
    }

    /// Project the CLI onto the compiler options.
    pub fn to_compile_options(&self) -> CompileOptions {
        CompileOptions::new(
            &self.program,
            self.executable_path.clone(),
            self.output_dir.clone(),
            self.fact_dir.clone(),
            self.build_dir.clone(),
            self.check,
        )
    }
}
