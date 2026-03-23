//! Command line argument parsing for FlowLog tools.

use clap::{Parser, ValueEnum};
use std::path::{Path, PathBuf};
use std::{fs, process};

/// Execution strategy for FlowLog workflows
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum, Default)]
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

/// Command line arguments for FlowLog tools
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Path to the Datalog (.dl) program file
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Directory containing input fact files
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Path for the generated Rust executable
    #[arg(short = 'o', value_name = "PATH")]
    pub executable_path: Option<String>,

    /// Directory for writing output relations. Use `-` for stdout
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,

    /// Execution strategy: `datalog-batch` (default), `datalog-inc`,
    /// `extend-batch`, or `extend-inc`.
    /// Extended modes enable explicit `loop` blocks and forbid implicit recursion.
    #[arg(long, value_enum, default_value = "datalog-batch", value_name = "MODE")]
    pub mode: ExecutionMode,

    /// Collect per-rule execution statistics (timing, tuple counts)
    #[arg(long, short = 'P')]
    pub profile: bool,

    /// Enable Sideways Information Passing to propagate binding constraints
    /// from rule heads into body atoms, reducing intermediate results
    #[arg(long)]
    pub sip: bool,

    /// Intern string columns as compact integer keys at load time for faster
    /// joins, hashing, and lower memory usage. Recommended when the majority
    /// of join keys are string-typed
    #[arg(long)]
    pub str_intern: bool,

    /// Path to a Rust source file containing UDF implementations.
    /// Functions declared with `.extern fn` in the Datalog
    /// program must be defined in this file.
    #[arg(long, value_name = "PATH")]
    pub udf_file: Option<String>,

    /// Keep the intermediate generated Rust crate instead of cleaning it up
    /// after building the executable.
    #[arg(long)]
    pub save_temps: bool,
}

impl Config {
    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn should_process_all(&self) -> bool {
        self.program == "all" || self.program == "--all"
    }

    pub fn program_name(&self) -> String {
        Path::new(&self.program)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_program".into())
    }

    pub fn fact_dir(&self) -> Option<&str> {
        self.fact_dir.as_deref()
    }

    pub fn executable_path(&self) -> PathBuf {
        self.executable_path
            .as_ref()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(self.program_name()))
    }

    /// Intermediate build directory for the generated Rust crate.
    /// Uses a hidden dotfile name (e.g., `.galen.build/`) so it won't collide
    /// with the final executable or any user files.
    pub fn build_dir(&self) -> PathBuf {
        let exe = self.executable_path();
        let name = exe.file_name().and_then(|n| n.to_str()).unwrap_or("out");
        exe.with_file_name(format!(".{name}.build"))
    }

    pub fn executable_name(&self) -> String {
        self.executable_path()
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("out")
            .to_string()
    }

    /// Sanitized name suitable for use as a Cargo package/binary name.
    /// Replaces characters that Cargo rejects (dots, spaces, etc.) with
    /// underscores and ensures the result doesn't start with a digit.
    pub fn crate_name(&self) -> String {
        let raw = self.executable_name();
        let mut s: String = raw
            .chars()
            .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
            .collect();
        // Cargo rejects names starting with a digit.
        if s.starts_with(|c: char| c.is_ascii_digit()) {
            s.insert_str(0, "fl_");
        }
        if s.is_empty() {
            s = "out".to_string();
        }
        s
    }

    pub fn output_dir(&self) -> Option<&str> {
        self.output_dir.as_deref()
    }

    pub fn output_to_stdout(&self) -> bool {
        self.output_dir.as_deref() == Some("-")
    }

    pub fn mode(&self) -> ExecutionMode {
        self.mode
    }

    /// Whether the mode is an incremental mode (`DatalogInc` or `ExtendInc`).
    pub fn is_incremental(&self) -> bool {
        matches!(
            self.mode,
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc
        )
    }

    /// Whether the mode is a batch mode (`DatalogBatch` or `ExtendBatch`).
    pub fn is_batch(&self) -> bool {
        matches!(
            self.mode,
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch
        )
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

    /// Returns the configured fact directory, panicking if unset.
    pub fn fact_dir_required(&self) -> &str {
        self.fact_dir
            .as_ref()
            .expect("--fact-dir is required for this tool")
    }

    /// Returns the configured output directory, panicking if unset.
    pub fn output_dir_required(&self) -> &str {
        self.output_dir
            .as_ref()
            .expect("--output-dir is required for this tool")
    }

    /// Whether profiling instrumentation is enabled.
    pub fn profiling_enabled(&self) -> bool {
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

    /// Whether to keep the intermediate generated Rust crate.
    pub fn save_temps(&self) -> bool {
        self.save_temps
    }
}

/// Get all .dl files from the example directory, sorted alphabetically
pub fn get_example_files() -> Vec<std::path::PathBuf> {
    let example_dir = "example";

    // Check if example directory exists
    if !Path::new(example_dir).exists() {
        eprintln!("Error: Directory '{}' not found", example_dir);
        process::exit(1);
    }

    // Recursively collect all .dl files under example/
    let mut files = Vec::new();
    let mut dirs = vec![PathBuf::from(example_dir)];
    while let Some(dir) = dirs.pop() {
        let entries = match fs::read_dir(&dir) {
            Ok(entries) => entries,
            Err(e) => {
                eprintln!("Error reading dir '{}': {}", dir.display(), e);
                continue;
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.extension().and_then(|s| s.to_str()) == Some("dl") {
                files.push(path);
            }
        }
    }

    files.sort();

    if files.is_empty() {
        eprintln!("No .dl files found in {}", example_dir);
        process::exit(1);
    }

    files
}
