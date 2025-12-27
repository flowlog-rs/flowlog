//! Command line argument parsing for FlowLog tools.

use clap::{ArgAction, Parser, ValueEnum};
use std::path::{Path, PathBuf};
use std::{fs, process};

/// Execution strategy for FlowLog workflows
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum, Default)]
pub enum ExecutionMode {
    /// Maintain state across updates.
    /// Tracking how many times each fact is derived,
    /// supporting incremental view maintenance.
    Incremental,
    /// Recompute outputs in a single pass.
    /// Only tracks whether facts are present or absent,
    /// making it suitable for high-performance static execution.
    #[default]
    Batch,
}

/// Command line arguments for FlowLog tools
#[derive(Parser, Debug, Clone)]
#[command(version, about, long_about = None)]
pub struct Config {
    /// Path of the Datalog program, or "all" to process all example files
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Specify directory for fact files (only used by compiler/executor)
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Override the generated executable path (only used by compiler/executor)
    #[arg(short = 'o', value_name = "PATH")]
    pub executable_path: Option<String>,

    /// Specify directory for output files (only used by compiler/executor). If <DIR> is `-` then stdout is used.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,

    /// Choose execution strategy (incremental = maintain state across updates, batch = recompute each run; only used by compiler/executor)
    #[arg(long, value_enum, default_value = "batch", value_name = "MODE")]
    pub mode: ExecutionMode,

    /// Enable profiling (collect execution statistics; only used by planner, compiler/executor)
    #[arg(long, short = 'P', action = ArgAction::SetTrue)]
    pub profile: bool,
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

    pub fn executable_name(&self) -> String {
        self.executable_path()
            .file_name()
            .and_then(|name| name.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "Compiler error: invalid executable name".into())
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

    pub fn is_incremental(&self) -> bool {
        self.mode == ExecutionMode::Incremental
    }

    /// For compiler/executor: get fact_dir with validation
    pub fn fact_dir_required(&self) -> &str {
        self.fact_dir
            .as_ref()
            .expect("--fact-dir is required for this tool")
    }

    /// For compiler/executor: get output_dir with validation  
    pub fn output_dir_required(&self) -> &str {
        self.output_dir
            .as_ref()
            .expect("--output-dir is required for this tool")
    }

    /// For planner, compiler/executor: check if profiling is enabled
    pub fn profiling_enabled(&self) -> bool {
        self.profile
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

    // Read and collect .dl files
    let entries = match fs::read_dir(example_dir) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Error reading example dir: {}", e);
            process::exit(1);
        }
    };

    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("dl") {
            files.push(path);
        }
    }

    files.sort();

    if files.is_empty() {
        eprintln!("No .dl files found in {}", example_dir);
        process::exit(1);
    }

    files
}
