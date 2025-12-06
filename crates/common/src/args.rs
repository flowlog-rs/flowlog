//! Command line argument parsing for FlowLog tools.

use clap::{Parser, ValueEnum};
use std::{fs, path::Path, process};

/// Execution strategy for FlowLog workflows
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum ExecutionMode {
    /// Maintain state across updates.
    /// Tracking how many times each fact is derived,
    /// supporting incremental view maintenance.
    Incremental,
    /// Recompute outputs in a single pass.
    /// Only tracks whether facts are present or absent,
    /// making it suitable for high-performance static execution.
    Batch,
}

/// Command line arguments for FlowLog tools
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path of the Datalog program, or "all" to process all example files
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Specify directory for fact files (only used by compiler/executor)
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Override the generated executable/package name (only used by compiler/executor)
    #[arg(short = 'o', long = "output", value_name = "NAME")]
    pub output: Option<String>,

    /// Specify directory for output files (only used by compiler/executor). If <DIR> is `-` then stdout is used.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,

    /// Choose execution strategy (incremental = maintain state across updates, batch = recompute each run)
    #[arg(long, value_enum, default_value = "batch", value_name = "MODE")]
    pub mode: ExecutionMode,
}

impl Args {
    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn should_process_all(&self) -> bool {
        self.program == "all" || self.program == "--all"
    }

    pub fn program_name(&self) -> String {
        std::path::Path::new(&self.program)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_program".into())
    }

    pub fn fact_dir(&self) -> Option<&str> {
        self.fact_dir.as_deref()
    }

    pub fn output(&self) -> Option<&str> {
        self.output.as_deref()
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
