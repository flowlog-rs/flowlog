//! Command line argument parsing for Macaron tools.

use clap::Parser;
use std::{fs, path::Path, process};

/// Command line arguments for Macaron tools
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path of the Datalog program, or "all" to process all example files
    #[arg(value_name = "PROGRAM")]
    pub program: String,

    /// Specify directory for fact files (only used by generator/executor)
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: Option<String>,

    /// Specify directory for output files (only used by generator/executor). If <DIR> is `-` then stdout is used.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: Option<String>,
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

    pub fn output_dir(&self) -> Option<&str> {
        self.output_dir.as_deref()
    }

    pub fn output_to_stdout(&self) -> bool {
        self.output_dir.as_deref() == Some("-")
    }

    /// For generator/executor: get fact_dir with validation
    pub fn fact_dir_required(&self) -> &str {
        self.fact_dir
            .as_ref()
            .expect("--fact-dir is required for this tool")
    }

    /// For generator/executor: get output_dir with validation  
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
