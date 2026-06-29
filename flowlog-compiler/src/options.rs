//! Compile options for the build stage.

use std::path::Path;
use std::path::PathBuf;

use flowlog_common::program_stem;

/// Compile options, independent of how they were parsed.
#[derive(Debug, Clone)]
pub struct CompileOptions {
    /// Resolved path for the generated executable (CLI default already applied).
    executable_path: PathBuf,
    /// Directory for writing output relations, if any.
    output_dir: Option<String>,
    /// Directory containing input fact files, if any.
    fact_dir: Option<String>,
    /// Keep the intermediate generated Rust crate instead of cleaning it up.
    save_temps: bool,
}

impl CompileOptions {
    /// Build options for `program`. `executable_path` is the user's `-o`
    /// override; when absent it defaults to the program's file stem.
    pub fn new(
        program: &str,
        executable_path: Option<String>,
        output_dir: Option<String>,
        fact_dir: Option<String>,
        save_temps: bool,
    ) -> Self {
        let executable_path = executable_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(program_stem(program)));
        Self {
            executable_path,
            output_dir,
            fact_dir,
            save_temps,
        }
    }

    pub fn executable_path(&self) -> &Path {
        &self.executable_path
    }

    pub fn executable_name(&self) -> &str {
        self.executable_path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("out")
    }

    /// Intermediate build directory for the generated Rust crate.
    /// Uses a hidden dotfile name so it won't collide
    /// with the final executable or any user files.
    pub fn build_dir(&self) -> PathBuf {
        self.executable_path
            .with_file_name(format!(".{}.build", self.executable_name()))
    }

    /// Sanitized name suitable for use as a Cargo package/binary name.
    /// Replaces characters that Cargo rejects (dots, spaces, etc.) with
    /// underscores and ensures the result doesn't start with a digit.
    pub fn crate_name(&self) -> String {
        let mut s: String = self
            .executable_name()
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                    c
                } else {
                    '_'
                }
            })
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

    pub fn fact_dir(&self) -> Option<&str> {
        self.fact_dir.as_deref()
    }

    pub fn save_temps(&self) -> bool {
        self.save_temps
    }
}
