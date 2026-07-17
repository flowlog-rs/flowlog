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
    /// User-named build directory for the generated crate. Naming one opts
    /// into persistence: the directory survives the build and doubles as a
    /// recompile cache (an unchanged program rebuilds as a cargo no-op, an
    /// edited one incrementally). `None` builds in a hidden scratch
    /// directory that is removed after a successful build.
    build_dir: Option<PathBuf>,
    /// Type-check the emitted crate with `cargo check`.
    check_only: bool,
}

impl CompileOptions {
    /// Build options for `program`. `executable_path` is the user's `-o`
    /// override; when absent it defaults to the program's file stem.
    pub fn new(
        program: &str,
        executable_path: Option<String>,
        output_dir: Option<String>,
        fact_dir: Option<String>,
        build_dir: Option<String>,
        check_only: bool,
    ) -> Self {
        let executable_path = executable_path
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(program_stem(program)));
        Self {
            executable_path,
            output_dir,
            fact_dir,
            build_dir: build_dir.map(PathBuf::from),
            check_only,
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

    /// Build directory for the generated Rust crate: the user-named one
    /// when given, otherwise a scratch sibling of the executable. The
    /// scratch default uses a hidden dotfile name so it won't collide
    /// with the final executable or any user files.
    pub fn build_dir(&self) -> PathBuf {
        self.build_dir.clone().unwrap_or_else(|| {
            self.executable_path
                .with_file_name(format!(".{}.build", self.executable_name()))
        })
    }

    /// Returns `true` if the user named a build directory, which then
    /// persists after the build.
    pub fn keeps_build_dir(&self) -> bool {
        self.build_dir.is_some()
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

    pub fn check_only(&self) -> bool {
        self.check_only
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options(executable: Option<&str>, build_dir: Option<&str>) -> CompileOptions {
        CompileOptions::new(
            "analysis.dl",
            executable.map(String::from),
            None,
            None,
            build_dir.map(String::from),
            false,
        )
    }

    /// Scratch builds must not collide with the executable or user files,
    /// so the default is a hidden dot-directory beside the binary, and it
    /// is not kept.
    #[test]
    fn build_dir_defaults_to_hidden_sibling_of_executable() {
        let opts = options(Some("out/bin"), None);
        assert_eq!(opts.build_dir(), PathBuf::from("out/.bin.build"));
        assert!(!opts.keeps_build_dir());
    }

    /// Naming a directory replaces the scratch default and opts into
    /// keeping it.
    #[test]
    fn named_build_dir_wins_and_is_kept() {
        let opts = options(Some("out/bin"), Some("cache/dir"));
        assert_eq!(opts.build_dir(), PathBuf::from("cache/dir"));
        assert!(opts.keeps_build_dir());
    }
}
