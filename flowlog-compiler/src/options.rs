//! Compile options for the build stage.

use std::path::Path;
use std::path::PathBuf;

use flowlog_common::compute_fp;
use flowlog_common::program_stem;

/// Compile options, independent of how they were parsed.
#[derive(Debug, Clone)]
pub struct CompileOptions {
    executable_path: PathBuf,
    /// Directory for writing output relations.
    output_dir: Option<String>,
    /// Directory containing input fact files.
    fact_dir: Option<String>,
    build_dir: Option<PathBuf>,
    target_dir: Option<PathBuf>,
    /// Type-check the emitted crate with `cargo check`.
    check_only: bool,
}

impl CompileOptions {
    /// Uses the program's file stem when `executable_path` is absent.
    pub fn new(
        program: &str,
        executable_path: Option<String>,
        output_dir: Option<String>,
        fact_dir: Option<String>,
        build_dir: Option<String>,
        target_dir: Option<String>,
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
            target_dir: target_dir.map(PathBuf::from),
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

    /// Returns the named project directory, or a hidden scratch directory
    /// beside the executable. See [`Self::keeps_build_dir`] for retention.
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

    /// Relative paths start at the compiler's working directory. `None`
    /// leaves directory selection to Cargo's environment and configuration.
    pub fn target_dir(&self) -> Option<&Path> {
        self.target_dir.as_deref()
    }

    /// Returns the Cargo package and binary name: the executable's name, with
    /// the characters Cargo rejects (dots, spaces, etc.) replaced by
    /// underscores and `fl_` before a leading digit, then `_` and a hash of
    /// the absolute [`Self::build_dir`]. Every compile into the same build
    /// directory gets the same name, so Cargo can reuse its earlier build.
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
        // Each generated crate is the root of its own Cargo workspace, and
        // Cargo keys a package's build (fingerprint, artifact names) by its
        // name, version, and path relative to its workspace root
        // (`PackageId::stable_hash`), which is empty for every such crate.
        // Without the hash, two build directories compiling executables of
        // the same name into one target directory share one build, whose
        // freshness Cargo judges by mtimes, so a compile can install the
        // other program's binary. The hash may change with the Rust version
        // FlowLog is built with, which costs one rebuild under the new name.
        let build_dir = self.build_dir();
        let build_dir = std::path::absolute(&build_dir).unwrap_or(build_dir);
        format!("{s}_{:016x}", compute_fp(&build_dir))
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
    use rstest::rstest;

    use super::*;

    #[test]
    fn build_dir_defaults_to_hidden_sibling_of_executable() {
        let opts = CompileOptions::new(
            "analysis.dl",
            Some("out/bin".into()),
            None,
            None,
            None,
            None,
            false,
        );
        assert_eq!(opts.build_dir(), PathBuf::from("out/.bin.build"));
        assert!(!opts.keeps_build_dir());
    }

    #[test]
    fn named_build_dir_wins_and_is_kept() {
        let opts = CompileOptions::new(
            "analysis.dl",
            Some("out/bin".into()),
            None,
            None,
            Some("cache/dir".into()),
            None,
            false,
        );
        assert_eq!(opts.build_dir(), PathBuf::from("cache/dir"));
        assert!(opts.keeps_build_dir());
    }

    fn options_for(executable: &str, build_dir: Option<&str>) -> CompileOptions {
        CompileOptions::new(
            "analysis.dl",
            Some(executable.into()),
            None,
            None,
            build_dir.map(String::from),
            None,
            false,
        )
    }

    // Cases: executable path, expected name before the hash.
    #[rstest]
    #[case("out/bin", "bin")]
    #[case("out/my prog.v2", "my_prog_v2")]
    #[case("out/2pass", "fl_2pass")]
    fn crate_name_is_sanitized_executable_name_then_hash(
        #[case] executable: &str,
        #[case] stem: &str,
    ) {
        let name = options_for(executable, None).crate_name();
        let hash = name
            .strip_prefix(stem)
            .and_then(|rest| rest.strip_prefix('_'))
            .unwrap_or_else(|| panic!("'{name}' does not start with '{stem}_'"));
        assert_eq!(hash.len(), 16, "{name}");
        assert!(hash.chars().all(|c| c.is_ascii_hexdigit()), "{name}");
    }

    /// Two programs compiled to executables of the same name in different
    /// directories must not share Cargo's build in a shared target
    /// directory, or one could be installed as the other.
    #[test]
    fn crate_name_differs_between_build_dirs() {
        assert_ne!(
            options_for("a/bin", None).crate_name(),
            options_for("b/bin", None).crate_name()
        );
    }

    /// A named build directory decides the name, wherever the executable
    /// goes, so every compile into it reuses its build.
    #[test]
    fn crate_name_follows_a_named_build_dir() {
        assert_eq!(
            options_for("a/bin", Some("cache/build")).crate_name(),
            options_for("b/bin", Some("cache/build")).crate_name()
        );
        assert_ne!(
            options_for("a/bin", Some("cache/build")).crate_name(),
            options_for("a/bin", Some("cache/other")).crate_name()
        );
    }

    /// The name depends on where the build directory is, not on how its path
    /// is spelled: a relative path and the absolute one name the same build.
    #[test]
    fn crate_name_is_the_same_for_relative_and_absolute_spellings() {
        let absolute = std::path::absolute("out/bin").unwrap();
        assert_eq!(
            options_for("out/bin", None).crate_name(),
            options_for(absolute.to_str().unwrap(), None).crate_name()
        );
    }
}
