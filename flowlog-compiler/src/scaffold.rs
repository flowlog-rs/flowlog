//! Write the scaffolded Rust crate to disk and render its Cargo metadata.
//!
//! The emitted project tree is:
//!
//! ```text
//! <build_dir>/
//! ├── Cargo.toml
//! ├── .cargo/config.toml          # `-Dwarnings` so generated code stays clean
//! └── src/
//!     ├── main.rs                 # assembled dataflow + runtime shell
//!     ├── relation.rs             # `Relation` trait + per-EDB handlers
//!     ├── cmd.rs, prompt.rs       # incremental-mode only (shell)
//!     └── udf.rs                  # optional, copied from `Config::udf_file`
//! ```
//!
//! `write_project` lays out these files from already-rendered strings;
//! `render_cargo_toml` / `render_cargo_config` produce the metadata.

use std::env;
use std::fs;
use std::io;
use std::path::Path;

use flowlog_build::Features;
use flowlog_common::Config;
use flowlog_common::ExecutionMode;
use toml_edit::Array;
use toml_edit::DocumentMut;
use toml_edit::InlineTable;
use toml_edit::Item;
use toml_edit::Table;
use toml_edit::Value;
use toml_edit::value;

use crate::Compiler;

// =========================================================================
// Project layout
// =========================================================================

impl Compiler {
    /// Materialize the scaffolded crate under [`CompileOptions::build_dir`].
    ///
    /// Arguments are pre-rendered file contents; this function only decides
    /// _where_ they go and creates intermediate directories. Optional files
    /// (incremental-mode shell, semiring modules, UDF stub) are written
    /// only when the program needs them.
    pub(crate) fn write_project(
        &self,
        main_rs: &str,
        relation_rs: &str,
        cargo_toml: &str,
        cargo_config: &str,
    ) -> io::Result<()> {
        let config = &self.config;
        let root = self.options.build_dir();
        let src_dir = root.join("src");
        ensure_dir(&src_dir)?;

        // Core files.
        write_file(&root.join("Cargo.toml"), cargo_toml.trim_start())?;
        write_file(
            &root.join(".cargo").join("config.toml"),
            cargo_config.trim_start(),
        )?;
        write_file(&src_dir.join("main.rs"), main_rs)?;
        write_file(&src_dir.join("relation.rs"), relation_rs.trim_start())?;

        // Incremental shell: REPL command parser + readline wrapper.
        match config.mode() {
            ExecutionMode::Inc => {
                write_file(&src_dir.join("cmd.rs"), CMD_RS_TMPL.trim_start())?;
                write_file(&src_dir.join("prompt.rs"), PROMPT_RS_TMPL.trim_start())?;
            }
            ExecutionMode::Batch => {}
        }

        // Optional UDF module — copied verbatim from a user-supplied file.
        if let Some(udf_path) = config.udf_file() {
            let content = fs::read_to_string(udf_path).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("failed to read UDF file '{udf_path}': {e}"),
                )
            })?;
            write_file(&src_dir.join("udf.rs"), content.trim_start())?;
        }

        Ok(())
    }
}

// =========================================================================
// Cargo metadata
// =========================================================================

/// Render the emitted crate's `Cargo.toml`.
///
/// Dependencies are feature-gated: we emit only what the generated code
/// actually references so the downstream `cargo build` pulls the minimum
/// set of crates.
pub(crate) fn render_cargo_toml(
    crate_name: &str,
    config: &Config,
    features: &Features,
    keep_build_dir: bool,
) -> String {
    let mut doc = DocumentMut::new();

    doc["package"] = Item::Table(Table::new());
    {
        let pkg = doc["package"].as_table_mut().unwrap();
        pkg["name"] = crate_name.into();
        pkg["version"] = "0.1.0".into();
        pkg["edition"] = "2024".into();
    }

    // The generated crate is standalone; the empty `[workspace]` detaches
    // it from any enclosing cargo workspace when it's built inside one.
    doc["workspace"] = Item::Table(Table::new());

    // Build the emitted crate at opt-level 2 without unwinding: both
    // measured runtime-neutral on the generated dataflow code while
    // cutting `cargo build --release` about 3x on large programs.
    // Incremental compilation pays off only when the build directory
    // survives to the next compile and costs cold-build time otherwise,
    // so it is emitted only for kept (user-named) directories.
    {
        let mut profile = Table::new();
        profile.set_implicit(true);
        doc["profile"] = Item::Table(profile);
        doc["profile"]["release"] = Item::Table(Table::new());
        let release = doc["profile"]["release"].as_table_mut().unwrap();
        release["opt-level"] = value(2);
        release["panic"] = "abort".into();
        if keep_build_dir {
            release["incremental"] = value(true);
        }
    }

    doc["dependencies"] = Item::Table(Table::new());
    {
        let deps = doc["dependencies"].as_table_mut().unwrap();
        deps["timely"] = "0.31".into();
        deps["differential-dataflow"] = "0.25".into();
        deps["mimalloc"] = "0.1".into();
        // The release PR will publish the next compatible 0.3.x runtime before
        // the compiler reaches `main`. Development builds patch this dependency
        // to the workspace runtime below, so the feature PR does not name an
        // unpublished exact version.
        deps["flowlog-runtime"] = "0.3".into();

        if features.string_intern() {
            deps["lasso"] = value(inline_versioned_dep(
                "0.7",
                &["multi-threaded", "serialize"],
            ));
            // Fast, non-cryptographic hasher for the interner (keys are
            // program-controlled, so SipHash's HashDoS resistance is wasted).
            deps["rustc-hash"] = "2.0".into();
        }
        if features.ordered_float() {
            deps["ordered-float"] = value(inline_versioned_dep("5.0", &["serde"]));
        }
        if features.parallel_output() {
            // The parallel `.output` file drain formats worker buffers with rayon.
            deps["rayon"] = "1.0".into();
        }
        if features.itoa() {
            // Integer formatting on the parallel file-output path; emitted
            // fully qualified (`::itoa::Buffer`), so no `use` import exists
            // to trip `-Dwarnings`.
            deps["itoa"] = "1.0".into();
        }
        if features.string_intern() {
            deps["serde"] = value(inline_versioned_dep("1.0", &["derive"]));
        }
        match config.mode() {
            ExecutionMode::Inc => deps["rustyline"] = "18".into(),
            ExecutionMode::Batch => {}
        }
    }

    // `FLOWLOG_RUNTIME_PATH` redirects the runtime dependency to a local
    // checkout via `[patch.crates-io]`. The test harness sets it so generated
    // crates build against the workspace runtime while compiler/runtime changes
    // are still on `main-next` and not yet published.
    if let Ok(path) = env::var("FLOWLOG_RUNTIME_PATH") {
        let mut patch = InlineTable::new();
        patch.insert("path", path.into());
        doc["patch"] = Item::Table(Table::new());
        doc["patch"]["crates-io"] = Item::Table(Table::new());
        doc["patch"]["crates-io"]["flowlog-runtime"] = value(patch);
    }

    let mut rendered = doc.to_string();
    if !rendered.ends_with('\n') {
        rendered.push('\n');
    }
    rendered
}

/// Render `.cargo/config.toml` with `-Dwarnings` so any unused imports or
/// dead code in the generated crate surface as errors instead of silent
/// warnings — a forcing function to keep the generator honest.
pub(crate) fn render_cargo_config() -> String {
    let mut doc = DocumentMut::new();
    let mut flags = Array::new();
    flags.push("-Dwarnings");
    doc["build"]["rustflags"] = value(flags);
    doc.to_string()
}

// =========================================================================
// TOML helpers
// =========================================================================

fn inline_versioned_dep(version: &str, features: &[&str]) -> InlineTable {
    let mut tbl = InlineTable::new();
    tbl.insert("version", version.into());
    let arr: Array = features.iter().copied().collect();
    tbl.insert("features", Value::Array(arr));
    tbl
}

// =========================================================================
// Filesystem helpers
// =========================================================================

fn ensure_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)
}

/// Write a UTF-8 text file, creating parent directories as needed.
/// Skips the write when the file already holds `contents`: preserving the
/// mtime lets cargo fingerprint an unchanged generated source as fresh,
/// so recompiling an unmodified program is a no-op.
fn write_file(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
    }
    if fs::read_to_string(path).is_ok_and(|current| current == contents) {
        return Ok(());
    }
    fs::write(path, contents)
}

// =========================================================================
// Embedded templates
// =========================================================================

const CMD_RS_TMPL: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/cmd_rs.tpl"));
const PROMPT_RS_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/prompt_rs.tpl"
));

#[cfg(test)]
mod tests {
    use super::*;

    /// `incremental = true` costs cold-build time and pays off only when
    /// the build directory survives to a later compile, so it must ride
    /// with kept directories and never with scratch ones.
    #[test]
    fn incremental_is_emitted_only_for_kept_build_dirs() {
        let config = Config::default();
        let features = Features::default();
        let kept = render_cargo_toml("bin", &config, &features, true);
        let scratch = render_cargo_toml("bin", &config, &features, false);
        assert!(kept.contains("incremental = true"));
        assert!(!scratch.contains("incremental"));
    }

    /// An unchanged file must keep its mtime so cargo fingerprints it as
    /// fresh; a changed one must be rewritten. Exercised directly because
    /// the mtime effect is unobservable through the public API without
    /// running cargo.
    #[test]
    fn write_file_rewrites_only_on_content_change() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("main.rs");
        write_file(&path, "fn main() {}").expect("initial write");

        let old = std::time::SystemTime::UNIX_EPOCH + std::time::Duration::from_secs(100);
        let file = fs::File::options()
            .write(true)
            .open(&path)
            .expect("open for mtime");
        file.set_modified(old).expect("set mtime");
        drop(file);

        write_file(&path, "fn main() {}").expect("no-op rewrite");
        let unchanged = fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime");
        assert_eq!(
            unchanged, old,
            "content-equal write must not touch the file"
        );

        write_file(&path, "fn main() { run() }").expect("changed rewrite");
        let changed = fs::metadata(&path)
            .expect("metadata")
            .modified()
            .expect("mtime");
        assert_ne!(changed, old, "changed content must be written out");
        let body = fs::read_to_string(&path).expect("read back");
        assert_eq!(body, "fn main() { run() }");
    }
}
