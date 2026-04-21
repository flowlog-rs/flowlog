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
//!     ├── udf.rs                  # optional, copied from `Config::udf_file`
//!     └── semiring/…              # optional, one file per semiring variant
//! ```
//!
//! `write_project` lays out these files from already-rendered strings;
//! `render_cargo_toml` / `render_cargo_config` produce the metadata.

use std::fs;
use std::io;
use std::path::Path;

use toml_edit::{value, Array, DocumentMut, InlineTable, Item, Value};

use flowlog_build::common::Config;
use flowlog_build::{CodeParts, Features};
use flowlog_build::profiler::{with_profiler_ref, Profiler};

use crate::Compiler;

// =========================================================================
// Project layout
// =========================================================================

impl Compiler {
    /// Materialize the scaffolded crate under [`Config::build_dir`].
    ///
    /// Arguments are pre-rendered file contents; this function only decides
    /// _where_ they go and creates intermediate directories. Optional files
    /// (incremental-mode shell, semiring modules, UDF stub, profiler log)
    /// are written only when the program needs them.
    pub(crate) fn write_project(
        &self,
        parts: &CodeParts,
        main_rs: &str,
        relation_rs: &str,
        cargo_toml: &str,
        cargo_config: &str,
        profiler: &Option<Profiler>,
    ) -> io::Result<()> {
        let config = &self.config;
        let root = config.build_dir();
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
        if config.is_incremental() {
            write_file(&src_dir.join("cmd.rs"), CMD_RS_TMPL.trim_start())?;
            write_file(&src_dir.join("prompt.rs"), PROMPT_RS_TMPL.trim_start())?;
        }

        // Aggregation-specific semiring modules (paths are relative to src/).
        if !parts.semiring_modules.is_empty() {
            for (rel_path, content) in &parts.semiring_modules {
                write_file(&src_dir.join(rel_path), content)?;
            }
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

        // Profiler log sibling-directory to the executable, written only when
        // `--profile` was requested.
        with_profiler_ref(profiler, |profiler| {
            let log_dir = config.executable_path().with_file_name("log");
            ensure_dir(&log_dir)?;
            profiler.write_json(log_dir.join("ops.json"))
        })?;

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
pub(crate) fn render_cargo_toml(config: &Config, features: &Features) -> String {
    let mut doc = DocumentMut::new();

    doc["package"] = Item::Table(toml_edit::Table::new());
    {
        let pkg = doc["package"].as_table_mut().unwrap();
        pkg["name"] = config.crate_name().into();
        pkg["version"] = "0.1.0".into();
        pkg["edition"] = "2024".into();
    }

    // The generated crate is standalone; the empty `[workspace]` detaches
    // it from any enclosing cargo workspace when it's built inside one.
    doc["workspace"] = Item::Table(toml_edit::Table::new());

    doc["dependencies"] = Item::Table(toml_edit::Table::new());
    {
        let deps = doc["dependencies"].as_table_mut().unwrap();
        deps["timely"] = "0.28".into();
        deps["differential-dataflow"] = "0.22".into();
        deps["mimalloc"] = "0.1".into();
        deps["flowlog-runtime"] = "0.1".into();

        if features.string_intern() {
            deps["lasso"] = value(inline_versioned_dep(
                "0.7",
                &["multi-threaded", "serialize"],
            ));
        }
        if features.ordered_float() {
            deps["ordered-float"] = value(inline_versioned_dep("5", &["serde"]));
        }
        if features.agg_semiring() || features.string_intern() {
            deps["serde"] = value(inline_versioned_dep("1", &["derive"]));
        }
        if config.is_incremental() {
            deps["rustyline"] = "17".into();
        }
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
    let mut arr = Array::new();
    for f in features {
        arr.push(*f);
    }
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
fn write_file(path: &Path, contents: &str) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)?;
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
