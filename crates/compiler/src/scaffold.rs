//! Project scaffolding for the FlowLog compiler.
//!
//! This module materializes a standalone Rust crate produced by the compiler.
//! It is responsible for writing:
//! - `Cargo.toml`
//! - `.cargo/config.toml` (rustflags, etc.)
//! - `src/main.rs` (compiler-generated)
//! - `src/cmd.rs` (optional: interactive txn command parser for incremental mode)

use std::io;

use toml_edit::{value, Array, DocumentMut, Item};

use super::Compiler;
use crate::fs_utils::{ensure_dir, write_file};

/// Embedded template for the incremental interactive command parser.
///
/// Notes:
/// - `include_str!` is evaluated at *compile time* of the compiler crate.
/// - The released binary does NOT need the template file at runtime.
const CMD_RS_TMPL: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/templates/cmd_rs.tpl"));
const PROMPT_RS_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/prompt_rs.tpl"
));

// =========================================================================
// Project File Generation
// =========================================================================
impl Compiler {
    /// Create the project layout and write Cargo.toml + .cargo/config.toml + src/main.rs.
    ///
    /// If `config.is_incremental()` is enabled, also writes `src/cmd.rs` from a template.
    pub(crate) fn write_project(&self, main_rs: &str) -> io::Result<()> {
        let root = self.config.executable_path();
        let src_dir = root.join("src");

        ensure_dir(&src_dir)?;

        // Project metadata + dependencies
        let cargo_toml = self.render_cargo_toml();
        self.write_cargo_toml(&cargo_toml)?;

        // Optional build flags (e.g., -Dwarnings)
        let cargo_cfg = self.render_cargo_config();
        self.write_cargo_config(&cargo_cfg)?;

        // Compiler-generated entrypoint
        self.write_src_main(main_rs)?;

        // Optional incremental interactive command parser
        if self.config.is_incremental() {
            self.write_src_cmd()?;
            self.write_src_prompt()?;
            self.write_src_relation()?;
        }

        Ok(())
    }

    // -------------------------
    // Cargo.toml
    // -------------------------

    /// Render a minimal Cargo.toml for the generated crate.
    fn render_cargo_toml(&self) -> String {
        let mut doc = DocumentMut::new();

        // package
        doc["package"] = Item::Table(toml_edit::Table::new());
        {
            let pkg = doc["package"].as_table_mut().unwrap();
            pkg["name"] = self.config.executable_name().into();
            pkg["version"] = "0.1.0".into();
            pkg["edition"] = "2024".into();
        }

        // workspace
        doc["workspace"] = Item::Table(toml_edit::Table::new());

        // dependencies
        doc["dependencies"] = Item::Table(toml_edit::Table::new());
        {
            let deps = doc["dependencies"].as_table_mut().unwrap();
            deps["timely"] = "0.25".into();
            deps["differential-dataflow"] = "0.18".into();

            if self.config.is_incremental() {
                deps["rustyline"] = "17".into();
            }
        }

        // Nice trailing newline.
        let mut s = doc.to_string();
        if !s.ends_with('\n') {
            s.push('\n');
        }
        s
    }

    /// Write Cargo.toml into the generated project directory.
    fn write_cargo_toml(&self, cargo_toml: &str) -> io::Result<()> {
        let path = self.config.executable_path().join("Cargo.toml");
        write_file(&path, cargo_toml.trim_start())
    }

    // -------------------------
    // src/main.rs
    // -------------------------

    /// Write src/main.rs into the generated project directory.
    fn write_src_main(&self, main_rs: &str) -> io::Result<()> {
        let src_dir = self.config.executable_path().join("src");
        ensure_dir(&src_dir)?;
        write_file(&src_dir.join("main.rs"), main_rs)
    }

    // -------------------------
    // .cargo/config.toml
    // -------------------------

    /// Render `.cargo/config.toml` with build rustflags.
    fn render_cargo_config(&self) -> String {
        let mut doc = DocumentMut::new();

        let mut flags = Array::new();
        flags.push("-Dwarnings");

        doc["build"]["rustflags"] = value(flags);
        doc.to_string()
    }

    /// Write `.cargo/config.toml` to the generated project directory.
    fn write_cargo_config(&self, cargo_config: &str) -> io::Result<()> {
        let cargo_dir = self.config.executable_path().join(".cargo");
        ensure_dir(&cargo_dir)?;
        write_file(&cargo_dir.join("config.toml"), cargo_config.trim_start())
    }

    // -------------------------
    // src/cmd.rs (template)
    // -------------------------

    /// Write `src/cmd.rs` (incremental-only) into the generated project directory.
    fn write_src_cmd(&self) -> io::Result<()> {
        let src_dir = self.config.executable_path().join("src");
        ensure_dir(&src_dir)?;

        // Optional: normalize CRLF to LF for stable diffs across platforms.
        let rendered = CMD_RS_TMPL.replace("\r\n", "\n");

        write_file(&src_dir.join("cmd.rs"), rendered.trim_start())
    }

    // -------------------------
    // src/relation.rs (generated)
    // -------------------------

    /// Write `src/relation.rs` (incremental-only) into the generated project directory.
    fn write_src_relation(&self) -> io::Result<()> {
        let src_dir = self.config.executable_path().join("src");
        ensure_dir(&src_dir)?;

        let edbs = self.program.edbs();

        let rendered = self.render_relops(edbs).replace("\r\n", "\n");

        write_file(&src_dir.join("relation.rs"), rendered.trim_start())
    }

    /// Write `src/prompt.rs` (incremental-only) into the generated project directory.
    fn write_src_prompt(&self) -> io::Result<()> {
        let src_dir = self.config.executable_path().join("src");
        ensure_dir(&src_dir)?;

        let rendered = PROMPT_RS_TMPL.replace("\r\n", "\n");
        write_file(&src_dir.join("prompt.rs"), rendered.trim_start())
    }
}
