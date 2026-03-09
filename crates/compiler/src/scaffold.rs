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
use profiler::{with_profiler_ref, Profiler};

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
const MIN_INT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/min_int.tpl"
));
const MIN_FLOAT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/min_float.tpl"
));
const MAX_INT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/max_int.tpl"
));
const MAX_FLOAT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/max_float.tpl"
));
const SUM_INT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/sum_int.tpl"
));
const SUM_FLOAT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/sum_float.tpl"
));
const AVG_INT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/avg_int.tpl"
));
const AVG_FLOAT_TMPL: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/templates/semiring/avg_float.tpl"
));

// =========================================================================
// Project File Generation
// =========================================================================
impl Compiler {
    /// Create the project layout and write Cargo.toml + .cargo/config.toml + src/main.rs.
    ///
    /// If `config.is_incremental()` is enabled, also writes `src/cmd.rs` from a template.
    pub(crate) fn write_project(
        &self,
        main_rs: &str,
        profiler: &Option<Profiler>,
    ) -> io::Result<()> {
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

        // Semiring modules for aggregations that require them (e.g., via `needs_semiring()`)
        if self.imports.needs_semiring() {
            self.write_src_semiring()?;
        }

        // UDF module if a --udf-file was provided
        if self.config.udf_file().is_some() {
            self.write_src_udf()?;
        }

        // Profiler logs if enabled
        with_profiler_ref(profiler, |profiler| {
            self.write_profiler_logs(profiler, &self.config.executable_name())
        })?;

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
            deps["timely"] = "0.27".into();
            deps["differential-dataflow"] = "0.20".into();
            deps["mimalloc"] = "0.1".into();

            if self.imports.needs_memchr() {
                deps["memchr"] = "2".into();
            }

            if self.imports.needs_string_intern() {
                let mut lasso_tbl = toml_edit::InlineTable::new();
                lasso_tbl.insert("version", "0.7".into());
                let mut lasso_features = Array::new();
                lasso_features.push("multi-threaded");
                lasso_features.push("serialize");
                lasso_tbl.insert("features", toml_edit::Value::Array(lasso_features));
                deps["lasso"] = toml_edit::value(lasso_tbl);
            }

            if self.imports.needs_ordered_float() {
                let mut of_tbl = toml_edit::InlineTable::new();
                of_tbl.insert("version", "5".into());
                let mut of_features = Array::new();
                of_features.push("serde");
                of_tbl.insert("features", toml_edit::Value::Array(of_features));
                deps["ordered-float"] = toml_edit::value(of_tbl);
            }

            if self.imports.needs_semiring() || self.imports.needs_string_intern() {
                let mut serde_tbl = toml_edit::InlineTable::new();
                serde_tbl.insert("version", "1".into());
                let mut features = Array::new();
                features.push("derive");
                serde_tbl.insert("features", toml_edit::Value::Array(features));
                deps["serde"] = toml_edit::value(serde_tbl);
            }

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

    // -------------------------
    // src/udf.rs (user-supplied)
    // -------------------------

    /// Copy the user-supplied UDF file into `src/udf.rs` of the generated project.
    /// Runs `rustc` to verify the file compiles before copying.
    fn write_src_udf(&self) -> io::Result<()> {
        let udf_path = self.config.udf_file().expect("udf_file must be set");
        let content = std::fs::read_to_string(udf_path).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!("Failed to read UDF file '{udf_path}': {e}"),
            )
        })?;

        // Quick compile check before copying into the generated project.
        let output = std::process::Command::new("rustc")
            .args(["--edition", "2024", "--crate-type", "lib", udf_path])
            .arg("--out-dir")
            .arg(std::env::temp_dir())
            .output()
            .map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to run rustc on '{udf_path}': {e}"),
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("UDF file '{udf_path}' failed to compile:\n{stderr}"),
            ));
        }

        let src_dir = self.config.executable_path().join("src");
        ensure_dir(&src_dir)?;
        write_file(&src_dir.join("udf.rs"), content.trim_start())
    }

    /// Write semiring modules into `src/semiring/` of the generated project,
    /// splitting integer and float types into separate files.
    fn write_src_semiring(&self) -> io::Result<()> {
        let semiring_dir = self.config.executable_path().join("src").join("semiring");
        ensure_dir(&semiring_dir)?;

        let s = self.imports.semirings();

        // Int/uint type table: (suffix, rust_type)
        const INT_TYPES: [(&str, &str); 8] = [
            ("I8", "i8"),
            ("I16", "i16"),
            ("I32", "i32"),
            ("I64", "i64"),
            ("U8", "u8"),
            ("U16", "u16"),
            ("U32", "u32"),
            ("U64", "u64"),
        ];
        // Float type table: (suffix, inner_type)
        const FLOAT_TYPES: [(&str, &str); 2] = [("F32", "f32"), ("F64", "f64")];

        let mut modules: Vec<String> = Vec::new();

        // (int_tmpl, float_tmpl, kind, macro_name, type_prefix, bound_keyword,
        //  int_needs[8], float_needs[2])
        for (int_tmpl, float_tmpl, kind, mac, pfx, bound_kw, int_needs, float_needs) in [
            (
                MIN_INT_TMPL,
                MIN_FLOAT_TMPL,
                "min",
                "define_min",
                "Min",
                Some("MAX"),
                [
                    s.min_i8, s.min_i16, s.min_i32, s.min_i64, s.min_u8, s.min_u16, s.min_u32,
                    s.min_u64,
                ],
                [s.min_f32, s.min_f64],
            ),
            (
                MAX_INT_TMPL,
                MAX_FLOAT_TMPL,
                "max",
                "define_max",
                "Max",
                Some("MIN"),
                [
                    s.max_i8, s.max_i16, s.max_i32, s.max_i64, s.max_u8, s.max_u16, s.max_u32,
                    s.max_u64,
                ],
                [s.max_f32, s.max_f64],
            ),
            (
                SUM_INT_TMPL,
                SUM_FLOAT_TMPL,
                "sum",
                "define_sum",
                "Sum",
                None,
                [
                    s.sum_i8, s.sum_i16, s.sum_i32, s.sum_i64, s.sum_u8, s.sum_u16, s.sum_u32,
                    s.sum_u64,
                ],
                [s.sum_f32, s.sum_f64],
            ),
            (
                AVG_INT_TMPL,
                AVG_FLOAT_TMPL,
                "avg",
                "define_avg",
                "Avg",
                None,
                [
                    s.avg_i8, s.avg_i16, s.avg_i32, s.avg_i64, s.avg_u8, s.avg_u16, s.avg_u32,
                    s.avg_u64,
                ],
                [s.avg_f32, s.avg_f64],
            ),
        ] {
            // Int/uint file
            if int_needs.iter().any(|n| *n) {
                let mut rendered = int_tmpl.replace("\r\n", "\n");
                for (i, (suffix, ty)) in INT_TYPES.iter().enumerate() {
                    if int_needs[i] {
                        match bound_kw {
                            Some(bk) => rendered
                                .push_str(&format!("\n{mac}!({pfx}{suffix}, {ty}, {ty}::{bk});\n")),
                            None => rendered.push_str(&format!("\n{mac}!({pfx}{suffix}, {ty});\n")),
                        }
                    }
                }
                let file_name = format!("{kind}_int.rs");
                write_file(&semiring_dir.join(&file_name), rendered.trim_start())?;
                modules.push(format!("{kind}_int"));
            }

            // Float file
            if float_needs.iter().any(|n| *n) {
                let mut rendered = float_tmpl.replace("\r\n", "\n");
                for (i, (suffix, inner)) in FLOAT_TYPES.iter().enumerate() {
                    if float_needs[i] {
                        match bound_kw {
                            Some(bk) => rendered.push_str(&format!(
                                "\n{mac}!({pfx}{suffix}, OrderedFloat<{inner}>, OrderedFloat({inner}::{bk}));\n"
                            )),
                            None => rendered.push_str(&format!(
                                "\n{mac}!({pfx}{suffix}, {inner});\n"
                            )),
                        }
                    }
                }
                let file_name = format!("{kind}_float.rs");
                write_file(&semiring_dir.join(&file_name), rendered.trim_start())?;
                modules.push(format!("{kind}_float"));
            }
        }

        // Write mod.rs declaring all generated submodules.
        let mut mod_rs = String::new();
        for m in &modules {
            mod_rs.push_str(&format!("pub mod {m};\n"));
        }
        write_file(&semiring_dir.join("mod.rs"), &mod_rs)?;

        Ok(())
    }

    // -------------------------
    // log/log.tsv & log/ops.json
    // -------------------------

    /// Write profiler output files into the generated project directory.
    pub(crate) fn write_profiler_logs(
        &self,
        profiler: &Profiler,
        program_name: &str,
    ) -> io::Result<()> {
        let log_dir = self.config.executable_path().join("log");
        ensure_dir(&log_dir)?;

        let ops_path = log_dir.join(format!("{}_ops.json", program_name));
        profiler.write_json(&ops_path)
    }
}
