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
use crate::import::SemiringKind;
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

impl Compiler {
    /// Create the project layout and write Cargo.toml + .cargo/config.toml + src/main.rs.
    ///
    /// If `config.is_incremental()` is enabled, also writes `src/cmd.rs` from a template.
    pub(crate) fn write_project(
        &self,
        main_rs: &str,
        profiler: &Option<Profiler>,
    ) -> io::Result<()> {
        let root = self.config.build_dir();
        let src_dir = root.join("src");
        ensure_dir(&src_dir)?;

        // Project metadata + dependencies
        let cargo_toml = self.render_cargo_toml();
        write_file(&root.join("Cargo.toml"), cargo_toml.trim_start())?;

        // Build flags (e.g., -Dwarnings)
        let cargo_cfg = self.render_cargo_config();
        let cargo_dir = root.join(".cargo");
        ensure_dir(&cargo_dir)?;
        write_file(&cargo_dir.join("config.toml"), cargo_cfg.trim_start())?;

        // Compiler-generated entrypoint
        write_file(&src_dir.join("main.rs"), main_rs)?;

        // Incremental mode: interactive command parser + prompt + relation ops
        if self.config.is_incremental() {
            write_file(&src_dir.join("cmd.rs"), CMD_RS_TMPL.trim_start())?;
            write_file(&src_dir.join("prompt.rs"), PROMPT_RS_TMPL.trim_start())?;

            let relops = self.render_relops(self.program.edbs());
            write_file(&src_dir.join("relops.rs"), relops.trim_start())?;
        }

        // Semiring modules for aggregations
        if self.imports.needs_semiring() {
            self.write_src_semiring(&src_dir)?;
        }

        // UDF module if a --udf-file was provided
        if let Some(udf_path) = self.config.udf_file() {
            let content = std::fs::read_to_string(udf_path).map_err(|e| {
                io::Error::new(
                    e.kind(),
                    format!("Failed to read UDF file '{udf_path}': {e}"),
                )
            })?;
            write_file(&src_dir.join("udf.rs"), content.trim_start())?;
        }

        // Profiler logs — write into a `log/` directory alongside the final
        // executable so they survive build-directory cleanup when --save-temps
        // is not set.
        with_profiler_ref(profiler, |profiler| {
            let exe = self.config.executable_path();
            let log_dir = exe.with_file_name("log");
            ensure_dir(&log_dir)?;
            let ops_path = log_dir.join("ops.json");
            profiler.write_json(&ops_path)
        })?;

        Ok(())
    }

    /// Render a minimal Cargo.toml for the generated crate.
    fn render_cargo_toml(&self) -> String {
        let mut doc = DocumentMut::new();

        // package
        doc["package"] = Item::Table(toml_edit::Table::new());
        {
            let pkg = doc["package"].as_table_mut().unwrap();
            pkg["name"] = self.config.crate_name().into();
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

    /// Render `.cargo/config.toml` with build rustflags.
    fn render_cargo_config(&self) -> String {
        let mut doc = DocumentMut::new();

        let mut flags = Array::new();
        flags.push("-Dwarnings");

        doc["build"]["rustflags"] = value(flags);
        doc.to_string()
    }

    /// Write semiring modules into `src/semiring/` of the generated project,
    /// splitting integer and float types into separate files.
    fn write_src_semiring(&self, src_dir: &std::path::Path) -> io::Result<()> {
        let semiring_dir = src_dir.join("semiring");
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

        // (int_tmpl, float_tmpl, kind, macro_name, bound_keyword)
        for (int_tmpl, float_tmpl, kind, mac, bound_kw) in [
            (
                MIN_INT_TMPL,
                MIN_FLOAT_TMPL,
                SemiringKind::Min,
                "define_min",
                Some("MAX"),
            ),
            (
                MAX_INT_TMPL,
                MAX_FLOAT_TMPL,
                SemiringKind::Max,
                "define_max",
                Some("MIN"),
            ),
            (
                SUM_INT_TMPL,
                SUM_FLOAT_TMPL,
                SemiringKind::Sum,
                "define_sum",
                None,
            ),
            (
                AVG_INT_TMPL,
                AVG_FLOAT_TMPL,
                SemiringKind::Avg,
                "define_avg",
                None,
            ),
        ] {
            let kind_str = kind.as_str();
            let pfx = kind.prefix();

            let int_needs = s.int_needs(kind);
            let float_needs = s.float_needs(kind);

            // Int/uint file
            if int_needs.iter().any(|n| *n) {
                let mut rendered = int_tmpl.to_string();
                for (i, (suffix, ty)) in INT_TYPES.iter().enumerate() {
                    if int_needs[i] {
                        match bound_kw {
                            Some(bk) => rendered
                                .push_str(&format!("\n{mac}!({pfx}{suffix}, {ty}, {ty}::{bk});\n")),
                            None => rendered.push_str(&format!("\n{mac}!({pfx}{suffix}, {ty});\n")),
                        }
                    }
                }
                let file_name = format!("{kind_str}_int.rs");
                write_file(&semiring_dir.join(&file_name), rendered.trim_start())?;
                modules.push(format!("{kind_str}_int"));
            }

            // Float file
            if float_needs.iter().any(|n| *n) {
                let mut rendered = float_tmpl.to_string();
                for (i, (suffix, inner)) in FLOAT_TYPES.iter().enumerate() {
                    if float_needs[i] {
                        match bound_kw {
                            Some(bk) => {
                                let float_bound_kw = match bk {
                                    "MAX" => "INFINITY",
                                    "MIN" => "NEG_INFINITY",
                                    other => other,
                                };
                                rendered.push_str(&format!(
                                    "\n{mac}!({pfx}{suffix}, OrderedFloat<{inner}>, OrderedFloat({inner}::{float_bound_kw}));\n"
                                ));
                            }
                            None => {
                                rendered.push_str(&format!("\n{mac}!({pfx}{suffix}, {inner});\n"))
                            }
                        }
                    }
                }
                let file_name = format!("{kind_str}_float.rs");
                write_file(&semiring_dir.join(&file_name), rendered.trim_start())?;
                modules.push(format!("{kind_str}_float"));
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
}
