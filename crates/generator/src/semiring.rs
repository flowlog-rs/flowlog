//! Semiring module rendering for aggregation operators.
//!
//! Renders the semiring source files (min/max/sum/avg for int/float types)
//! as `(relative_path, content)` pairs, ready for the downstream compiler
//! to write into the generated project.

use parser::AggregationOperator;

use crate::Generator;

/// Embedded semiring templates (evaluated at *compile time* of the generator crate).
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

impl Generator {
    /// Render semiring modules as `(relative_path, content)` pairs.
    ///
    /// Paths are relative to `src/` (e.g. `"semiring/min_int.rs"`,
    /// `"semiring/mod.rs"`).  Returns an empty vec when no semiring
    /// modules are needed.
    pub(crate) fn render_semiring_modules(&self) -> Vec<(String, String)> {
        if !self.features.agg_semiring() {
            return Vec::new();
        }

        let s = self.features.agg_semirings();

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

        let mut files: Vec<(String, String)> = Vec::new();
        let mut modules: Vec<String> = Vec::new();

        use AggregationOperator::*;
        for (int_tmpl, float_tmpl, op, mac, bound_kw) in [
            (MIN_INT_TMPL, MIN_FLOAT_TMPL, Min, "define_min", Some("MAX")),
            (MAX_INT_TMPL, MAX_FLOAT_TMPL, Max, "define_max", Some("MIN")),
            (SUM_INT_TMPL, SUM_FLOAT_TMPL, Sum, "define_sum", None),
            (AVG_INT_TMPL, AVG_FLOAT_TMPL, Avg, "define_avg", None),
        ] {
            let kind_str = op.semiring_mod();
            let pfx = op.semiring_prefix();

            let int_needs = s.int_needs(op);
            let float_needs = s.float_needs(op);

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
                files.push((
                    format!("semiring/{file_name}"),
                    rendered.trim_start().to_string(),
                ));
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
                files.push((
                    format!("semiring/{file_name}"),
                    rendered.trim_start().to_string(),
                ));
                modules.push(format!("{kind_str}_float"));
            }
        }

        // mod.rs declaring all generated submodules.
        let mut mod_rs = String::new();
        for m in &modules {
            mod_rs.push_str(&format!("pub mod {m};\n"));
        }
        files.push(("semiring/mod.rs".to_string(), mod_rs));

        files
    }
}
