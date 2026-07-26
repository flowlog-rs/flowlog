//! Semiring module rendering for aggregation operators.
//!
//! Renders the semiring source files (min/max/sum/avg for int/float types)
//! as `(relative_path, content)` pairs, ready for the downstream compiler
//! to write into the generated project.

use flowlog_parser::AggregationOperator;

use crate::codegen::CodeGen;

/// The four generated semirings; `Count` aggregations share `Sum`'s.
///
/// Semiring naming is a codegen concern: the parser's
/// [`AggregationOperator`] carries no knowledge of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Semiring {
    Min,
    Max,
    Sum,
    Avg,
}

impl Semiring {
    /// The semiring implementing an aggregation operator.
    pub(crate) fn of(op: AggregationOperator) -> Self {
        match op {
            AggregationOperator::Min => Self::Min,
            AggregationOperator::Max => Self::Max,
            AggregationOperator::Sum | AggregationOperator::Count => Self::Sum,
            AggregationOperator::Avg => Self::Avg,
        }
    }

    /// Type-name prefix of the generated semiring types (`Sum` in `SumI64`).
    #[must_use]
    pub fn name(self) -> &'static str {
        match self {
            Self::Min => "Min",
            Self::Max => "Max",
            Self::Sum => "Sum",
            Self::Avg => "Avg",
        }
    }

    /// Stem of the generated module files (`sum` in `sum_int.rs`).
    #[must_use]
    pub fn module_stem(self) -> String {
        self.name().to_lowercase()
    }
}

/// Embed a `templates/semiring/<name>.tpl` file at this crate's compile time.
macro_rules! tpl {
    ($name:literal) => {
        include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/templates/semiring/",
            $name,
            ".tpl"
        ))
    };
}

const MIN_INT_TMPL: &str = tpl!("min_int");
const MIN_FLOAT_TMPL: &str = tpl!("min_float");
const MAX_INT_TMPL: &str = tpl!("max_int");
const MAX_FLOAT_TMPL: &str = tpl!("max_float");
const SUM_INT_TMPL: &str = tpl!("sum_int");
const SUM_FLOAT_TMPL: &str = tpl!("sum_float");
const AVG_INT_TMPL: &str = tpl!("avg_int");
const AVG_FLOAT_TMPL: &str = tpl!("avg_float");

/// Int/uint type table: `(suffix, rust_type)`. Order must match
/// `AggSemiringNeeds::int_needs`.
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

/// Float type table: `(suffix, inner_type)`. Order must match
/// `AggSemiringNeeds::float_needs`.
const FLOAT_TYPES: [(&str, &str); 2] = [("F32", "f32"), ("F64", "f64")];

/// Translate an integer bound keyword (`MAX` / `MIN`) into the
/// corresponding float bound keyword used by `f32` / `f64`.
fn float_bound_kw(int_bound: &str) -> &str {
    match int_bound {
        "MAX" => "INFINITY",
        "MIN" => "NEG_INFINITY",
        other => other,
    }
}

impl CodeGen {
    /// Render semiring modules as `(relative_path, content)` pairs.
    ///
    /// Paths are relative to `src/` (e.g. `"semiring/min_int.rs"`,
    /// `"semiring/mod.rs"`).  Returns an empty vec when no semiring
    /// modules are needed.
    pub(crate) fn render_semiring_modules(&self) -> Vec<(String, String)> {
        if !self.features.agg_semiring() {
            return Vec::new();
        }

        let semirings = self.features.agg_semirings();

        let mut files: Vec<(String, String)> = Vec::new();
        let mut modules: Vec<String> = Vec::new();

        use Semiring::*;
        for (int_tmpl, float_tmpl, semiring, mac, bound_kw) in [
            (MIN_INT_TMPL, MIN_FLOAT_TMPL, Min, "define_min", Some("MAX")),
            (MAX_INT_TMPL, MAX_FLOAT_TMPL, Max, "define_max", Some("MIN")),
            (SUM_INT_TMPL, SUM_FLOAT_TMPL, Sum, "define_sum", None),
            (AVG_INT_TMPL, AVG_FLOAT_TMPL, Avg, "define_avg", None),
        ] {
            let pfx = semiring.name();
            let kind_str = semiring.module_stem();

            let int_needs = semirings.int_needs(semiring);
            let float_needs = semirings.float_needs(semiring);

            // Int/uint file
            if int_needs.iter().any(|&n| n) {
                let mut rendered = int_tmpl.to_string();
                for ((suffix, ty), _) in INT_TYPES.iter().zip(int_needs).filter(|(_, n)| *n) {
                    let line = match bound_kw {
                        Some(bk) => format!("\n{mac}!({pfx}{suffix}, {ty}, {ty}::{bk});\n"),
                        None => format!("\n{mac}!({pfx}{suffix}, {ty});\n"),
                    };
                    rendered.push_str(&line);
                }
                files.push((
                    format!("semiring/{kind_str}_int.rs"),
                    rendered.trim_start().to_string(),
                ));
                modules.push(format!("{kind_str}_int"));
            }

            // Float file
            if float_needs.iter().any(|&n| n) {
                let mut rendered = float_tmpl.to_string();
                for ((suffix, inner), _) in FLOAT_TYPES.iter().zip(float_needs).filter(|(_, n)| *n)
                {
                    let line = match bound_kw {
                        Some(bk) => {
                            let kw = float_bound_kw(bk);
                            format!(
                                "\n{mac}!({pfx}{suffix}, OrderedFloat<{inner}>, OrderedFloat({inner}::{kw}));\n"
                            )
                        }
                        None => format!("\n{mac}!({pfx}{suffix}, {inner});\n"),
                    };
                    rendered.push_str(&line);
                }
                files.push((
                    format!("semiring/{kind_str}_float.rs"),
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

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    /// One row per operator: the semiring that implements it (`Count`
    /// shares `Sum`'s) and the generated names.
    #[rstest]
    #[case(AggregationOperator::Min, Semiring::Min, "Min", "min")]
    #[case(AggregationOperator::Max, Semiring::Max, "Max", "max")]
    #[case(AggregationOperator::Count, Semiring::Sum, "Sum", "sum")]
    #[case(AggregationOperator::Sum, Semiring::Sum, "Sum", "sum")]
    #[case(AggregationOperator::Avg, Semiring::Avg, "Avg", "avg")]
    fn semiring_of_each_operator(
        #[case] op: AggregationOperator,
        #[case] semiring: Semiring,
        #[case] name: &str,
        #[case] stem: &str,
    ) {
        assert_eq!(Semiring::of(op), semiring);
        assert_eq!(semiring.name(), name);
        assert_eq!(semiring.module_stem(), stem);
    }
}
