//! Project metadata rendering for the generated Rust crate.
//!
//! Renders `Cargo.toml` and `.cargo/config.toml` content as strings.
//! The actual file writing is the responsibility of the downstream
//! compiler crate.

use toml_edit::{value, Array, DocumentMut, Item};

use crate::Generator;

impl Generator {
    /// Render a minimal `Cargo.toml` for the generated crate.
    pub(crate) fn render_cargo_toml(&self) -> String {
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
            deps["timely"] = "0.28".into();
            deps["differential-dataflow"] = "0.21".into();
            deps["mimalloc"] = "0.1".into();

            if self.features.string_intern() {
                let mut lasso_tbl = toml_edit::InlineTable::new();
                lasso_tbl.insert("version", "0.7".into());
                let mut lasso_features = Array::new();
                lasso_features.push("multi-threaded");
                lasso_features.push("serialize");
                lasso_tbl.insert("features", toml_edit::Value::Array(lasso_features));
                deps["lasso"] = toml_edit::value(lasso_tbl);
            }

            if self.features.ordered_float() {
                let mut of_tbl = toml_edit::InlineTable::new();
                of_tbl.insert("version", "5".into());
                let mut of_features = Array::new();
                of_features.push("serde");
                of_tbl.insert("features", toml_edit::Value::Array(of_features));
                deps["ordered-float"] = toml_edit::value(of_tbl);
            }

            if self.features.agg_semiring() || self.features.string_intern() {
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
    pub(crate) fn render_cargo_config(&self) -> String {
        let mut doc = DocumentMut::new();

        let mut flags = Array::new();
        flags.push("-Dwarnings");

        doc["build"]["rustflags"] = value(flags);
        doc.to_string()
    }
}
