use std::io;
use toml_edit::DocumentMut;

use super::Compiler;
use crate::fs_utils::{ensure_dir, write_file};

// =========================================================================
// Project File Generation
// =========================================================================
impl Compiler {
    /// Render a basic Cargo.toml.
    pub fn render_cargo_toml(&self) -> String {
        let mut doc = DocumentMut::new();

        // Package info
        doc["package"]["name"] = self.args.executable_name().into();
        doc["package"]["version"] = "0.1.0".into();
        doc["package"]["edition"] = "2024".into();

        // Make generated crate standalone even inside another workspace
        doc["workspace"] = toml_edit::table();

        // dependencies
        doc["dependencies"]["timely"] = "0.25".into();
        doc["dependencies"]["differential-dataflow"] = "0.18".into();

        doc.to_string()
    }

    /// Write Cargo.toml into the given project directory.
    pub fn write_cargo_toml(&self, cargo_toml: &str) -> io::Result<()> {
        let path = self.args.executable_path().join("Cargo.toml");
        write_file(&path, cargo_toml.trim_start())
    }

    /// Write src/main.rs into the given project directory.
    pub fn write_src_main(&self, main_rs: &str) -> io::Result<()> {
        let src_dir = self.args.executable_path().join("src");
        ensure_dir(&src_dir)?;
        let path = src_dir.join("main.rs");
        write_file(&path, main_rs)
    }

    /// Create the project layout and write Cargo.toml + src/main.rs.
    pub fn write_project(&self, main_rs: &str) -> io::Result<()> {
        ensure_dir(&self.args.executable_path().join("src"))?;
        let cargo = self.render_cargo_toml();
        self.write_cargo_toml(&cargo)?;
        self.write_src_main(main_rs)?;
        Ok(())
    }
}
