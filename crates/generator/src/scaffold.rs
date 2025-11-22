use std::io;
use std::path::Path;

use crate::fs_utils::{ensure_dir, write_file};
use toml_edit::DocumentMut;

// =========================================================================
// Project File Generation
// =========================================================================

/// Render a basic Cargo.toml.
pub fn render_cargo_toml(package_name: &str) -> String {
    let mut doc = DocumentMut::new();
    doc["package"]["name"] = package_name.into();
    doc["package"]["version"] = "0.1.0".into();
    doc["package"]["edition"] = "2024".into();

    // dependencies
    doc["dependencies"]["timely"] = "0.25".into();
    doc["dependencies"]["differential-dataflow"] = "0.18".into();

    doc.to_string()
}

/// Write Cargo.toml into the given project directory.
pub fn write_cargo_toml(out_dir: &Path, cargo_toml: &str) -> io::Result<()> {
    let path = out_dir.join("Cargo.toml");
    write_file(&path, cargo_toml.trim_start())
}

/// Write src/main.rs into the given project directory.
pub fn write_src_main(out_dir: &Path, main_rs: &str) -> io::Result<()> {
    let src_dir = out_dir.join("src");
    ensure_dir(&src_dir)?;
    let path = src_dir.join("main.rs");
    write_file(&path, main_rs)
}

/// Create the project layout and write Cargo.toml + src/main.rs.
pub fn write_project(out_dir: &Path, package_name: &str, main_rs: &str) -> io::Result<()> {
    ensure_dir(&out_dir.join("src"))?;
    let cargo = render_cargo_toml(package_name);
    write_cargo_toml(out_dir, &cargo)?;
    write_src_main(out_dir, main_rs)?;
    Ok(())
}
