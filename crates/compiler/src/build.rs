//! Build the generated Rust crate into a standalone executable.
//!
//! Runs `cargo build --release` in the generated crate directory, copies
//! the final binary to the desired output location, and removes the
//! intermediate build directory (unless `--save-temps`).

use std::path::Path;
use std::{env, fs, io, process};

use tracing::info;

/// Build the generated crate, copy the binary out, and optionally clean up.
pub fn build_and_collect(
    build_dir: &Path,
    executable_path: &Path,
    save_temps: bool,
) -> io::Result<()> {
    let executable_name = executable_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("out");
    let output = process::Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(build_dir)
        .output()
        .map_err(|e| {
            if e.kind() == io::ErrorKind::NotFound {
                io::Error::new(
                    io::ErrorKind::NotFound,
                    "cargo not found — install Rust via https://rustup.rs",
                )
            } else {
                io::Error::new(e.kind(), format!("failed to run cargo build: {e}"))
            }
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::other(format!(
            "cargo build --release failed:\n{stderr}"
        )));
    }

    let binary_name = format!("{executable_name}{}", env::consts::EXE_SUFFIX);
    let built_binary = build_dir.join("target").join("release").join(&binary_name);

    let dest = executable_path.with_file_name(&binary_name);
    fs::copy(&built_binary, &dest)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&dest, fs::Permissions::from_mode(0o755))?;
    }

    info!("Executable written to '{}'", dest.display());

    if !save_temps {
        fs::remove_dir_all(build_dir).map_err(|e| {
            io::Error::new(
                e.kind(),
                format!(
                    "failed to clean up build directory '{}': {e}",
                    build_dir.display()
                ),
            )
        })?;
    }

    Ok(())
}
