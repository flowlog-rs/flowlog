//! Pipeline stages behind [`Compiler::compile`].
//!
//! The compile pipeline has two halves:
//!
//! 1. **`emit_sources`** — run the generator over the program and write a
//!    complete Cargo project (`main.rs`, `relation.rs`, `Cargo.toml`, etc.)
//!    under [`Config::build_dir`]. No external tools are invoked.
//! 2. **`build`** — shell out to `cargo build --release` in that directory,
//!    copy the resulting binary to [`Config::executable_path`], and remove
//!    the intermediate crate unless [`Config::save_temps`] is set.
//!
//! Both are `pub(crate)`; external callers use [`Compiler::compile`] which
//! runs them in sequence.

use std::path::{Path, PathBuf};
use std::{env, fs, io, process};

use flowlog_build::planner::StratumPlanner;
use flowlog_build::profiler::Profiler;
use quote::quote;
use tracing::info;

use crate::{Compiler, imports, relation, scaffold};

impl Compiler {
    /// Produce the scaffolded Rust crate in [`Config::build_dir`].
    ///
    /// Runs all code-generation passes (dataflow graph, relation handlers,
    /// output drain, imports) and writes the resulting source files to disk.
    /// Pure codegen — cargo is not invoked here.
    pub(crate) fn emit_sources(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> Result<(), flowlog_build::common::BoxError> {
        let parts = self.codegen.generate(strata, profiler)?;
        let features = self.codegen.features();

        // `src/relation.rs` — Relation trait + per-EDB `Rel{name}` input handlers.
        let relation_body =
            relation::gen_relation(&self.program, features, self.config.is_batch())?;
        let relation_extras = imports::gen_binary_relation_extras(&self.program, features);
        let relation_rs = flowlog_build::common::pretty_print(quote! {
            #![allow(non_camel_case_types)]
            #relation_body
            #relation_extras
        });

        // `src/main.rs` — dataflow scope, timely::execute, EDB registry, drain.
        let bin_imports = imports::gen_imports(&self.config, features);
        let worker_helpers = imports::gen_worker_helpers();
        let main_rs = self.assemble_main(&parts, &bin_imports, &worker_helpers)?;

        // Cargo project metadata.
        let cargo_toml = scaffold::render_cargo_toml(&self.config, features);
        let cargo_config = scaffold::render_cargo_config();

        self.write_project(&parts, &main_rs, &relation_rs, &cargo_toml, &cargo_config)
            .map_err(crate::CompilerError::from)?;
        Ok(())
    }

    /// Compile the emitted crate with `cargo build --release`, install the
    /// binary at [`Config::executable_path`], and (unless `--save-temps`)
    /// remove the intermediate crate directory.
    pub(crate) fn build(&self) -> io::Result<()> {
        let build_dir = self.config.build_dir();
        let crate_name = self.config.crate_name();
        let executable_path = self.config.executable_path();

        run_cargo_build(&build_dir)?;

        // Cargo produces the binary under the sanitized crate name; copy it
        // to the user's requested path (appending `.exe` on Windows if the
        // user omitted it).
        let built = build_dir
            .join("target")
            .join("release")
            .join(format!("{crate_name}{}", env::consts::EXE_SUFFIX));
        let dest = exe_with_platform_suffix(&executable_path);
        install_binary(&built, &dest)?;
        info!("Executable written to '{}'", dest.display());

        if !self.config.save_temps() {
            fs::remove_dir_all(&build_dir).map_err(|e| {
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
}

/// Invoke `cargo build --release` in `build_dir` and propagate any failure.
///
/// Surfaces a friendly "install Rust via rustup" hint if `cargo` isn't on
/// `PATH`, and otherwise forwards cargo's stderr so users can see the
/// underlying compiler error.
fn run_cargo_build(build_dir: &Path) -> io::Result<()> {
    let output = process::Command::new("cargo")
        .args(["build", "--release"])
        .current_dir(build_dir)
        .output()
        .map_err(|e| match e.kind() {
            io::ErrorKind::NotFound => io::Error::new(
                io::ErrorKind::NotFound,
                "cargo not found — install Rust via https://rustup.rs",
            ),
            kind => io::Error::new(kind, format!("failed to run cargo build: {e}")),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::other(format!(
            "cargo build --release failed:\n{stderr}"
        )));
    }
    Ok(())
}

/// Copy a built binary into place and make it executable on Unix.
fn install_binary(src: &Path, dest: &Path) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dest)?;

    // On Unix, `fs::copy` preserves the source's permission bits which may
    // not include the executable flag if the cargo target dir was created
    // with an unusual umask — set it explicitly.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dest, fs::Permissions::from_mode(0o755))?;
    }

    Ok(())
}

/// Ensure the destination path carries the platform-specific executable
/// extension (`.exe` on Windows). No-op on Unix or when the user already
/// provided the suffix.
fn exe_with_platform_suffix(path: &Path) -> PathBuf {
    let suffix = env::consts::EXE_SUFFIX;
    if suffix.is_empty() {
        return path.to_path_buf();
    }
    let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("out");
    if name.ends_with(suffix) {
        path.to_path_buf()
    } else {
        path.with_file_name(format!("{name}{suffix}"))
    }
}
