//! Source generation, Cargo invocation, and executable installation.
//! Cargo messages provide artifact paths; project cleanup handles scratch
//! directories separately from reusable Cargo artifacts.

use std::env;
use std::fs;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use std::process;

use flowlog_common::ExecutionMode;
use flowlog_planner::planner::ProgramPlanner;
use flowlog_profiler::PlanGraph;
use quote::quote;
use tracing::info;

use crate::Compiler;
use crate::dispatch;
use crate::imports;
use crate::scaffold;

impl Compiler {
    /// Writes the generated Cargo project without invoking Cargo.
    pub(crate) fn emit_sources(
        &mut self,
        program_planner: &ProgramPlanner,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(), flowlog_common::BoxError> {
        let parts = self.codegen.generate(program_planner, plan_graph)?;
        let features = self.codegen.features();

        let relation_body = flowlog_build::gen_relations(&self.program, features.string_intern())?;
        let dispatch = match self.config.mode() {
            ExecutionMode::Batch => quote! {},
            ExecutionMode::Inc => dispatch::gen_dispatch(&self.program),
        };
        let relation_rs = flowlog_common::pretty_print(quote! {
            #![allow(non_camel_case_types)]
            #relation_body
            #dispatch
        });

        let bin_imports = imports::gen_imports(&self.config, features);
        let main_rs = self.assemble(&parts, &bin_imports)?;

        let cargo_toml = scaffold::render_cargo_toml(
            &self.options.crate_name(),
            &self.config,
            features,
            self.options.keeps_build_dir(),
        );
        let cargo_config = scaffold::render_cargo_config();

        self.write_project(&main_rs, &relation_rs, &cargo_toml, &cargo_config)
            .map_err(crate::CompilerError::from)?;
        Ok(())
    }

    /// Builds and installs the generated executable. Scratch projects are
    /// removed only after installation succeeds.
    pub(crate) fn build(&self) -> io::Result<()> {
        let build_dir = self.options.build_dir();
        let crate_name = self.options.crate_name();
        let executable_path = self.options.executable_path();

        // Cargo settings can change both the artifact directory and platform
        // layout. Reading its JSON costs a parser dependency but avoids
        // duplicating those rules when locating the executable.
        let messages = run_cargo(
            &build_dir,
            self.options.target_dir(),
            &[
                "build",
                "--release",
                "--message-format=json-render-diagnostics",
            ],
        )?;
        let built = find_executable(&messages, &crate_name).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cargo build in '{}' succeeded but reported no executable for '{crate_name}'",
                    build_dir.display()
                ),
            )
        })?;
        let dest = exe_with_platform_suffix(executable_path);
        install_binary(&built, &dest)?;
        info!("Executable written to '{}'", dest.display());

        self.cleanup_build_dir(&build_dir)?;

        Ok(())
    }

    /// Type-checks the generated project without producing an executable.
    pub(crate) fn check(&self) -> io::Result<()> {
        let build_dir = self.options.build_dir();
        run_cargo(&build_dir, self.options.target_dir(), &["check"])?;
        self.cleanup_build_dir(&build_dir)?;
        Ok(())
    }

    /// Removes the generated project unless
    /// [`crate::CompileOptions::keeps_build_dir`] holds.
    fn cleanup_build_dir(&self, build_dir: &Path) -> io::Result<()> {
        if !self.options.keeps_build_dir() {
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
}

/// Finds the named binary in Cargo artifact messages, including cached
/// builds. Ignores non-JSON stdout from procedural macros.
fn find_executable(messages: &[u8], crate_name: &str) -> Option<PathBuf> {
    for line in messages.split(|byte| *byte == b'\n') {
        let Ok(message) = serde_json::from_slice::<serde_json::Value>(line) else {
            continue;
        };
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == crate_name
            && message["target"]["kind"]
                .as_array()
                .is_some_and(|kinds| kinds.iter().any(|kind| kind == "bin"))
            && let Some(path) = message["executable"].as_str()
        {
            return Some(PathBuf::from(path));
        }
    }
    None
}

/// Runs Cargo in the generated project and returns its stdout on success.
/// Target paths follow [`crate::CompileOptions::target_dir`]. Cargo failures
/// include its stderr.
fn run_cargo(build_dir: &Path, target_dir: Option<&Path>, args: &[&str]) -> io::Result<Vec<u8>> {
    let mut command = process::Command::new("cargo");
    command.args(args).current_dir(build_dir);
    if let Some(dir) = target_dir {
        // Resolve before Cargo changes directory so -T follows other CLI paths.
        let absolute = std::path::absolute(dir).map_err(|error| {
            io::Error::new(
                error.kind(),
                format!(
                    "failed to resolve Cargo target directory '{}': {error}",
                    dir.display()
                ),
            )
        })?;
        command.arg("--target-dir").arg(absolute);
    }
    let output = command.output().map_err(|e| match e.kind() {
        io::ErrorKind::NotFound => io::Error::new(
            io::ErrorKind::NotFound,
            "cargo not found; install Rust via https://rustup.rs",
        ),
        kind => io::Error::new(kind, format!("failed to run cargo: {e}")),
    })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(io::Error::other(format!(
            "cargo {} failed (generated crate kept at '{}'):\n{stderr}",
            args.join(" "),
            build_dir.display()
        )));
    }
    Ok(output.stdout)
}

/// Copies a built binary into place and makes it executable on Unix.
fn install_binary(src: &Path, dest: &Path) -> io::Result<()> {
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(src, dest)?;

    // Copying preserves source permissions, which may lack executable bits
    // under a restrictive umask.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dest, fs::Permissions::from_mode(0o755))?;
    }

    Ok(())
}

/// Appends the host platform's executable suffix when it is absent.
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
