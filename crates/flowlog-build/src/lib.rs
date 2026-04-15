//! Build-script integration for FlowLog library mode.
//!
//! Compiles a `.dl` Datalog program into a Rust module that your crate
//! `include!()`s from a `build.rs` script. Two entry points, mirroring
//! [`tonic-build`](https://docs.rs/tonic-build):
//!
//! - [`compile`] — single-file shortcut with defaults.
//! - [`configure`] — chained [`Builder`] for advanced options (multiple
//!   inputs, UDFs, string interning, optimizations).
//!
//! # Example: minimal
//!
//! ```no_run
//! // build.rs
//! fn main() -> std::io::Result<()> {
//!     flowlog_build::compile("policy.dl")
//! }
//! ```
//!
//! ```ignore
//! // src/lib.rs
//! pub mod policy { include!(concat!(env!("OUT_DIR"), "/policy.rs")); }
//!
//! use policy::DatalogBatchEngine;
//!
//! let mut engine = DatalogBatchEngine::new(4); // 4 timely workers
//! engine.insert_edge(vec![(1, 2), (2, 3)]); // rel::Edge = (i32, i32)
//! let results = engine.run();
//! for (src, dst) in &results.reach { println!("{src} -> {dst}"); }
//! ```
//!
//! # Example: multi-file with options
//!
//! ```no_run
//! // build.rs
//! fn main() -> std::io::Result<()> {
//!     flowlog_build::configure()
//!         .sip(true)
//!         .string_intern(true)
//!         .compile(&["policy.dl", "auth.dl"], &[] as &[&std::path::Path])
//! }
//! ```

mod assembly;
mod engine;
mod imports;
mod pipeline;
mod relation;
mod results;

// `codegen` is the shared codegen core — used by this crate's library mode
// and, via the re-exports below, by `flowlog-compiler`'s binary mode.
mod codegen;

pub use codegen::CodeGen;
pub use codegen::code_parts::CodeParts;
pub use codegen::features::Features;
pub use codegen::ty::data::data_type_tokens;
pub use codegen::idb_buffers::{field_accessor, gen_drain_block};

use std::io;
use std::path::{Path, PathBuf};

pub use common::ExecutionMode;

/// Return Cargo's `$OUT_DIR` — set automatically when a `build.rs` runs.
fn cargo_out_dir() -> io::Result<PathBuf> {
    std::env::var("OUT_DIR").map(PathBuf::from).map_err(|_| {
        io::Error::new(
            io::ErrorKind::NotFound,
            "OUT_DIR not set — run from a build.rs",
        )
    })
}

/// Compile a single FlowLog `.dl` program with default settings.
///
/// Equivalent to [`configure().compile(&[program_path], &[])`](Builder::compile).
/// The generated file is named after the input's file stem (e.g.
/// `policy.dl` → `$OUT_DIR/policy.rs`).
pub fn compile<P: AsRef<Path>>(program_path: P) -> io::Result<()> {
    configure().compile(&[program_path], &[] as &[&Path])
}

/// Start a [`Builder`] chain for advanced compilation options.
pub fn configure() -> Builder {
    Builder::new()
}

/// Builder for advanced compilation options. For default settings prefer
/// the free [`compile`] function.
pub struct Builder {
    /// Enable Sideways Information Passing optimization. See [`Self::sip`].
    pub(crate) sip: bool,
    /// Enable transparent string interning. See [`Self::string_intern`].
    pub(crate) string_intern: bool,
    /// Execution mode — defaults to [`ExecutionMode::DatalogBatch`]; set
    /// to `ExtendBatch` to accept `loop` / `fixpoint` blocks. See
    /// [`Self::extended`].
    pub(crate) mode: ExecutionMode,
    /// Extra search path for `.include` directives. Populated from
    /// [`Self::compile`]'s second argument.
    pub(crate) include_dirs: Vec<PathBuf>,
    /// Rust source file implementing the program's `.extern fn` UDFs.
    pub(crate) udf_file: Option<PathBuf>,
}

impl Default for Builder {
    fn default() -> Self {
        Self::new()
    }
}

impl Builder {
    /// Create a new builder with default settings. Prefer [`configure`]
    /// for the chained pattern.
    pub fn new() -> Self {
        Self {
            sip: false,
            string_intern: false,
            mode: ExecutionMode::DatalogBatch,
            include_dirs: Vec::new(),
            udf_file: None,
        }
    }

    /// Enable Sideways Information Passing optimization.
    pub fn sip(mut self, enabled: bool) -> Self {
        self.sip = enabled;
        self
    }

    /// Enable string interning. User-facing tuple slots remain `String`;
    /// interning is applied transparently at `insert_<rel>` / drain.
    pub fn string_intern(mut self, enabled: bool) -> Self {
        self.string_intern = enabled;
        self
    }

    /// Select the execution mode. Defaults to [`ExecutionMode::DatalogBatch`].
    ///
    /// - `DatalogBatch` — plain Datalog, `Present` diffs, fast path.
    /// - `ExtendBatch` — adds `loop` / `fixpoint` blocks; uses `i32` diffs.
    /// - `DatalogInc` / `ExtendInc` — not yet supported by the library-mode
    ///   engine; selecting them fails at [`compile`](Self::compile) time.
    pub fn mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Path to a Rust source file implementing the program's `.extern fn`
    /// UDFs. The file is included via `#[path]` as `mod udf` inside the
    /// generated module; generated code references UDFs as
    /// `udf::<fn_name>(…)`.
    pub fn udf_file(mut self, path: impl AsRef<Path>) -> Self {
        self.udf_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Compile one or more FlowLog `.dl` programs.
    ///
    /// Each entry in `program_paths` produces a separate `<stem>.rs` file
    /// in the output directory (`policy.dl` → `policy.rs`).
    ///
    /// `include_dirs` is an extra search path for `.include` directives.
    /// Includes resolve relative to the parent file's directory first, then
    /// each entry in `include_dirs` in order. Pass `&[]` if not needed.
    ///
    /// Builder settings (optimizations, interning, UDF file) apply to
    /// every input.
    pub fn compile<P, I>(mut self, program_paths: &[P], include_dirs: &[I]) -> io::Result<()>
    where
        P: AsRef<Path>,
        I: AsRef<Path>,
    {
        self.include_dirs = include_dirs
            .iter()
            .map(|p| p.as_ref().to_path_buf())
            .collect();

        let out_dir = cargo_out_dir()?;
        for program_path in program_paths {
            self.compile_one(program_path.as_ref(), &out_dir)?;
        }
        Ok(())
    }

    fn compile_one(&self, program_path: &Path, out_dir: &Path) -> io::Result<()> {
        if matches!(
            self.mode,
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc
        ) {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "library mode does not yet support {:?}; only \
                     DatalogBatch and ExtendBatch are wired into the engine",
                    self.mode
                ),
            ));
        }

        let stem = program_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("program path has no file stem: {}", program_path.display()),
                )
            })?;

        let output = pipeline::Pipeline::build(self, program_path)?;
        let source = assembly::assemble(&output, out_dir, self.udf_file.as_deref())?;

        self.emit_semiring_modules(&output, out_dir)?;
        std::fs::write(out_dir.join(format!("{stem}.rs")), source)?;
        self.emit_rerun_if_changed(program_path);
        Ok(())
    }

    /// Write aggregation-specific semiring modules to `<out_dir>/semiring/`.
    ///
    /// Semiring templates are shared verbatim with binary mode, which reaches
    /// `serde`, `ordered_float`, and `differential_dataflow` as direct deps.
    /// Library mode only has `flowlog-runtime` in its `[dependencies]`, so we
    /// prepend aliases that route the bare paths through `::flowlog_runtime::`
    /// — keeping the emitted semiring code mode-agnostic.
    fn emit_semiring_modules(
        &self,
        output: &pipeline::Pipeline,
        out_dir: &Path,
    ) -> io::Result<()> {
        if output.parts.semiring_modules.is_empty() {
            return Ok(());
        }
        let semiring_dir = out_dir.join("semiring");
        std::fs::create_dir_all(&semiring_dir)?;

        const LIB_ALIASES: &str = "\
use ::flowlog_runtime::serde;
use ::flowlog_runtime::ordered_float;
use ::flowlog_runtime::differential_dataflow;
";

        for (rel_path, content) in &output.parts.semiring_modules {
            let fname = Path::new(rel_path)
                .file_name()
                .expect("semiring module path has no file name");
            let dst = semiring_dir.join(fname);
            if fname == "mod.rs" {
                std::fs::write(dst, content)?;
            } else {
                std::fs::write(dst, format!("{LIB_ALIASES}{content}"))?;
            }
        }
        Ok(())
    }

    /// Tell Cargo to re-run the build script when any input changes — the
    /// `.dl` program, the UDF file, and every include directory.
    fn emit_rerun_if_changed(&self, program_path: &Path) {
        println!("cargo:rerun-if-changed={}", program_path.display());
        if let Some(udf) = &self.udf_file {
            println!("cargo:rerun-if-changed={}", udf.display());
        }
        for inc in &self.include_dirs {
            println!("cargo:rerun-if-changed={}", inc.display());
        }
    }
}
