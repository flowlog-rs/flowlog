//! Build-script integration for FlowLog library mode.
//!
//! Compiles a `.dl` program into a Rust module your crate `include!`s
//! from `build.rs`.
//!
//! # Minimal
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
//! let mut engine = DatalogBatchEngine::new(4);
//! engine.insert_edge(vec![(1, 2), (2, 3)]);
//! let results = engine.run();
//! ```
//!
//! # Structured errors
//!
//! [`Builder::compile`] returns a [`BoxError`] for callers that want to
//! render the diagnostic themselves rather than surface it through
//! [`io::Error`]:
//!
//! ```no_run
//! use flowlog_build::Builder;
//!
//! // build.rs
//! if let Err(err) = Builder::default()
//!     .sip(true)
//!     .string_intern(true)
//!     .compile(&["policy.dl", "auth.dl"], &[] as &[&std::path::Path])
//! {
//!     eprintln!("{err}");
//!     std::process::exit(1);
//! }
//! ```

mod assembly;
mod engine;
mod error;
mod imports;
mod pipeline;
mod relation;
mod results;

// Shared codegen core — consumed by this crate's library mode and, via
// the re-exports below, by `flowlog-compiler`'s binary mode.
mod codegen;

pub use codegen::code_parts::CodeParts;
pub use codegen::error::CodegenError;
pub use codegen::features::Features;
pub use codegen::idb_buffers::{field_accessor, gen_drain_block};
pub use codegen::ty::data::data_type_tokens;
pub use codegen::CodeGen;
pub use error::BuildError;

use std::io;
use std::path::{Path, PathBuf};

use common::diag::{self, BoxError};
pub use common::ExecutionMode;
use common::SourceMap;

/// Compile a single `.dl` program with default options.
///
/// Any pipeline diagnostic is rendered against its source map into the
/// returned [`io::Error`]'s body, so `cargo build` shows a
/// source-annotated message. For structured error access, use
/// [`Builder::compile`].
pub fn compile<P: AsRef<Path>>(program_path: P) -> io::Result<()> {
    let out_dir = cargo_out_dir()?;
    let mut sm = SourceMap::new();
    Builder::default()
        .compile_one(program_path.as_ref(), &out_dir, &mut sm)
        .map_err(|err| {
            let mut buf = Vec::new();
            let _ = diag::emit(&err, &sm, &mut buf);
            io::Error::other(String::from_utf8_lossy(&buf).into_owned())
        })
}

/// Chained configuration for advanced compilation options. For default
/// settings prefer the free [`compile`] function.
#[derive(Default)]
pub struct Builder {
    pub(crate) sip: bool,
    pub(crate) string_intern: bool,
    pub(crate) mode: ExecutionMode,
    pub(crate) include_dirs: Vec<PathBuf>,
    pub(crate) udf_file: Option<PathBuf>,
}

impl Builder {
    /// Enable Sideways Information Passing.
    pub fn sip(mut self, enabled: bool) -> Self {
        self.sip = enabled;
        self
    }

    /// Enable string interning. User-facing tuple slots stay `String`;
    /// interning is applied at `insert_<rel>` / drain.
    pub fn string_intern(mut self, enabled: bool) -> Self {
        self.string_intern = enabled;
        self
    }

    /// Set the execution mode. Defaults to [`ExecutionMode::DatalogBatch`].
    ///
    /// Only `DatalogBatch` and `ExtendBatch` are wired into the
    /// library-mode engine; `DatalogInc` / `ExtendInc` panic at
    /// compile time.
    pub fn mode(mut self, mode: ExecutionMode) -> Self {
        self.mode = mode;
        self
    }

    /// Path to the UDF source file, included as `mod udf` inside the
    /// generated module. Generated code calls UDFs as `udf::<fn_name>(…)`.
    pub fn udf_file(mut self, path: impl AsRef<Path>) -> Self {
        self.udf_file = Some(path.as_ref().to_path_buf());
        self
    }

    /// Compile one or more `.dl` programs. Each input produces a
    /// `<stem>.rs` file under `$OUT_DIR`.
    ///
    /// `include_dirs` is searched for `.include` directives after each
    /// file's own directory. Builder settings apply to every input.
    pub fn compile<P, I>(mut self, program_paths: &[P], include_dirs: &[I]) -> Result<(), BoxError>
    where
        P: AsRef<Path>,
        I: AsRef<Path>,
    {
        self.include_dirs = include_dirs
            .iter()
            .map(|p| p.as_ref().to_path_buf())
            .collect();

        let out_dir = cargo_out_dir().map_err(BuildError::from)?;
        for program_path in program_paths {
            let mut sm = SourceMap::new();
            self.compile_one(program_path.as_ref(), &out_dir, &mut sm)?;
        }
        Ok(())
    }

    /// Compile one `.dl` program, populating the caller's [`SourceMap`]
    /// so any returned [`BoxError`] can be rendered against the source.
    fn compile_one(
        &self,
        program_path: &Path,
        out_dir: &Path,
        sm: &mut SourceMap,
    ) -> Result<(), BoxError> {
        assert!(
            !matches!(
                self.mode,
                ExecutionMode::DatalogInc | ExecutionMode::ExtendInc
            ),
            "library mode does not yet support {:?}; only DatalogBatch \
             and ExtendBatch are wired in",
            self.mode,
        );

        let stem = program_path
            .file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                BuildError::from(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "program path has no usable file stem: {}",
                        program_path.display()
                    ),
                ))
            })?;

        let output = pipeline::Pipeline::build(self, program_path, sm)?;
        let source = assembly::assemble(&output, out_dir, self.udf_file.as_deref())
            .map_err(BuildError::from)?;
        self.emit_semiring_modules(&output, out_dir)
            .map_err(BuildError::from)?;
        std::fs::write(out_dir.join(format!("{stem}.rs")), source).map_err(BuildError::from)?;
        self.emit_rerun_if_changed(program_path);
        Ok(())
    }

    /// Write aggregation-specific semiring modules to `$OUT_DIR/semiring/`.
    ///
    /// Library mode only has `flowlog-runtime` as a runtime dep, so we
    /// prepend aliases that route `serde` / `ordered_float` /
    /// `differential_dataflow` through `::flowlog_runtime::` — keeping
    /// the templates mode-agnostic with binary mode.
    fn emit_semiring_modules(&self, output: &pipeline::Pipeline, out_dir: &Path) -> io::Result<()> {
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

    /// Emit `cargo:rerun-if-changed` for the program, the UDF file, and
    /// every include directory.
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

fn cargo_out_dir() -> io::Result<PathBuf> {
    std::env::var_os("OUT_DIR")
        .map(PathBuf::from)
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                "OUT_DIR not set — run from a build.rs",
            )
        })
}
