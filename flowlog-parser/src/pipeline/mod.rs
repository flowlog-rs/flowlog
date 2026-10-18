//! Compilation pipeline: every stage from source text to a checked,
//! optimized [`Program`], driven by [`parse_file`] in execution order:
//!
//! 1. `include`: splice `.include` files into one source string.
//! 2. `assemble`: pest-parse, build the [`Program`] (inlining,
//!    directives, validation, assignment substitution); a program with no
//!    `.output` or `.printsize` is rejected here.
//! 3. `typecheck`: check types and subtypes; pin literals, lower casts.
//! 4. `fold`: constant folding and dead-rule elimination.
//! 5. `prune`: dead-component pruning, orphan materialization, and
//!    inline-fact dedup.
//! 6. `validate`: reject semantically broken rules.
//!
//! [`parse_str`] runs the same stages over text held in memory. The
//! individual stages ([`check_program`], [`fold_constants`], [`prune`]) are
//! also exported at the crate root.

use std::path::Path;
use std::path::PathBuf;

use flowlog_common::Config;
use flowlog_common::SourceMap;
use include::resolve_includes;
use tracing::debug;
use tracing::info;

use crate::error::ParseError;
use crate::program::Program;

mod assemble;
pub(crate) mod fold;
mod include;
pub(crate) mod prune;
pub(crate) mod typecheck;
mod validate;

/// Parses a program from a file, resolving `.include` directives
/// recursively, then runs the semantic stages in the order the module
/// docs list. On `Ok` the returned [`Program`] is a fully-typed,
/// immutable AST.
///
/// `.include "name.dl"` is resolved by trying, in order:
/// 1. The including file's own directory (always tried first).
/// 2. Each entry in `include_dirs`. Pass `&[]` for none.
///
/// Source text is loaded into `sm` so later diagnostics can cite it.
/// `config` supplies config-gated builtins (e.g. `--str-intern`).
///
/// Errors from any stage surface as a single [`ParseError`]; a type
/// error is just another variant.
pub fn parse_file(
    path: &str,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
    config: &mut Config,
) -> Result<Program, ParseError> {
    // Stages 1-2: resolve `.include`s and assemble the program.
    let mut program = parse_syntactic_file(path, include_dirs, sm)?;
    check(&mut program, config)?;
    info!("Successfully parsed program from '{}'.", path);
    Ok(program)
}

/// [`parse_file`] for a program held in memory: `source` is registered
/// in `sm` under `name`, which diagnostics cite as the file. `.include`
/// resolves as for [`parse_file`], with `name`'s directory (the working
/// directory for a bare name) in place of the file's own.
pub fn parse_str(
    name: &str,
    source: &str,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
    config: &mut Config,
) -> Result<Program, ParseError> {
    let mut program = parse_syntactic_str(name, source, include_dirs, sm)?;
    check(&mut program, config)?;
    info!("Successfully parsed program '{}' from memory.", name);
    Ok(program)
}

/// Stages 3-6 over an assembled program.
fn check(program: &mut Program, config: &mut Config) -> Result<(), ParseError> {
    // Stage 3: type-check (pin literals, lower casts).
    typecheck::check_program(program, config)?;
    // Stage 4: constant-fold. Before prune, because folding strands dead
    // relations that prune then removes.
    fold::fold_constants(program)?;
    // Stage 5: prune dead components and materialize orphan relations.
    prune::prune(program);
    // Stage 6: reject semantically broken rules, after prune so dead rules
    // are not reported.
    validate::validate(program)?;

    debug!("\n{}", program);
    Ok(())
}

/// Stages 1-2 only: resolve `.include`s and assemble the `Program`,
/// stopping before type-check.
pub(crate) fn parse_syntactic_file(
    path: &str,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
) -> Result<Program, ParseError> {
    let file_path = PathBuf::from(path);

    // Stage 1: resolve `.include`s into one combined source string.
    let combined = resolve_includes(&file_path, include_dirs, sm)?;
    collect(file_path, combined, sm)
}

/// [`parse_syntactic_file`] for a program held in memory; see [`parse_str`].
pub(crate) fn parse_syntactic_str(
    name: &str,
    source: &str,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
) -> Result<Program, ParseError> {
    let path = PathBuf::from(name);
    let root_file = sm.add(path.clone(), source.to_string());
    let combined = include::resolve_includes_of(root_file, &path, include_dirs, sm)?;
    collect(path, combined, sm)
}

/// Stage 2: parses and assembles the combined source into a `Program`.
fn collect(path: PathBuf, combined: String, sm: &mut SourceMap) -> Result<Program, ParseError> {
    // Register the combined text as the authoritative "file": Pest spans point
    // into it, while the individual include files stay in `sm` for I/O errors.
    let combined_file = sm.add(path, combined);
    assemble::collect_program(sm.text(combined_file), combined_file)
}
