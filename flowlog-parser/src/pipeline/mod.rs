//! Compilation pipeline: every stage from source text to a checked,
//! optimized [`Program`], driven by [`parse`] in execution order:
//!
//! 1. `include`: splice `.include` files into one source string.
//! 2. `assemble`: pest-parse, build the [`Program`] (inlining,
//!    directives, validation, assignment substitution).
//! 3. `typecheck`: check types and subtypes; pin literals, lower casts.
//! 4. `fold`: constant folding and dead-rule elimination.
//! 5. `prune`: dead-component pruning + orphan materialization.
//! 6. `validate`: reject semantically broken rules.
//!
//! The individual stages ([`check_program`], [`fold_constants`], [`prune`])
//! are also exported at the crate root.

use std::path::Path;
use std::path::PathBuf;

use flowlog_common::Config;
use flowlog_error::SourceMap;
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
/// docs list. On
/// `Ok` the returned [`Program`] is a fully-typed, immutable AST; this
/// is the only supported way to build one.
///
/// `.include "name.dl"` is resolved by trying, in order:
/// 1. The including file's own directory (always tried first).
/// 2. Each entry in `include_dirs`. Pass `&[]` for none.
///
/// Source text is loaded into `sm` so later diagnostics can cite it.
/// `config` supplies the execution mode (extended vs. standard) and
/// config-gated builtins (e.g. `--str-intern`).
///
/// Errors from any stage surface as a single [`ParseError`]; a type
/// error is just another variant.
pub fn parse(
    path: &str,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
    config: &mut Config,
) -> Result<Program, ParseError> {
    // Stages 1-2: resolve `.include`s and assemble the program.
    let mut program = parse_syntactic(path, config.is_extended(), include_dirs, sm)?;
    // Stage 3: type-check (pin literals, lower casts).
    typecheck::check_program(&mut program, config)?;
    // Stage 4: constant-fold. Before prune, because folding strands dead
    // relations that prune then removes.
    fold::fold_constants(&mut program)?;
    // Stage 5: prune dead components and materialize orphan relations.
    prune::prune(&mut program);
    // Stage 6: reject semantically broken rules, after prune so dead rules
    // are not reported.
    validate::validate(&program)?;

    debug!("\n{}", program);
    info!("Successfully parsed program from '{}'.", path);

    Ok(program)
}

/// Stages 1-2 only: resolve `.include`s and assemble the `Program`, stopping
/// before type-check. Not public API; the crate's own tests reach it through
/// the `test_util` stage ladder to drive one pass at a time on realistic input.
pub(crate) fn parse_syntactic(
    path: &str,
    extended: bool,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
) -> Result<Program, ParseError> {
    let file_path = PathBuf::from(path);

    // Stage 1: resolve `.include`s into one combined source string.
    let combined = resolve_includes(&file_path, include_dirs, sm)?;

    // Register the combined text as the authoritative "file": Pest spans point
    // into it, while the individual include files stay in `sm` for I/O errors.
    let combined_file = sm.add(file_path.clone(), combined);

    // Stage 2: parse and assemble the combined source into a `Program`.
    assemble::collect_program(sm.text(combined_file), extended, combined_file)
}
