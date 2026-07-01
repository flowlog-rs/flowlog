//! `.include "path"` resolution: textually inline included files into one
//! combined source string before parsing, so the parser never has to merge
//! partially-parsed programs. See [`resolve_includes`].

use std::collections::HashSet;
use std::fs;
use std::path::Path;
use std::path::PathBuf;

use flowlog_common::FileId;
use flowlog_common::SourceMap;
use flowlog_common::Span;
use pest::Parser;
use tracing::debug;
use tracing::warn;

use crate::FlowLogParser;
use crate::Rule;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::primitive::unquote;

/// Recursively inline every `.include "path"` in `source` into one combined
/// string, parseable in a single pass.
///
/// Locates `include_directive` nodes via pest (not a raw text scan, so comments
/// and strings are handled correctly) and splices each included file's resolved
/// content between the verbatim spans around it. `in_progress` is the current
/// DFS stack — a re-encounter is a [`ParseError::CircularInclude`]; `completed`
/// holds already-inlined files — a re-encounter (diamond include) is skipped.
pub(super) fn resolve_includes(
    source: String,
    source_file: FileId,
    base_dir: &Path,
    include_dirs: &[&Path],
    in_progress: &mut HashSet<PathBuf>,
    completed: &mut HashSet<PathBuf>,
    sm: &mut SourceMap,
) -> Result<String, ParseError> {
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, &source)
        .map_err(|e| ParseError::syntax_from_pest(&e, source_file))?;
    let root = pairs
        .next()
        .ok_or_else(|| grammar_bug("no parsed rule found"))?;

    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize; // byte offset into `source` of the last consumed position

    for node in root.into_inner() {
        if node.as_rule() != Rule::include_directive {
            continue;
        }

        let span = node.as_span();
        let directive_span = Span::new(source_file, span.start() as u32, span.end() as u32);

        // Append all source text between the previous directive and this one.
        out.push_str(&source[cursor..span.start()]);
        cursor = span.end();

        // The grammar child is the `string` token; its text includes the quotes.
        let path_node = node
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("include directive missing path"))?;
        let raw = unquote(path_node.as_str());

        let full_path = resolve_one_include(&raw, base_dir, include_dirs);
        let canonical = fs::canonicalize(&full_path).unwrap_or_else(|_| full_path.clone());

        if in_progress.contains(&canonical) {
            return Err(ParseError::CircularInclude {
                span: directive_span,
                path: full_path.clone(),
                chain: in_progress.iter().cloned().collect(),
            });
        }
        if completed.contains(&canonical) {
            warn!("Skipping duplicate include '{}'.", full_path.display());
            continue;
        }

        debug!("Including '{}'.", full_path.display());
        let included_file = sm
            .load(&full_path)
            .map_err(|source| ParseError::IncludeIo {
                span: directive_span,
                path: full_path.clone(),
                source,
            })?;
        let included_source = sm.text(included_file).to_string();
        let included_base = full_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf();

        in_progress.insert(canonical.clone());
        let inlined = resolve_includes(
            included_source,
            included_file,
            &included_base,
            include_dirs,
            in_progress,
            completed,
            sm,
        )?;
        in_progress.remove(&canonical);
        completed.insert(canonical);

        // Keep the inlined block newline-separated from its surroundings so a
        // directive flush mid-line can't glue two statements together.
        ensure_trailing_newline(&mut out);
        out.push_str(&inlined);
        ensure_trailing_newline(&mut out);
    }

    // Append any remaining source after the last include directive.
    out.push_str(&source[cursor..]);
    Ok(out)
}

/// Append a `\n` if `s` is non-empty and doesn't already end in whitespace.
fn ensure_trailing_newline(s: &mut String) {
    if s.chars().last().is_some_and(|c| !c.is_whitespace()) {
        s.push('\n');
    }
}

/// Resolve the include path: try `base_dir` first, then each `include_dirs`
/// entry in order, returning the first that exists. Falls back to
/// `base_dir.join(raw)` so the caller surfaces a precise I/O error path.
fn resolve_one_include(raw: &str, base_dir: &Path, include_dirs: &[&Path]) -> PathBuf {
    let parent_relative = base_dir.join(raw);
    if parent_relative.exists() {
        return parent_relative;
    }
    for dir in include_dirs {
        let candidate = dir.join(raw);
        if candidate.exists() {
            return candidate;
        }
    }
    parent_relative
}
