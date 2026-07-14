//! `.include "path"` resolution.
//!
//! [`resolve_includes`] inlines every `.include` in a file (and its includes,
//! transitively) and returns the combined source.

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
use crate::decode_string;
use crate::error::ParseError;
use crate::error::grammar_bug;
use crate::span_of;

/// Resolve every `.include "path"` in the file at `root_path` (and its
/// includes, transitively), returning the combined source. A cyclic include is
/// a [`ParseError::CircularInclude`]; a file reached twice (a diamond include)
/// is inlined only once.
pub(super) fn resolve_includes(
    root_path: &Path,
    include_dirs: &[&Path],
    sm: &mut SourceMap,
) -> Result<String, ParseError> {
    let root_file = sm.load(root_path).map_err(|source| ParseError::IncludeIo {
        span: Span::DUMMY,
        path: root_path.to_path_buf(),
        source,
    })?;
    // Relative includes resolve against the root file's own directory.
    let base_dir = root_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));
    // Seed the DFS stack with the root so an include pointing back at it is
    // caught as a cycle; `completed` then dedups diamond includes.
    let mut in_progress = HashSet::new();
    in_progress.insert(fs::canonicalize(root_path).unwrap_or_else(|_| root_path.to_path_buf()));
    let mut completed = HashSet::new();
    inline_includes(
        sm.text(root_file).to_string(),
        root_file,
        &base_dir,
        include_dirs,
        &mut in_progress,
        &mut completed,
        sm,
    )
}

/// Recursive worker for [`resolve_includes`]: inline the `.include`s in
/// `source` (parsed as file `source_file`) into the returned combined string.
/// `in_progress` is the current DFS stack (a re-encounter is a cycle);
/// `completed` holds already-inlined files (a re-encounter is skipped).
fn inline_includes(
    source: String,
    source_file: FileId,
    base_dir: &Path,
    include_dirs: &[&Path],
    in_progress: &mut HashSet<PathBuf>,
    completed: &mut HashSet<PathBuf>,
    sm: &mut SourceMap,
) -> Result<String, ParseError> {
    // Find `.include` directives via pest (not a raw text scan) so occurrences
    // inside comments or strings are ignored. The verbatim text between them is
    // copied through; each directive is replaced by its resolved content.
    let mut pairs = FlowLogParser::parse(Rule::main_grammar, &source)
        .map_err(|e| ParseError::syntax_from_pest(&e, source_file))?;
    let root = pairs
        .next()
        .ok_or_else(|| grammar_bug("no parsed rule found"))?;

    let mut out = String::with_capacity(source.len());
    let mut cursor = 0usize; // byte offset of the last consumed position

    for node in root.into_inner() {
        if node.as_rule() != Rule::include_directive {
            continue;
        }

        let span = node.as_span();
        let directive_span = span_of(&node, source_file);

        // Copy the verbatim text between the previous directive and this one.
        out.push_str(&source[cursor..span.start()]);
        cursor = span.end();

        // The grammar child is the `string` token; its text includes the quotes.
        let path_node = node
            .into_inner()
            .next()
            .ok_or_else(|| grammar_bug("include directive missing path"))?;
        let raw = decode_string(path_node.as_str(), span_of(&path_node, source_file))?;

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
        let inlined = inline_includes(
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

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::assert_err;

    /// A `.include` of a file that doesn't exist surfaces as `IncludeIo`.
    #[test]
    fn resolve_includes_rejects_a_missing_file() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("root.dl"), ".include \"nonexistent.dl\"\n").unwrap();
        assert_err!(
            resolve_includes(&dir.path().join("root.dl"), &[], &mut SourceMap::new()),
            ParseError::IncludeIo { .. }
        );
    }

    /// A `.include` not found next to the including file resolves against an
    /// `-I` include directory (searched after the base directory).
    #[test]
    fn resolve_includes_searches_include_dirs() {
        let base = tempdir().unwrap();
        let inc = tempdir().unwrap();
        fs::write(base.path().join("root.dl"), ".include \"lib.dl\"\n").unwrap();
        fs::write(inc.path().join("lib.dl"), ".decl lib_rel(x: number)\n").unwrap();
        let combined = resolve_includes(
            &base.path().join("root.dl"),
            &[inc.path()],
            &mut SourceMap::new(),
        )
        .expect("include should resolve via the include dir");
        assert!(
            combined.contains(".decl lib_rel"),
            "lib.dl (only in the include dir) must be inlined"
        );
    }

    /// `a.dl` includes `b.dl`, which includes `a.dl` back: `CircularInclude`.
    /// The entry seeds cycle detection with the root (`a.dl`), so the cycle is
    /// caught on the way back to `a`.
    #[test]
    fn resolve_includes_rejects_a_cycle() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join("a.dl"), ".include \"b.dl\"\n").unwrap();
        fs::write(dir.path().join("b.dl"), ".include \"a.dl\"\n").unwrap();
        assert_err!(
            resolve_includes(&dir.path().join("a.dl"), &[], &mut SourceMap::new()),
            ParseError::CircularInclude { .. }
        );
    }

    /// Diamond include: `root` includes `left` and `right`, both include
    /// `leaf`. The `completed` set must inline `leaf.dl` exactly once;
    /// otherwise `.decl leaf_rel` would appear twice in the combined source
    /// and later fail with `DuplicateDecl`. Guards the warn-and-skip branch
    /// at the `completed.contains` check.
    #[test]
    fn diamond_include_dedups_leaf() {
        let dir = tempdir().unwrap();
        fs::write(
            dir.path().join("root.dl"),
            ".include \"left.dl\"\n.include \"right.dl\"\n",
        )
        .unwrap();
        fs::write(dir.path().join("leaf.dl"), ".decl leaf_rel(x: number)\n").unwrap();
        fs::write(dir.path().join("left.dl"), ".include \"leaf.dl\"\n").unwrap();
        fs::write(dir.path().join("right.dl"), ".include \"leaf.dl\"\n").unwrap();
        let combined = resolve_includes(&dir.path().join("root.dl"), &[], &mut SourceMap::new())
            .expect("diamond include should dedup");
        assert_eq!(
            combined.matches(".decl leaf_rel").count(),
            1,
            "leaf.dl must be inlined exactly once"
        );
    }
}
