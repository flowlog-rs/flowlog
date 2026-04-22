//! Source location primitives — spans, file identifiers, and the source map.
//!
//! A [`Span`] is a byte-offset range into a file tracked by a [`FileId`].
//! The [`SourceMap`] owns the loaded source text for each file and
//! resolves byte offsets to `(line, column)` via [`SourceMap::line_col`].
//! `SourceMap` implements [`codespan_reporting::files::Files`] so it can
//! be passed directly to the diagnostic renderer.

use std::io;
use std::path::{Path, PathBuf};

/// Opts a field out of derived `PartialEq` / `Eq` / `Hash` while leaving
/// every other trait behaviour intact.
///
/// Wrap a field that carries metadata — source locations, profiling
/// counters, debug-only state — whose value should not affect whether two
/// enclosing values are considered "the same logical value". `Ignored<T>`
/// always compares equal to itself and hashes to nothing; `Debug`,
/// `Clone`, `Copy`, `Default` continue to delegate to `T`.
///
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Ignored<T>(pub(crate) T);

impl<T> PartialEq for Ignored<T> {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}
impl<T> Eq for Ignored<T> {}
impl<T> std::hash::Hash for Ignored<T> {
    fn hash<H: std::hash::Hasher>(&self, _state: &mut H) {}
}

/// Identifier for a source file registered with a [`SourceMap`].
///
/// `FileId::DUMMY` marks synthesized nodes that have no real source location.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FileId(pub(crate) u32);

impl FileId {
    pub const DUMMY: FileId = FileId(u32::MAX);
}

/// A byte range within one file. Resolve to `(line, column)` via
/// [`SourceMap::line_col`] and to a source slice via [`SourceMap::snippet`].
///
/// `Span::DUMMY` is the placeholder for synthesized AST nodes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Span {
    pub(crate) file: FileId,
    pub(crate) start: u32,
    pub(crate) end: u32,
}

impl Span {
    pub const DUMMY: Span = Span {
        file: FileId::DUMMY,
        start: 0,
        end: 0,
    };

    pub fn new(file: FileId, start: u32, end: u32) -> Self {
        debug_assert!(start <= end, "span start must be <= end");
        Self { file, start, end }
    }

    pub fn is_dummy(&self) -> bool {
        self.file == FileId::DUMMY
    }

    /// Smallest span that covers both. Both must come from the same file.
    pub fn merge(self, other: Span) -> Span {
        assert_eq!(
            self.file, other.file,
            "cannot merge spans from different files"
        );
        Span {
            file: self.file,
            start: self.start.min(other.start),
            end: self.end.max(other.end),
        }
    }

    pub fn range(&self) -> std::ops::Range<usize> {
        self.start as usize..self.end as usize
    }
}

pub(crate) struct SourceFile {
    path: PathBuf,
    text: String,
    line_starts: Vec<u32>,
}

impl SourceFile {
    fn new(path: PathBuf, text: String) -> Self {
        let line_starts = codespan_reporting::files::line_starts(&text)
            .map(|n| n as u32)
            .collect();
        Self {
            path,
            text,
            line_starts,
        }
    }
}

/// Registry of source files loaded during one compilation.
///
/// Populated via [`load`](SourceMap::load) or [`add`](SourceMap::add);
/// each call returns a new [`FileId`]. Resolves byte offsets to
/// `(line, column)` and exposes source text through
/// [`codespan_reporting::files::Files`].
#[derive(Default)]
pub struct SourceMap {
    files: Vec<SourceFile>,
}

impl SourceMap {
    pub fn new() -> Self {
        Self::default()
    }

    /// Read `path` from disk and register it. Returns its [`FileId`].
    pub fn load(&mut self, path: impl Into<PathBuf>) -> io::Result<FileId> {
        let path = path.into();
        let text = std::fs::read_to_string(&path)?;
        Ok(self.add(path, text))
    }

    /// Register a file whose text is already in memory.
    pub fn add(&mut self, path: PathBuf, text: String) -> FileId {
        let id = FileId(self.files.len() as u32);
        self.files.push(SourceFile::new(path, text));
        id
    }

    pub(crate) fn file(&self, id: FileId) -> &SourceFile {
        &self.files[id.0 as usize]
    }

    pub fn path(&self, id: FileId) -> &Path {
        &self.file(id).path
    }

    pub fn text(&self, id: FileId) -> &str {
        &self.file(id).text
    }

    /// Slice the source text covered by `span`. Dummy spans render as `""`.
    pub fn snippet(&self, span: Span) -> &str {
        if span.is_dummy() {
            return "";
        }
        &self.file(span.file).text[span.range()]
    }

    /// Resolve a byte offset within `file` to a 1-indexed `(line, col)` pair.
    ///
    /// Columns are byte-based. Offsets past end-of-file clamp to the last
    /// position in the file.
    pub fn line_col(&self, file: FileId, byte: u32) -> (u32, u32) {
        let sf = self.file(file);
        let line_idx = line_index_0(&sf.line_starts, byte);
        let col = byte.saturating_sub(sf.line_starts[line_idx]);
        (line_idx as u32 + 1, col + 1)
    }
}

/// 0-indexed line containing `byte`. `line_starts` must start with `0` and be
/// strictly ascending.
fn line_index_0(line_starts: &[u32], byte: u32) -> usize {
    match line_starts.binary_search(&byte) {
        Ok(i) => i,
        Err(i) => i.saturating_sub(1),
    }
}

impl<'a> codespan_reporting::files::Files<'a> for SourceMap {
    type FileId = FileId;
    type Name = String;
    type Source = &'a str;

    fn name(&'a self, id: FileId) -> Result<Self::Name, codespan_reporting::files::Error> {
        Ok(self.path(id).display().to_string())
    }

    fn source(&'a self, id: FileId) -> Result<Self::Source, codespan_reporting::files::Error> {
        Ok(self.text(id))
    }

    fn line_index(
        &'a self,
        id: FileId,
        byte_index: usize,
    ) -> Result<usize, codespan_reporting::files::Error> {
        Ok(line_index_0(&self.file(id).line_starts, byte_index as u32))
    }

    fn line_range(
        &'a self,
        id: FileId,
        line_index: usize,
    ) -> Result<std::ops::Range<usize>, codespan_reporting::files::Error> {
        let sf = self.file(id);
        let n = sf.line_starts.len();
        if line_index >= n {
            return Err(codespan_reporting::files::Error::LineTooLarge {
                given: line_index,
                max: n - 1,
            });
        }
        let start = sf.line_starts[line_index] as usize;
        let end = if line_index + 1 < n {
            sf.line_starts[line_index + 1] as usize
        } else {
            sf.text.len()
        };
        Ok(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_same_file() {
        let f = FileId(0);
        let a = Span::new(f, 3, 7);
        let b = Span::new(f, 5, 12);
        assert_eq!(a.merge(b), Span::new(f, 3, 12));
        assert_eq!(b.merge(a), Span::new(f, 3, 12));
    }

    #[test]
    #[should_panic(expected = "different files")]
    fn merge_different_files_panics() {
        Span::new(FileId(0), 0, 1).merge(Span::new(FileId(1), 0, 1));
    }

    #[test]
    fn line_col_multi_line() {
        let mut sm = SourceMap::new();
        let f = sm.add("x.dl".into(), "abc\ndefg\nhij".into());
        assert_eq!(sm.line_col(f, 0), (1, 1));
        assert_eq!(sm.line_col(f, 2), (1, 3));
        assert_eq!(sm.line_col(f, 3), (1, 4));
        assert_eq!(sm.line_col(f, 4), (2, 1));
        assert_eq!(sm.line_col(f, 7), (2, 4));
        assert_eq!(sm.line_col(f, 9), (3, 1));
        assert_eq!(sm.line_col(f, 11), (3, 3));
    }

    #[test]
    fn line_col_past_eof_clamps() {
        let mut sm = SourceMap::new();
        let f = sm.add("x.dl".into(), "abc".into());
        let (line, col) = sm.line_col(f, 100);
        assert_eq!(line, 1);
        assert!(col >= 4);
    }

    #[test]
    fn snippet_dummy_is_empty() {
        let sm = SourceMap::new();
        assert_eq!(sm.snippet(Span::DUMMY), "");
    }

    #[test]
    #[allow(clippy::clone_on_copy)]
    fn ignored_collapses_eq_and_hash_but_preserves_debug_clone() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = Ignored(42i32);
        let b = Ignored(99i32);
        assert_eq!(a, b); // values differ, but Ignored says equal
        assert_eq!(a, a);

        let hash_of = |x: &Ignored<i32>| {
            let mut h = DefaultHasher::new();
            x.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash_of(&a), hash_of(&b)); // hashes nothing → same hash

        // Debug / Clone / Copy still delegate
        assert_eq!(format!("{a:?}"), "Ignored(42)");
        let c = a;
        let d = a.clone();
        assert_eq!(c.0, 42);
        assert_eq!(d.0, 42);
    }

    #[test]
    fn load_reads_file_and_indexes_lines() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("a.dl");
        std::fs::write(&path, "line one\nline two\n").unwrap();

        let mut sm = SourceMap::new();
        let f = sm.load(&path).unwrap();
        assert_eq!(sm.text(f), "line one\nline two\n");
        assert_eq!(sm.line_col(f, 9), (2, 1));
    }
}
