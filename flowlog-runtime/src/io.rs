//! Atomic file writing for generated engine output.
//!
//! [`input`] owns relation loading; [`write_atomic`] replaces completed
//! output files without exposing a partial write.

pub mod input;

use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use tempfile::NamedTempFile;

// =========================================================================
// Atomic file write
// =========================================================================

/// Write `path` atomically: stream through `write` into a temp file in the
/// same directory, then persist it over `path` in a single rename. A failed
/// or interrupted write leaves `path` untouched, so a concurrent reader never
/// observes a half-written file. Delegates the platform-specific atomic
/// replace to `tempfile`, which handles the Unix and Windows differences.
///
/// The temp file is a sibling of `path` so the rename stays within one
/// filesystem (a metadata move, not a copy). `path` must have a parent or be
/// relative to the current directory.
pub fn write_atomic(
    path: impl AsRef<Path>,
    write: impl FnOnce(&mut dyn Write) -> io::Result<()>,
) -> io::Result<()> {
    let path = path.as_ref();
    let mut tmp = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
        Some(dir) => NamedTempFile::new_in(dir)?,
        None => NamedTempFile::new()?,
    };
    {
        let mut buf = BufWriter::new(&mut tmp);
        write(&mut buf)?;
        buf.flush()?;
    }
    tmp.persist(path).map_err(|e| e.error)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A completed write leaves the destination with exactly the bytes
    /// written and no leftover temp sibling in the directory.
    #[test]
    fn write_atomic_persists_content_and_leaves_no_temp() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");
        write_atomic(&path, |w| write!(w, "hello")).expect("write");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(
            names.len(),
            1,
            "only the persisted file should remain: {names:?}"
        );
    }

    /// A second write replaces the destination rather than appending or
    /// erroring on the existing file.
    #[test]
    fn write_atomic_overwrites_existing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");
        write_atomic(&path, |w| write!(w, "first")).expect("first");
        write_atomic(&path, |w| write!(w, "second")).expect("second");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "second");
    }

    /// The atomicity guarantee: a closure error propagates, the existing
    /// destination keeps its old contents (the write never clobbers the
    /// target), and the temp sibling is cleaned up rather than left behind.
    #[test]
    fn write_atomic_failed_write_preserves_existing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");
        write_atomic(&path, |w| write!(w, "original")).expect("seed");

        let err = write_atomic(&path, |w| {
            write!(w, "partial")?;
            Err(io::Error::other("boom"))
        })
        .expect_err("closure error must propagate");
        assert_eq!(err.to_string(), "boom");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "original");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(
            names.len(),
            1,
            "temp sibling should be cleaned up: {names:?}"
        );
    }
}
