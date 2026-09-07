//! Relation input and atomic output-file writing.
//!
//! [`input`] owns relation loading; [`write_atomic`] replaces completed
//! output files without exposing a partial write. The byte-range and
//! sharding helpers support the older generated input path.

pub mod input;

use std::fs::File;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use lasso::Spur;
use tempfile::NamedTempFile;

// =========================================================================
// Byte-range file reader
// =========================================================================

/// Open a byte-range slice of `path` for worker `index` out of `peers`.
///
/// Returns `Some((reader, bytes_to_read))` on success. The reader is
/// pre-seeked to the start of the worker's range (aligned to the next
/// line boundary for non-zero workers). The caller should read up to
/// `bytes_to_read` bytes, stopping at the first complete line beyond
/// that budget.
///
/// Returns `None` on I/O error (logged to stderr).
pub fn byte_range_reader(
    path: &Path,
    index: usize,
    peers: usize,
) -> Option<(BufReader<File>, u64)> {
    let open = || -> io::Result<_> {
        let file = File::open(path)?;
        let len = file.metadata()?.len();
        let range = input::reader::file::byte_range(file, len, index, peers)?;
        Ok((range.reader, range.budget))
    };
    open()
        .inspect_err(|e| {
            eprintln!(
                "[flowlog-runtime::io] failed to open {}: {e}",
                path.display()
            );
        })
        .ok()
}

// =========================================================================
// First-column sharding
// =========================================================================

/// Shard an integer-typed first column across `peers` workers.
///
/// Returns `true` if worker `index` owns this tuple.
#[inline]
pub fn shard_int(first: i64, peers: usize, index: usize) -> bool {
    first.rem_euclid(peers as i64) as usize == index
}

/// Shard a string-typed first column across `peers` workers.
///
/// Returns `true` if worker `index` owns this tuple, hashing with 32-bit
/// FNV-1a for a uniform distribution.
#[inline]
pub fn shard_str(first: &str, peers: usize, index: usize) -> bool {
    let mut hash: u32 = 0x811c9dc5;
    for &b in first.as_bytes() {
        hash ^= b as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    (hash as usize) % peers == index
}

/// Shard an interned-string first column ([`lasso::Spur`]) across `peers`.
///
/// Returns `true` if worker `index` owns this tuple.
#[inline]
pub fn shard_spur(first: Spur, peers: usize, index: usize) -> bool {
    (first.into_inner().get() as usize) % peers == index
}

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
