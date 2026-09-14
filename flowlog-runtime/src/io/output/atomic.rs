//! Atomic file replacement with buffered writes.

use std::io;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;

use tempfile::NamedTempFile;

/// Replaces `path` atomically after `write` completes and its buffer flushes.
/// Failed writes leave an existing destination untouched.
///
/// A path with a parent uses a temporary sibling. A bare filename uses the
/// system temporary directory; persisting requires the same filesystem.
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
    use std::io;

    use crate::io::write_atomic;

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

    #[test]
    fn write_atomic_overwrites_existing() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");
        write_atomic(&path, |w| write!(w, "first")).expect("first");
        write_atomic(&path, |w| write!(w, "second")).expect("second");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "second");
    }

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
        assert_eq!(err.kind(), io::ErrorKind::Other);
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
