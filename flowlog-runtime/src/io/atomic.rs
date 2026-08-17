//! Replacing a file in one step, so a reader never sees a partial write.

use std::io;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;

use tempfile::Builder;
use tempfile::NamedTempFile;

/// A file that appears at its destination only once it is complete.
///
/// Writes stream into a temp file and [`commit`](Self::commit) renames that
/// over the destination in one step. An interrupted or failed write leaves
/// the destination untouched, so a run that dies partway through does not
/// destroy the output of the run before it, and a concurrent reader never
/// observes half a file.
///
/// The temp file is a sibling of the destination, which keeps the rename
/// inside one filesystem: a metadata move rather than a copy. The
/// destination must therefore have a parent, or be relative to the current
/// directory.
///
/// Streaming rather than whole-file, so one helper serves both a relation
/// drain writing row by row and a profiler table written in a single pass.
/// Every file FlowLog writes goes through here.
#[derive(Debug)]
pub struct AtomicFile {
    tmp: NamedTempFile,
    path: PathBuf,
}

impl AtomicFile {
    /// Begin writing `path`. Nothing appears there until
    /// [`commit`](Self::commit).
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref();
        let mut builder = Builder::new();

        // `tempfile` defaults to 0600, which is right for a temp file but
        // would ride along through the rename and leave outputs readable
        // only by the user who ran the program. Asking for 0666 reproduces
        // `File::create`: the crate applies `& !umask`, so this can only
        // ever be narrowed by the caller's umask, never widened past it.
        #[cfg(unix)]
        {
            use std::fs::Permissions;
            use std::os::unix::fs::PermissionsExt as _;
            builder.permissions(Permissions::from_mode(0o666));
        }

        let tmp = match path.parent().filter(|p| !p.as_os_str().is_empty()) {
            Some(dir) => builder.tempfile_in(dir)?,
            None => builder.tempfile()?,
        };
        Ok(Self {
            tmp,
            path: path.to_path_buf(),
        })
    }

    /// Publish everything written so far at the destination.
    ///
    /// Delegates the platform-specific atomic replace to `tempfile`, which
    /// handles the Unix and Windows differences.
    pub fn commit(self) -> io::Result<()> {
        self.tmp.persist(self.path).map_err(|e| e.error)?;
        Ok(())
    }
}

impl Write for AtomicFile {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.tmp.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.tmp.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A committed file holds exactly what was written, with no temp
    /// sibling left in the directory.
    #[test]
    fn a_committed_file_holds_its_content_and_leaves_no_temp() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");

        let mut file = AtomicFile::create(&path).expect("create");
        write!(file, "hello").expect("write");
        file.commit().expect("commit");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(names.len(), 1, "only the committed file remains: {names:?}");
    }

    /// Committing again over an existing destination replaces it.
    #[test]
    fn a_commit_replaces_an_existing_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");

        for content in ["first", "second"] {
            let mut file = AtomicFile::create(&path).expect("create");
            write!(file, "{content}").expect("write");
            file.commit().expect("commit");
        }

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "second");
    }

    /// The guarantee this type exists for: a write abandoned before
    /// `commit` leaves the previous contents in place and cleans up after
    /// itself. Dropping without committing is how an error path unwinds.
    #[test]
    fn an_abandoned_write_preserves_the_previous_file() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("out.log");

        let mut file = AtomicFile::create(&path).expect("create");
        write!(file, "original").expect("write");
        file.commit().expect("commit");

        let mut abandoned = AtomicFile::create(&path).expect("create");
        write!(abandoned, "partial").expect("write");
        drop(abandoned);

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "original");
        let names: Vec<_> = std::fs::read_dir(dir.path())
            .expect("read dir")
            .map(|e| e.expect("entry").file_name())
            .collect();
        assert_eq!(names.len(), 1, "temp sibling is cleaned up: {names:?}");
    }

    /// The committed file carries the mode `File::create` would give it,
    /// not the 0600 a temp file defaults to. Without this, every output
    /// would silently become unreadable to anyone but the running user.
    #[cfg(unix)]
    #[test]
    fn a_committed_file_keeps_the_ordinary_output_mode() {
        use std::os::unix::fs::PermissionsExt as _;

        let dir = tempfile::tempdir().expect("temp dir");
        let plain = dir.path().join("plain");
        std::fs::write(&plain, "x").expect("plain write");

        let path = dir.path().join("atomic");
        let mut file = AtomicFile::create(&path).expect("create");
        write!(file, "x").expect("write");
        file.commit().expect("commit");

        let mode = |p: &Path| std::fs::metadata(p).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode(&path), mode(&plain));
    }
}
