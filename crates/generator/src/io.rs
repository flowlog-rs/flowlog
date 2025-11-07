use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Ensure a directory exists (creates all parents if needed).
pub fn ensure_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)
}

/// Ensure the parent directory for a file path exists.
pub fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)
    } else {
        Ok(())
    }
}

/// Write a UTF-8 text file, creating parent directories if necessary.
pub fn write_file(path: &Path, contents: &str) -> io::Result<()> {
    ensure_parent_dir(path)?;
    fs::write(path, contents)
}

/// Join two paths in a slightly more ergonomic way.
pub fn join<P: AsRef<Path>>(base: &Path, rel: P) -> PathBuf {
    base.join(rel)
}
