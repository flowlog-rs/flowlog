//! File system utilities for FlowLog compiler.
//!
//! This module provides helper functions for common file system operations
//! needed by the FlowLog compiler, such as ensuring directories exist and
//! writing files with automatic parent directory creation.

use std::fs;
use std::io;
use std::path::Path;

// =========================================================================
// File system utilities
// =========================================================================

/// Ensure a directory exists (creates all parents if needed).
pub(super) fn ensure_dir(dir: &Path) -> io::Result<()> {
    fs::create_dir_all(dir)
}

/// Ensure the parent directory for a file path exists.
pub(super) fn ensure_parent_dir(path: &Path) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        ensure_dir(parent)
    } else {
        Ok(())
    }
}

/// Write a UTF-8 text file, creating parent directories if necessary.
pub(super) fn write_file(path: &Path, contents: &str) -> io::Result<()> {
    ensure_parent_dir(path)?;
    fs::write(path, contents)
}
