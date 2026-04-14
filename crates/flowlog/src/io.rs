//! I/O utilities for parallel data ingestion.
//!
//! Provides two categories of helpers used by the generated
//! `DatalogBatchEngine`:
//!
//! - **Byte-range file reader** ([`byte_range_reader`]) — splits a CSV
//!   file across timely workers so each reads its own byte slice.
//! - **First-column sharding** ([`shard_int`], [`shard_str`],
//!   [`shard_spur`]) — determines which worker owns a given tuple based
//!   on the first column's value.
//!
//! # Byte-range reader example
//!
//! ```ignore
//! if let Some((reader, budget)) = byte_range_reader(path, index, peers) {
//!     let mut buf = Vec::new();
//!     let mut consumed = 0u64;
//!     while consumed < budget {
//!         buf.clear();
//!         let n = reader.read_until(b'\n', &mut buf).unwrap_or(0);
//!         if n == 0 { break; }
//!         consumed += n as u64;
//!         // parse &buf …
//!     }
//! }
//! ```

use std::fs::File;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom};
use std::path::Path;

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
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("[flowlog::io] failed to open {}: {e}", path.display());
            return None;
        }
    };

    let file_size = match file.metadata() {
        Ok(m) => m.len(),
        Err(e) => {
            eprintln!("[flowlog::io] failed to stat {}: {e}", path.display());
            return None;
        }
    };

    let chunk = file_size / peers as u64;
    let start = chunk * index as u64;
    let end = if index == peers - 1 {
        file_size
    } else {
        chunk * (index + 1) as u64
    };

    // Nothing to read for this worker.
    if start >= end {
        return Some((BufReader::new(file), 0));
    }

    // Any worker whose range begins at byte 0 reads from the start with no
    // alignment skip — there's no previous byte to peek at. Worker 0 always
    // hits this; others hit it when `chunk == 0` (peers > file_size), which
    // puts the whole file on the last worker.
    if start == 0 {
        return Some((BufReader::new(file), end));
    }

    // Non-zero start: seek to `start - 1` and peek the byte just before our
    // range. If it's a newline we're on a line boundary; otherwise skip the
    // rest of the partial line.
    if file.seek(SeekFrom::Start(start - 1)).is_err() {
        return Some((BufReader::new(file), 0));
    }

    let mut reader = BufReader::new(file);
    let mut peek = [0u8; 1];
    if reader.read_exact(&mut peek).is_err() {
        return Some((reader, 0));
    }

    if peek[0] == b'\n' {
        // Exactly on a line boundary.
        return Some((reader, end - start));
    }

    // Mid-line: skip the rest of this partial line.
    let mut discard = Vec::new();
    let skipped = reader.read_until(b'\n', &mut discard).unwrap_or(0);
    Some((reader, (end - start).saturating_sub(skipped as u64)))
}

// =========================================================================
// First-column sharding
// =========================================================================

/// Shard an integer-typed first column across `peers` workers.
///
/// Returns `true` if worker `index` should own this tuple.
#[inline]
pub fn shard_int(first: i64, peers: usize, index: usize) -> bool {
    first.rem_euclid(peers as i64) as usize == index
}

/// Shard a string-typed first column across `peers` workers.
///
/// Uses a 32-bit FNV-1a hash to distribute strings uniformly.
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
#[inline]
pub fn shard_spur(first: lasso::Spur, peers: usize, index: usize) -> bool {
    (first.into_inner().get() as usize) % peers == index
}
