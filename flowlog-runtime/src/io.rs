//! I/O and partition helpers used by the generated engine code.
//!
//! - [`partition`] — split an owned `Vec` into per-worker slices for the
//!   library-mode batch engine's ingest path.
//! - [`byte_range_reader`] — split a CSV file across timely workers so each
//!   reads its own byte slice (binary mode).
//! - [`shard_int`] / [`shard_str`] / [`shard_spur`] — pick the owning
//!   worker for a tuple based on its first column (binary mode).
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
use std::io::BufRead;
use std::io::BufReader;
use std::io::Read;
use std::io::Seek;
use std::io::SeekFrom;
use std::path::Path;

use lasso::Spur;

// =========================================================================
// Per-worker partitioning
// =========================================================================

/// Split `v` into `n` roughly-equal owned partitions, in order.
///
/// Used by the generated library-mode engine to hand each timely worker its
/// own slice by value — no `Arc` sharing, no per-tuple clone, each tuple
/// moves directly into the worker's `InputSession`.
///
/// `n.max(1)` partitions are produced; if `v.len() < n` some partitions
/// are empty. The last partition absorbs any remainder when the division
/// doesn't come out evenly.
pub fn partition<T>(v: Vec<T>, n: usize) -> Vec<Vec<T>> {
    let n = n.max(1);
    let chunk = v.len() / n;
    let mut iter = v.into_iter();
    (0..n)
        .map(|i| {
            let take = if i + 1 == n { iter.len() } else { chunk };
            iter.by_ref().take(take).collect()
        })
        .collect()
}

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
    let mut file = File::open(path)
        .inspect_err(|e| {
            eprintln!(
                "[flowlog-runtime::io] failed to open {}: {e}",
                path.display()
            );
        })
        .ok()?;

    let file_size = file
        .metadata()
        .inspect_err(|e| {
            eprintln!(
                "[flowlog-runtime::io] failed to stat {}: {e}",
                path.display()
            );
        })
        .ok()?
        .len();

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
pub fn shard_spur(first: Spur, peers: usize, index: usize) -> bool {
    (first.into_inner().get() as usize) % peers == index
}
