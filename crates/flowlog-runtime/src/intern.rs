//! Thread-safe string interning via `lasso::ThreadedRodeo`.

use lasso::{Key, Spur, ThreadedRodeo};
use std::sync::{LazyLock, OnceLock};

/// Global string interner shared across all FlowLog engines in the process.
///
/// **Limitation**: this is a process-local pool. In a distributed DD
/// deployment (multiple machines), each process gets its own independent
/// `INTERNER`, so `Spur` values are NOT comparable across machines.
/// Distributed support would require a global interning protocol or
/// switching back to `String`-keyed collections.
pub static INTERNER: LazyLock<ThreadedRodeo> = LazyLock::new(ThreadedRodeo::default);

const MAX_RETRIES: usize = 1024;

/// Intern a string, returning its [`Spur`] handle.
#[inline(always)]
pub fn intern(s: &str) -> Spur {
    for _ in 0..MAX_RETRIES {
        match INTERNER.try_get_or_intern(s) {
            Ok(key) => return key,
            Err(_) => std::thread::yield_now(),
        }
    }
    panic!("string interner failed after {MAX_RETRIES} attempts for {s:?}");
}

/// Resolve a [`Spur`] handle back to a `&'static str`.
#[inline(always)]
pub fn resolve(key: Spur) -> &'static str {
    INTERNER.resolve(&key)
}

/// Flat snapshot of the interner (`Spur::into_usize()` → string) used for
/// O(1) resolution at output/drain time. `Spur` keys are dense in
/// `[0, len)`, so a plain `Vec` index replaces the concurrent
/// [`ThreadedRodeo::resolve`] path (which hashes the key and takes a
/// `DashMap` read lock on every call).
static RESOLVED: OnceLock<Box<[&'static str]>> = OnceLock::new();

/// Build the flat snapshot from the current interner contents.
///
/// `INTERNER` is borrowed from a `static`, so its strings are genuinely
/// `'static`; the dense `Spur` keying lets us address them by index.
fn build_snapshot() -> Box<[&'static str]> {
    let mut table: Vec<&'static str> = vec![""; INTERNER.len()];
    for (key, string) in INTERNER.iter() {
        table[key.into_usize()] = string;
    }
    table.into_boxed_slice()
}

/// Resolve a [`Spur`] at output time via a flat index instead of the
/// concurrent `DashMap` path taken by [`resolve`].
///
/// The snapshot is built lazily on first use. Output runs after the
/// dataflow reaches fixpoint (no concurrent interning), so the snapshot is
/// complete in batch mode. Keys interned after the snapshot was frozen
/// (e.g. later epochs in incremental mode) fall outside its range and fall
/// back to [`resolve`], keeping resolution correct without a rebuild.
#[inline]
pub fn resolve_out(key: Spur) -> &'static str {
    let table = RESOLVED.get_or_init(build_snapshot);
    match table.get(key.into_usize()) {
        Some(&string) => string,
        None => resolve(key),
    }
}
