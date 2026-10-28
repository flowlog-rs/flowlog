//! Thread-safe string interning via `lasso::ThreadedRodeo`.

use std::sync::LazyLock;
use std::sync::OnceLock;
use std::thread;

use lasso::Key;
use lasso::Spur;
use lasso::ThreadedRodeo;
use rustc_hash::FxBuildHasher;

/// Global string interner shared across all FlowLog engines in the process.
///
/// Uses `FxBuildHasher` instead of lasso's default SipHash: interner keys are
/// program-controlled (`.dl` literals + input facts), not adversarial, so
/// SipHash's HashDoS resistance is pure per-byte overhead on every intern and
/// resolve.
///
/// **Limitation**: this is a process-local pool. In a distributed DD
/// deployment (multiple machines), each process gets its own independent
/// `INTERNER`, so `Spur` values are NOT comparable across machines.
/// Distributed support would require a global interning protocol or
/// switching back to `String`-keyed collections.
pub static INTERNER: LazyLock<ThreadedRodeo<Spur, FxBuildHasher>> =
    LazyLock::new(|| ThreadedRodeo::with_hasher(FxBuildHasher));

const MAX_RETRIES: usize = 1024;

/// Intern a string, returning its [`Spur`] handle.
#[inline(always)]
pub fn intern(s: &str) -> Spur {
    for _ in 0..MAX_RETRIES {
        match INTERNER.try_get_or_intern(s) {
            Ok(key) => return key,
            Err(_) => thread::yield_now(),
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
/// `DashMap` read lock on every call). A slot is `None` when its string was
/// still being interned while the snapshot was built.
static RESOLVED: OnceLock<Box<[Option<&'static str>]>> = OnceLock::new();

/// Build the flat snapshot from the current interner contents.
///
/// `INTERNER` is borrowed from a `static`, so its strings are genuinely
/// `'static`; the dense `Spur` keying lets us address them by index.
fn build_snapshot() -> Box<[Option<&'static str>]> {
    let mut table: Vec<Option<&'static str>> = vec![None; INTERNER.len()];
    for (key, string) in INTERNER.iter() {
        // Neither the length nor the walk is atomic, and keys can be stored
        // out of order, so while other threads intern, the walk can meet a
        // key past the length read above. `resolve_out` resolves such a key
        // through the interner instead.
        if let Some(slot) = table.get_mut(key.into_usize()) {
            *slot = Some(string);
        }
    }
    table.into_boxed_slice()
}

/// Resolve a [`Spur`] at output time via a flat index instead of the
/// concurrent `DashMap` path taken by [`resolve`].
///
/// Built lazily on first use. Output runs after fixpoint, so the snapshot is
/// complete in batch mode. Keys interned later (e.g. new epochs in incremental
/// mode), or while the snapshot was built, are missing from it and fall back
/// to [`resolve`], staying correct without a rebuild.
#[inline]
pub fn resolve_out(key: Spur) -> &'static str {
    let table = RESOLVED.get_or_init(build_snapshot);
    match table.get(key.into_usize()) {
        Some(Some(string)) => string,
        Some(None) | None => resolve(key),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;

    use super::*;

    /// A snapshot built while other threads intern holds, for each key it
    /// has, the string that key resolves to. Drives `build_snapshot`
    /// directly: `resolve_out` builds its snapshot once per process, so a
    /// test cannot make that build race the interning.
    #[test]
    fn snapshot_built_while_interning_holds_only_resolved_strings() {
        const WRITERS: usize = 4;
        const STRINGS_PER_WRITER: usize = 20_000;
        let finished = AtomicUsize::new(0);
        thread::scope(|scope| {
            for writer in 0..WRITERS {
                let finished = &finished;
                // A fixed count, not a stop flag: a panicking snapshot
                // below must not leave the scope waiting on a writer.
                scope.spawn(move || {
                    for i in 0..STRINGS_PER_WRITER {
                        intern(&format!("snapshot race {writer} {i}"));
                    }
                    finished.fetch_add(1, Ordering::Release);
                });
            }
            while finished.load(Ordering::Acquire) < WRITERS {
                let table = build_snapshot();
                for (index, slot) in table.iter().enumerate() {
                    let key = Spur::try_from_usize(index).expect("a snapshot index is a key");
                    if let Some(string) = slot {
                        assert_eq!(*string, resolve(key), "snapshot slot {index}");
                    }
                }
            }
        });
    }
}
