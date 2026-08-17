//! Thread-safe string interning via `lasso::ThreadedRodeo`.

use std::sync::LazyLock;
use std::sync::OnceLock;

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

/// Intern a string, returning its [`Spur`] handle.
///
/// # Panics
///
/// If the interner is out of room for good: its key space is spent, or an
/// allocation failed. That is the same class as an allocation failure
/// anywhere else, which Rust aborts on rather than reporting, and a run
/// cannot continue past it either way, since every later string needs a
/// key too.
#[inline(always)]
pub fn intern(s: &str) -> Spur {
    match INTERNER.try_get_or_intern(s) {
        Ok(key) => key,
        // No retry: the interner blocks while another thread holds it and
        // only reports a failure once it is out of room permanently.
        Err(e) => panic!("the string interner refused {s:?}: {}", e.kind()),
    }
}

/// Resolve a [`Spur`] handle back to a `&'static str`.
#[inline(always)]
pub fn resolve(key: Spur) -> &'static str {
    INTERNER.resolve(&key)
}

/// Flat snapshot of the interner (`Spur::into_usize()` to string) used for
/// O(1) resolution at output/drain time. `Spur` keys are dense in
/// `[0, len)`, so a plain `Vec` index replaces the concurrent
/// [`ThreadedRodeo::resolve`] path (which hashes the key and takes a
/// `DashMap` read lock on every call).
///
/// Slots are `Option`, not a `""` sentinel: an interned empty string is a
/// legitimate value, so a sentinel could not be told apart from a slot the
/// snapshot never filled.
static RESOLVED: OnceLock<Box<[Option<&'static str>]>> = OnceLock::new();

/// Build the flat snapshot from the current interner contents.
///
/// `INTERNER` is borrowed from a `static`, so its strings are genuinely
/// `'static`; the dense `Spur` keying lets us address them by index.
fn build_snapshot() -> Box<[Option<&'static str>]> {
    let mut table: Vec<Option<&'static str>> = vec![None; INTERNER.len()];
    for (key, string) in INTERNER.iter() {
        // The interner is concurrent, so a key minted around this walk can
        // land past the table, and one counted by `len` can still be
        // invisible to `iter` and leave its slot unfilled. Both cases read
        // back as a miss, and [`resolve_out`] falls back to [`resolve`].
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
/// complete in batch mode. A key the snapshot does not hold, whether interned
/// after it was taken or missed by the concurrent walk that built it, falls
/// back to [`resolve`] and stays correct without a rebuild.
#[inline]
pub fn resolve_out(key: Spur) -> &'static str {
    let table = RESOLVED.get_or_init(build_snapshot);
    match table.get(key.into_usize()) {
        Some(&Some(string)) => string,
        _ => resolve(key),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty string is an ordinary interned value, which is why the
    /// snapshot cannot use `""` to mark a slot it never filled: a hole and
    /// a legitimately empty column would be indistinguishable, and the
    /// column would silently resolve to the wrong value.
    #[test]
    fn an_interned_empty_string_resolves_to_empty() {
        let key = intern("");
        assert_eq!(resolve(key), "");
        assert_eq!(resolve_out(key), "");
    }

    /// A key minted after the snapshot was taken falls back rather than
    /// reading off its end.
    #[test]
    fn a_key_past_the_snapshot_still_resolves() {
        resolve_out(intern("before-the-snapshot"));
        let later = intern("minted-after-the-snapshot");
        assert_eq!(resolve_out(later), "minted-after-the-snapshot");
    }
}
