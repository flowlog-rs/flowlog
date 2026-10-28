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

/// Flat snapshot of the interner, indexed by `Spur::into_usize()`, for
/// resolving strings at output and drain time.
///
/// [`ThreadedRodeo::resolve`] hashes the key and takes a `DashMap` read lock
/// on every call, and output resolves every string column of every row. We
/// copy the interner once into a slice indexed by the dense `Spur` keys, so
/// each lookup is a bounds check, accepting that a key interned after the
/// copy misses it and pays the `DashMap` path. A slot is `None` when its key
/// was taken but its string not yet stored while the copy ran.
static RESOLVED: OnceLock<Box<[Option<&'static str>]>> = OnceLock::new();

/// Copy every string the interner holds into a slice indexed by key.
///
/// A string interned while the copy runs may be missing: its slot is `None`,
/// or its key lies past the end of the slice.
fn build_snapshot() -> Box<[Option<&'static str>]> {
    let mut table: Vec<Option<&'static str>> = vec![None; INTERNER.len()];
    for (key, string) in INTERNER.iter() {
        // `ThreadedRodeo` takes a key from a counter before it stores the
        // string, so two threads can store their keys out of order, and
        // neither `len` nor `iter` is atomic. While other threads intern,
        // the walk can meet a key at or past the length read above; skip it.
        if let Some(slot) = table.get_mut(key.into_usize()) {
            *slot = Some(string);
        }
    }
    table.into_boxed_slice()
}

/// Resolve a [`Spur`] as [`resolve`] does, through a snapshot of the interner
/// taken on the first call.
///
/// Output runs after fixpoint, so in batch mode the snapshot holds every key.
/// A key it lacks, interned later (a new epoch in incremental mode) or while
/// the snapshot was taken, falls back to [`resolve`].
#[inline]
pub fn resolve_out(key: Spur) -> &'static str {
    let table = RESOLVED.get_or_init(build_snapshot);
    match table.get(key.into_usize()) {
        Some(Some(string)) => string,
        // A caller holds a key only after `intern` stored its string, so the
        // interner resolves a key the snapshot lacks.
        Some(None) | None => resolve(key),
    }
}
