//! Thread-safe string interning via `lasso::ThreadedRodeo`.
//!
//! [`intern`] and [`resolve`] answer a repeated lookup from caches owned by
//! the calling thread and fall back to the shared [`INTERNER`]; output reads
//! strings through [`resolve_out`], from a copy of the interner taken once.

use std::cell::Cell;
use std::hash::BuildHasher;
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

// Every lookup in `INTERNER` takes a read lock on a `DashMap` shard, and
// generated code looks strings up per row: a string literal in a rule is
// interned for every row the rule derives, and a string passed to a
// user-defined function is resolved on every call. Workers that look up the
// same hot strings all write the same lock words, so with many workers the
// lookups queue on a few cache lines. Each thread therefore answers repeated
// lookups from caches of its own, which no other thread writes. A cached
// answer never goes stale, because the interner never frees a string or
// reassigns a key. The cost is the caches' memory on every thread that
// interns or resolves (see `CACHE_SLOTS`) and, on an `intern` miss, a hash of
// the string that `INTERNER` then computes again.

/// Slot count of each per-thread cache.
// Each slot takes 40 bytes, 24 in `RESOLVE_CACHE` and 16 in `INTERN_CACHE`,
// so every thread that interns or resolves holds 640 KiB. Picked by a sweep
// on a string-heavy taint analysis (CTADL) with 32 workers: from 2^10 to
// 2^18 slots, its index took 2.8-2.9 s and its query 5.1-5.3 s of dataflow
// time, against 12.9 s and 10.6 s without the caches; 2^8 slots took 6%
// longer on the index. 2^14 is 16 times the smallest size on that plateau,
// as a margin for programs with more hot strings. More slots cost memory on
// every such thread for little gain: 2^16 slots, 2.5 MiB, took 2% less time
// on the query in a direct comparison, and 2^18 added 370 MiB of peak memory
// at 32 workers. Fewer slots bring back shared lookups. Keep it a power of
// two, so that `%` compiles to a mask.
const CACHE_SLOTS: usize = 1 << 14;

/// A cached [`resolve`] answer; `key` is `None` until the slot is filled.
#[derive(Clone, Copy)]
struct ResolveSlot {
    key: Option<Spur>,
    string: &'static str,
}

/// A cached [`intern`] answer for a string whose `FxBuildHasher` hash is
/// `hash`; `key` is `None` until the slot is filled.
#[derive(Clone, Copy)]
struct InternSlot {
    hash: u64,
    key: Option<Spur>,
}

thread_local! {
    static RESOLVE_CACHE: Box<[Cell<ResolveSlot>]> =
        vec![Cell::new(ResolveSlot { key: None, string: "" }); CACHE_SLOTS].into_boxed_slice();
    static INTERN_CACHE: Box<[Cell<InternSlot>]> =
        vec![Cell::new(InternSlot { hash: 0, key: None }); CACHE_SLOTS].into_boxed_slice();
}

/// Returns the key of `s`, interning `s` first if no thread has yet.
///
/// Every thread receives the same key for the same text.
///
/// # Panics
///
/// If the shared interner keeps failing to store `s` because it has run out
/// of keys or memory.
#[inline(always)]
pub fn intern(s: &str) -> Spur {
    let hash = FxBuildHasher.hash_one(s);
    INTERN_CACHE.with(|cache| {
        let slot = &cache[hash as usize % CACHE_SLOTS];
        let cached = slot.get();
        // Different strings can share a hash, so only the text confirms a hit.
        if let Some(key) = cached.key
            && cached.hash == hash
            && resolve(key) == s
        {
            return key;
        }
        let key = intern_shared(s);
        slot.set(InternSlot {
            hash,
            key: Some(key),
        });
        key
    })
}

/// Interns `s` in [`INTERNER`], bypassing the per-thread caches.
///
/// # Panics
///
/// As [`intern`].
// Out of line, so that the code generated for each `intern` call carries
// the cache lookup but not this retry loop.
fn intern_shared(s: &str) -> Spur {
    for _ in 0..MAX_RETRIES {
        match INTERNER.try_get_or_intern(s) {
            Ok(key) => return key,
            Err(_) => thread::yield_now(),
        }
    }
    panic!("string interner failed after {MAX_RETRIES} attempts for {s:?}");
}

/// Returns the text interned under `key`.
///
/// # Panics
///
/// If no string was interned under `key`.
#[inline(always)]
pub fn resolve(key: Spur) -> &'static str {
    RESOLVE_CACHE.with(|cache| {
        let slot = &cache[key.into_usize() % CACHE_SLOTS];
        let cached = slot.get();
        if cached.key == Some(key) {
            return cached.string;
        }
        let string = INTERNER.resolve(&key);
        slot.set(ResolveSlot {
            key: Some(key),
            string,
        });
        string
    })
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use rstest::rstest;

    use super::*;

    #[rstest]
    #[case::empty("")]
    #[case::ascii("roundtrip")]
    #[case::non_ascii("\u{e9}t\u{e9}")]
    fn resolve_returns_the_interned_text(#[case] text: &str) {
        assert_eq!(resolve(intern(text)), text);
    }

    /// Twice as many strings as slots: by pigeonhole, slots of both caches
    /// answer for more than one string, and the second pass finds answers
    /// evicted.
    #[test]
    fn evicted_strings_keep_their_keys() {
        let texts: Vec<String> = (0..2 * CACHE_SLOTS)
            .map(|i| format!("evicted-{i}"))
            .collect();
        let keys: Vec<Spur> = texts.iter().map(|text| intern(text)).collect();
        let distinct: HashSet<Spur> = keys.iter().copied().collect();
        assert_eq!(distinct.len(), texts.len(), "two strings share a key");
        for (text, &key) in texts.iter().zip(&keys) {
            assert_eq!(intern(text), key);
            assert_eq!(resolve(key), text);
        }
    }

    /// Each thread fills caches of its own, so the threads interning the same
    /// strings at once must still agree on every key.
    #[test]
    fn threads_agree_on_every_key() {
        let texts: Vec<String> = (0..1000).map(|i| format!("shared-{i}")).collect();
        let intern_all = || -> Vec<Spur> { texts.iter().map(|text| intern(text)).collect() };
        let per_thread: Vec<Vec<Spur>> = thread::scope(|scope| {
            let handles: Vec<thread::ScopedJoinHandle<'_, Vec<Spur>>> =
                (0..8).map(|_| scope.spawn(intern_all)).collect();
            handles
                .into_iter()
                .map(|handle| handle.join().expect("interning thread panicked"))
                .collect()
        });
        let expected = intern_all();
        for keys in per_thread {
            assert_eq!(keys, expected);
        }
    }

    #[test]
    fn a_key_resolves_on_a_thread_that_never_interned_it() {
        let key = intern("handed over");
        let text = thread::spawn(move || resolve(key))
            .join()
            .expect("resolving thread panicked");
        assert_eq!(text, "handed over");
    }

    #[test]
    #[should_panic(expected = "Key out of bounds")]
    fn resolving_a_key_never_interned_panics() {
        let key = Spur::try_from_usize(u32::MAX as usize - 1).expect("fits a Spur");
        resolve(key);
    }
}
