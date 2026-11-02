//! Thread-safe string interning.
//!
//! [`intern`] and [`resolve`] answer a repeated lookup from caches owned by
//! the calling thread and fall back to the shared [`INTERNER`], a table that
//! numbers every distinct string by a [`Symbol`].

use std::cell::Cell;
use std::hash::BuildHasher;
use std::num::NonZeroU32;
use std::sync::LazyLock;

use rustc_hash::FxBuildHasher;
use serde::Deserialize;
use serde::Serialize;

use crate::intern::table::Interner;

mod table;

// =============================================================================
// Symbol
// =============================================================================

/// The key of an interned string: its position in the shared table, plus one.
///
/// Two symbols are equal exactly when their texts are. Symbols order by
/// position, the order in which the process first interned their texts, not
/// by text. The representation is a `NonZeroU32`, so an `Option<Symbol>`
/// costs no more than a `Symbol`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(transparent)]
#[repr(transparent)]
pub struct Symbol(NonZeroU32);

impl Symbol {
    /// Returns the position of the symbol's text in the shared table.
    #[must_use]
    pub fn index(self) -> usize {
        self.0.get() as usize - 1
    }

    /// Returns the symbol's number, its position plus one: the identity that
    /// `ord` reports for its text.
    #[must_use]
    pub fn ordinal(self) -> u32 {
        self.0.get()
    }

    /// Returns the symbol of the text at `index`, or `None` if `index` lies
    /// beyond the last position a symbol can name.
    #[must_use]
    fn from_index(index: usize) -> Option<Self> {
        let key = u32::try_from(index).ok()?.checked_add(1)?;
        NonZeroU32::new(key).map(Self)
    }
}

// =============================================================================
// Shared interner and per-thread caches
// =============================================================================

/// Global string interner shared across all FlowLog engines in the process.
///
/// **Limitation**: this is a process-local pool. In a distributed DD
/// deployment (multiple machines), each process gets its own independent
/// `INTERNER`, so `Symbol` values are NOT comparable across machines.
/// Distributed support would require a global interning protocol or
/// switching back to `String`-keyed collections.
static INTERNER: LazyLock<Interner> = LazyLock::new(Interner::default);

// An `intern` that reaches `INTERNER` hashes the string and takes a read lock
// on a `DashMap` shard, and generated code looks strings up per row: a string
// literal in a rule is interned for every row the rule derives, and a string
// passed to a user-defined function is resolved on every call. Workers that
// look up the same hot strings all write the same lock words, so with many
// workers the lookups queue on a few cache lines. Each thread therefore
// answers repeated lookups from caches of its own, which no other thread
// writes. A cached answer never goes stale, because the interner never frees
// a string or reassigns a symbol. The cost is the caches' memory on every
// thread that interns or resolves (see `CACHE_SLOTS`) and, on an `intern`
// miss, a hash of the string that `INTERNER` then computes again. A `resolve`
// that reaches `INTERNER` takes no lock, only an index into its table, so its
// cache saves less; it stays until a measurement shows it costs more than it
// saves.
//
// Both caches are direct-mapped: a lookup reads the one slot its key or hash
// picks, and a miss overwrites that slot. Two hot strings that share a slot
// then evict each other on every lookup, and each pays a shared lookup, no
// more than without the caches. A set-associative cache would evict less,
// but would compare several entries on every lookup, and the lookup is
// inlined into every call site in generated code.

/// Slot count of each per-thread cache.
// Each slot takes 40 bytes, 24 in `RESOLVE_CACHE` and 16 in `INTERN_CACHE`,
// so every thread that interns or resolves holds 640 KiB. The caches pay off
// only on the strings a program looks up again and again, the literals of
// its rules and the values it joins on, and a direct-mapped cache keeps them
// only while few of them share a slot: with slots well beyond that hot set,
// a hot string rarely evicts another, and fewer slots bring back shared
// lookups. More slots than that buy little and cost memory on every such
// thread. Keep it a power of two, so that `%` compiles to a mask. Tests use
// a handful of slots, so that a few strings already evict one another.
const CACHE_SLOTS: usize = if cfg!(test) { 1 << 4 } else { 1 << 14 };

/// A cached [`resolve`] answer; `key` is `None` until the slot is filled.
#[derive(Clone, Copy)]
struct ResolveSlot {
    key: Option<Symbol>,
    string: &'static str,
}

/// A cached [`intern`] answer for a string whose `FxBuildHasher` hash is
/// `hash`; `key` is `None` until the slot is filled.
#[derive(Clone, Copy)]
struct InternSlot {
    hash: u64,
    key: Option<Symbol>,
}

thread_local! {
    static RESOLVE_CACHE: Box<[Cell<ResolveSlot>]> =
        vec![Cell::new(ResolveSlot { key: None, string: "" }); CACHE_SLOTS].into_boxed_slice();
    static INTERN_CACHE: Box<[Cell<InternSlot>]> =
        vec![Cell::new(InternSlot { hash: 0, key: None }); CACHE_SLOTS].into_boxed_slice();
}

/// Returns the symbol of `s`, interning `s` first if no thread has yet.
///
/// Every thread receives the same symbol for the same text.
///
/// # Panics
///
/// If `s` is new and the shared interner already holds `u32::MAX` strings.
#[inline(always)]
pub fn intern(s: &str) -> Symbol {
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
// the cache lookup but not the shared table's lookup and insertion.
#[inline(never)]
fn intern_shared(s: &str) -> Symbol {
    INTERNER.intern(s)
}

/// Returns the text interned under `key`.
///
/// # Panics
///
/// If no string was interned under `key`.
#[inline(always)]
pub fn resolve(key: Symbol) -> &'static str {
    RESOLVE_CACHE.with(|cache| {
        let slot = &cache[key.index() % CACHE_SLOTS];
        let cached = slot.get();
        if cached.key == Some(key) {
            return cached.string;
        }
        let string = resolve_shared(key);
        slot.set(ResolveSlot {
            key: Some(key),
            string,
        });
        string
    })
}

/// Resolves `key` in [`INTERNER`], bypassing the per-thread caches.
///
/// # Panics
///
/// As [`resolve`].
// Out of line, so that the code generated for each `resolve` call carries
// the cache lookup but not the shared table's lookup and its panic.
#[inline(never)]
fn resolve_shared(key: Symbol) -> &'static str {
    INTERNER
        .resolve(key)
        .unwrap_or_else(|| panic!("no string was interned under symbol {}", key.ordinal()))
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn resolve_returns_the_interned_text() {
        assert_eq!(resolve(intern("roundtrip")), "roundtrip");
    }

    #[test]
    fn a_symbol_names_the_index_it_was_made_from() {
        let symbol = Symbol::from_index(41).expect("fits a symbol");
        assert_eq!(symbol.index(), 41);
    }

    #[test]
    fn an_ordinal_is_the_index_plus_one() {
        let symbol = Symbol::from_index(0).expect("fits a symbol");
        assert_eq!(symbol.ordinal(), 1);
    }

    #[test]
    fn the_index_past_the_last_symbol_makes_no_symbol() {
        assert_eq!(Symbol::from_index(u32::MAX as usize), None);
    }

    /// Twice as many strings as slots: by pigeonhole, slots of both caches
    /// answer for more than one string, and the second pass finds answers
    /// evicted.
    #[test]
    fn evicted_strings_keep_their_keys() {
        let texts: Vec<String> = (0..2 * CACHE_SLOTS)
            .map(|i| format!("evicted-{i}"))
            .collect();
        let keys: Vec<Symbol> = texts.iter().map(|text| intern(text)).collect();
        for (text, &key) in texts.iter().zip(&keys) {
            assert_eq!(intern(text), key);
            assert_eq!(resolve(key), text);
        }
    }

    /// A thread whose caches hold nothing yet answers from the shared
    /// interner: the same key for the same text, and the text of a key
    /// another thread interned.
    #[test]
    fn a_new_thread_agrees_on_keys_and_texts() {
        let key = intern("handed over");
        let (same, text) = thread::spawn(move || (intern("handed over"), resolve(key)))
            .join()
            .expect("the other thread panicked");
        assert_eq!((same, text), (key, "handed over"));
    }

    /// Different strings can share a hash, so a slot holding another
    /// string's key under the same hash is a miss: only the text confirms a
    /// hit. Two texts with one 64-bit hash are impractical to find, so the
    /// test plants the collision in the slot.
    #[test]
    fn a_shared_hash_with_another_text_is_a_miss() {
        let other = intern("collision-other");
        let hash = FxBuildHasher.hash_one("collision-text");
        INTERN_CACHE.with(|cache| {
            cache[hash as usize % CACHE_SLOTS].set(InternSlot {
                hash,
                key: Some(other),
            });
        });
        let key = intern("collision-text");
        assert_ne!(key, other);
        assert_eq!(resolve(key), "collision-text");
    }

    #[test]
    #[should_panic(expected = "no string was interned under symbol")]
    fn resolving_a_key_never_interned_panics() {
        let key = Symbol::from_index(u32::MAX as usize - 1).expect("fits a symbol");
        resolve(key);
    }
}
