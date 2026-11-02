//! The shared table behind [`super::intern`] and [`super::resolve`].

use dashmap::DashMap;
use rustc_hash::FxBuildHasher;

use crate::intern::Symbol;

// =============================================================================
// Interner
// =============================================================================

/// A thread-safe table of strings, each stored once and numbered in the
/// order the table first received it.
///
/// The table never frees a string or reassigns a [`Symbol`], so a text
/// borrowed from it lives as long as the process.
pub(super) struct Interner {
    symbols: DashMap<&'static str, Symbol, FxBuildHasher>,
    texts: boxcar::Vec<&'static str>,
}

impl Default for Interner {
    fn default() -> Self {
        // Interned texts come from the program (`.dl` literals and input
        // facts), never from an adversary, so we take `FxBuildHasher` over
        // the default SipHash for the per-byte win, accepting that the table
        // would be wide open to HashDoS if untrusted input ever reached it.
        Self {
            symbols: DashMap::with_hasher(FxBuildHasher),
            texts: boxcar::Vec::new(),
        }
    }
}

impl Interner {
    /// Returns the symbol of `s`, storing `s` first if no thread has yet.
    ///
    /// Every thread receives the same symbol for the same text, and a text
    /// stored after another receives the greater symbol.
    ///
    /// # Panics
    ///
    /// If `s` is new and the table already holds `u32::MAX` strings.
    pub(super) fn intern(&self, s: &str) -> Symbol {
        if let Some(symbol) = self.symbols.get(s) {
            return *symbol;
        }
        // The map owns its key for the life of the process, so the text is
        // leaked rather than arena-packed: each distinct string costs one
        // allocation, on its first `intern` only, and no code here is
        // `unsafe`. The entry API needs that owned key before it says whether
        // the text is new, so when two threads store the same new text at
        // once, the loser's copy leaks too; such races are rare, and the
        // table frees nothing anyway.
        let text: &'static str = Box::leak(Box::from(s));
        *self.symbols.entry(text).or_insert_with(|| {
            let index = self.texts.push(text);
            Symbol::from_index(index).unwrap_or_else(|| {
                panic!("string interner cannot hold more than {} strings", u32::MAX)
            })
        })
    }

    /// Returns the text stored under `symbol`, or `None` if the table has
    /// not stored one.
    #[must_use]
    pub(super) fn resolve(&self, symbol: Symbol) -> Option<&'static str> {
        self.texts.get(symbol.index()).copied()
    }
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn the_same_text_receives_one_symbol() {
        let table = Interner::default();
        assert_eq!(table.intern("same"), table.intern("same"));
    }

    #[test]
    fn distinct_texts_receive_distinct_symbols() {
        let table = Interner::default();
        assert_ne!(table.intern("one"), table.intern("two"));
    }

    /// Symbols number texts densely from the first index in storing order,
    /// which lets a caller index a table of its own by symbol.
    #[test]
    fn symbols_number_texts_in_storing_order() {
        let table = Interner::default();
        let indices: Vec<usize> = ["first", "second", "third"]
            .map(|text| table.intern(text).index())
            .to_vec();
        assert_eq!(indices, [0, 1, 2]);
    }

    #[test]
    fn resolve_returns_the_stored_text() {
        let table = Interner::default();
        let symbol = table.intern("stored");
        assert_eq!(table.resolve(symbol), Some("stored"));
    }

    #[test]
    fn resolve_is_none_for_a_symbol_the_table_never_stored() {
        let table = Interner::default();
        let symbol = Symbol::from_index(7).expect("fits a symbol");
        assert_eq!(table.resolve(symbol), None);
    }

    /// Threads storing one set of new texts at once agree on every symbol
    /// and store each text once, however the races go.
    #[test]
    fn racing_threads_agree_on_symbols() {
        const THREADS: usize = 8;
        const TEXTS: usize = 256;
        let table = Interner::default();
        let texts: Vec<String> = (0..TEXTS).map(|i| format!("raced-{i}")).collect();
        let answers: Vec<Vec<Symbol>> = thread::scope(|scope| {
            let workers: Vec<_> = (0..THREADS)
                .map(|_| scope.spawn(|| texts.iter().map(|text| table.intern(text)).collect()))
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().expect("a worker panicked"))
                .collect()
        });
        for answer in &answers {
            assert_eq!(answer, &answers[0]);
        }
        let mut indices: Vec<usize> = answers[0].iter().map(|symbol| symbol.index()).collect();
        indices.sort_unstable();
        assert_eq!(indices, (0..TEXTS).collect::<Vec<_>>());
        for (text, &symbol) in texts.iter().zip(&answers[0]) {
            assert_eq!(table.resolve(symbol), Some(text.as_str()));
        }
    }
}
