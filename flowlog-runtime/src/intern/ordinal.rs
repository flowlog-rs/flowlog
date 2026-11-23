//! What `ord` reports for a value.
//!
//! [`ord`] is the built-in. It numbers its argument through [`Ordinal`],
//! which a symbol implements with its intern key and a tuple with a
//! `TupleTable` of its own Rust type, found through `TABLES`.

use std::any::Any;
use std::any::TypeId;
use std::cell::RefCell;
use std::hash::Hash;
use std::num::NonZeroU32;
use std::sync::LazyLock;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;

use dashmap::DashMap;
use rustc_hash::FxBuildHasher;
use rustc_hash::FxHashMap;

use crate::intern::Symbol;

// =============================================================================
// ord and Ordinal
// =============================================================================

/// Returns the `int32` that Datalog's `ord` reports for `value`.
///
/// Two values of one type receive the same number exactly when they are
/// equal, and numbers start at 1. Values of different types may share a
/// number, as a record and a symbol may in Souffle. A number is fixed across
/// runs only for a symbol loaded from input; any other value is numbered when
/// a thread first asks for it.
///
/// # Panics
///
/// As [`Ordinal::ord`].
#[must_use]
pub fn ord<V: Ordinal>(value: V) -> i32 {
    // Past `i32::MAX` the number wraps negative instead of failing: the
    // program sees `int32`, and a wrapped number stays distinct, since `as`
    // maps each `u32` to its own `i32`.
    value.ord() as i32
}

/// A value [`ord`] can number.
pub trait Ordinal {
    /// Returns the value's number, unique among the values of its type and
    /// starting at 1.
    ///
    /// # Panics
    ///
    /// For a tuple, if it is new and its type already numbers `u32::MAX`
    /// tuples.
    #[must_use]
    fn ord(self) -> u32;
}

impl Ordinal for Symbol {
    fn ord(self) -> u32 {
        self.ordinal()
    }
}

/// Implements [`Ordinal`] for the tuple of the given field types, numbering
/// it in its type's table.
// A macro because a trait cannot be implemented once for tuples of every
// arity. The arities stop at 12, the widest tuple that implements `Hash`
// and so the widest a FlowLog column can lower to.
macro_rules! impl_tuple_ordinal {
    ($($field:ident),+) => {
        impl<$($field: Hash + Eq + Send + Sync + 'static),+> Ordinal for ($($field,)+) {
            fn ord(self) -> u32 {
                table::<Self>().ordinal(self)
            }
        }
    };
}

impl_tuple_ordinal!(A);
impl_tuple_ordinal!(A, B);
impl_tuple_ordinal!(A, B, C);
impl_tuple_ordinal!(A, B, C, D);
impl_tuple_ordinal!(A, B, C, D, E);
impl_tuple_ordinal!(A, B, C, D, E, F);
impl_tuple_ordinal!(A, B, C, D, E, F, G);
impl_tuple_ordinal!(A, B, C, D, E, F, G, H);
impl_tuple_ordinal!(A, B, C, D, E, F, G, H, I);
impl_tuple_ordinal!(A, B, C, D, E, F, G, H, I, J);
impl_tuple_ordinal!(A, B, C, D, E, F, G, H, I, J, K);
impl_tuple_ordinal!(A, B, C, D, E, F, G, H, I, J, K, L);

// =============================================================================
// TupleTable
// =============================================================================

/// A thread-safe table that numbers the distinct values of one type from 1,
/// in the order the table first receives them.
///
/// The table never forgets a value or renumbers one, and keeps no way back
/// from a number to its value.
// Unlike the string interner, a tuple table keeps no reverse map, so it
// cannot write its tuples to a symbol table the way output writes symbols.
// Nothing needs one: tuple values already reach output as their own
// columns, and a program that wants the number of each tuple outputs that
// mapping as a relation of its own, as DOOP's incremental-output addon does.
// The cost of adding one later is a second store per table.
struct TupleTable<K> {
    ordinals: DashMap<K, NonZeroU32, FxBuildHasher>,
    count: AtomicU32,
}

impl<K> Default for TupleTable<K>
where
    K: Hash + Eq,
{
    fn default() -> Self {
        Self {
            ordinals: DashMap::with_hasher(FxBuildHasher),
            count: AtomicU32::new(0),
        }
    }
}

impl<K> TupleTable<K>
where
    K: Hash + Eq,
{
    /// Returns the ordinal of `key`, numbering it first if no thread has yet.
    ///
    /// Every thread receives the same ordinal for the same key, and a key
    /// numbered after another receives the greater ordinal.
    ///
    /// # Panics
    ///
    /// If `key` is new and the table already numbers `u32::MAX` keys.
    fn ordinal(&self, key: K) -> u32 {
        if let Some(ordinal) = self.ordinals.get(&key) {
            return ordinal.get();
        }
        // `or_insert_with` runs under the shard's write lock and only for a
        // key no thread has numbered, so each count is taken once and the
        // ordinals stay dense.
        let ordinal: NonZeroU32 = *self.ordinals.entry(key).or_insert_with(|| {
            // A refused update leaves the count at its limit, so every later
            // new key is refused too. A `fetch_add` would wrap the count to
            // zero first, and the next key would take ordinal 1 again.
            let taken = self
                .count
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                    count.checked_add(1)
                })
                .unwrap_or_else(|_| {
                    panic!(
                        "ord cannot number more than {} tuples of one type",
                        u32::MAX
                    )
                });
            // `taken` is below `u32::MAX`, so the sum neither saturates nor
            // repeats an ordinal.
            NonZeroU32::MIN.saturating_add(taken)
        });
        ordinal.get()
    }
}

// =============================================================================
// Tables by type
// =============================================================================

/// A table of any type, leaked so that every thread can hold it for the life
/// of the process.
type AnyTable = &'static (dyn Any + Send + Sync);

/// The table of each tuple type `ord` has numbered, keyed by the type.
// One table per type rather than one shared table keyed by type and value:
// each type's numbers stay dense, which keeps `% n` buckets even and each
// type far from the `int32` limit, and two types never contend for one map.
// Souffle also keeps one record table per arity. The cost is this map, since
// a generic function cannot own a static per instantiation, and a lookup in
// it per call, which `TABLE_CACHE` keeps off the shared shards.
static TABLES: LazyLock<DashMap<TypeId, AnyTable, FxBuildHasher>> =
    LazyLock::new(|| DashMap::with_hasher(FxBuildHasher));

// Every `ord` of a tuple looks up its type's table, and every row looks up
// the same few types, so workers reading `TABLES` directly would all take
// the read lock of the same shard, the contention `super::INTERN_CACHE`
// exists to avoid. Each thread therefore remembers the tables it has found,
// at the cost of a small map per thread and a hash of the `TypeId` per call.
// A remembered table never goes stale, because `TABLES` never replaces one.
thread_local! {
    static TABLE_CACHE: RefCell<FxHashMap<TypeId, AnyTable>> = RefCell::new(FxHashMap::default());
}

/// Returns the table that numbers the values of `K`, creating it on first use.
fn table<K>() -> &'static TupleTable<K>
where
    K: Hash + Eq + Send + Sync + 'static,
{
    let id = TypeId::of::<K>();
    let table: AnyTable = TABLE_CACHE.with(|cache| {
        *cache.borrow_mut().entry(id).or_insert_with(|| {
            *TABLES.entry(id).or_insert_with(|| {
                let table: &'static TupleTable<K> = Box::leak(Box::default());
                table
            })
        })
    });
    table
        .downcast_ref()
        .expect("a table is stored under the TypeId of the type it numbers")
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::panic;
    use std::panic::AssertUnwindSafe;
    use std::sync::Barrier;
    use std::thread;

    use super::*;
    use crate::intern::intern;

    #[test]
    fn a_symbol_is_numbered_by_its_intern_key() {
        let symbol = intern("ordinal");
        assert_eq!(symbol.ord(), symbol.ordinal());
    }

    /// `ord` reports a number past `i32::MAX` as the negative `int32` with
    /// the same bits, so it stays distinct from every other number.
    #[test]
    fn ord_wraps_a_number_past_the_int32_range() {
        let symbol = Symbol::from_ordinal(1 << 31).expect("a nonzero ordinal");
        assert_eq!(ord(symbol), i32::MIN);
    }

    #[test]
    fn the_same_tuple_receives_one_ordinal() {
        let table = TupleTable::<(u8, u8)>::default();
        assert_eq!(table.ordinal((1, 2)), table.ordinal((1, 2)));
    }

    #[test]
    fn distinct_tuples_receive_distinct_ordinals() {
        let table = TupleTable::<(u8, u8)>::default();
        assert_ne!(table.ordinal((1, 2)), table.ordinal((2, 1)));
    }

    /// Ordinals number tuples densely from 1, the first ordinal a symbol
    /// receives too.
    #[test]
    fn ordinals_number_tuples_from_one_in_first_asked_order() {
        let table = TupleTable::<(u8,)>::default();
        let ordinals: Vec<u32> = [(7,), (3,), (7,), (9,)]
            .map(|key| table.ordinal(key))
            .to_vec();
        assert_eq!(ordinals, [1, 2, 1, 3]);
    }

    /// Threads asking for the same fresh tuples in different orders agree
    /// on each tuple's ordinal, and the ordinals are exactly 1 to N. The key
    /// type is private to the test, so no other test shares its table.
    #[test]
    fn concurrent_threads_agree_on_dense_ordinals() {
        #[derive(Clone, Copy, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
        struct Key(u8);

        const KEYS: u8 = 16;
        const THREADS: usize = 8;
        let barrier = Barrier::new(THREADS);
        let views: Vec<BTreeMap<Key, i32>> = thread::scope(|scope| {
            let workers: Vec<_> = (0..THREADS)
                .map(|worker| {
                    let barrier = &barrier;
                    scope.spawn(move || {
                        // Each thread starts at its own key and wraps around.
                        let offset = u8::try_from(worker).expect("fewer than 256 threads");
                        barrier.wait();
                        (0..KEYS)
                            .map(|i| Key((i + offset) % KEYS))
                            .map(|key| (key, ord((key,))))
                            .collect()
                    })
                })
                .collect();
            workers
                .into_iter()
                .map(|worker| worker.join().expect("worker finished"))
                .collect()
        });

        assert!(views.iter().all(|view| *view == views[0]), "{views:?}");
        let mut ordinals: Vec<i32> = views[0].values().copied().collect();
        ordinals.sort_unstable();
        assert_eq!(ordinals, (1..=i32::from(KEYS)).collect::<Vec<_>>());
    }

    /// A table that has numbered `u32::MAX` keys refuses every new one,
    /// not only the first, and still answers for the keys it holds.
    #[test]
    fn a_full_table_keeps_refusing_new_tuples() {
        let table = TupleTable::<(u8,)> {
            ordinals: DashMap::with_hasher(FxBuildHasher),
            count: AtomicU32::new(u32::MAX - 1),
        };
        assert_eq!(table.ordinal((1,)), u32::MAX);
        for key in [2, 3] {
            let refused = panic::catch_unwind(AssertUnwindSafe(|| table.ordinal((key,))));
            assert!(refused.is_err(), "key {key} was numbered past the limit");
        }
        assert_eq!(table.ordinal((1,)), u32::MAX);
    }

    /// Each tuple type numbers from 1 in a table of its own, so one type's
    /// values do not move another's ordinals. The field types are private to
    /// the test, so no other test shares their tables.
    #[test]
    fn each_tuple_type_numbers_in_its_own_table() {
        #[derive(Hash, PartialEq, Eq)]
        struct Left(u8);
        #[derive(Hash, PartialEq, Eq)]
        struct Right(u8);

        assert_eq!((Left(5),).ord(), 1);
        assert_eq!((Left(6),).ord(), 2);
        assert_eq!((Right(6),).ord(), 1);
        assert_eq!((Left(5),).ord(), 1);
    }
}
