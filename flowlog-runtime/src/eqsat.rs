//! Equality saturation: the rebuild behind `union<T>` relations.
//!
//! An e-graph here is a set of tables plus a union-find over e-class ids.
//! A `union<T>` table maps its key (every other column) to one e-class id;
//! a set table has no union column and keeps each canonical row once; an
//! `eqrel` relation stores nothing and turns each row `(a, b)` into a union.
//!
//! The loop runs one round per egglog iteration. Rules at round `k` read the
//! canonical tables of round `k - 1` and their outputs, together with the
//! round's seeds, go to [`rebuild`], which inserts them, merges e-classes
//! on key collisions, and repairs rows until every table is canonical
//! again (congruence closure). Its output is the exact change to the
//! canonical tables, `-old` and `+new`, so the tables in the loop always
//! hold one copy of each canonical row.
//!
//! The rebuild is sharded. Rows live on the worker that owns the hash of
//! their table and canonical key; each worker keeps a full copy of the
//! union-find and applies the same broadcast unions, so any worker can
//! canonicalize any row. One round runs as passes of a nested scope: a pass
//! applies the unions found by the previous pass, repairs the rows that name
//! a merged-away id, and inserts the rows routed to it, which may find more
//! collisions. A round ends when a pass finds nothing new.
//!
//! Fresh terms come from [`skolem`], which a rule computes from a tag and
//! the canonical ids it builds on. The rebuild stamps each fresh id with one
//! past the round that made it, so ids order by age: facts' own ids, below
//! [`ID_LIMIT`], first. The smaller id of a merge leads, so an existing
//! class always absorbs a fresh duplicate and only rows of the current
//! round need repair. A round asserts only the rows its updates sum to a
//! positive count, so a later round can assert a term in one row while the
//! row defining it cancels against a lost match. The worker owning a fresh
//! id remembers its first stamp and unions any later one with it, so every
//! stamp of a term names one class.
//!
//! Generated code reaches the rebuild through [`rows`], which turns a
//! rule's tuples into [`Row`]s, and [`table`], which turns the rebuilt
//! changes back; [`limit`] stops a loop after the rounds `.run` asks for,
//! and [`finish`] reads what a table holds after its loop.

use std::collections::BTreeMap;
use std::collections::hash_map::Entry;

use differential_dataflow::AsCollection;
use differential_dataflow::ExchangeData;
use differential_dataflow::VecCollection;
use differential_dataflow::hashable::Hashable;
use rustc_hash::FxHashMap;
use rustc_hash::FxHashSet;
use serde::Deserialize;
use serde::Serialize;
use timely::PartialOrder;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::Capability;
use timely::dataflow::operators::Concat;
use timely::dataflow::operators::ConnectLoop;
use timely::dataflow::operators::Enter;
use timely::dataflow::operators::Feedback;
use timely::dataflow::operators::InputCapability;
use timely::dataflow::operators::Leave;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::operators::vec::Broadcast;
use timely::dataflow::operators::vec::Filter;
use timely::dataflow::operators::vec::Map;
use timely::order::Product;
use timely::progress::Timestamp;
use timely::progress::frontier::MutableAntichain;

use crate::diff;
use crate::diff::Weight;
use crate::time::OnceLoop;

/// Most columns a row of an e-graph table can have.
pub const ARITY: usize = 4;

/// Marks an id [`skolem`] made and the rebuild has not stamped yet.
pub const FRESH: i64 = 1 << 62;

/// Bits of a stamped id below its round.
const ROUND_SHIFT: u32 = 48;

/// Facts' own ids lie in `0..ID_LIMIT`; a stamped id lies above.
pub const ID_LIMIT: i64 = 1 << ROUND_SHIFT;

/// Rounds a loop can run, the seeds' round 0 among them. A stamp holds one
/// past its round in the bits between the hash and [`FRESH`], so even a
/// fresh id of round 0 stays above every fact's own id.
const MAX_ROUND: u16 = (1 << (62 - ROUND_SHIFT)) - 1;

/// The time of one rebuild pass: the loop's round, then the pass.
type Pass = Product<OnceLoop, u32>;

/// A row of any e-graph table, its columns padded to [`ARITY`].
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Row {
    /// The table, as an index into the rebuild's schemas.
    pub rel: u16,
    /// The columns; those past the table's arity are zero.
    pub cols: [i64; ARITY],
}

impl Row {
    /// A row of table `rel` with the given columns.
    ///
    /// # Panics
    ///
    /// Panics if there are more than [`ARITY`] columns.
    #[must_use]
    pub fn new(rel: u16, cols: &[i64]) -> Self {
        let mut padded = [0; ARITY];
        padded[..cols.len()].copy_from_slice(cols);
        Self { rel, cols: padded }
    }
}

/// How the rebuild reads the rows of one table.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Schema {
    /// Columns in use.
    pub arity: u8,
    /// The `union<T>` column, if any; the other columns form the key.
    pub value: Option<u8>,
    /// Bit `c` is set when column `c` holds an e-class id.
    pub ids: u8,
    /// Rows are unions to apply, not rows to keep.
    pub eqrel: bool,
}

impl Schema {
    /// A table of `arity` columns, with `value` as its `union<T>` column
    /// and the columns of the bitmask `ids` holding e-class ids.
    ///
    /// # Panics
    ///
    /// Panics if `arity` exceeds [`ARITY`] or `value` is not one of the
    /// id columns.
    #[must_use]
    pub const fn table(arity: u8, value: Option<u8>, ids: u8) -> Self {
        assert!(
            arity as usize <= ARITY,
            "an e-graph table has too many columns"
        );
        if let Some(value) = value {
            assert!(ids >> value & 1 == 1, "the union column must hold ids");
        }
        Self {
            arity,
            value,
            ids,
            eqrel: false,
        }
    }

    /// An `eqrel` relation: each row `(a, b)` unions two e-classes.
    #[must_use]
    pub const fn eqrel() -> Self {
        Self {
            arity: 2,
            value: None,
            ids: 0b11,
            eqrel: true,
        }
    }

    fn is_id(&self, column: usize) -> bool {
        self.ids >> column & 1 == 1
    }

    /// The row with its union column cleared.
    fn key(&self, row: &Row) -> [i64; ARITY] {
        let mut key = row.cols;
        if let Some(value) = self.value {
            key[value as usize] = 0;
        }
        key
    }
}

/// A fresh e-class id for the term a rule builds from `args`.
///
/// The first argument should be a tag naming the term's place in the rule,
/// the rest the canonical ids and values it is built from. Equal arguments
/// give equal ids, so every row that names the term names the same class,
/// whichever round asserts it. The id keeps 62 bits of the arguments'
/// hash; the rebuild stamps it with its round; see the
/// [module docs](self).
#[must_use]
pub fn skolem(args: &[i64]) -> i64 {
    let mut hash = 0x243F_6A88_85A3_08D3_u64;
    for &arg in args {
        hash = mix(hash ^ arg as u64);
    }
    FRESH | (hash as i64 & (FRESH - 1))
}

/// An integer column, as the rebuild and [`skolem`] hold it: its bits in an
/// `i64`.
pub trait Column: Copy {
    /// The value's bits as an `i64`; a `u64` wraps.
    fn bits(self) -> i64;

    /// The value whose [`bits`](Self::bits) are `bits`.
    ///
    /// # Panics
    ///
    /// Panics if no value of the type has those bits, as for a fresh id in
    /// a column narrower than 64 bits.
    fn from_bits(bits: i64) -> Self;
}

impl Column for i64 {
    #[inline]
    fn bits(self) -> i64 {
        self
    }

    #[inline]
    fn from_bits(bits: i64) -> Self {
        bits
    }
}

impl Column for u64 {
    #[inline]
    fn bits(self) -> i64 {
        self as i64
    }

    #[inline]
    fn from_bits(bits: i64) -> Self {
        bits as Self
    }
}

macro_rules! narrow_column {
    ($($ty:ty),*) => {$(
        impl Column for $ty {
            #[inline]
            fn bits(self) -> i64 {
                i64::from(self)
            }

            #[inline]
            fn from_bits(bits: i64) -> Self {
                Self::try_from(bits).expect("an e-graph value does not fit its column")
            }
        }
    )*};
}

narrow_column!(i8, i16, i32, u8, u16, u32);

/// A row of an e-graph table as rules hold it: a tuple of up to [`ARITY`]
/// integer columns.
pub trait Tuple: Sized {
    /// The row of table `rel` with these columns.
    fn to_row(&self, rel: u16) -> Row;

    /// The columns of `row`.
    ///
    /// # Panics
    ///
    /// Panics if a column of `row` does not fit its type; see
    /// [`Column::from_bits`].
    fn from_row(row: &Row) -> Self;
}

macro_rules! tuple {
    ($(($($column:ident $index:tt),+))+) => {$(
        impl<$($column: Column),+> Tuple for ($($column,)+) {
            #[inline]
            fn to_row(&self, rel: u16) -> Row {
                let mut cols = [0; ARITY];
                $(cols[$index] = self.$index.bits();)+
                Row { rel, cols }
            }

            #[inline]
            fn from_row(row: &Row) -> Self {
                ($($column::from_bits(row.cols[$index]),)+)
            }
        }
    )+};
}

// One shape per width up to `ARITY`.
tuple! {
    (A 0)
    (A 0, B 1)
    (A 0, B 1, C 2)
    (A 0, B 1, C 2, D 3)
}

/// Returns `true` for an id [`skolem`] made: its top two bits are `01`, so
/// no negative id counts.
const fn is_fresh(id: i64) -> bool {
    id >> 62 == 1
}

/// The splitmix64 finalizer.
fn mix(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// The worker hash of a row with key `key` in table `rel`.
fn route(rel: u16, key: &[i64; ARITY]) -> u64 {
    key.iter().fold(mix(u64::from(rel)), |hash, &column| {
        mix(hash ^ column as u64)
    })
}

/// A union-find over e-class ids whose roots are the least id of a class.
struct UnionFind {
    parent: FxHashMap<i64, i64>,
    /// A bit for the hash of each id `parent` holds, so most roots, their
    /// bits clear, are found without a lookup.
    seen: Vec<u64>,
    /// `seen` has `1 << shift` bits.
    shift: u32,
}

impl Default for UnionFind {
    fn default() -> Self {
        Self {
            parent: FxHashMap::default(),
            seen: vec![0; 1 << 10],
            shift: 16,
        }
    }
}

/// The word and bit of `id` in a [`UnionFind`] filter of `1 << shift` bits.
const fn seen_bit(shift: u32, id: i64) -> (usize, u64) {
    let hash = (id as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) >> (64 - shift);
    ((hash >> 6) as usize, 1 << (hash & 63))
}

impl UnionFind {
    fn find(&mut self, id: i64) -> i64 {
        let (word, bit) = seen_bit(self.shift, id);
        if self.seen[word] & bit == 0 {
            return id;
        }
        let Some(&first) = self.parent.get(&id) else {
            return id;
        };
        let mut root = first;
        while let Some(&next) = self.parent.get(&root) {
            root = next;
        }
        let mut node = id;
        while node != root {
            let next = self.parent.insert(node, root).unwrap_or(root);
            node = next;
        }
        root
    }

    /// Merges the classes of `a` and `b`, returning the root that lost.
    fn union(&mut self, a: i64, b: i64) -> Option<i64> {
        let (a, b) = (self.find(a), self.find(b));
        if a == b {
            return None;
        }
        let (root, loser) = if a < b { (a, b) } else { (b, a) };
        self.parent.insert(loser, root);
        if self.parent.len() << 3 > 1 << self.shift {
            // Keep the filter at least eight bits an id, so few roots pass it.
            self.shift += 1;
            self.seen = vec![0; 1 << (self.shift - 6)];
            for &id in self.parent.keys() {
                let (word, bit) = seen_bit(self.shift, id);
                self.seen[word] |= bit;
            }
        } else {
            let (word, bit) = seen_bit(self.shift, loser);
            self.seen[word] |= bit;
        }
        Some(loser)
    }
}

/// A row this worker holds, live until a repair moves it away.
#[derive(Clone, Copy)]
struct Slot {
    row: Row,
    live: bool,
    /// The loop's tables hold `row`, as the last round to change the slot
    /// left it.
    shown: bool,
    /// The last round to change the slot.
    round: u16,
}

/// One worker's share of the e-graph.
struct Shard {
    schemas: Vec<Schema>,
    classes: UnionFind,
    /// Per table, each canonical key's slot.
    tables: Vec<FxHashMap<[i64; ARITY], u32>>,
    slots: Vec<Slot>,
    /// Slots that died in an earlier round, for new rows to reuse.
    free: Vec<u32>,
    /// Slots whose row named an id when it was stored. Entries go stale as
    /// rows move; a repair skips those, and redoes nothing for a reused
    /// slot whose row is canonical.
    uses: FxHashMap<i64, Vec<u32>>,
    /// Per fresh id this worker owns, the stamp it was first given.
    stamps: FxHashMap<i64, i64>,
    /// The round the passes are for.
    round: u16,
    /// Slots the round has changed, each once.
    changed: Vec<u32>,
    /// Rows the round has taken from the loop's tables.
    retracted: Vec<(Row, i32)>,
    index: u64,
    peers: u64,
}

/// What one pass on one worker sends on.
#[derive(Default)]
struct Sent {
    /// Rows for another worker, or for the next pass.
    rows: Vec<Row>,
    /// Unions for every worker to apply in the next pass.
    unions: Vec<(i64, i64)>,
}

impl Shard {
    fn new(schemas: Vec<Schema>, index: usize, peers: usize) -> Self {
        let tables = schemas.iter().map(|_| FxHashMap::default()).collect();
        Self {
            schemas,
            classes: UnionFind::default(),
            tables,
            slots: Vec::new(),
            free: Vec::new(),
            uses: FxHashMap::default(),
            stamps: FxHashMap::default(),
            round: 0,
            changed: Vec::new(),
            retracted: Vec::new(),
            index: index as u64,
            peers: peers as u64,
        }
    }

    /// Notes that the round changes `slot`, before it does so: a row the
    /// loop's tables hold leaves them.
    fn change(&mut self, slot: u32) {
        let entry = &mut self.slots[slot as usize];
        if entry.round != self.round {
            entry.round = self.round;
            self.changed.push(slot);
        }
        if entry.shown {
            entry.shown = false;
            self.retracted.push((entry.row, -1));
        }
    }

    /// Ends the round: returns its net change to the loop's tables, every
    /// row the round left out and every live row it changed, and frees the
    /// slots it killed. A row stored and repaired within the round never
    /// reaches the tables.
    fn end_round(&mut self) -> Vec<(Row, i32)> {
        let mut changes = std::mem::take(&mut self.retracted);
        let mut changed = std::mem::take(&mut self.changed);
        for slot in changed.drain(..) {
            let entry = &mut self.slots[slot as usize];
            if entry.live {
                entry.shown = true;
                changes.push((entry.row, 1));
            } else {
                self.free.push(slot);
            }
        }
        self.changed = changed;
        changes
    }

    fn canonical(&mut self, mut row: Row) -> Row {
        let schema = self.schemas[row.rel as usize];
        for column in 0..schema.arity as usize {
            if schema.is_id(column) {
                row.cols[column] = self.classes.find(row.cols[column]);
            }
        }
        row
    }

    /// Runs one pass: applies `unions`, repairs the rows they leave stale,
    /// unions each fresh id's new stamp in `stamps` with its first, then
    /// stores `rows`.
    fn pass(
        &mut self,
        rows: Vec<Row>,
        unions: Vec<(i64, i64)>,
        stamps: Vec<(i64, i64)>,
        sent: &mut Sent,
    ) {
        let mut losers = Vec::new();
        for (a, b) in unions {
            losers.extend(self.classes.union(a, b));
        }
        let mut moved = Vec::new();
        for loser in losers {
            for slot in self.uses.remove(&loser).unwrap_or_default() {
                self.repair(slot, &mut moved);
            }
        }
        for (fresh, stamped) in stamps {
            let first = *self.stamps.entry(fresh).or_insert(stamped);
            if first != stamped {
                sent.unions.push((first, stamped));
            }
        }
        for row in rows.into_iter().chain(moved) {
            let row = self.canonical(row);
            let schema = self.schemas[row.rel as usize];
            if schema.eqrel {
                let [a, b, ..] = row.cols;
                if a != b {
                    sent.unions.push((a, b));
                }
                continue;
            }
            let key = schema.key(&row);
            if route(row.rel, &key) % self.peers == self.index {
                self.store(row, key, schema, sent);
            } else {
                sent.rows.push(row);
            }
        }
    }

    /// Canonicalizes a stored row. A row whose key keeps its value is
    /// updated in place; any other leaves to be stored again.
    fn repair(&mut self, slot: u32, moved: &mut Vec<Row>) {
        let Slot { row: old, live, .. } = self.slots[slot as usize];
        if !live {
            return;
        }
        let new = self.canonical(old);
        if new == old {
            return;
        }
        self.change(slot);
        let schema = self.schemas[old.rel as usize];
        let key = schema.key(&old);
        if schema.key(&new) == key {
            // Only the union column changed, to a root.
            self.slots[slot as usize].row = new;
            let value = new.cols[schema.value.expect("a key change otherwise") as usize];
            self.uses.entry(value).or_default().push(slot);
        } else {
            self.slots[slot as usize].live = false;
            self.tables[old.rel as usize].remove(&key);
            moved.push(new);
        }
    }

    /// Stores a canonical row this worker owns, or merges its union column
    /// with the row already under its key.
    fn store(&mut self, row: Row, key: [i64; ARITY], schema: Schema, sent: &mut Sent) {
        match self.tables[row.rel as usize].entry(key) {
            Entry::Occupied(held) => {
                if let Some(value) = schema.value {
                    let value = value as usize;
                    let held = self.slots[*held.get() as usize].row.cols[value];
                    let held = self.classes.find(held);
                    if held != row.cols[value] {
                        sent.unions.push((held, row.cols[value]));
                    }
                }
            }
            Entry::Vacant(vacant) => {
                let stored = Slot {
                    row,
                    live: true,
                    shown: false,
                    round: self.round,
                };
                let slot = if let Some(slot) = self.free.pop() {
                    self.slots[slot as usize] = stored;
                    slot
                } else {
                    self.slots.push(stored);
                    u32::try_from(self.slots.len() - 1).expect("too many rows for one worker")
                };
                vacant.insert(slot);
                self.changed.push(slot);
                for column in 0..schema.arity as usize {
                    if schema.is_id(column) {
                        self.uses.entry(row.cols[column]).or_default().push(slot);
                    }
                }
            }
        }
    }
}

/// Stamps the fresh ids of `row` with `round`, adding each fresh id not in
/// `minted` yet to it.
///
/// # Panics
///
/// Panics if a row of round 0, the facts' round, names an id that is
/// neither fresh nor in `0..ID_LIMIT`.
fn stamp(mut row: Row, schema: &Schema, round: u16, minted: &mut FxHashSet<i64>) -> Row {
    for column in 0..schema.arity as usize {
        let id = row.cols[column];
        if !schema.is_id(column) {
            continue;
        }
        if is_fresh(id) {
            minted.insert(id);
            row.cols[column] = stamped(id, round);
        } else if round == 0 {
            assert!(
                (0..ID_LIMIT).contains(&id),
                "a fact's e-class id {id} is outside 0..2^48"
            );
        }
    }
    row
}

/// The id fresh id `id` takes in `round`.
const fn stamped(id: i64, round: u16) -> i64 {
    ((round as i64 + 1) << ROUND_SHIFT) | (id & (ID_LIMIT - 1))
}

/// The rows one round brings [`rebuild`], gathered until the round has
/// brought them all.
struct Gathered {
    /// Where the round's asserted rows and stamps go out, at its first
    /// pass, and its deferred rows, at its second.
    at: [Capability<Pass>; 3],
    /// Rows of new terms, their union columns fresh, with their weights
    /// summed: many matches build the same term.
    made: FxHashMap<Row, i64>,
    /// The other rows, as they came.
    rows: Vec<(Row, i32)>,
}

impl Gathered {
    fn add(&mut self, row: Row, weight: i32, schema: &Schema) {
        if schema
            .value
            .is_some_and(|value| is_fresh(row.cols[value as usize]))
        {
            *self.made.entry(row).or_default() += i64::from(weight);
        } else {
            self.rows.push((row, weight));
        }
    }

    /// The round's rows whose weights sum to a positive count, each at
    /// least once and stamped with `round`, with each fresh id they name
    /// and its stamp, once. A row whose key names a fresh id is deferred,
    /// returned apart from the others.
    ///
    /// A round retracts few rows next to those it asserts, so each
    /// retraction cancels assertions of its row by lookup rather than the
    /// round's rows being sorted.
    fn assert(&mut self, schemas: &[Schema], round: u16) -> Asserted {
        let (made, rows) = (
            std::mem::take(&mut self.made),
            std::mem::take(&mut self.rows),
        );
        let mut retracted: FxHashMap<Row, i64> = FxHashMap::default();
        for &(row, weight) in &rows {
            if weight < 0 {
                *retracted.entry(row).or_default() -= i64::from(weight);
            }
        }
        let mut minted = FxHashSet::default();
        let mut asserted = Asserted::default();
        for (row, weight) in made {
            if weight > 0 {
                asserted.push(row, &schemas[row.rel as usize], round, &mut minted);
            }
        }
        for (row, weight) in rows {
            if weight <= 0 {
                continue;
            }
            if let Some(left) = retracted.get_mut(&row) {
                let weight = i64::from(weight);
                if *left >= weight {
                    *left -= weight;
                    continue;
                }
                *left = 0;
            }
            asserted.push(row, &schemas[row.rel as usize], round, &mut minted);
        }
        asserted.stamps = minted
            .into_iter()
            .map(|id| (id, stamped(id, round)))
            .collect();
        asserted
    }
}

/// What one round asserts, from one worker.
#[derive(Default)]
struct Asserted {
    /// Rows for their owners' first pass.
    rows: Vec<Row>,
    /// Rows whose keys name a fresh id, for this worker's second pass: the
    /// first most often merges the id into an older class, and every
    /// worker can then route the row by its canonical key.
    deferred: Vec<Row>,
    /// Each fresh id the rows name, with its stamp.
    stamps: Vec<(i64, i64)>,
}

impl Asserted {
    fn push(&mut self, row: Row, schema: &Schema, round: u16, minted: &mut FxHashSet<i64>) {
        let defer = !schema.eqrel
            && (0..schema.arity as usize).any(|column| {
                Some(column as u8) != schema.value
                    && schema.is_id(column)
                    && is_fresh(row.cols[column])
            });
        let row = stamp(row, schema, round, minted);
        if defer {
            self.deferred.push(row);
        } else {
            self.rows.push(row);
        }
    }
}

/// Work held for one pass on one worker.
struct Held {
    capabilities: [Capability<Pass>; 3],
    rows: Vec<Row>,
    unions: Vec<(i64, i64)>,
    stamps: Vec<(i64, i64)>,
}

/// Rebuilds the e-graph of `schemas` from the rows asserted each round.
///
/// `input` holds, at each round, the rows the round's rules and seeds
/// assert, as [`Row`]s indexed into `schemas`. The rows that sum to a
/// positive weight within the round are asserted; the rest are ignored, as
/// an e-graph keeps a term once it has it. The output at each round is the
/// change to the canonical tables, each canonical row held once.
///
/// Each union column and id column must hold ids from [`skolem`], or ids
/// below [`ID_LIMIT`]; a fact's own ids, those of round 0, must lie in
/// `0..ID_LIMIT`.
///
/// # Panics
///
/// Panics if a round reaches 2^14 - 1, a row names a table `schemas`
/// lacks, or a fact names an id outside `0..ID_LIMIT`.
#[must_use]
pub fn rebuild<'scope>(
    input: VecCollection<'scope, OnceLoop, Row, diff::Mutable>,
    schemas: Vec<Schema>,
) -> VecCollection<'scope, OnceLoop, Row, diff::Mutable> {
    let scope = input.scope();
    let rows = input.inner;
    let changes = scope.scoped::<Pass, _, _>("EqsatRebuild", |inner| {
        let (rows_back, rows_again) =
            inner.feedback::<Vec<Row>>(Product::new(Default::default(), 1));
        let (unions_back, unions_again) =
            inner.feedback::<Vec<(i64, i64)>>(Product::new(Default::default(), 1));

        let assert_schemas = schemas.clone();
        let mut builder = OperatorBuilder::new("EqsatAssert".to_owned(), inner);
        let mut asserting = builder.new_input(rows.enter(inner), Pipeline);
        let (mut asserted_out, asserted) = builder.new_output::<Vec<Row>>();
        let (mut stamps_out, stamps) = builder.new_output::<Vec<(i64, i64)>>();
        let (mut deferred_out, deferred) = builder.new_output::<Vec<Row>>();
        builder.build(move |_| {
            let mut waiting: Vec<Gathered> = Vec::new();
            move |frontiers| {
                asserting.for_each(|capability, data| {
                    for (row, time, weight) in data.drain(..) {
                        let pass = Product::new(time, 0);
                        let held = waiting
                            .iter()
                            .rposition(|gathered| *gathered.at[0].time() == pass);
                        let index = held.unwrap_or_else(|| {
                            let next = Product::new(pass.outer, 1);
                            waiting.push(Gathered {
                                at: [
                                    capability.delayed(&pass, 0),
                                    capability.delayed(&pass, 1),
                                    capability.delayed(&next, 2),
                                ],
                                made: FxHashMap::default(),
                                rows: Vec::new(),
                            });
                            waiting.len() - 1
                        });
                        waiting[index].add(row, weight, &assert_schemas[row.rel as usize]);
                    }
                });
                let mut asserted_out = asserted_out.activate();
                let mut stamps_out = stamps_out.activate();
                let mut deferred_out = deferred_out.activate();
                waiting.retain_mut(|gathered| {
                    if frontiers[0].frontier().less_equal(gathered.at[0].time()) {
                        return true;
                    }
                    let round = gathered.at[0].time().outer.inner;
                    assert!(round < MAX_ROUND, "an e-graph loop ran out of rounds");
                    let Asserted {
                        mut rows,
                        mut deferred,
                        mut stamps,
                    } = gathered.assert(&assert_schemas, round);
                    let [rows_at, stamps_at, deferred_at] = &gathered.at;
                    if !rows.is_empty() {
                        asserted_out.give(rows_at, &mut rows);
                    }
                    if !stamps.is_empty() {
                        stamps_out.give(stamps_at, &mut stamps);
                    }
                    if !deferred.is_empty() {
                        deferred_out.give(deferred_at, &mut deferred);
                    }
                    false
                });
            }
        });

        let route_schemas = schemas.clone();
        let exchange = Exchange::new(move |row: &Row| {
            route(row.rel, &route_schemas[row.rel as usize].key(row))
        });
        let mut builder = OperatorBuilder::new("EqsatMerge".to_owned(), inner);
        let mut rows_in = builder.new_input(asserted.concat(rows_again), exchange);
        let mut unions_in = builder.new_input(unions_again.broadcast(), Pipeline);
        let owner = Exchange::new(|&(fresh, _): &(i64, i64)| mix(fresh as u64));
        let mut stamps_in = builder.new_input(stamps, owner);
        let mut deferred_in = builder.new_input(deferred, Pipeline);
        let (mut changes_out, changes) = builder.new_output::<Vec<(Row, OnceLoop, i32)>>();
        let (mut rows_out, rows_next) = builder.new_output::<Vec<Row>>();
        let (mut unions_out, unions_next) = builder.new_output::<Vec<(i64, i64)>>();
        let mut shard = Shard::new(schemas, inner.index(), inner.peers());
        builder.build(move |_| {
            let mut held: BTreeMap<(u16, u32), Held> = BTreeMap::new();
            // Where the changes of the round the shard is in go out.
            let mut gathering: Option<Capability<Pass>> = None;
            move |frontiers| {
                rows_in.for_each(|capability, data| hold(&mut held, &capability).rows.append(data));
                unions_in
                    .for_each(|capability, data| hold(&mut held, &capability).unions.append(data));
                stamps_in
                    .for_each(|capability, data| hold(&mut held, &capability).stamps.append(data));
                deferred_in
                    .for_each(|capability, data| hold(&mut held, &capability).rows.append(data));
                let mut changes_out = changes_out.activate();
                let mut rows_out = rows_out.activate();
                let mut unions_out = unions_out.activate();
                loop {
                    // A round ends once no input can bring it another pass.
                    if let Some(changes_at) = gathering.take_if(|changes_at| {
                        let round = changes_at.time().outer.inner;
                        !frontiers.iter().any(|frontier| {
                            frontier
                                .frontier()
                                .iter()
                                .any(|time| time.outer.inner <= round)
                        })
                    }) {
                        let round = changes_at.time().outer;
                        let mut changes: Vec<_> = shard
                            .end_round()
                            .into_iter()
                            .map(|(row, weight)| (row, round, weight))
                            .collect();
                        if !changes.is_empty() {
                            changes_out.give(&changes_at, &mut changes);
                        }
                    }
                    let Some(entry) = held.first_entry() else {
                        break;
                    };
                    let pass = *entry.get().capabilities[0].time();
                    let pending = |frontier: &MutableAntichain<Pass>| {
                        frontier.frontier().iter().any(|time| {
                            time.less_equal(&pass) || time.outer.inner < pass.outer.inner
                        })
                    };
                    if frontiers.iter().any(pending) {
                        break;
                    }
                    let Held {
                        capabilities: [changes_at, rows_at, unions_at],
                        rows,
                        unions,
                        stamps,
                    } = entry.remove();
                    if gathering.is_none() {
                        shard.round = pass.outer.inner;
                        gathering = Some(changes_at);
                    }
                    let mut sent = Sent::default();
                    shard.pass(rows, unions, stamps, &mut sent);
                    if !sent.rows.is_empty() {
                        rows_out.give(&rows_at, &mut sent.rows);
                    }
                    if !sent.unions.is_empty() {
                        // Copies of a new term from many workers meet one
                        // stored row, so they find one union many times.
                        for union in &mut sent.unions {
                            if union.0 > union.1 {
                                *union = (union.1, union.0);
                            }
                        }
                        sent.unions.sort_unstable();
                        sent.unions.dedup();
                        unions_out.give(&unions_at, &mut sent.unions);
                    }
                }
            }
        });
        rows_next.connect_loop(rows_back);
        unions_next.connect_loop(unions_back);
        changes.leave(scope)
    });
    changes.as_collection()
}

/// The work held for the pass of `capability`.
fn hold<'a>(
    held: &'a mut BTreeMap<(u16, u32), Held>,
    capability: &InputCapability<Pass>,
) -> &'a mut Held {
    // Every message inside the rebuild goes out under one capability.
    let pass = capability.stamp().expect_singleton();
    held.entry((pass.outer.inner, pass.inner))
        .or_insert_with(|| Held {
            capabilities: [0, 1, 2].map(|port| capability.delayed(pass, port)),
            rows: Vec::new(),
            unions: Vec::new(),
            stamps: Vec::new(),
        })
}

/// The rows `collection` asserts into table `rel`, for [`rebuild`]. Each
/// update keeps its count, so any weight can assert.
#[must_use]
pub fn rows<'scope, D, R>(
    collection: VecCollection<'scope, OnceLoop, D, R>,
    rel: u16,
) -> VecCollection<'scope, OnceLoop, Row, diff::Mutable>
where
    D: Tuple + Clone + 'static,
    R: Weight,
{
    collection
        .inner
        .map(move |(tuple, time, weight)| (tuple.to_row(rel), time, weight.count()))
        .as_collection()
}

/// The changes [`rebuild`] makes to table `rel`, as tuples.
#[must_use]
pub fn table<'scope, D>(
    changes: VecCollection<'scope, OnceLoop, Row, diff::Mutable>,
    rel: u16,
) -> VecCollection<'scope, OnceLoop, D, diff::Mutable>
where
    D: Tuple + Clone + 'static,
{
    changes
        .inner
        .flat_map(move |(row, time, weight)| {
            (row.rel == rel).then(|| (D::from_row(&row), time, weight))
        })
        .as_collection()
}

/// The changes of a loop's first `rounds` rounds. Fed back alone, they let
/// the rules run `rounds` times, as egglog's `(run rounds)` does, or fewer
/// if the e-graph saturates first.
#[must_use]
pub fn limit<'scope, D>(
    changes: VecCollection<'scope, OnceLoop, D, diff::Mutable>,
    rounds: u16,
) -> VecCollection<'scope, OnceLoop, D, diff::Mutable>
where
    D: Clone + 'static,
{
    changes
        .inner
        .filter(move |(_, time, _)| time.inner < rounds)
        .as_collection()
}

/// The rows of a rebuilt table left after its loop, as a set.
///
/// The changes [`rebuild`] makes sum to one copy of each canonical row.
#[must_use]
pub fn finish<'scope, T, D>(
    changes: VecCollection<'scope, T, D, diff::Mutable>,
) -> VecCollection<'scope, T, D, diff::Static>
where
    T: Timestamp + differential_dataflow::lattice::Lattice,
    D: ExchangeData + Hashable,
{
    changes
        .consolidate()
        .inner
        .flat_map(|(row, time, weight)| (weight > 0).then_some((row, time, diff::Static)))
        .as_collection()
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;

    use differential_dataflow::input::Input;
    use differential_dataflow::operators::iterate::Variable;
    use flowlog_common::MAX_EGRAPH_ARITY;
    use flowlog_common::MAX_EGRAPH_ROUNDS;
    use rstest::rstest;

    use super::*;

    type Table<'scope> = VecCollection<'scope, OnceLoop, Row, diff::Mutable>;

    /// The tables `rules` leave from `facts` on `peers` workers, after
    /// `rounds` rounds or at saturation.
    fn saturate(
        peers: usize,
        schemas: &[Schema],
        facts: &[Row],
        rounds: Option<u16>,
        rules: for<'scope> fn(Table<'scope>) -> Table<'scope>,
    ) -> Vec<(Row, i32)> {
        let (schemas, facts) = (schemas.to_vec(), facts.to_vec());
        let mut rows: Vec<_> = timely::execute(timely::Config::process(peers), move |worker| {
            let seen = Rc::new(RefCell::new(Vec::new()));
            let mut input = worker.dataflow::<(), _, _>(|scope| {
                let (input, seeds) = scope.new_collection::<Row, diff::Mutable>();
                let seen = Rc::clone(&seen);
                let changes = scope.iterative::<u16, _, _>(|inner| {
                    let (variable, tables) = Variable::new(inner, Product::new((), 1));
                    let changes =
                        rebuild(seeds.enter(inner).concat(rules(tables)), schemas.clone());
                    let limit = rounds.unwrap_or(u16::MAX);
                    variable.set(
                        changes
                            .clone()
                            .inner
                            .filter(move |(_, time, _)| time.inner < limit)
                            .as_collection(),
                    );
                    changes.leave(scope)
                });
                changes
                    .consolidate()
                    .inspect(move |&(row, (), weight)| seen.borrow_mut().push((row, weight)));
                input
            });
            if worker.index() == 0 {
                for &row in &facts {
                    input.update(row, 1);
                }
            }
            input.close();
            while worker.step() {}
            seen.take()
        })
        .unwrap()
        .join()
        .into_iter()
        .flat_map(Result::unwrap)
        .collect();
        rows.sort_unstable();
        rows
    }

    fn no_rules(tables: Table<'_>) -> Table<'_> {
        tables.filter(|_| false)
    }

    /// `Mul(x, y) = a, Mul(a, z) = e` gives `Mul(x, Mul(y, z)) = e`, and
    /// `Mul(y, z) = a, Mul(x, a) = e` gives `Mul(Mul(x, y), z) = e`.
    fn associativity(tables: Table<'_>) -> Table<'_> {
        let by_left = tables
            .clone()
            .map(|row| (row.cols[0], (row.cols[1], row.cols[2])))
            .arrange_by_key();
        let by_right = tables
            .clone()
            .map(|row| (row.cols[1], (row.cols[0], row.cols[2])))
            .arrange_by_key();
        let by_value = tables
            .map(|row| (row.cols[2], (row.cols[0], row.cols[1])))
            .arrange_by_key();
        let right = by_value.clone().join_core(by_left, |_, &(x, y), &(z, e)| {
            let t = skolem(&[1, y, z]);
            [Row::new(0, &[y, z, t]), Row::new(0, &[x, t, e])]
        });
        let left = by_value.join_core(by_right, |_, &(y, z), &(x, e)| {
            let t = skolem(&[2, x, y]);
            [Row::new(0, &[x, y, t]), Row::new(0, &[t, z, e])]
        });
        right.concat(left)
    }

    /// The left-nested product of `leaves` copies of leaf `1`, the product
    /// of the first `k` leaves named `k`.
    fn chain(leaves: i64) -> Vec<Row> {
        (2..=leaves).map(|k| Row::new(0, &[k - 1, 1, k])).collect()
    }

    /// Merging two children merges their parents, and the parents' parents,
    /// across tables, workers, and repairs in place.
    #[rstest]
    fn a_union_closes_over_congruence(#[values(1, 2, 3, 4)] peers: usize) {
        let schemas = [
            Schema::table(2, Some(1), 0b11),
            Schema::table(2, Some(1), 0b11),
            Schema::table(2, Some(1), 0b11),
            Schema::table(1, None, 0b1),
            Schema::eqrel(),
        ];
        let facts = [
            Row::new(0, &[1, 3]),
            Row::new(0, &[2, 4]),
            Row::new(1, &[3, 5]),
            Row::new(1, &[4, 6]),
            Row::new(2, &[7, 2]),
            Row::new(3, &[4]),
            Row::new(3, &[3]),
            Row::new(4, &[1, 2]),
        ];
        let expected = [
            Row::new(0, &[1, 3]),
            Row::new(1, &[3, 5]),
            Row::new(2, &[7, 1]),
            Row::new(3, &[3]),
        ];
        let expected: Vec<_> = expected.into_iter().map(|row| (row, 1)).collect();
        assert_eq!(saturate(peers, &schemas, &facts, None, no_rules), expected);
    }

    /// Associativity over `n` equal leaves saturates to every split of
    /// every sub-product, `n (n - 1) / 2` products, each held once.
    #[rstest]
    fn associativity_saturates_to_every_split(
        #[values(1, 2, 4)] peers: usize,
        #[values(4, 21)] leaves: i64,
    ) {
        let schemas = [Schema::table(3, Some(2), 0b111)];
        let rows = saturate(peers, &schemas, &chain(leaves), None, associativity);
        assert_eq!(rows.len() as i64, leaves * (leaves - 1) / 2);
        assert!(rows.iter().all(|&(_, weight)| weight == 1));
        let mut classes: Vec<_> = rows.iter().map(|(row, _)| row.cols[2]).collect();
        classes.sort_unstable();
        classes.dedup();
        assert_eq!(classes, (2..=leaves).collect::<Vec<_>>());
    }

    /// Associativity over `n` distinct leaves saturates to every split of
    /// every run of two or more leaves: `(n - 1) n (n + 1) / 6` products in
    /// `n (n - 1) / 2` classes. Past a merge, a rule firing can rebuild a
    /// term in one row while the row defining it cancels against a lost
    /// match; the term must still name its class.
    #[rstest]
    fn associativity_over_distinct_leaves_saturates(#[values(1, 2, 4)] peers: usize) {
        let leaves = 6;
        let product = |k: i64| if k == 1 { 1 } else { 100 + k };
        let facts: Vec<_> = (2..=leaves)
            .map(|k| Row::new(0, &[product(k - 1), k, product(k)]))
            .collect();
        let schemas = [Schema::table(3, Some(2), 0b111)];
        let rows = saturate(peers, &schemas, &facts, None, associativity);
        assert_eq!(rows.len() as i64, (leaves - 1) * leaves * (leaves + 1) / 6);
        let mut classes: Vec<_> = rows.iter().map(|(row, _)| row.cols[2]).collect();
        classes.sort_unstable();
        classes.dedup();
        assert_eq!(classes.len() as i64, leaves * (leaves - 1) / 2);
    }

    /// A run of one round applies the rules once.
    #[rstest]
    fn a_run_limit_stops_the_rounds(#[values(1, 3)] peers: usize) {
        let schemas = [Schema::table(3, Some(2), 0b111)];
        let rows = saturate(peers, &schemas, &chain(4), Some(1), associativity);
        let rows: Vec<_> = rows.into_iter().map(|(row, _)| row.cols).collect();
        assert_eq!(
            rows,
            [
                [1, 1, 2, 0],
                [1, 2, 3, 0],
                [2, 1, 3, 0],
                [2, 2, 4, 0],
                [3, 1, 4, 0]
            ]
        );
    }

    /// A fresh id among the facts is stamped above every fact's own id, so
    /// it never names a fact's class.
    #[test]
    fn a_fresh_fact_stays_apart_from_the_facts_ids() {
        let schemas = [Schema::table(2, Some(1), 0b11)];
        let facts = [Row::new(0, &[1, 2]), Row::new(0, &[2, skolem(&[7])])];
        let rows = saturate(1, &schemas, &facts, None, no_rules);
        let fresh = rows
            .iter()
            .find(|(row, _)| row.cols[0] == 2)
            .expect("both facts stay")
            .0
            .cols[1];
        assert!(fresh >= ID_LIMIT && !is_fresh(fresh), "{fresh:#x}");
    }

    /// The compiler's e-graph limits are the rebuild's: a table's columns
    /// fill a row, and the longest `.run`, which reaches its round count,
    /// reaches the last round a stamp holds.
    #[test]
    fn the_compilers_limits_fit_the_rebuild() {
        assert_eq!(ARITY, MAX_EGRAPH_ARITY);
        assert_eq!(MAX_EGRAPH_ROUNDS + 1, u32::from(MAX_ROUND));
    }
}
