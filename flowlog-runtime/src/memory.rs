//! Heap accounting by timely operator, for profiled runs.
//!
//! [`ProfilingAlloc`] wraps the program's global allocator. Every block it
//! hands out carries a header naming its owner: the worker the allocating
//! thread registered as with [`register_worker`], and the operator timely
//! was scheduling on that thread ([`current::operator_id`]). Freeing or
//! resizing the block, on any thread, charges the owner. [`operator_memory`]
//! reads one worker's counters.
//!
//! The allocator is written with `unsafe` because [`GlobalAlloc`] is an
//! `unsafe` trait: no safe API hands out raw blocks or installs a global
//! allocator.

use std::alloc::GlobalAlloc;
use std::alloc::Layout;
use std::alloc::alloc_zeroed;
use std::alloc::handle_alloc_error;
use std::cell::Cell;
use std::ptr;
use std::slice;
use std::sync::OnceLock;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use timely::scheduling::current;

// =============================================================================
// Limits
// =============================================================================

/// Workers that can register. Worker indices count up from zero, so this
/// bounds the worker count of a profiled run. Each registered worker costs
/// one table of [`SLOTS`]; an unregistered one costs a pointer.
const MAX_WORKERS: usize = 1024;

/// Operators tracked per worker. A worker's operator identifiers count up
/// from zero as its dataflows are built, and a generated program builds
/// one dataflow, so the largest identifier is the program's operator count:
/// 1,115 for DOOP, the largest example program, measured on batik. An
/// operator beyond the limit is charged to [`Owner::Overflow`], so a limit
/// too small shows up in the output instead of misattributing memory. A
/// table's pages are zeroed lazily by the system, so untouched slots cost
/// address space, not memory.
const MAX_OPERATORS: usize = 1 << 16;

/// Slots per worker table: [`Owner::Outside`], one per operator, and
/// [`Owner::Overflow`].
const SLOTS: usize = MAX_OPERATORS + 2;

/// The worker index of a thread that never registered. Its blocks carry
/// this index and are charged to no one.
const UNREGISTERED: u32 = u32::MAX;

/// Bytes reserved in front of every block. The [`Header`] sits in the last
/// eight of them, directly before the caller's pointer. Eight would hold
/// the header; sixteen keeps the pointers this allocator returns aligned
/// to 16, as the system allocator's are, at eight more bytes per block.
/// The difference was not measured. Larger alignments widen the
/// reservation to match.
const HEADER: usize = 16;

// =============================================================================
// ProfilingAlloc
// =============================================================================

/// A global allocator that charges each block to the worker and operator
/// that allocated it, delegating the memory itself to `A`.
///
/// Every block costs [`HEADER`] bytes more than requested, or the block's
/// alignment if larger; the counters report requested bytes only.
pub struct ProfilingAlloc<A> {
    inner: A,
}

impl<A> ProfilingAlloc<A> {
    /// Usable in the `static` a `#[global_allocator]` attribute requires.
    pub const fn new(inner: A) -> Self {
        Self { inner }
    }
}

// SAFETY: every block comes from `inner` with the padded layout of the
// caller's layout, and returns to `inner` with that same padded layout, so
// `inner`'s contract carries over. The caller's pointer is `offset` bytes
// into the inner block, where `offset` is a multiple of the caller's
// alignment and the inner block is aligned to at least `offset`, so the
// caller's pointer is aligned and has `layout.size()` usable bytes.
unsafe impl<A: GlobalAlloc> GlobalAlloc for ProfilingAlloc<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let Some((padded, offset)) = pad(layout) else {
            return ptr::null_mut();
        };
        // SAFETY: `padded` is at least `HEADER` bytes, never zero-sized.
        let base = unsafe { self.inner.alloc(padded) };
        // SAFETY: `base` is null or a fresh block of `padded`.
        unsafe { claim(base, offset, layout.size()) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let Some((padded, offset)) = pad(layout) else {
            return ptr::null_mut();
        };
        // SAFETY: `padded` is at least `HEADER` bytes, never zero-sized.
        let base = unsafe { self.inner.alloc_zeroed(padded) };
        // SAFETY: `base` is null or a fresh block of `padded`.
        unsafe { claim(base, offset, layout.size()) }
    }

    unsafe fn dealloc(&self, block: *mut u8, layout: Layout) {
        let (padded, offset) =
            pad(layout).expect("the caller frees with the layout it allocated with, which padded");
        // SAFETY: `block` came from `claim`, which wrote its header.
        let header = unsafe { Header::read(block) };
        header.charge(Change::Shrink(layout.size()));
        // SAFETY: `claim` returned `block` at `offset` into an inner block
        // of `padded`.
        unsafe { self.inner.dealloc(block.sub(offset), padded) };
    }

    unsafe fn realloc(&self, block: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let (padded, offset) = pad(layout)
            .expect("the caller resizes with the layout it allocated with, which padded");
        // `realloc` requires the new size, rounded up to the alignment, to
        // fit in a layout; checking the padded size checks that.
        let Some(new_padded_size) = new_size
            .checked_add(offset)
            .filter(|&size| Layout::from_size_align(size, offset).is_ok())
        else {
            return ptr::null_mut();
        };
        // Read before the inner realloc, which may free the old block. The
        // header moves with the block's bytes, so the owner stays the one
        // that first allocated it.
        // SAFETY: `block` came from `claim`, which wrote its header.
        let header = unsafe { Header::read(block) };
        // SAFETY: `block.sub(offset)` is an inner block of `padded`, and
        // `new_padded_size` keeps `padded`'s alignment valid.
        let base = unsafe {
            self.inner
                .realloc(block.sub(offset), padded, new_padded_size)
        };
        if base.is_null() {
            // The old block is untouched and still charged as it was.
            return ptr::null_mut();
        }
        if new_size > layout.size() {
            header.charge(Change::Grow(new_size - layout.size()));
        } else if new_size < layout.size() {
            header.charge(Change::Shrink(layout.size() - new_size));
        }
        // SAFETY: `base` is a block of `new_padded_size`, past `offset`.
        unsafe { base.add(offset) }
    }
}

/// Returns the inner layout for a caller's `layout`, and the offset of the
/// caller's pointer into it. `None` when the padded size overflows.
fn pad(layout: Layout) -> Option<(Layout, usize)> {
    let offset = layout.align().max(HEADER);
    let size = layout.size().checked_add(offset)?;
    let padded = Layout::from_size_align(size, offset).ok()?;
    Some((padded, offset))
}

/// Writes the current owner's header into the fresh block `base` and
/// charges it `size` bytes, returning the caller's pointer `offset` bytes
/// in. Returns null when `base` is null.
///
/// # Safety
///
/// `base` is null or a block of at least `offset + size` bytes aligned to
/// `offset`, with `offset >= HEADER`.
unsafe fn claim(base: *mut u8, offset: usize, size: usize) -> *mut u8 {
    if base.is_null() {
        return base;
    }
    // SAFETY: `offset` is within the block, per the caller.
    let block = unsafe { base.add(offset) };
    let header = Header::current();
    // SAFETY: the header's bytes lie in the reservation in front of `block`.
    unsafe { header.write(block) };
    header.charge(Change::Grow(size));
    block
}

// =============================================================================
// Header
// =============================================================================

/// The owner of a block, stored in the eight bytes before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Header {
    /// The registered worker index, or [`UNREGISTERED`].
    worker: u32,
    /// The owner within that worker, as [`Owner::slot`] encodes it.
    slot: u32,
}

impl Header {
    /// The owner of a block allocated on this thread now.
    fn current() -> Self {
        Self {
            worker: WORKER.get(),
            slot: Owner::current().slot(),
        }
    }

    /// Stores the header in front of `block`.
    ///
    /// # Safety
    ///
    /// `block` is at least [`HEADER`] bytes into an allocation and aligned
    /// to 8.
    unsafe fn write(self, block: *mut u8) {
        // SAFETY: the eight bytes before `block` are in the allocation and
        // aligned to 8, per the caller.
        unsafe { block.sub(size_of::<Self>()).cast::<Self>().write(self) }
    }

    /// Reads the header stored in front of `block`.
    ///
    /// # Safety
    ///
    /// `block` was returned by [`claim`] and not yet freed.
    unsafe fn read(block: *const u8) -> Self {
        // SAFETY: `claim` wrote a header there, aligned to 8.
        unsafe { block.sub(size_of::<Self>()).cast::<Self>().read() }
    }

    /// Applies `change` to the owner's counters. Does nothing for a block
    /// of an unregistered thread.
    fn charge(self, change: Change) {
        let Some(table) = table(self.worker as usize) else {
            return;
        };
        let counters = &table[self.slot as usize];
        // Sizes come from a `Layout`, which caps them at `isize::MAX`, so
        // the casts are lossless.
        match change {
            Change::Grow(bytes) => {
                let bytes = bytes as u64;
                counters.allocated.fetch_add(bytes, Ordering::Relaxed);
                let live = counters.live.fetch_add(bytes, Ordering::Relaxed) + bytes;
                counters.peak.fetch_max(live, Ordering::Relaxed);
            }
            Change::Shrink(bytes) => {
                counters.live.fetch_sub(bytes as u64, Ordering::Relaxed);
            }
        }
    }
}

/// A change in one owner's bytes.
#[derive(Clone, Copy, Debug)]
enum Change {
    /// Bytes allocated, or added by a resize.
    Grow(usize),
    /// Bytes freed, or removed by a resize.
    Shrink(usize),
}

// =============================================================================
// Worker tables
// =============================================================================

/// One owner's counters. All zero is the valid empty value, which lets a
/// table come from zeroed memory.
#[derive(Debug, Default)]
struct Slot {
    allocated: AtomicU64,
    live: AtomicU64,
    peak: AtomicU64,
}

thread_local! {
    // Const-initialized without a destructor, so the allocator can read it
    // without allocating or registering anything.
    static WORKER: Cell<u32> = const { Cell::new(UNREGISTERED) };
}

/// Each registered worker's table of [`SLOTS`], empty until it registers.
/// Tables are never freed: a block can be freed on any thread at any time,
/// and it must still find its owner's table.
static TABLES: [OnceLock<&'static [Slot]>; MAX_WORKERS] = [const { OnceLock::new() }; MAX_WORKERS];

/// Registers this thread as worker `index`: what it allocates from now on
/// is charged to that worker. Creates the worker's table the first time.
///
/// Returns `false`, and leaves the thread unregistered, when `index` is not
/// below the supported worker count; that worker's memory goes uncounted.
#[must_use]
pub fn register_worker(index: usize) -> bool {
    let Some(entry) = TABLES.get(index) else {
        return false;
    };
    entry.get_or_init(new_table);
    WORKER.set(u32::try_from(index).expect("index is below MAX_WORKERS, which fits in u32"));
    true
}

/// Allocates a table of [`SLOTS`] zeroed slots that lives forever.
///
/// Built from zeroed memory rather than by collecting `Slot::default()`:
/// collecting writes every slot and commits the table's whole size per
/// worker, where zeroed memory leaves the pages of untouched slots
/// uncommitted.
fn new_table() -> &'static [Slot] {
    let layout = Layout::array::<Slot>(SLOTS).expect("SLOTS slots fit in a layout");
    // SAFETY: the layout is not zero-sized.
    let table = unsafe { alloc_zeroed(layout) }.cast::<Slot>();
    if table.is_null() {
        handle_alloc_error(layout);
    }
    // SAFETY: `table` holds `SLOTS` slots, each all zero, which is a valid
    // `Slot`, and it is never freed.
    unsafe { slice::from_raw_parts(table, SLOTS) }
}

/// The table of worker `index`, if it registered.
fn table(index: usize) -> Option<&'static [Slot]> {
    TABLES.get(index)?.get().copied()
}

// =============================================================================
// Owner
// =============================================================================

/// Who a block is charged to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Owner {
    /// Allocated on the worker thread outside any operator: dataflow
    /// construction, communication, or the host's own work between steps.
    Outside,
    /// Allocated while timely scheduled the operator with this worker-local
    /// identifier, the one timely's `Operates` and `Schedule` log events
    /// report. Always below [`MAX_OPERATORS`].
    Operator(usize),
    /// Allocated by an operator whose identifier is too large to track.
    Overflow,
}

impl Owner {
    /// The owner of an allocation on this thread now.
    fn current() -> Self {
        match current::operator_id() {
            None => Self::Outside,
            Some(id) if id < MAX_OPERATORS => Self::Operator(id),
            Some(_) => Self::Overflow,
        }
    }

    /// The owner's index in a worker table, below [`SLOTS`]: `Outside` is
    /// first, then operators by identifier, then `Overflow`.
    fn slot(self) -> u32 {
        // `MAX_OPERATORS + 1` fits in `u32`, so the casts are lossless for
        // an identifier below `MAX_OPERATORS`, which `current` guarantees.
        match self {
            Self::Outside => 0,
            Self::Operator(id) => id as u32 + 1,
            Self::Overflow => MAX_OPERATORS as u32 + 1,
        }
    }

    /// Decodes [`slot`](Self::slot).
    fn from_slot(slot: usize) -> Self {
        match slot {
            0 => Self::Outside,
            slot if slot <= MAX_OPERATORS => Self::Operator(slot - 1),
            _ => Self::Overflow,
        }
    }
}

// =============================================================================
// Reading
// =============================================================================

/// One owner's heap usage, in bytes the program requested.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OperatorMemory {
    /// Who the bytes are charged to.
    pub owner: Owner,
    /// Every byte ever allocated, counting growth by resizing.
    pub allocated: u64,
    /// Bytes allocated and not yet freed.
    pub live: u64,
    /// The largest `live` has been.
    pub peak: u64,
}

/// Reads worker `index`'s counters, one entry per owner that ever
/// allocated, ordered [`Owner::Outside`], then operators by identifier,
/// then [`Owner::Overflow`]. Empty when the worker never registered.
///
/// Counters change while other threads run, so entries are not a
/// consistent snapshot of one instant.
#[must_use]
pub fn operator_memory(index: usize) -> Vec<OperatorMemory> {
    let Some(table) = table(index) else {
        return Vec::new();
    };
    let mut entries = Vec::new();
    for (slot, counters) in table.iter().enumerate() {
        let allocated = counters.allocated.load(Ordering::Relaxed);
        if allocated == 0 {
            continue;
        }
        entries.push(OperatorMemory {
            owner: Owner::from_slot(slot),
            allocated,
            live: counters.live.load(Ordering::Relaxed),
            peak: counters.peak.load(Ordering::Relaxed),
        });
    }
    entries
}

#[cfg(test)]
mod tests {
    use std::alloc::System;
    use std::thread;

    use rstest::rstest;

    use super::*;

    /// Returns worker `index`'s entry for `owner`.
    fn entry(index: usize, owner: Owner) -> Option<OperatorMemory> {
        operator_memory(index)
            .into_iter()
            .find(|e| e.owner == owner)
    }

    /// Returns `owner`'s usage with the given counters.
    fn usage(owner: Owner, allocated: u64, live: u64, peak: u64) -> OperatorMemory {
        OperatorMemory {
            owner,
            allocated,
            live,
            peak,
        }
    }

    // Each test that charges memory registers its own worker index, so
    // tests running in parallel never share a table.

    #[test]
    fn an_allocation_is_charged_to_the_scheduled_operator() {
        let alloc = ProfilingAlloc::new(System);
        assert!(register_worker(1));
        let layout = Layout::from_size_align(100, 8).unwrap();
        let _operator = current::enter(3);
        // SAFETY: a nonzero layout, freed below with the same layout.
        let block = unsafe { alloc.alloc(layout) };
        assert_eq!(
            entry(1, Owner::Operator(3)),
            Some(usage(Owner::Operator(3), 100, 100, 100))
        );
        // SAFETY: `block` came from `alloc` with `layout`.
        unsafe { alloc.dealloc(block, layout) };
    }

    #[test]
    fn a_free_returns_live_bytes_and_keeps_the_peak() {
        let alloc = ProfilingAlloc::new(System);
        assert!(register_worker(2));
        let big = Layout::from_size_align(100, 8).unwrap();
        let small = Layout::from_size_align(50, 8).unwrap();
        let _operator = current::enter(0);
        // SAFETY: nonzero layouts; each block is freed with its own layout.
        unsafe {
            let first = alloc.alloc(big);
            let second = alloc.alloc(small);
            alloc.dealloc(first, big);
            alloc.dealloc(second, small);
        }
        assert_eq!(
            entry(2, Owner::Operator(0)),
            Some(usage(Owner::Operator(0), 150, 0, 150))
        );
    }

    /// Operators exchange data across workers, so a block is often freed
    /// on a thread other than the one that allocated it.
    #[test]
    fn a_free_on_another_thread_is_charged_to_the_allocating_operator() {
        let alloc = ProfilingAlloc::new(System);
        assert!(register_worker(3));
        let layout = Layout::from_size_align(64, 8).unwrap();
        let block = {
            let _operator = current::enter(5);
            // SAFETY: a nonzero layout, freed below with the same layout.
            unsafe { alloc.alloc(layout) as usize }
        };
        thread::spawn(move || {
            assert!(register_worker(4));
            let _operator = current::enter(9);
            let alloc = ProfilingAlloc::new(System);
            // SAFETY: `block` came from a `ProfilingAlloc<System>` with
            // `layout`, and nothing else frees it.
            unsafe { alloc.dealloc(block as *mut u8, layout) };
        })
        .join()
        .unwrap();
        assert_eq!(
            entry(3, Owner::Operator(5)),
            Some(usage(Owner::Operator(5), 64, 0, 64))
        );
        assert_eq!(operator_memory(4), Vec::new());
    }

    /// A resize under a later operator charges the size difference to the
    /// operator that allocated the block.
    #[test]
    fn realloc_charges_the_difference_to_the_allocating_operator() {
        let alloc = ProfilingAlloc::new(System);
        assert!(register_worker(5));
        let layout = Layout::from_size_align(100, 8).unwrap();
        // SAFETY: each call passes the block and layout the previous one
        // returned; the last layout frees the block.
        unsafe {
            let block = {
                let _operator = current::enter(1);
                alloc.alloc(layout)
            };
            let _later = current::enter(7);
            let block = alloc.realloc(block, layout, 300);
            let grown = Layout::from_size_align(300, 8).unwrap();
            let block = alloc.realloc(block, grown, 40);
            assert_eq!(
                operator_memory(5),
                vec![usage(Owner::Operator(1), 300, 40, 300)]
            );
            alloc.dealloc(block, Layout::from_size_align(40, 8).unwrap());
        }
    }

    /// Below and above the header size, where the offset into the inner
    /// block changes.
    // Cases: alignment.
    #[rstest]
    #[case(1)]
    #[case(4096)]
    fn a_block_keeps_its_requested_alignment(#[case] align: usize) {
        let alloc = ProfilingAlloc::new(System);
        let layout = Layout::from_size_align(3, align).unwrap();
        // SAFETY: a nonzero layout; writes stay within its three bytes;
        // freed with the same layout.
        unsafe {
            let block = alloc.alloc(layout);
            assert_eq!(block as usize % align, 0);
            block.write_bytes(0xAB, 3);
            alloc.dealloc(block, layout);
        }
    }

    // Cases: owner.
    #[rstest]
    #[case(Owner::Outside)]
    #[case(Owner::Operator(0))]
    #[case(Owner::Operator(MAX_OPERATORS - 1))]
    #[case(Owner::Overflow)]
    fn an_owner_round_trips_through_its_slot(#[case] owner: Owner) {
        let slot = owner.slot() as usize;
        assert!(slot < SLOTS);
        assert_eq!(Owner::from_slot(slot), owner);
    }

    #[test]
    fn a_worker_index_beyond_the_limit_is_refused() {
        assert!(!register_worker(MAX_WORKERS));
        assert_eq!(operator_memory(MAX_WORKERS), Vec::new());
    }
}
