//! Import tracking utilities for the FlowLog compiler.
//!
//! This module tracks which external crates and traits need to be imported
//! based on the transformations being generated within a stratum.

use common::{ExecutionMode, INTERN_MAX_RETRIES};
use parser::DataType;

use proc_macro2::TokenStream;
use quote::quote;

use std::collections::HashSet;

/// Identifies which semiring family is needed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum SemiringKind {
    Min,
    Max,
    Sum,
    Avg,
}

/// All numeric DataTypes in the canonical order used for semiring code generation.
///
/// Integer types come first (for `*_int.rs` files), then floats (for `*_float.rs` files).
const INT_DATA_TYPES: [DataType; 8] = [
    DataType::Int8,
    DataType::Int16,
    DataType::Int32,
    DataType::Int64,
    DataType::UInt8,
    DataType::UInt16,
    DataType::UInt32,
    DataType::UInt64,
];
const FLOAT_DATA_TYPES: [DataType; 2] = [DataType::Float32, DataType::Float64];

/// Tracks which semiring modules (min/max/sum/avg × numeric types) are needed.
#[derive(Default, Clone)]
pub(crate) struct SemiringNeeds(HashSet<(SemiringKind, DataType)>);

impl SemiringNeeds {
    /// Returns whether any semiring module is needed.
    pub fn any(&self) -> bool {
        !self.0.is_empty()
    }

    /// Mark that a specific semiring kind is needed for a specific numeric type.
    pub fn insert(&mut self, kind: SemiringKind, dt: DataType) {
        assert!(
            dt.is_numeric(),
            "Compiler error: semiring only supports numeric types, got {dt}"
        );
        self.0.insert((kind, dt));
    }

    /// Check whether a specific (kind, type) pair is needed.
    pub fn needs(&self, kind: SemiringKind, dt: DataType) -> bool {
        self.0.contains(&(kind, dt))
    }

    /// Returns which integer DataTypes are needed for a given kind, in canonical order.
    pub fn int_needs(&self, kind: SemiringKind) -> [bool; 8] {
        INT_DATA_TYPES.map(|dt| self.needs(kind, dt))
    }

    /// Returns which float DataTypes are needed for a given kind.
    pub fn float_needs(&self, kind: SemiringKind) -> [bool; 2] {
        FLOAT_DATA_TYPES.map(|dt| self.needs(kind, dt))
    }
}

/// Records the import requirements gathered while compiling.
#[must_use]
#[derive(Default)]
pub(crate) struct ImportTracker {
    /// Current execution mode (controls diff type and import selection).
    mode: ExecutionMode,
    /// Profiling support.
    profiling: bool,

    /// Standard file IO support.
    std_file: bool,
    /// Standard buffered IO support.
    std_buf_io: bool,
    /// Standard hashmap support.
    std_hashmap: bool,

    /// Differential dataflow Input trait.
    dd_input: bool,
    /// AsCollection conversions.
    as_collection: bool,
    /// ThresholdTotal operator trait.
    threshold_total: bool,
    /// Timely map operator trait (used on raw streams).
    timely_map: bool,

    /// Whether recursion is used.
    recursive: bool,
    /// Whether aggregation operators are used.
    aggregation: bool,
    /// Whether semiring one value is needed.
    semiring_one: bool,
    /// Semiring requirements per aggregation kind.
    semirings: SemiringNeeds,

    /// Whether string interning is needed (any relation uses string columns).
    string_intern: bool,
    /// Whether string resolve is needed (an output IDB has string columns).
    string_resolve: bool,

    /// Whether ordered-float is needed (any relation uses float columns).
    ordered_float: bool,

    /// Whether memchr is needed for SIMD delimiter scanning.
    memchr: bool,

    /// Whether a user-defined function module is needed.
    udf: bool,

    /// Whether in-memory output buffers are used (needs Arc + Mutex).
    output_buffers: bool,
}

impl ImportTracker {
    /// Clears all captured requirements so a new stratum starts from scratch.
    pub(crate) fn reset(&mut self, mode: ExecutionMode, profiling: bool) {
        *self = Self::default();
        self.mode = mode;
        self.profiling = profiling;
    }

    /// Materializes the required import statements as a single token stream.
    ///
    /// Ordering goal:
    /// 1) module prelude (only for incremental)
    /// 2) std imports
    /// 3) external crate imports (dd + timely)
    /// 4) project-level types/constants
    pub(crate) fn render(&mut self) -> TokenStream {
        let prelude = self.prelude_imports();

        // Std
        let std = self.std_imports();

        // External crates
        let dd = self.dd_imports();
        let timely = self.timely_imports();

        // Local aliases/constants
        let diff_type = self.diff_type();
        let semiring_one = self.semiring_one_value();
        let iter_type = self.iter_type();

        // Global allocator (mimalloc for better multi-threaded allocation performance).
        let allocator = self.allocator();

        // String interning support (lasso).
        let string_intern = self.string_intern_imports();

        // Ordered float support.
        let ordered_float = self.ordered_float_imports();

        // SIMD delimiter scanning (memchr).
        let memchr = self.memchr_import();

        // User-defined function module.
        let udf = self.udf_import();

        quote! {
            #prelude

            #std

            #dd
            #timely

            #allocator

            #string_intern
            #ordered_float
            #memchr
            #udf

            #diff_type
            #semiring_one
            #iter_type
        }
    }

    // ---------------------------------------------------------------------
    // Markers
    // ---------------------------------------------------------------------

    /// Marks that `std::fs::File` support is needed for direct file IO.
    pub(crate) fn mark_std_file(&mut self) {
        self.std_file = true;
    }

    /// Marks that buffered IO helpers are required.
    pub(crate) fn mark_std_buf_io(&mut self) {
        self.std_buf_io = true;
    }

    /// Marks that differential dataflow's `Input` trait is needed.
    pub(crate) fn mark_input(&mut self) {
        self.dd_input = true;
    }

    /// Marks that `AsCollection` conversions are required.
    pub(crate) fn mark_as_collection(&mut self) {
        self.as_collection = true;
    }

    /// Marks that the `ThresholdTotal` operator is required.
    pub(crate) fn mark_threshold_total(&mut self) {
        self.threshold_total = true;
    }

    /// Marks that raw stream `Map`/`FlatMap` helpers are required.
    pub(crate) fn mark_timely_map(&mut self) {
        self.timely_map = true;
    }

    /// Marks that the current stratum contains recursion.
    pub(crate) fn mark_recursive(&mut self) {
        self.recursive = true;
    }

    /// Marks that at least one aggregation operator was encountered.
    pub(crate) fn mark_aggregation(&mut self) {
        self.aggregation = true;
    }

    /// Marks that semiring one value is needed.
    pub(crate) fn mark_semiring_one(&mut self) {
        self.semiring_one = true;
    }

    /// Marks that a semiring module is required for a specific kind and numeric type.
    ///
    /// `kind` identifies the aggregation family (Min, Max, Sum, Avg).
    /// Count aggregation reuses the Sum semiring.
    pub(crate) fn mark_semiring(&mut self, kind: SemiringKind, dt: DataType) {
        self.semirings.insert(kind, dt);
    }

    /// Returns whether any semiring module (min, max, sum, or avg) should be written.
    pub(crate) fn needs_semiring(&self) -> bool {
        self.semirings.any()
    }

    /// Returns a reference to the semiring needs for use in code generation.
    pub(crate) fn semirings(&self) -> &SemiringNeeds {
        &self.semirings
    }

    /// Marks that string interning is needed (at least one string-typed column exists).
    pub(crate) fn mark_string_intern(&mut self) {
        self.string_intern = true;
    }

    /// Returns whether string interning is needed.
    pub(crate) fn needs_string_intern(&self) -> bool {
        self.string_intern
    }

    /// Marks that string resolve is needed (an output IDB has string columns).
    pub(crate) fn mark_string_resolve(&mut self) {
        self.string_resolve = true;
    }

    /// Marks that ordered-float is needed (at least one float-typed column exists).
    pub(crate) fn mark_ordered_float(&mut self) {
        self.ordered_float = true;
    }

    /// Returns whether ordered-float is needed.
    pub(crate) fn needs_ordered_float(&self) -> bool {
        self.ordered_float
    }

    /// Marks that memchr is needed for SIMD delimiter scanning.
    pub(crate) fn mark_memchr(&mut self) {
        self.memchr = true;
    }

    /// Returns whether memchr is needed.
    pub(crate) fn needs_memchr(&self) -> bool {
        self.memchr
    }

    /// Marks that the UDF module is needed.
    pub(crate) fn mark_udf(&mut self) {
        self.udf = true;
    }

    /// Marks that in-memory output buffers are used (needs Arc + Mutex).
    pub(crate) fn mark_output_buffers(&mut self) {
        self.output_buffers = true;
    }

    // ---------------------------------------------------------------------
    // Render helpers: grouping + ordering
    // ---------------------------------------------------------------------

    /// Incremental-only module prelude and related imports.
    fn prelude_imports(&mut self) -> TokenStream {
        match self.mode {
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                // Incremental runtime needs HashMap for cmd/prompt/relation helpers.
                self.std_hashmap = true;

                quote! {
                    mod cmd;
                    mod prompt;
                    mod relops;

                    use cmd::{Cmd, TxnAction, TxnOp, TxnState};
                    use prompt::Prompt;
                    use relops::*;

                    use std::sync::{Arc, RwLock};
                }
            }
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => quote! {},
        }
    }

    /// Imports needed for in-memory output buffers (Arc/Mutex, Rc/RefCell).
    fn output_buffer_imports(&self) -> TokenStream {
        if !self.output_buffers {
            return quote! {};
        }
        // Incremental already imports Arc via prelude; batch needs it explicitly.
        let sync_imports = match self.mode {
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                quote! { use std::sync::Mutex; }
            }
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => {
                quote! { use std::sync::{Arc, Mutex}; }
            }
        };
        quote! {
            #sync_imports
            use std::rc::Rc;
            use std::cell::RefCell;
        }
    }

    /// All std imports in a consistent order, with minimal duplication.
    fn std_imports(&mut self) -> TokenStream {
        let file = self.std_file_import();
        let io = self.std_io_import();
        let hashmap = self.std_hashmap_import();
        let output_buf = self.output_buffer_imports();

        if self.profiling {
            // output_buffer_imports() already provides Rc + RefCell when active.
            let rc_refcell = if self.output_buffers {
                quote! {}
            } else {
                quote! {
                    use std::cell::RefCell;
                    use std::rc::Rc;
                }
            };
            return quote! {
                #rc_refcell
                use std::collections::HashMap;
                use std::fs::File;
                #io
                use std::io::{BufWriter, Write};
                #output_buf
                use std::time::{Duration, Instant};
            };
        }

        quote! {
            #file
            #io
            #hashmap
            #output_buf
            use std::time::Instant;
        }
    }

    /// Differential Dataflow imports (operators, traits, etc).
    fn dd_imports(&self) -> TokenStream {
        let input = self.input_import();
        let as_collection = self.as_collection_import();
        let operators = self.operator_imports();
        let recursive = self.recursive_imports();
        let aggregation = self.aggregation_imports();
        let semiring = self.semiring_import();

        quote! {
            #input
            #operators
            #as_collection
            #recursive
            #aggregation
            #semiring
        }
    }

    /// Timely imports (operator traits, probes, logging, etc).
    fn timely_imports(&self) -> TokenStream {
        let timely_map = self.timely_map_import();
        let probe = self.probe_imports();

        let logging = if self.profiling {
            quote! {
                use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
                use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
            }
        } else {
            quote! {}
        };

        quote! {
            #timely_map
            #probe
            #logging
        }
    }

    // ---------------------------------------------------------------------
    // Individual import fragments
    // ---------------------------------------------------------------------

    fn operator_imports(&self) -> TokenStream {
        let mut traits = Vec::new();
        if self.threshold_total {
            traits.push(quote! { ThresholdTotal });
        }

        if traits.is_empty() {
            quote! {}
        } else {
            quote! {
                use differential_dataflow::operators::{ #(#traits),* };
            }
        }
    }

    fn std_file_import(&self) -> TokenStream {
        if self.std_file
            && matches!(
                self.mode,
                ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch
            )
        {
            quote! { use std::fs::File; }
        } else {
            quote! {}
        }
    }

    fn std_io_import(&self) -> TokenStream {
        if self.std_buf_io
            && matches!(
                self.mode,
                ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch
            )
        {
            quote! { use std::io::{BufRead, BufReader, Read, Seek, SeekFrom}; }
        } else {
            quote! {}
        }
    }

    fn std_hashmap_import(&self) -> TokenStream {
        if self.std_hashmap {
            quote! { use std::collections::HashMap; }
        } else {
            quote! {}
        }
    }

    fn input_import(&self) -> TokenStream {
        if self.dd_input {
            quote! { use differential_dataflow::input::Input; }
        } else {
            quote! {}
        }
    }

    fn as_collection_import(&self) -> TokenStream {
        if self.as_collection {
            quote! { use differential_dataflow::AsCollection; }
        } else {
            quote! {}
        }
    }

    fn timely_map_import(&self) -> TokenStream {
        if self.timely_map {
            quote! { use timely::dataflow::operators::vec::Map; }
        } else {
            quote! {}
        }
    }

    fn recursive_imports(&self) -> TokenStream {
        if self.recursive {
            quote! {
                use differential_dataflow::operators::iterate::Variable;
                use timely::dataflow::Scope;
            }
        } else {
            quote! {}
        }
    }

    fn aggregation_imports(&self) -> TokenStream {
        if self.aggregation {
            quote! {
                use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
            }
        } else {
            quote! {}
        }
    }

    fn semiring_import(&self) -> TokenStream {
        if !self.needs_semiring() {
            return quote! {};
        }

        let s = &self.semirings;
        let min = semiring_uses("min", "Min", s);
        let max = semiring_uses("max", "Max", s);
        let sum = semiring_uses("sum", "Sum", s);
        let avg = semiring_uses("avg", "Avg", s);

        quote! {
            mod semiring;
            #min
            #max
            #sum
            #avg
            use differential_dataflow::difference::IsZero;
        }
    }

    fn probe_imports(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::DatalogInc | ExecutionMode::ExtendInc => {
                quote! { use timely::dataflow::operators::probe::Handle as ProbeHandle; }
            }
            ExecutionMode::DatalogBatch | ExecutionMode::ExtendBatch => quote! {},
        }
    }

    fn memchr_import(&self) -> TokenStream {
        if self.memchr {
            quote! { use memchr::memchr_iter; }
        } else {
            quote! {}
        }
    }

    fn ordered_float_imports(&self) -> TokenStream {
        if self.ordered_float {
            quote! { use ordered_float::OrderedFloat; }
        } else {
            quote! {}
        }
    }

    fn udf_import(&self) -> TokenStream {
        if self.udf {
            quote! {
                #[allow(dead_code)]
                mod udf;
            }
        } else {
            quote! {}
        }
    }

    // ---------------------------------------------------------------------
    // Types / constants
    // ---------------------------------------------------------------------

    /// Emit string interning infrastructure using `lasso` crate.
    ///
    /// Provides a thread-safe global `INTERNER` that maps strings to `u32` keys
    /// (and back) so all differential-dataflow computation operates on u32 IDs
    /// instead of heap-allocated `String`s.
    fn string_intern_imports(&self) -> TokenStream {
        if !self.string_intern {
            return quote! {};
        }

        let max_retries = INTERN_MAX_RETRIES;
        let base = quote! {
            use lasso::{ThreadedRodeo, Spur};
            use std::sync::LazyLock;

            /// Global, thread-safe string interner.
            ///
            /// Every unique string encountered at input time is assigned a compact
            /// `Spur` key (internally a `NonZeroU32`). All dataflow computation uses
            /// these keys. Resolution back to `&str` happens only at output time.
            static INTERNER: LazyLock<ThreadedRodeo> = LazyLock::new(ThreadedRodeo::default);

            /// Intern a string, returning its compact `Spur` key.
            /// Retries on transient allocation failures under high thread contention.
            #[inline(always)]
            fn intern(s: &str) -> Spur {
                for _ in 0..#max_retries {
                    match INTERNER.try_get_or_intern(s) {
                        Ok(key) => return key,
                        Err(_) => std::thread::yield_now(),
                    }
                }
                panic!(
                    "string interner failed after {} attempts for {:?}",
                    #max_retries,
                    s
                );
            }
        };

        let resolve_fn = if self.string_resolve {
            quote! {
                /// Resolve a `Spur` key back to the original `&str`.
                #[inline(always)]
                fn resolve(key: Spur) -> &'static str {
                    INTERNER.resolve(&key)
                }
            }
        } else {
            quote! {}
        };

        quote! {
            #base
            #resolve_fn
        }
    }

    fn diff_type(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::DatalogBatch => {
                quote! { type Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { type Diff = i32; },
        }
    }

    fn semiring_one_value(&self) -> TokenStream {
        if !self.semiring_one {
            return quote! {};
        }

        match self.mode {
            ExecutionMode::DatalogBatch => {
                quote! { const SEMIRING_ONE: Diff = differential_dataflow::difference::Present; }
            }
            _ => quote! { const SEMIRING_ONE: Diff = 1; },
        }
    }

    fn iter_type(&self) -> TokenStream {
        if self.recursive {
            quote! { type Iter = u16; }
        } else {
            quote! {}
        }
    }

    /// Emit mimalloc global allocator for better multi-threaded allocation performance.
    fn allocator(&self) -> TokenStream {
        quote! {
            use mimalloc::MiMalloc;

            #[global_allocator]
            static GLOBAL: MiMalloc = MiMalloc;
        }
    }
}

/// Emit `use` statements for a single semiring kind (e.g., min, max, sum, avg),
/// referencing submodules under `semiring::`.
fn semiring_uses(kind: &str, prefix: &str, needs: &SemiringNeeds) -> TokenStream {
    let kind_enum = match kind {
        "min" => SemiringKind::Min,
        "max" => SemiringKind::Max,
        "sum" => SemiringKind::Sum,
        "avg" => SemiringKind::Avg,
        _ => unreachable!(),
    };

    let mut uses = Vec::new();

    let int_needs = needs.int_needs(kind_enum);
    if int_needs.iter().any(|n| *n) {
        let mod_ident =
            proc_macro2::Ident::new(&format!("{kind}_int"), proc_macro2::Span::call_site());
        for (needed, dt) in int_needs.iter().zip(INT_DATA_TYPES.iter()) {
            if *needed {
                let ty = proc_macro2::Ident::new(
                    &format!("{prefix}{}", dt.semiring_suffix()),
                    proc_macro2::Span::call_site(),
                );
                uses.push(quote! { use semiring::#mod_ident::#ty; });
            }
        }
    }

    let float_needs = needs.float_needs(kind_enum);
    if float_needs.iter().any(|n| *n) {
        let mod_ident =
            proc_macro2::Ident::new(&format!("{kind}_float"), proc_macro2::Span::call_site());
        for (needed, dt) in float_needs.iter().zip(FLOAT_DATA_TYPES.iter()) {
            if *needed {
                let ty = proc_macro2::Ident::new(
                    &format!("{prefix}{}", dt.semiring_suffix()),
                    proc_macro2::Span::call_site(),
                );
                uses.push(quote! { use semiring::#mod_ident::#ty; });
            }
        }
    }

    quote! { #(#uses)* }
}
