//! Import tracking utilities for the FlowLog compiler.
//!
//! This module tracks which external crates and traits need to be imported
//! based on the transformations being generated within a stratum.

use common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

/// Records the import requirements gathered while compiling.
#[must_use]
#[derive(Default)]
pub(crate) struct ImportTracker {
    /// Batch or incremental execution mode.
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
    /// ArrangeByKey operator.
    arrange_by_key: bool,
    /// ArrangeBySelf operator.
    arrange_by_self: bool,
    /// AsCollection conversions.
    as_collection: bool,
    /// Threshold operator trait.
    threshold: bool,
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
    /// Whether the Min semiring module is needed (min aggregation optimization).
    min_semiring: bool,
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

        quote! {
            #prelude

            #std

            #dd
            #timely

            #allocator

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

    /// Marks that `ArrangeByKey` must be imported.
    pub(crate) fn mark_arrange_by_key(&mut self) {
        self.arrange_by_key = true;
    }

    /// Marks that `ArrangeBySelf` must be imported.
    pub(crate) fn mark_arrange_by_self(&mut self) {
        self.arrange_by_self = true;
    }

    /// Marks that `AsCollection` conversions are required.
    pub(crate) fn mark_as_collection(&mut self) {
        self.as_collection = true;
    }

    /// Marks that the `Threshold` operator is required.
    pub(crate) fn mark_threshold(&mut self) {
        self.threshold = true;
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

    /// Marks that the Min semiring module is required (min aggregation).
    pub(crate) fn mark_min_semiring(&mut self) {
        self.min_semiring = true;
    }

    /// Returns whether the Min semiring module should be written to the project.
    pub(crate) fn needs_min_semiring(&self) -> bool {
        self.min_semiring
    }

    // ---------------------------------------------------------------------
    // Render helpers: grouping + ordering
    // ---------------------------------------------------------------------

    /// Incremental-only module prelude and related imports.
    fn prelude_imports(&mut self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => {
                // Incremental runtime needs HashMap for cmd/prompt/relation helpers.
                self.std_hashmap = true;

                quote! {
                    mod cmd;
                    mod prompt;
                    mod relation;

                    use cmd::{Cmd, TxnAction, TxnOp, TxnState};
                    use prompt::Prompt;
                    use relation::*;

                    use std::sync::{Arc, Barrier, RwLock};
                }
            }
            ExecutionMode::Batch => quote! {},
        }
    }

    /// All std imports in a consistent order, with minimal duplication.
    fn std_imports(&mut self) -> TokenStream {
        let file = self.std_file_import();
        let io = self.std_io_import();
        let hashmap = self.std_hashmap_import();

        // Profiling implies a set of std imports.
        if self.profiling {
            return quote! {
                use std::cell::RefCell;
                use std::collections::HashMap;
                use std::fs::File;
                #io
                use std::io::{BufWriter, Write};
                use std::rc::Rc;
                use std::time::{Duration, Instant};
            };
        }

        quote! {
            #file
            #io
            #hashmap
            use std::time::Instant;
        }
    }

    /// Differential Dataflow imports (operators, traits, etc).
    fn dd_imports(&self) -> TokenStream {
        let input = self.input_import();
        let arrange = self.arrange_import();
        let as_collection = self.as_collection_import();
        let operators = self.operator_imports();
        let recursive = self.recursive_imports();
        let aggregation = self.aggregation_imports();
        let min_semiring = self.min_semiring_import();

        quote! {
            #input
            #operators
            #arrange
            #as_collection
            #recursive
            #aggregation
            #min_semiring
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
        if self.threshold {
            traits.push(quote! { Threshold });
        }
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
        if self.std_file && self.mode == ExecutionMode::Batch {
            quote! { use std::fs::File; }
        } else {
            quote! {}
        }
    }

    fn std_io_import(&self) -> TokenStream {
        if self.std_buf_io && self.mode == ExecutionMode::Batch {
            quote! { use std::io::{BufRead, BufReader}; }
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

    fn arrange_import(&self) -> TokenStream {
        let mut traits = Vec::new();

        if self.arrange_by_key {
            traits.push(quote! { ArrangeByKey });
        }
        if self.arrange_by_self {
            traits.push(quote! { ArrangeBySelf });
        }

        if traits.is_empty() {
            quote! {}
        } else {
            quote! { use differential_dataflow::operators::arrange::{ #(#traits),* }; }
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
            quote! { use timely::dataflow::operators::Map; }
        } else {
            quote! {}
        }
    }

    fn recursive_imports(&self) -> TokenStream {
        if self.recursive {
            quote! {
                use differential_dataflow::operators::iterate::SemigroupVariable;
                use timely::dataflow::Scope;
            }
        } else {
            quote! {}
        }
    }

    fn aggregation_imports(&self) -> TokenStream {
        if self.aggregation {
            quote! {
                use differential_dataflow::operators::reduce::ReduceCore;
                use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
            }
        } else {
            quote! {}
        }
    }

    fn min_semiring_import(&self) -> TokenStream {
        if self.min_semiring {
            quote! {
                mod min_semiring;
                use min_semiring::Min;
                use differential_dataflow::difference::IsZero;
            }
        } else {
            quote! {}
        }
    }

    fn probe_imports(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => {
                quote! { use timely::dataflow::operators::probe::Handle as ProbeHandle; }
            }
            ExecutionMode::Batch => quote! {},
        }
    }

    // ---------------------------------------------------------------------
    // Types / constants
    // ---------------------------------------------------------------------

    fn diff_type(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => quote! { type Diff = i32; },
            ExecutionMode::Batch => {
                quote! { type Diff = differential_dataflow::difference::Present; }
            }
        }
    }

    fn semiring_one_value(&self) -> TokenStream {
        if !self.semiring_one {
            return quote! {};
        }

        match self.mode {
            ExecutionMode::Incremental => quote! { const SEMIRING_ONE: Diff = 1; },
            ExecutionMode::Batch => {
                quote! { const SEMIRING_ONE: Diff = differential_dataflow::difference::Present; }
            }
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
