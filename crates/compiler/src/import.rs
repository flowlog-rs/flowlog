//! Import tracking utilities for FlowLog compiler.
//!
//! This module tracks which external crates and traits need to be imported
//! based on the transformations being generated within a stratum.

use proc_macro2::TokenStream;
use quote::quote;

use common::ExecutionMode;

/// Records the import requirements gathered while compiling.
#[must_use]
#[derive(Default)]
pub(crate) struct ImportTracker {
    /// Batch or incremental execution mode.
    mode: ExecutionMode,

    /// Standard file IO support.
    std_file: bool,

    /// Standard buffered IO support.
    std_buf_io: bool,

    /// Differential dataflow Input trait.
    dd_input: bool,

    /// ArrangeByKey operator.
    arrange_by_key: bool,

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
}

impl ImportTracker {
    /// Clears all captured requirements so a new stratum starts from scratch.
    pub(crate) fn reset(&mut self, mode: ExecutionMode) {
        *self = Self::default();
        self.mode = mode;
    }

    /// Materializes the required import statements as a single token stream.
    pub(crate) fn render(&self) -> TokenStream {
        let precludes = self.preclude();

        let std_file = self.std_file_import();
        let std_io = self.std_io_import();
        let input = self.input_import();
        let arrange = self.arrange_import();
        let as_collection = self.as_collection_import();
        let timely_map = self.timely_map_import();
        let operator_imports = self.operator_imports();
        let recursive_imports = self.recursive_imports();
        let aggregation_imports = self.aggregation_imports();
        let probe_imports = self.probe_imports();

        let diff_type = self.diff_type();
        let semiring_one = self.semiring_one_value();
        let iter_type = self.iter_type();

        quote! {
            #precludes

            #std_file
            #std_io
            use std::time::Instant;

            #input
            #operator_imports
            #arrange
            #as_collection
            #timely_map
            #recursive_imports
            #aggregation_imports
            #probe_imports

            #diff_type
            #semiring_one
            #iter_type
        }
    }

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

    /// Marks that raw stream map/flat_map helpers are required.
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

    pub(crate) fn mark_semiring_one(&mut self) {
        self.semiring_one = true;
    }

    /// Emits the operator trait imports required so far.
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
            quote! { use differential_dataflow::operators::{ #(#traits),* }; }
        }
    }

    /// Emits `std::fs::File` if requested.
    fn std_file_import(&self) -> TokenStream {
        if self.std_file && self.mode == ExecutionMode::Batch {
            quote! { use std::fs::File; }
        } else {
            quote! {}
        }
    }

    /// Emits buffered IO helpers if requested.
    fn std_io_import(&self) -> TokenStream {
        if self.std_buf_io && self.mode == ExecutionMode::Batch {
            quote! { use std::io::{BufRead, BufReader}; }
        } else {
            quote! {}
        }
    }

    /// Emits the differential dataflow input trait if requested.
    fn input_import(&self) -> TokenStream {
        if self.dd_input {
            quote! { use differential_dataflow::input::Input; }
        } else {
            quote! {}
        }
    }

    /// Emits `ArrangeByKey` if requested.
    fn arrange_import(&self) -> TokenStream {
        if self.arrange_by_key {
            quote! { use differential_dataflow::operators::arrange::ArrangeByKey; }
        } else {
            quote! {}
        }
    }

    /// Emits `AsCollection` if requested.
    fn as_collection_import(&self) -> TokenStream {
        if self.as_collection {
            quote! { use differential_dataflow::AsCollection; }
        } else {
            quote! {}
        }
    }

    /// Emits the timely `Map` helper if requested.
    fn timely_map_import(&self) -> TokenStream {
        if self.timely_map {
            quote! { use timely::dataflow::operators::Map; }
        } else {
            quote! {}
        }
    }

    /// Emits recursion-only imports, such as `SemigroupVariable`.
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

    /// Emits aggregation-only imports.
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

    /// Emits probe imports if in incremental mode.
    fn probe_imports(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => quote! {
                use timely::dataflow::operators::probe::Handle as ProbeHandle;
            },
            ExecutionMode::Batch => quote! {},
        }
    }

    /// Precludes crate modules.
    fn preclude(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => quote! {
                mod cmd;
                mod prompt;
                mod relation;

                use cmd::{Cmd, TxnAction, TxnOp, TxnState};
                use relation::*;
                use prompt::Prompt;

                use std::collections::HashMap;
                use std::sync::{Arc, Barrier, RwLock};
            },
            ExecutionMode::Batch => quote! {},
        }
    }

    /// Differential dataflow diff type.
    fn diff_type(&self) -> TokenStream {
        match self.mode {
            ExecutionMode::Incremental => quote! { type Diff = i32; },
            ExecutionMode::Batch => {
                quote! { type Diff = differential_dataflow::difference::Present; }
            }
        }
    }

    /// Semiring one value.
    fn semiring_one_value(&self) -> TokenStream {
        if self.semiring_one {
            match self.mode {
                ExecutionMode::Incremental => quote! { const SEMIRING_ONE: Diff = 1; },
                ExecutionMode::Batch => {
                    quote! { const SEMIRING_ONE: Diff = differential_dataflow::difference::Present; }
                }
            }
        } else {
            quote! {}
        }
    }

    /// Iterator type.
    fn iter_type(&self) -> TokenStream {
        if self.recursive {
            quote! { type Iter = u16; }
        } else {
            quote! {}
        }
    }
}
