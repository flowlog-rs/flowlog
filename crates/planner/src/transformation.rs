//! Transformation operations for query planning in Macaron Datalog programs.
//!
//! This module provides the core transformation abstractions that define how data flows
//! through query execution plans. Transformations represent operations like filtering,
//! projection, joins, and aggregation that convert input collections into output collections.

use crate::collection::Collection;
use std::fmt;
use std::sync::Arc;
use tracing::trace;

pub mod flow;
pub mod info;

pub use flow::TransformationFlow;
pub use info::{KeyValueLayout, TransformationInfo};

/// Represents a data transformation operation in a query execution plan.
#[derive(Clone, Hash, Eq, PartialEq)]
pub enum Transformation {
    // === Unary Transformations ===
    /// Row-to-row transformation (filtering, projection, aggregation)
    RowToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Row-to-key transformation (extract keys from rows)
    RowToK {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Row-to-key-value transformation (structure rows into KV pairs)
    RowToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Key-value to key-value transformation
    KvToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Key-value to key-only transformation
    KvToK {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Key-value to row transformation (drop keys)
    KvToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    // === Binary Transformations ===
    /// Join: Key-only ⋈ Key-only
    JnKK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Join: Key-only ⋈ Key-value (right has values)
    JnKKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Join: Key-value ⋈ Key-value
    JnKvKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Cartesian product (no join keys)
    Cartesian {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Antijoin: Key-value ¬ Key-only
    NjKKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Antijoin: Key-only ¬ Key-only
    NjKK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
}

// ========================
// Inspectors
// ========================
impl Transformation {
    /// Returns `true` if this is a unary transformation.
    pub fn is_unary(&self) -> bool {
        matches!(
            self,
            Self::RowToRow { .. }
                | Self::RowToKv { .. }
                | Self::RowToK { .. }
                | Self::KvToKv { .. }
                | Self::KvToK { .. }
                | Self::KvToRow { .. }
        )
    }
}

// ========================
// Getters
// ========================
impl Transformation {
    /// Returns the input collection for unary transformations.
    ///
    /// # Panics
    ///
    /// Panics if called on a binary transformation. Use `is_unary()` to check first.
    pub fn unary_input(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::RowToK { input, .. }
            | Self::KvToKv { input, .. }
            | Self::KvToK { input, .. }
            | Self::KvToRow { input, .. } => input,
            _ => panic!("Planner error: unary_input called on binary transformation"),
        }
    }

    /// Returns the input collections for binary transformations.
    ///
    /// # Panics
    ///
    /// Panics if called on a unary transformation. Use `is_unary()` to check first.
    pub fn binary_input(&self) -> &(Arc<Collection>, Arc<Collection>) {
        match self {
            Self::JnKK { input, .. }
            | Self::JnKKv { input, .. }
            | Self::JnKvKv { input, .. }
            | Self::Cartesian { input, .. }
            | Self::NjKK { input, .. }
            | Self::NjKKv { input, .. } => input,
            _ => panic!("Planner error: binary_input called on unary transformation"),
        }
    }

    /// Returns the output collection for any transformation.
    pub fn output(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { output, .. }
            | Self::RowToKv { output, .. }
            | Self::RowToK { output, .. }
            | Self::KvToKv { output, .. }
            | Self::KvToK { output, .. }
            | Self::KvToRow { output, .. }
            | Self::JnKK { output, .. }
            | Self::JnKKv { output, .. }
            | Self::JnKvKv { output, .. }
            | Self::Cartesian { output, .. }
            | Self::NjKKv { output, .. }
            | Self::NjKK { output, .. } => output,
        }
    }

    /// Returns the transformation flow for any transformation.
    pub fn flow(&self) -> &TransformationFlow {
        match self {
            Self::RowToRow { flow, .. }
            | Self::RowToKv { flow, .. }
            | Self::RowToK { flow, .. }
            | Self::KvToKv { flow, .. }
            | Self::KvToK { flow, .. }
            | Self::KvToRow { flow, .. }
            | Self::JnKK { flow, .. }
            | Self::JnKKv { flow, .. }
            | Self::JnKvKv { flow, .. }
            | Self::Cartesian { flow, .. }
            | Self::NjKKv { flow, .. }
            | Self::NjKK { flow, .. } => flow,
        }
    }
}

// ========================
// Constructors
// ========================
impl Transformation {
    /// Creates a key-value to key-value transformation.
    ///
    /// This method analyzes the input and output layouts to determine the specific
    /// transformation type needed (RowToRow, RowToK, RowToKv, KvToKv, KvToK, or KvToRow).
    /// It automatically detects whether the transformation is a no-op optimization.
    ///
    /// # Arguments
    ///
    /// * `info` - TransformationInfo containing input/output layouts and constraints
    ///
    /// # Returns
    ///
    /// A Transformation variant appropriate for the input/output layout combination:
    /// - `RowToRow`: Row input → Row output (filtering/projection)
    /// - `RowToK`: Row input → Key-only output (key extraction)
    /// - `RowToKv`: Row input → Key-value output (structuring)
    /// - `KvToKv`: Key-value input → Key-value output (transformation)
    /// - `KvToK`: Key-value input → Key-only output (key extraction)
    /// - `KvToRow`: Key-value input → Row output (flattening)
    pub fn kv_to_kv(info: &TransformationInfo) -> Self {
        trace!("Creating kv_to_kv transformation with info: {:?}", info);
        // Create the transformation flow that defines how data moves through the operation
        let flow = TransformationFlow::kv_to_kv(
            info.input_kv_layout().0,
            info.output_kv_layout(),
            info.const_eq_constraints(),
            info.var_eq_constraints(),
            info.compare_exprs(),
        );

        let is_row_in = info.input_kv_layout().0.key().is_empty(); // Input is row-based (no keys)
        let is_row_out = info.output_kv_layout().key().is_empty(); // Output is row-based (no keys)
        let is_key_only_out = info.output_kv_layout().value().is_empty(); // Output has keys but no values

        let input = Arc::new(Collection::new(
            info.input_info_fp().0,
            info.input_kv_layout().0.key(),
            info.input_kv_layout().0.value(),
        ));

        let output = Arc::new(Collection::new(
            info.output_info_fp(),
            info.output_kv_layout().key(),
            info.output_kv_layout().value(),
        ));

        match (is_row_in, is_row_out, is_key_only_out) {
            // Row input -> Row output: filtering, projection, or aggregation on row data
            (true, true, _) => Self::RowToRow {
                input,
                output,
                flow,
            },
            // Row input -> Key-only output: extract keys from row data
            (true, false, true) => Self::RowToK {
                input,
                output,
                flow,
            },
            // Row input -> Key-value output: structure row data into key-value pairs
            (true, false, false) => Self::RowToKv {
                input,
                output,
                flow,
            },
            // Key-value input -> Key-value output: transform existing key-value structure
            (false, false, false) => Self::KvToKv {
                input,
                output,
                flow,
            },
            // Key-value input -> Key-only output: extract keys from key-value pairs
            (false, false, true) => Self::KvToK {
                input,
                output,
                flow,
            },
            // Key-value input -> Row output: drop keys and produce rows
            (false, true, _) => Self::KvToRow {
                input,
                output,
                flow,
            },
        }
    }

    /// Creates a join transformation between two collections.
    ///
    /// This method automatically determines the appropriate join type based on the
    /// input collection characteristics and join key presence. It supports equi-joins,
    /// cartesian products, and various key/value combinations.
    ///
    /// # Arguments
    ///
    /// * `info` - TransformationInfo containing both input layouts and output structure
    ///
    /// # Returns
    ///
    /// A binary Transformation variant based on the input collection types:
    /// - `JnKK`: Key-only ⋈ Key-only join
    /// - `JnKKv`: Key-only ⋈ Key-value join (right has values)
    /// - `JnKvK`: Key-value ⋈ Key-only join (left has values)
    /// - `JnKvKv`: Key-value ⋈ Key-value join (both have values)
    /// - `Cartesian`: Cartesian product (no join keys)
    pub fn join(info: &TransformationInfo) -> Self {
        // Create transformation flow that defines how the join operation processes data
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            info.compare_exprs(),
        );

        let is_key_only_left = info.input_kv_layout().0.value().is_empty(); // Left collection has only keys
        let is_key_only_right = info.input_kv_layout().1.unwrap().value().is_empty(); // Right collection has only keys
        let is_cartesian = info.input_kv_layout().0.key().is_empty(); // No join keys = cartesian product

        let input = (
            Arc::new(Collection::new(
                info.input_info_fp().0,
                info.input_kv_layout().0.key(),
                info.input_kv_layout().0.value(),
            )),
            Arc::new(Collection::new(
                info.input_info_fp().1.unwrap(),
                info.input_kv_layout().1.unwrap().key(),
                info.input_kv_layout().1.unwrap().value(),
            )),
        );

        let output = Arc::new(Collection::new(
            info.output_info_fp(),
            info.output_kv_layout().key(),
            info.output_kv_layout().value(),
        ));

        if is_cartesian {
            // Special case: Cartesian product (no join keys)
            Self::Cartesian {
                input,
                output,
                flow,
            }
        } else {
            // Standard joins based on collection types
            match (is_key_only_left, is_key_only_right) {
                // Key-only ⋈ Key-only: both collections have keys but no values
                (true, true) => Self::JnKK {
                    input,
                    output,
                    flow,
                },
                // Key-value ⋈ Key-value: both collections have keys and values
                (false, false) => Self::JnKvKv {
                    input,
                    output,
                    flow,
                },
                // Key-only ⋈ Key-value: left is key-only, right has values
                (true, false) => Self::JnKKv {
                    input,
                    output,
                    flow,
                },
                // Key-value ⋈ Key-only: left has values, right is key-only
                (false, true) => unreachable!(
                    "Planner error: join - left collection cannot have values when right is key-only"
                ),
            }
        }
    }

    /// Creates an antijoin transformation.
    ///
    /// Antijoins are used for filtering operations where tuples from the left collection
    /// are excluded if they have matching keys in the right collection. This is commonly
    /// used for implementing logical negation in Datalog rules.
    ///
    /// # Arguments
    ///
    /// * `info` - TransformationInfo containing both input layouts (left must be key-only)
    ///
    /// # Returns
    ///
    /// A binary antijoin Transformation variant:
    /// - `NjKK`: Key-only ¬⋈ Key-only antijoin
    /// - `NjKvK`: Key-value ¬⋈ Key-only antijoin
    ///
    /// # Panics
    ///
    /// Panics if the left collection is not key-only, as antijoins require the left
    /// collection to contain only keys for filtering purposes.
    pub fn antijoin(info: &TransformationInfo) -> Self {
        // Antijoins require the left collection to be key-only (used for filtering)
        assert!(
            info.input_kv_layout().0.value().is_empty(),
            "Planner error: antijoin - left collection must be key-only"
        );

        // Create transformation flow (no comparison expressions for antijoins)
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            &[], // No comparison expressions for antijoins
        );

        // Determine antijoin type based on left collection characteristics
        let is_key_only_right = info.input_kv_layout().1.unwrap().value().is_empty(); // Right collection has only keys

        let input = (
            Arc::new(Collection::new(
                info.input_info_fp().0,
                info.input_kv_layout().0.key(),
                info.input_kv_layout().0.value(),
            )),
            Arc::new(Collection::new(
                info.input_info_fp().1.unwrap(),
                info.input_kv_layout().1.unwrap().key(),
                info.input_kv_layout().1.unwrap().value(),
            )),
        );

        let output = Arc::new(Collection::new(
            info.output_info_fp(),
            info.output_kv_layout().key(),
            info.output_kv_layout().value(),
        ));

        if is_key_only_right {
            // Key-only ¬⋈ Key-only: filter key-only records using key-only filter
            Self::NjKK {
                input,
                output,
                flow,
            }
        } else {
            // Key-only ¬⋈ Key-value: filter key-value records using key-only filter
            Self::NjKKv {
                input,
                output,
                flow,
            }
        }
    }
}

impl fmt::Display for Transformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RowToRow {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[RowToRow]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::RowToK {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[RowToK]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::RowToKv {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[RowToKv]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::KvToKv {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[KvToKv]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::KvToK {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[KvToK]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::KvToRow {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[KvToRow]\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    input, flow, output
                )
            }
            Self::JnKK {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[JnKK]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
            Self::JnKKv {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[JnKKv]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
            Self::JnKvKv {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[JnKvKv]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
            Self::Cartesian {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[Cartesian]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
            Self::NjKKv {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[NjKvK]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
            Self::NjKK {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[NjKK]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
                    l, r, flow, output
                )
            }
        }
    }
}

impl fmt::Debug for Transformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self)
    }
}
