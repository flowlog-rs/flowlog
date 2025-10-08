//! Transformation operations for query planning in Macaron Datalog programs.

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
        is_no_op: bool,
    },
    /// Row-to-key transformation (extract keys from rows)
    RowToK {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
        is_no_op: bool,
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
    /// Aggregation from row input to row output
    AggToRow {
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
    /// Join: Key-value ⋈ Key-only (left has values)
    JnKvK {
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
    NjKvK {
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

impl Transformation {
    /// Returns the input collection for unary transformations.
    pub fn unary_input(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::RowToK { input, .. }
            | Self::KvToKv { input, .. }
            | Self::KvToK { input, .. }
            | Self::KvToRow { input, .. }
            | Self::AggToRow { input, .. } => input,
            _ => panic!("Planner error: unary_input called on binary transformation"),
        }
    }

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
                | Self::AggToRow { .. }
        )
    }

    /// Returns the input collections for binary transformations.
    pub fn binary_input(&self) -> &(Arc<Collection>, Arc<Collection>) {
        match self {
            Self::JnKK { input, .. }
            | Self::JnKKv { input, .. }
            | Self::JnKvK { input, .. }
            | Self::JnKvKv { input, .. }
            | Self::Cartesian { input, .. }
            | Self::NjKvK { input, .. }
            | Self::NjKK { input, .. } => input,
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
            | Self::AggToRow { output, .. }
            | Self::JnKK { output, .. }
            | Self::JnKKv { output, .. }
            | Self::JnKvK { output, .. }
            | Self::JnKvKv { output, .. }
            | Self::Cartesian { output, .. }
            | Self::NjKvK { output, .. }
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
            | Self::AggToRow { flow, .. }
            | Self::JnKK { flow, .. }
            | Self::JnKKv { flow, .. }
            | Self::JnKvK { flow, .. }
            | Self::JnKvKv { flow, .. }
            | Self::Cartesian { flow, .. }
            | Self::NjKvK { flow, .. }
            | Self::NjKK { flow, .. } => flow,
        }
    }

    /// Creates a key-value to key-value transformation.
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

        // Check if this transformation is a no-op (identity transformation)
        let is_no_op = is_row_in
            && (is_row_out || is_key_only_out)
            && info.const_eq_constraints().is_empty()
            && info.var_eq_constraints().is_empty()
            && info.compare_exprs().is_empty()
            && info
                .input_kv_layout()
                .0
                .key()
                .iter()
                .chain(info.input_kv_layout().0.value().iter())
                .eq(info
                    .output_kv_layout()
                    .key()
                    .iter()
                    .chain(info.output_kv_layout().value().iter()));

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
                is_no_op,
            },
            // Row input -> Key-only output: extract keys from row data
            (true, false, true) => Self::RowToK {
                input,
                output,
                flow,
                is_no_op,
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
                // Key-value ⋈ Key-only: left has values, right is key-only
                (false, true) => Self::JnKvK {
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
            }
        }
    }

    /// Creates an antijoin transformation.
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
        let is_key_only_left = info.input_kv_layout().0.value().is_empty(); // Left collection has only keys

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

        if is_key_only_left {
            // Key-only ¬⋈ Key-only: filter key-only records using key-only filter
            Self::NjKK {
                input,
                output,
                flow,
            }
        } else {
            // Key-value ¬⋈ Key-only: filter key-value records using key-only filter
            Self::NjKvK {
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
                is_no_op,
            } => {
                let no_op = if *is_no_op { " [no-op]" } else { "" };
                write!(
                    f,
                    "[RowToRow]{}\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    no_op, input, flow, output
                )
            }
            Self::RowToK {
                input,
                output,
                flow,
                is_no_op,
            } => {
                let no_op = if *is_no_op { " [no-op]" } else { "" };
                write!(
                    f,
                    "[RowToK]{}\n   In   : {}\n   Flow : {}\n   Out  : {}",
                    no_op, input, flow, output
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
            Self::AggToRow {
                input,
                output,
                flow,
            } => {
                write!(
                    f,
                    "[AggToRow]\n   In   : {}\n   Flow : {}\n   Out  : {}",
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
            Self::JnKvK {
                input: (l, r),
                output,
                flow,
            } => {
                write!(
                    f,
                    "[JnKvK]\n   Left : {}\n   Right: {}\n   Flow : {}\n   Out  : {}",
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
            Self::NjKvK {
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
