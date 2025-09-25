//! Transformation operations for query planning in Macaron Datalog programs.

use crate::collection::{Collection, CollectionSignature};
use catalog::ArithmeticPos;
use parser::AggregationOperator;
use std::fmt;
use std::sync::Arc;

pub mod flow;
pub mod info;

pub use flow::TransformationFlow;
pub use info::TransformationInfo;

/// Represents a data transformation operation in a query execution plan.
#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum Transformation {
    // === Unary Transformations ===
    /// Row-to-row transformation (filtering, projection, aggregation)
    RowToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
        is_no_op: bool,
    },

    /// Row-to-key transformation
    RowToK {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
        is_no_op: bool,
    },

    /// Row-to-key-value transformation
    RowToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Key-value-to-key-value transformation (filtering intermediates)
    KvToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Key-value-to-key transformation
    KvToK {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    // === Binary Transformations ===
    /// Join between two key-only collections
    JnKK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Join between key-only and key-value collections
    JnKKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Join between key-value and key-only collections
    JnKvK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Join between two key-value collections
    JnKvKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Cartesian product of two collections
    Cartesian {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Antijoin between key-value and key-only collections
    NjKvK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    /// Antijoin between two key-only collections
    NjKK {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    // === Aggregation Transformations ===
    /// Row-to-row aggregation transformation (GROUP BY with aggregation functions)
    AggToRow {
        input: Arc<Collection>,
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
        // Create the transformation flow that defines how data moves through the operation
        let flow = TransformationFlow::kv_to_kv(
            info.input_key(),
            info.input_value().0,
            info.output_key(),
            info.output_value(),
            info.const_eq_constraints(),
            info.var_eq_constraints(),
            info.compare_exprs(),
        );

        let is_row_in = info.input_key().is_empty(); // Input is row-based (no keys)
        let is_row_out = info.output_key().is_empty(); // Output is row-based (no keys)
        let is_key_only_out = info.output_value().is_empty(); // Output has keys but no values

        // Check if this transformation is a no-op (identity transformation)
        let is_no_op = is_row_in
            && (is_row_out || is_key_only_out)
            && info.const_eq_constraints().is_empty()
            && info.var_eq_constraints().is_empty()
            && info.compare_exprs().is_empty()
            && info
                .input_key()
                .iter()
                .chain(info.input_value().0.iter())
                .eq(info.output_key().iter().chain(info.output_value().iter()));

        let input = Arc::new(Collection::new(
            CollectionSignature::from_unary(CollectionSignature(info.input_sig().0), &flow),
            info.input_key(),
            info.input_value().0,
        ));

        let output = Arc::new(Collection::new(
            CollectionSignature::from_unary(CollectionSignature(info.output_sig()), &flow),
            info.output_key(),
            info.output_value(),
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
            _ => panic!("Planner error: kv_to_kv - unexpected transformation type"),
        }
    }

    /// Creates a join transformation between two collections.
    pub fn join(info: &TransformationInfo) -> Self {
        // Create transformation flow that defines how the join operation processes data
        let flow = TransformationFlow::join_to_kv(
            info.input_key(),
            info.input_value().0,
            info.input_value().1.unwrap(),
            info.output_key(),
            info.output_value(),
            info.compare_exprs(),
        );

        let is_key_only_left = info.input_value().0.is_empty(); // Left collection has only keys
        let is_key_only_right = info.input_value().1.unwrap().is_empty(); // Right collection has only keys
        let is_cartesian = info.input_key().is_empty(); // No join keys = cartesian product

        let input = (
            Arc::new(Collection::new(
                CollectionSignature::from_unary(CollectionSignature(info.input_sig().0), &flow),
                info.input_key(),
                info.input_value().0,
            )),
            Arc::new(Collection::new(
                CollectionSignature::from_unary(
                    CollectionSignature(info.input_sig().1.unwrap()),
                    &flow,
                ),
                info.input_key(),
                info.input_value().1.unwrap(),
            )),
        );

        let output = Arc::new(Collection::new(
            CollectionSignature::from_join(
                CollectionSignature(info.input_sig().0),
                CollectionSignature(info.input_sig().1.unwrap()),
                &flow,
            ),
            info.output_key(),
            info.output_value(),
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
        // Antijoins require the right collection to be key-only (used for filtering)
        assert!(
            info.input_value().1.unwrap().is_empty(),
            "Planner error: antijoin - right collection must be key-only"
        );

        // Create transformation flow (no comparison expressions for antijoins)
        let flow = TransformationFlow::join_to_kv(
            info.input_key(),
            info.input_value().0,
            info.input_value().1.unwrap(),
            info.output_key(),
            info.output_value(),
            &[], // No comparison expressions for antijoins
        );

        // Determine antijoin type based on left collection characteristics
        let is_key_only_left = info.input_value().0.is_empty(); // Left collection has only keys

        let input = (
            Arc::new(Collection::new(
                CollectionSignature::from_unary(CollectionSignature(info.input_sig().0), &flow),
                info.input_key(),
                info.input_value().0,
            )),
            Arc::new(Collection::new(
                CollectionSignature::from_unary(
                    CollectionSignature(info.input_sig().1.unwrap()),
                    &flow,
                ),
                info.input_key(),
                info.input_value().1.unwrap(),
            )),
        );

        let output = Arc::new(Collection::new(
            CollectionSignature::from_neg_join(
                CollectionSignature(info.input_sig().0),
                CollectionSignature(info.input_sig().1.unwrap()),
                &flow,
            ),
            info.output_key(),
            info.output_value(),
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

    /// Creates an aggregation transformation from row input to row output.
    pub fn agg_to_row(
        input: Arc<Collection>,
        input_exprs: &[ArithmeticPos],
        output_exprs: &[ArithmeticPos],
        output_field_expr: &ArithmeticPos,
        operator: AggregationOperator,
    ) -> Self {
        // Create the aggregation flow using the flow module
        let flow = TransformationFlow::agg_to_row(input_exprs, output_field_expr, operator);

        // Create the output collection
        // Note: Both key and value signatures are empty for row-based aggregation results
        // TODO: here is actually a bit confusing, since from the collection you can tell no difference
        let output = Arc::new(Collection::new(
            CollectionSignature::from_unary(**input.signature(), &flow),
            &[],          // No keys for row-based output
            output_exprs, // All outputs are values in row-based aggregation
        ));

        Self::AggToRow {
            input,
            output,
            flow,
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
                if *is_no_op {
                    write!(f, "{} =={}==> {} [no-op]", input, flow, output)
                } else {
                    write!(f, "{} ----{}----> {}", input, flow, output)
                }
            }
            Self::RowToK {
                input,
                output,
                flow,
                is_no_op,
            } => {
                if *is_no_op {
                    write!(f, "{} =={}==> {} [no-op]", input, flow, output)
                } else {
                    write!(f, "{} ----{}----> {}", input, flow, output)
                }
            }
            Self::RowToKv {
                input,
                output,
                flow,
            } => {
                write!(f, "{} ----{}----> {}", input, flow, output)
            }
            Self::KvToKv {
                input,
                output,
                flow,
            } => {
                write!(f, "{} ----{}----> {}", input, flow, output)
            }
            Self::KvToK {
                input,
                output,
                flow,
            } => {
                write!(f, "{} ----{}----> {}", input, flow, output)
            }
            Self::AggToRow {
                input,
                output,
                flow,
            } => {
                write!(f, "{} ----{}----> {}", input, flow, output)
            }
            Self::JnKK {
                input: (l, r),
                output,
                flow,
            }
            | Self::JnKKv {
                input: (l, r),
                output,
                flow,
            }
            | Self::JnKvK {
                input: (l, r),
                output,
                flow,
            }
            | Self::JnKvKv {
                input: (l, r),
                output,
                flow,
            } => {
                write!(f, "({} ⋈ {}) ----{}----> {}", l, r, flow, output)
            }
            Self::Cartesian {
                input: (l, r),
                output,
                flow,
            } => {
                write!(f, "({} × {}) ----{}----> {}", l, r, flow, output)
            }
            Self::NjKvK {
                input: (l, r),
                output,
                flow,
            }
            | Self::NjKK {
                input: (l, r),
                output,
                flow,
            } => {
                write!(f, "({} ¬ {}) ----{}----> {}", l, r, flow, output)
            }
        }
    }
}
