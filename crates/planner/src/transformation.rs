//! Transformation operations for query planning in Macaron Datalog programs.

use crate::collection::{Collection, CollectionSignature};
use crate::flow::TransformationFlow;
use catalog::{ArithmeticPos, AtomArgumentSignature, ComparisonExprPos};
use parser::{AggregationOperator, ConstType};
use std::fmt;
use std::sync::Arc;

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
    pub fn kv_to_kv(
        input: Arc<Collection>,
        output_key_signatures: &[ArithmeticPos],
        output_value_signatures: &[ArithmeticPos],
        const_eq_constraints: &[(AtomArgumentSignature, ConstType)],
        var_eq_constraints: &[(AtomArgumentSignature, AtomArgumentSignature)],
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        let (input_key_signatures, input_value_signatures) = input.kv_argument_signatures();

        // Create the transformation flow that defines how data moves through the operation
        let flow = TransformationFlow::kv_to_kv(
            input_key_signatures,
            input_value_signatures,
            output_key_signatures,
            output_value_signatures,
            const_eq_constraints,
            var_eq_constraints,
            compare_exprs,
        );

        let is_row_in = input_key_signatures.is_empty(); // Input is row-based (no keys)
        let is_row_out = output_key_signatures.is_empty(); // Output is row-based (no keys)
        let is_key_only_out = output_value_signatures.is_empty(); // Output has keys but no values

        // Check if this transformation is a no-op (identity transformation)
        let is_no_op = is_row_in
            && (is_row_out || is_key_only_out)
            && const_eq_constraints.is_empty()
            && var_eq_constraints.is_empty()
            && compare_exprs.is_empty()
            && input_key_signatures
                .iter()
                .chain(input_value_signatures.iter())
                .eq(output_key_signatures
                    .iter()
                    .chain(output_value_signatures.iter()));

        let input_signature_name = input.signature().name();
        let output_name = match (is_row_out, is_key_only_out) {
            (true, false) => format!("Row({}){}", input_signature_name, flow), // Row output with values
            (false, true) => format!("K({}){}", input_signature_name, flow),   // Key-only output
            (false, false) => format!("Kv({}){}", input_signature_name, flow), // Key-value output
            (true, true) => panic!("Planner error: kv_to_kv - null signatures not allowed"),
        };

        let output = Arc::new(Collection::new(
            CollectionSignature::UnaryTransformationOutput { name: output_name },
            output_key_signatures,
            output_value_signatures,
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
    pub fn join(
        input: (Arc<Collection>, Arc<Collection>),
        output_key_signatures: &[ArithmeticPos],
        output_value_signatures: &[ArithmeticPos],
        compare_exprs: &[ComparisonExprPos],
    ) -> Self {
        let (left_key_signatures, left_value_signatures) = input.0.kv_argument_signatures();
        let (_, right_value_signatures) = input.1.kv_argument_signatures();

        // Create transformation flow that defines how the join operation processes data
        let flow = TransformationFlow::join_to_kv(
            left_key_signatures,
            left_value_signatures,
            right_value_signatures,
            output_key_signatures,
            output_value_signatures,
            compare_exprs,
        );

        let is_key_only_left = left_value_signatures.is_empty(); // Left collection has only keys
        let is_key_only_right = right_value_signatures.is_empty(); // Right collection has only keys
        let is_cartesian = left_key_signatures.is_empty(); // No join keys = cartesian product

        let left_name = input.0.signature().name();
        let right_name = input.1.signature().name();
        let output_name = match (is_cartesian, is_key_only_left, is_key_only_right) {
            (true, _, _) => format!("Cartesian({}, {}){}", left_name, right_name, flow), // Cartesian product
            (_, true, true) => format!("JnKK({}, {}){}", left_name, right_name, flow), // Key-Key join
            (_, false, true) => format!("JnKvK({}, {}){}", left_name, right_name, flow), // KeyValue-Key join
            (_, false, false) => format!("JnKvKv({}, {}){}", left_name, right_name, flow), // KeyValue-KeyValue join
            (_, true, false) => format!("JnKKv({}, {}){}", left_name, right_name, flow), // Key-KeyValue join
        };

        let output = Arc::new(Collection::new(
            CollectionSignature::JnOutput { name: output_name },
            output_key_signatures,
            output_value_signatures,
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
    pub fn antijoin(
        input: (Arc<Collection>, Arc<Collection>),
        output_key_signatures: &[ArithmeticPos],
        output_value_signatures: &[ArithmeticPos],
    ) -> Self {
        let (left_key_signatures, left_value_signatures) = input.0.kv_argument_signatures();
        let (_, right_value_signatures) = input.1.kv_argument_signatures();

        // Antijoins require the right collection to be key-only (used for filtering)
        assert!(
            right_value_signatures.is_empty(),
            "Planner error: antijoin - right collection must be key-only"
        );

        // Create transformation flow (no comparison expressions for antijoins)
        let flow = TransformationFlow::join_to_kv(
            left_key_signatures,
            left_value_signatures,
            right_value_signatures,
            output_key_signatures,
            output_value_signatures,
            &[], // No comparison expressions for antijoins
        );

        // Determine antijoin type based on left collection characteristics
        let is_key_only_left = left_value_signatures.is_empty(); // Left collection has only keys

        let left_name = input.0.signature().name();
        let right_name = input.1.signature().name();
        let output_name = if is_key_only_left {
            format!("NjKK({}, {}){}", left_name, right_name, flow) // Key-only antijoin
        } else {
            format!("NjKvK({}, {}){}", left_name, right_name, flow) // Key-value antijoin
        };

        let output = Arc::new(Collection::new(
            CollectionSignature::NegJnOutput { name: output_name },
            output_key_signatures,
            output_value_signatures,
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
        let flow = TransformationFlow::agg_to_row(input_exprs, output_field_expr, operator.clone());

        // Generate descriptive name for the output collection
        let input_signature_name = input.signature().name();
        let output_name = format!("Agg({}){}", input_signature_name, flow);

        // Create the output collection
        // Note: Both key and value signatures are empty for row-based aggregation results
        // TODO: here is actually a bit confusing, since from the collection you can tell no difference
        let output = Arc::new(Collection::new(
            CollectionSignature::UnaryTransformationOutput { name: output_name },
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
                output, is_no_op, ..
            } => {
                write!(f, "{} {}", if *is_no_op { "∅" } else { "→" }, output)
            }
            Self::RowToK {
                output, is_no_op, ..
            } => {
                write!(f, "{} {}", if *is_no_op { "∅" } else { "⟶" }, output)
            }
            Self::RowToKv { output, .. } => {
                write!(f, "⟶ {}", output)
            }
            Self::KvToKv { output, .. } | Self::KvToK { output, .. } => {
                write!(f, "⟶ {}", output)
            }
            Self::AggToRow { output, .. } => {
                write!(f, "Agg {}", output)
            }
            Self::JnKK { output, .. }
            | Self::JnKKv { output, .. }
            | Self::JnKvK { output, .. }
            | Self::JnKvKv { output, .. } => {
                write!(f, "⋈ {}", output)
            }
            Self::Cartesian { output, .. } => {
                write!(f, "⨯ {}", output)
            }
            Self::NjKvK { output, .. } | Self::NjKK { output, .. } => {
                write!(f, "¬ {}", output)
            }
        }
    }
}
