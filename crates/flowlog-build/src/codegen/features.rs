//! Codegen feature tracking for the FlowLog compiler.
//!
//! `Features` records which capabilities are needed by the current
//! compilation unit so that downstream passes (imports, scaffold, type
//! declarations) emit only what is required.

use crate::parser::{AggregationOperator, DataType};

use std::collections::HashSet;

// =========================================================================
// AggSemiringNeeds
// =========================================================================

/// Numeric DataTypes in canonical order for semiring code generation.
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

/// Tracks which semiring modules (operator × numeric type) are needed.
/// Count is normalized to Sum on insert.
#[derive(Default, Clone)]
pub struct AggSemiringNeeds(HashSet<(AggregationOperator, DataType)>);

impl AggSemiringNeeds {
    #[inline]
    pub(crate) fn any(&self) -> bool {
        !self.0.is_empty()
    }

    pub(crate) fn insert(&mut self, op: AggregationOperator, dt: DataType) {
        assert!(
            dt.is_numeric(),
            "CodeGen error: semiring only supports numeric types, got {dt}"
        );
        self.0.insert((op.semiring_canonical(), dt));
    }

    #[inline]
    pub(crate) fn int_needs(&self, op: AggregationOperator) -> [bool; 8] {
        let op = op.semiring_canonical();
        INT_DATA_TYPES.map(|dt| self.0.contains(&(op, dt)))
    }

    #[inline]
    pub(crate) fn float_needs(&self, op: AggregationOperator) -> [bool; 2] {
        let op = op.semiring_canonical();
        FLOAT_DATA_TYPES.map(|dt| self.0.contains(&(op, dt)))
    }

    pub fn iter(&self) -> impl Iterator<Item = &(AggregationOperator, DataType)> {
        self.0.iter()
    }
}

// =========================================================================
// Features
// =========================================================================

/// Simple boolean mark/query pairs.
macro_rules! bool_features {
    ($(($field:ident, $marker:ident)),* $(,)?) => {
        $(
            #[inline]
            pub fn $field(&self) -> bool { self.$field }
            #[inline]
            pub(crate) fn $marker(&mut self) { self.$field = true; }
        )*
    };
}

/// Tracks which codegen features are active for the current compilation unit.
#[must_use]
#[derive(Default, Clone)]
pub struct Features {
    // -- differential-dataflow / timely --
    dd_input: bool,
    as_collection: bool,
    threshold_total: bool,
    timely_map: bool,
    // -- dataflow features --
    recursive: bool,
    aggregation: bool,
    agg_semirings: AggSemiringNeeds,
    // -- library support --
    string_intern: bool,
    string_resolve: bool,
    ordered_float: bool,
    udf: bool,
    output_buffers: bool,
}

impl Features {
    /// Clears all flags so a new compilation unit starts from scratch.
    pub(crate) fn reset(&mut self) {
        *self = Self::default();
    }

    // -- boolean features ---------------------------------------------

    bool_features! {
        // dd / timely
        (dd_input,       mark_dd_input),
        (as_collection,  mark_as_collection),
        (threshold_total, mark_threshold_total),
        (timely_map,     mark_timely_map),
        // dataflow
        (recursive,      mark_recursive),
        (aggregation,    mark_aggregation),
        (string_intern,  mark_string_intern),
        (string_resolve, mark_string_resolve),
        (ordered_float,  mark_ordered_float),
        (udf,            mark_udf),
        (output_buffers, mark_output_buffers),
    }

    // -- aggregation semirings (non-boolean) ----------------------------

    #[inline]
    pub(crate) fn mark_agg_semiring(&mut self, op: AggregationOperator, dt: DataType) {
        self.agg_semirings.insert(op, dt);
    }

    #[inline]
    pub fn agg_semiring(&self) -> bool {
        self.agg_semirings.any()
    }

    #[inline]
    pub fn agg_semirings(&self) -> &AggSemiringNeeds {
        &self.agg_semirings
    }
}
