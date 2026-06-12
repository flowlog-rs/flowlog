//! Transformation operations for query planning in FlowLog Datalog programs.
//!
//! This module provides the core transformation abstractions that define how
//! data flows through query execution plans. Transformations represent
//! operations like filtering, projection, joins, and aggregation that convert
//! input collections into output collections.
//!
//! Each [`Transformation`] is identified by the fingerprint of its output
//! [`Collection`]. [`Transformation::from_info`] materializes a planned
//! [`TransformationInfo`] into a transformation whose output fingerprint is
//! **content-canonical** — derived from a variant tag, the resolved input
//! fingerprints, and the [`TransformationFlow`], with no rule-local lineage —
//! so that identical operations originating in different rules collapse to one
//! fingerprint. That single fingerprint is what the dedup / factoring / prune
//! passes (see the [`planner`](crate::planner) module docs) key on to share work.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use tracing::trace;

use crate::catalog::JoinPredicates;
use crate::common::compute_fp;
use crate::planner::Collection;

mod flow;
mod info;

pub(crate) use flow::TransformationFlow;
pub(crate) use info::{KeyValueLayout, TransformationInfo};

/// Represents a data transformation operation in a query execution plan.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub(crate) enum Transformation {
    // === Unary Transformations ===
    /// Row-to-row transformation (filtering, projection, aggregation)
    RowToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Row-to-key-value transformation (structure rows into KV pairs).
    /// Includes key-value, key-only, and value-only outputs.
    RowToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Key-value to row transformation
    KvToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Key-value to key-value transformation.
    /// Includes key-value, key-only, and value-only outputs.
    KvToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },

    // === Binary Transformations ===
    /// Join: Key-value ⋈ Key-value to row transformation
    JnToRow {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Join: Key-value ⋈ Key-value to key-value transformation.
    /// Includes key-value, key-only, and value-only outputs.
    JnToKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Antijoin: Key-value ¬ Key-only to row transformation
    NJnToRow {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Antijoin: Key-only ¬ Key-only to key-value transformation.
    /// Includes key-value, key-only, and value-only outputs.
    NJnToKv {
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
    pub(crate) fn is_unary(&self) -> bool {
        matches!(
            self,
            Self::RowToRow { .. }
                | Self::RowToKv { .. }
                | Self::KvToRow { .. }
                | Self::KvToKv { .. }
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
    pub(crate) fn unary_input(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::KvToRow { input, .. }
            | Self::KvToKv { input, .. } => input,
            _ => panic!("Planner error: unary_input called on binary transformation"),
        }
    }

    /// Returns the input collections for binary transformations.
    ///
    /// # Panics
    ///
    /// Panics if called on a unary transformation. Use `is_unary()` to check first.
    pub(crate) fn binary_input(&self) -> &(Arc<Collection>, Arc<Collection>) {
        match self {
            Self::JnToRow { input, .. }
            | Self::JnToKv { input, .. }
            | Self::NJnToRow { input, .. }
            | Self::NJnToKv { input, .. } => input,
            _ => panic!("Planner error: binary_input called on unary transformation"),
        }
    }

    /// Returns the input fingerprint(s) for any transformation.
    pub(crate) fn input_fingerprints(&self) -> Vec<u64> {
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::KvToRow { input, .. }
            | Self::KvToKv { input, .. } => vec![input.fingerprint()],
            Self::JnToRow { input, .. }
            | Self::JnToKv { input, .. }
            | Self::NJnToRow { input, .. }
            | Self::NJnToKv { input, .. } => vec![input.0.fingerprint(), input.1.fingerprint()],
        }
    }

    /// Returns the output collection for any transformation.
    pub(crate) fn output(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { output, .. }
            | Self::RowToKv { output, .. }
            | Self::KvToRow { output, .. }
            | Self::KvToKv { output, .. }
            | Self::JnToRow { output, .. }
            | Self::JnToKv { output, .. }
            | Self::NJnToRow { output, .. }
            | Self::NJnToKv { output, .. } => output,
        }
    }

    /// Returns the transformation flow for any transformation.
    pub(crate) fn flow(&self) -> &TransformationFlow {
        match self {
            Self::RowToRow { flow, .. }
            | Self::RowToKv { flow, .. }
            | Self::KvToKv { flow, .. }
            | Self::KvToRow { flow, .. }
            | Self::JnToRow { flow, .. }
            | Self::JnToKv { flow, .. }
            | Self::NJnToRow { flow, .. }
            | Self::NJnToKv { flow, .. } => flow,
        }
    }

    /// Return the transformation operation name.
    pub(crate) fn operation_name(&self) -> &'static str {
        match self {
            Self::RowToRow { .. } => "[Row -> Row]",
            Self::RowToKv { .. } => "[Row -> KV]",
            Self::KvToRow { .. } => "[KV -> Row]",
            Self::KvToKv { .. } => "[KV -> KV]",
            Self::JnToRow { .. } => "[Join -> Row]",
            Self::JnToKv { .. } => "[Join -> KV]",
            Self::NJnToRow { .. } => "[AntiJoin -> Row]",
            Self::NJnToKv { .. } => "[AntiJoin -> KV]",
        }
    }

    /// Simplified operation label for profiler / visualizer output.
    pub(crate) fn profile_operation_name(&self) -> &'static str {
        match self {
            Self::RowToRow { .. } => "Map",
            Self::RowToKv { .. } => "Arrange",
            Self::KvToRow { .. } => "Flatten",
            Self::KvToKv { .. } => "Transform",
            Self::JnToRow { input, .. } => {
                if input.0.is_k_only() {
                    "SemiJoin"
                } else {
                    "Join"
                }
            }
            Self::JnToKv { input, .. } => {
                if input.0.is_k_only() {
                    "SemiJoinMap"
                } else {
                    "JoinMap"
                }
            }
            Self::NJnToRow { .. } => "AntiJoin",
            Self::NJnToKv { .. } => "AntiJoinMap",
        }
    }
}

/// Lineage fp → content fp; absent entries (name-based atom fps) pass through.
fn resolve_fp(fp_map: &HashMap<u64, u64>, fp: u64) -> u64 {
    fp_map.get(&fp).copied().unwrap_or(fp)
}

/// Build a planning [`Collection`] handle from a fingerprint, debug `name`, and
/// key/value `layout`. Shared by every [`Transformation::from_info`] constructor
/// so input/output handles are built one consistent way.
fn collection(fp: u64, name: &str, layout: &KeyValueLayout) -> Arc<Collection> {
    Arc::new(Collection::new(
        fp,
        name.to_string(),
        layout.key(),
        layout.value(),
    ))
}

// ========================
// Constructors
// ========================
impl Transformation {
    /// Materialize a [`TransformationInfo`] into a [`Transformation`] whose
    /// output fingerprint is content-canonical — `hash(variant tag, resolved
    /// input fps, flow)`, all rhs_id-free — unlike the lineage info
    /// fingerprints, which embed rule-local atom positions and defeat
    /// cross-rule sharing.
    ///
    /// `fp_map` (per rule, threaded in pipeline order) maps info fp →
    /// content fp so inputs resolve to their producers; many info fps
    /// mapping to one content fp is the intended sharing.
    pub(crate) fn from_info(info: &TransformationInfo, fp_map: &mut HashMap<u64, u64>) -> Self {
        let (left, right) = info.input_info_fp();
        let left_fp = resolve_fp(fp_map, left);
        let right_fp = right.map(|fp| resolve_fp(fp_map, fp));
        // Each constructor derives its own content-canonical output fingerprint
        // from a variant `tag` + resolved input fps + flow, so equal output
        // fingerprints imply the same variant.
        let tx = match info {
            TransformationInfo::KVToKV { .. } => Self::kv_to_kv(info, left_fp),
            TransformationInfo::JoinToKV { .. } => Self::join(info, left_fp, right_fp.unwrap()),
            TransformationInfo::AntiJoinToKV { .. } => {
                Self::antijoin(info, left_fp, right_fp.unwrap())
            }
        };
        fp_map.insert(info.output_info_fp(), tx.output().fingerprint());
        tx
    }

    /// Creates a unary transformation from input/output key-value layouts.
    ///
    /// This method analyzes the input and output layouts to determine the specific
    /// transformation type needed (RowToRow, RowToKv, KvToRow, or KvToKv).
    ///
    /// # Arguments
    ///
    /// * `info` - TransformationInfo containing input/output layouts and constraints
    ///
    /// # Returns
    ///
    /// A Transformation variant appropriate for the input/output layout combination:
    /// - `RowToRow`: Row input → Row output (filtering/projection on flat rows)
    /// - `RowToKv`: Row input → Key-value output (structuring rows into KV pairs)
    /// - `KvToRow`: Key-value input → Row output (flattening KV pairs into rows)
    /// - `KvToKv`: Key-value input → Key-value output (re-keying / re-structuring)
    fn kv_to_kv(info: &TransformationInfo, input_fp: u64) -> Self {
        trace!("Creating kv_to_kv transformation with info:\n{}", info);
        let kind = (info.is_row_input(), info.is_row_output());
        let tag = match kind {
            (true, true) => "row_to_row",
            (true, false) => "row_to_kv",
            (false, true) => "kv_to_row",
            (false, false) => "kv_to_kv",
        };
        // The transformation flow defines how data moves through the operation.
        let flow = TransformationFlow::kv_to_kv(
            info.input_kv_layout().0,
            info.output_kv_layout(),
            info.kv_predicates(),
        );

        let output_fp = compute_fp((tag, input_fp, &flow));

        let input = collection(input_fp, info.input_name().0, info.input_kv_layout().0);
        let output = collection(output_fp, info.output_name(), info.output_kv_layout());

        match kind {
            // Row in, Row out: filtering, projection, or aggregation on flat rows.
            (true, true) => Self::RowToRow {
                input,
                output,
                flow,
            },
            // Row in, KV out: structure flat rows into key-value pairs.
            (true, false) => Self::RowToKv {
                input,
                output,
                flow,
            },
            // KV in, Row out: flatten key-value pairs back into rows.
            (false, true) => Self::KvToRow {
                input,
                output,
                flow,
            },
            // KV in, KV out: re-key or re-structure an existing KV layout.
            (false, false) => Self::KvToKv {
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
    /// A binary join Transformation variant chosen by the output layout:
    /// - `JnToRow`: Key-value ⋈ Key-value producing a flat row output
    /// - `JnToKv`:  Key-value ⋈ Key-value producing a key-value output
    fn join(info: &TransformationInfo, left_fp: u64, right_fp: u64) -> Self {
        let tag = if info.is_row_output() {
            "jn_to_row"
        } else {
            "jn_to_kv"
        };
        // The transformation flow defines how the join operation processes data.
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            info.join_predicates(),
        );

        let (input, output) = Self::binary_collections(info, tag, left_fp, right_fp, &flow);

        if info.is_row_output() {
            Self::JnToRow {
                input,
                output,
                flow,
            }
        } else {
            Self::JnToKv {
                input,
                output,
                flow,
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
    /// A binary antijoin Transformation variant chosen by the output layout:
    /// - `NJnToRow`: Key-only ¬⋈ Key-only producing a flat row output
    /// - `NJnToKv`:  Key-only ¬⋈ Key-only producing a key-value output
    ///
    /// # Panics
    ///
    /// Panics if the left collection is not key-only, as antijoins require the left
    /// collection to contain only keys for filtering purposes.
    fn antijoin(info: &TransformationInfo, left_fp: u64, right_fp: u64) -> Self {
        // Antijoins require the left collection to be key-only (used for filtering)
        assert!(
            info.input_kv_layout().0.value().is_empty(),
            "Planner error: antijoin - left collection must be key-only"
        );

        let tag = if info.is_row_output() {
            "njn_to_row"
        } else {
            "njn_to_kv"
        };
        // Antijoins carry no join predicates.
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            &JoinPredicates::default(), // No predicates for antijoins
        );

        let (input, output) = Self::binary_collections(info, tag, left_fp, right_fp, &flow);

        if info.is_row_output() {
            Self::NJnToRow {
                input,
                output,
                flow,
            }
        } else {
            Self::NJnToKv {
                input,
                output,
                flow,
            }
        }
    }

    /// Build the `(left, right)` input pair and the output collection shared by
    /// the binary [`Self::join`] / [`Self::antijoin`] constructors. The output
    /// fingerprint is content-canonical — `hash(tag, left_fp, right_fp, flow)` —
    /// so two binary ops with the same recipe share regardless of which rule
    /// produced them.
    fn binary_collections(
        info: &TransformationInfo,
        tag: &'static str,
        left_fp: u64,
        right_fp: u64,
        flow: &TransformationFlow,
    ) -> ((Arc<Collection>, Arc<Collection>), Arc<Collection>) {
        let output_fp = compute_fp((tag, left_fp, right_fp, flow));

        let (left_layout, right_layout) = info.input_kv_layout();
        let right_layout = right_layout.unwrap();
        let input = (
            collection(left_fp, info.input_name().0, left_layout),
            collection(right_fp, info.input_name().1.unwrap(), right_layout),
        );
        let output = collection(output_fp, info.output_name(), info.output_kv_layout());

        (input, output)
    }
}

impl fmt::Display for Transformation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", self.operation_name())?;
        if self.is_unary() {
            writeln!(f, "    In   : {}", self.unary_input())?;
        } else {
            let (left, right) = self.binary_input();
            writeln!(f, "    Left : {}", left)?;
            writeln!(f, "    Right: {}", right)?;
        }
        writeln!(f, "    Flow : {}", self.flow())?;
        writeln!(f, "    Out  : {}", self.output())
    }
}
