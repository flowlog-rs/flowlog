//! Transformation operations for query planning in FlowLog Datalog programs.
//!
//! This module provides the core transformation abstractions that define how data flows
//! through query execution plans. Transformations represent operations like filtering,
//! projection, joins, and aggregation that convert input collections into output collections.

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

/// Build a collection handle from a fingerprint, hierarchical name, and
/// key/value layout. Centralizes the `Arc<Collection>` boilerplate that every
/// transformation constructor would otherwise repeat for each input and output.
fn collection_of(fp: u64, name: &str, layout: &KeyValueLayout) -> Arc<Collection> {
    Arc::new(Collection::new(
        fp,
        name.to_string(),
        layout.key(),
        layout.value(),
    ))
}

/// Builds a unary [`Transformation`] variant from its input, output, and flow.
/// Chosen *together* with the variant's fingerprint tag in a single match arm
/// (see [`Transformation::kv_to_kv`]) so the two can never drift apart.
type UnaryCtor = fn(Arc<Collection>, Arc<Collection>, TransformationFlow) -> Transformation;

/// Builds a binary (join / antijoin) [`Transformation`] variant. As with
/// [`UnaryCtor`], picked alongside its fingerprint tag in one match arm.
type BinaryCtor =
    fn((Arc<Collection>, Arc<Collection>), Arc<Collection>, TransformationFlow) -> Transformation;

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
    ///
    /// The fingerprint `tag` is *not* chosen here: each constructor picks it in
    /// the same match arm that selects the enum variant, keeping "equal
    /// fingerprint ⇒ same variant" structurally true rather than convention.
    pub(crate) fn from_info(info: &TransformationInfo, fp_map: &mut HashMap<u64, u64>) -> Self {
        let (left, right) = info.input_info_fp();
        let left_fp = resolve_fp(fp_map, left);
        let right_fp = right.map(|fp| resolve_fp(fp_map, fp));

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
        // Create the transformation flow that defines how data moves through the operation
        let flow = TransformationFlow::kv_to_kv(
            info.input_kv_layout().0,
            info.output_kv_layout(),
            info.kv_predicates(),
        );

        // Pick the fingerprint tag and the enum variant in the *same* arm so
        // they stay in lockstep (equal fingerprint ⇒ same variant).
        let (tag, ctor): (&'static str, UnaryCtor) =
            match (info.is_row_input(), info.is_row_output()) {
                // Row in, Row out: filtering, projection, or aggregation on flat rows.
                (true, true) => ("row_to_row", |input, output, flow| Self::RowToRow {
                    input,
                    output,
                    flow,
                }),
                // Row in, KV out: structure flat rows into key-value pairs.
                (true, false) => ("row_to_kv", |input, output, flow| Self::RowToKv {
                    input,
                    output,
                    flow,
                }),
                // KV in, Row out: flatten key-value pairs back into rows.
                (false, true) => ("kv_to_row", |input, output, flow| Self::KvToRow {
                    input,
                    output,
                    flow,
                }),
                // KV in, KV out: re-key or re-structure an existing KV layout.
                (false, false) => ("kv_to_kv", |input, output, flow| Self::KvToKv {
                    input,
                    output,
                    flow,
                }),
            };

        let output_fp = compute_fp((tag, input_fp, &flow));

        let input = collection_of(input_fp, info.input_name().0, info.input_kv_layout().0);
        let output = collection_of(output_fp, info.output_name(), info.output_kv_layout());

        ctor(input, output, flow)
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
        // Create transformation flow that defines how the join operation processes data
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            info.join_predicates(),
        );

        // Tag and variant chosen together (see `kv_to_kv`).
        let (tag, ctor): (&'static str, BinaryCtor) = if info.is_row_output() {
            ("jn_to_row", |input, output, flow| Self::JnToRow {
                input,
                output,
                flow,
            })
        } else {
            ("jn_to_kv", |input, output, flow| Self::JnToKv {
                input,
                output,
                flow,
            })
        };

        let output_fp = compute_fp((tag, left_fp, right_fp, &flow));

        let input = (
            collection_of(left_fp, info.input_name().0, info.input_kv_layout().0),
            collection_of(
                right_fp,
                info.input_name().1.unwrap(),
                info.input_kv_layout().1.unwrap(),
            ),
        );
        let output = collection_of(output_fp, info.output_name(), info.output_kv_layout());

        ctor(input, output, flow)
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

        // Create transformation flow (no comparison expressions for antijoins)
        let flow = TransformationFlow::join_to_kv(
            info.input_kv_layout().0,
            info.input_kv_layout().1.unwrap(),
            info.output_kv_layout(),
            &JoinPredicates::default(), // No predicates for antijoins
        );

        // Tag and variant chosen together (see `kv_to_kv`).
        let (tag, ctor): (&'static str, BinaryCtor) = if info.is_row_output() {
            ("njn_to_row", |input, output, flow| Self::NJnToRow {
                input,
                output,
                flow,
            })
        } else {
            ("njn_to_kv", |input, output, flow| Self::NJnToKv {
                input,
                output,
                flow,
            })
        };

        let output_fp = compute_fp((tag, left_fp, right_fp, &flow));

        let input = (
            collection_of(left_fp, info.input_name().0, info.input_kv_layout().0),
            collection_of(
                right_fp,
                info.input_name().1.unwrap(),
                info.input_kv_layout().1.unwrap(),
            ),
        );
        let output = collection_of(output_fp, info.output_name(), info.output_kv_layout());

        ctor(input, output, flow)
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
