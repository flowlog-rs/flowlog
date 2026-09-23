//! Materialized plan steps: each [`Transformation`] reads one or two
//! [`Collection`]s and produces one, following a [`TransformationFlow`].
//!
//! - `info`: the per-rule description a step is materialized from.
//! - `flow`: how output columns and filters read the input columns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use flowlog_common::compute_fp;

use crate::planner::CanonicalForm;
use crate::planner::Collection;
use crate::planner::PlanError;

mod flow;
mod info;

pub use flow::TransformationFlow;
pub(crate) use info::KeyValueLayout;
pub(crate) use info::TransformationInfo;

/// One step of a query plan. The variant names the input and output
/// shapes: rows, or key-value pairs whose value may be empty.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub enum Transformation {
    /// Filters and projects rows.
    RowToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Arranges rows into key-value pairs.
    RowToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Flattens key-value pairs into rows.
    KvToRow {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Re-keys or re-structures key-value pairs.
    KvToKv {
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Joins two arrangements on their keys into rows.
    JnToRow {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Joins two arrangements on their keys into key-value pairs.
    JnToKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Keeps the right arrangement's pairs whose key the key-only left
    /// arrangement lacks, as rows.
    NJnToRow {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Keeps the right arrangement's pairs whose key the key-only left
    /// arrangement lacks, as key-value pairs.
    NJnToKv {
        input: (Arc<Collection>, Arc<Collection>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
}

// =============================================================================
// Getters
// =============================================================================
impl Transformation {
    /// Returns `true` if this transformation reads one collection.
    pub fn is_unary(&self) -> bool {
        matches!(
            self,
            Self::RowToRow { .. }
                | Self::RowToKv { .. }
                | Self::KvToRow { .. }
                | Self::KvToKv { .. }
        )
    }

    /// Returns the input collection of a unary transformation.
    ///
    /// # Panics
    ///
    /// Panics on a binary transformation; check [`Self::is_unary`] first.
    pub fn unary_input(&self) -> &Arc<Collection> {
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::KvToRow { input, .. }
            | Self::KvToKv { input, .. } => input,
            Self::JnToRow { .. }
            | Self::JnToKv { .. }
            | Self::NJnToRow { .. }
            | Self::NJnToKv { .. } => {
                panic!("Planner error: unary_input called on binary transformation")
            }
        }
    }

    /// Returns the input collections of a binary transformation.
    ///
    /// # Panics
    ///
    /// Panics on a unary transformation; check [`Self::is_unary`] first.
    pub fn binary_input(&self) -> &(Arc<Collection>, Arc<Collection>) {
        match self {
            Self::JnToRow { input, .. }
            | Self::JnToKv { input, .. }
            | Self::NJnToRow { input, .. }
            | Self::NJnToKv { input, .. } => input,
            Self::RowToRow { .. }
            | Self::RowToKv { .. }
            | Self::KvToRow { .. }
            | Self::KvToKv { .. } => {
                panic!("Planner error: binary_input called on unary transformation")
            }
        }
    }

    /// Returns the input fingerprints, left before right.
    pub fn input_fingerprints(&self) -> Vec<u64> {
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

    /// Returns the output collection.
    pub fn output(&self) -> &Arc<Collection> {
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

    /// Returns the flow from the inputs to the output.
    pub fn flow(&self) -> &TransformationFlow {
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

    /// Returns the operation label used in plan dumps.
    pub fn operation_name(&self) -> &'static str {
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

    /// Returns the operation label used by the profiler and visualizer,
    /// where a join against a key-only left input is a semijoin.
    pub fn profile_operation_name(&self) -> &'static str {
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

// =============================================================================
// Construction
// =============================================================================
impl Transformation {
    /// Materializes `info` into a transformation whose output fingerprint
    /// is content-canonical: a hash of the operation, the inputs' content
    /// fingerprints, and the flow, free of rule-local atom positions, so
    /// the same step in two rules gets one fingerprint. The output
    /// collection also carries its canonical form, derived from the
    /// inputs' forms.
    ///
    /// `produced` holds the output collection of every info materialized
    /// so far in this rule, keyed by the info's lineage fingerprint; an
    /// input fingerprint absent from it names a relation read directly.
    /// This info's output is added on return.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the output's canonical form cannot be
    /// derived (see [`CanonicalForm::derive`]) or `info` has inputs its
    /// variant does not allow.
    pub(crate) fn from_info(
        info: &TransformationInfo,
        produced: &mut HashMap<u64, Arc<Collection>>,
    ) -> Result<Self, PlanError> {
        let (left_fp, right_fp) = info.input_info_fp();
        let (left_name, right_name) = info.input_name();
        let (left_layout, right_layout) = info.input_kv_layout();
        let left = Self::input(produced, (left_fp, left_name, left_layout));
        let right = right_fp
            .zip(right_name)
            .zip(right_layout)
            .map(|((fp, name), layout)| Self::input(produced, (fp, name, layout)));
        let form = CanonicalForm::derive(
            info,
            left.canonical(),
            right.as_ref().map(|right| right.canonical()),
        )?;
        let flow = info.flow();
        // The operation label names the variant built below, so equal
        // fingerprints imply the same variant.
        let fingerprints: Vec<u64> = std::iter::once(&left)
            .chain(&right)
            .map(|input| input.fingerprint())
            .collect();
        let output = Arc::new(Collection::new(
            compute_fp((info.operation_name(), &fingerprints, &flow)),
            info.output_name().to_string(),
            info.output_kv_layout().clone(),
            form,
        ));
        let tx = match (info, right) {
            (
                TransformationInfo::KVToKV {
                    is_row_input,
                    is_row_output,
                    ..
                },
                None,
            ) => {
                let input = left;
                match (is_row_input, is_row_output) {
                    (true, true) => Self::RowToRow {
                        input,
                        output,
                        flow,
                    },
                    (true, false) => Self::RowToKv {
                        input,
                        output,
                        flow,
                    },
                    (false, true) => Self::KvToRow {
                        input,
                        output,
                        flow,
                    },
                    (false, false) => Self::KvToKv {
                        input,
                        output,
                        flow,
                    },
                }
            }
            (TransformationInfo::JoinToKV { is_row_output, .. }, Some(right)) => {
                let input = (left, right);
                if *is_row_output {
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
            (TransformationInfo::AntiJoinToKV { is_row_output, .. }, Some(right)) => {
                let input = (left, right);
                if *is_row_output {
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
            (TransformationInfo::KVToKV { .. }, Some(_))
            | (
                TransformationInfo::JoinToKV { .. } | TransformationInfo::AntiJoinToKV { .. },
                None,
            ) => {
                return Err(PlanError::internal(format!(
                    "{} has {} inputs",
                    info.operation_name(),
                    fingerprints.len()
                )));
            }
        };
        produced.insert(info.output_info_fp(), Arc::clone(tx.output()));
        Ok(tx)
    }

    /// The collection an info reads under `layout`, its own view of the
    /// columns: the producer's content fingerprint and canonical form when
    /// `produced` knows lineage fingerprint `fp`, else the relation named
    /// `name` read as rows, whose fingerprint `fp` already is.
    fn input(
        produced: &HashMap<u64, Arc<Collection>>,
        (fp, name, layout): (u64, &str, &KeyValueLayout),
    ) -> Arc<Collection> {
        let (fingerprint, form) = match produced.get(&fp) {
            Some(producer) => (producer.fingerprint(), producer.canonical().clone()),
            None => (
                fp,
                CanonicalForm::relation(name, layout.key().len() + layout.value().len()),
            ),
        };
        Arc::new(Collection::new(
            fingerprint,
            name.to_string(),
            layout.clone(),
            form,
        ))
    }
}

impl fmt::Display for Transformation {
    /// Multi-line block: the operation, its inputs, flow, output, and the
    /// output's canonical form.
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
        writeln!(f, "    Out  : {}", self.output())?;
        writeln!(f, "    Form : {}", self.output().canonical())
    }
}
