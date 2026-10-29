//! Materialized plan steps: each [`Transformation`] reads one or two
//! [`Collection`]s and produces one, following a [`TransformationFlow`].
//!
//! - `info`: the per-rule description a step is materialized from.
//! - `flow`: how output columns and filters read the input columns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use flowlog_parser::Mutability;

use crate::planner::CanonicalForm;
use crate::planner::Collection;
use crate::planner::PlanError;
use crate::stratifier::aggregate_mutability;
use crate::stratifier::derived_mutability;

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
    /// Answers an aggregate per group over the matches arrangement's rows,
    /// the braces' matches keyed by group: for every group the key-only
    /// groups arrangement holds, or once over all the rows without one;
    /// as rows.
    AggToRow {
        input: (Arc<Collection>, Option<Arc<Collection>>),
        output: Arc<Collection>,
        flow: TransformationFlow,
    },
    /// Answers an aggregate per group over the matches arrangement's rows,
    /// the braces' matches keyed by group: for every group the key-only
    /// groups arrangement holds, or once over all the rows without one;
    /// as key-value pairs.
    AggToKv {
        input: (Arc<Collection>, Option<Arc<Collection>>),
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
                | Self::AggToRow {
                    input: (_, None),
                    ..
                }
                | Self::AggToKv {
                    input: (_, None),
                    ..
                }
        )
    }

    /// Returns `true` for a map: a filter, projection, or re-keying of one
    /// collection. A join or an aggregate is not one, even an aggregate
    /// without groups, which is unary: both read their inputs arranged.
    pub fn is_map(&self) -> bool {
        match self {
            Self::RowToRow { .. }
            | Self::RowToKv { .. }
            | Self::KvToRow { .. }
            | Self::KvToKv { .. } => true,
            Self::JnToRow { .. }
            | Self::JnToKv { .. }
            | Self::NJnToRow { .. }
            | Self::NJnToKv { .. }
            | Self::AggToRow { .. }
            | Self::AggToKv { .. } => false,
        }
    }

    /// Returns `true` if the output needs arranging by key, the shape a
    /// join or an aggregate reads. A row output does not, even when its
    /// layout has no key columns either.
    pub fn need_arrange(&self) -> bool {
        matches!(
            self,
            Self::RowToKv { .. }
                | Self::KvToKv { .. }
                | Self::JnToKv { .. }
                | Self::NJnToKv { .. }
                | Self::AggToKv { .. }
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
            | Self::KvToKv { input, .. }
            | Self::AggToRow {
                input: (input, None),
                ..
            }
            | Self::AggToKv {
                input: (input, None),
                ..
            } => input,
            Self::JnToRow { .. }
            | Self::JnToKv { .. }
            | Self::NJnToRow { .. }
            | Self::NJnToKv { .. }
            | Self::AggToRow {
                input: (_, Some(_)),
                ..
            }
            | Self::AggToKv {
                input: (_, Some(_)),
                ..
            } => {
                panic!("Planner error: unary_input called on binary transformation")
            }
        }
    }

    /// Returns the input collections of a binary transformation.
    ///
    /// # Panics
    ///
    /// Panics on a unary transformation; check [`Self::is_unary`] first.
    pub fn binary_input(&self) -> (&Arc<Collection>, &Arc<Collection>) {
        match self {
            Self::JnToRow { input, .. }
            | Self::JnToKv { input, .. }
            | Self::NJnToRow { input, .. }
            | Self::NJnToKv { input, .. } => (&input.0, &input.1),
            Self::AggToRow {
                input: (matches, Some(groups)),
                ..
            }
            | Self::AggToKv {
                input: (matches, Some(groups)),
                ..
            } => (matches, groups),
            Self::RowToRow { .. }
            | Self::RowToKv { .. }
            | Self::KvToRow { .. }
            | Self::KvToKv { .. }
            | Self::AggToRow {
                input: (_, None), ..
            }
            | Self::AggToKv {
                input: (_, None), ..
            } => {
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
            Self::AggToRow {
                input: (matches, groups),
                ..
            }
            | Self::AggToKv {
                input: (matches, groups),
                ..
            } => std::iter::once(matches)
                .chain(groups)
                .map(|input| input.fingerprint())
                .collect(),
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
            | Self::NJnToKv { output, .. }
            | Self::AggToRow { output, .. }
            | Self::AggToKv { output, .. } => output,
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
            | Self::NJnToKv { flow, .. }
            | Self::AggToRow { flow, .. }
            | Self::AggToKv { flow, .. } => flow,
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
            Self::AggToRow { .. } => "[Aggregate -> Row]",
            Self::AggToKv { .. } => "[Aggregate -> KV]",
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
            Self::AggToRow { .. } | Self::AggToKv { .. } => "Aggregate",
        }
    }
}

// =============================================================================
// Construction
// =============================================================================
impl Transformation {
    /// Materializes `info` into a transformation. The output collection
    /// keeps the info's fingerprint and gains its canonical form, derived
    /// from the inputs' forms, and its mutability, derived from the inputs'
    /// mutabilities.
    ///
    /// `produced` holds every collection materialized so far in this rule,
    /// keyed by fingerprint; an input fingerprint absent from it names a
    /// relation read directly, whose mutability `mutability_of` gives by
    /// fingerprint. This info's output is added on return.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the output's canonical form cannot be
    /// derived (see [`CanonicalForm::derive`]), a relation read directly has
    /// no mutability, or `info` has inputs its variant does not allow.
    pub(crate) fn from_info(
        info: &TransformationInfo,
        produced: &mut HashMap<u64, Arc<Collection>>,
        mutability_of: &impl Fn(u64) -> Option<Mutability>,
    ) -> Result<Self, PlanError> {
        let (left_fp, right_fp) = info.input_info_fp();
        let (left_name, right_name) = info.input_name();
        let (left_layout, right_layout) = info.input_kv_layout();
        let left = Self::input(produced, mutability_of, (left_fp, left_name, left_layout))?;
        let right = right_fp
            .zip(right_name)
            .zip(right_layout)
            .map(|((fp, name), layout)| Self::input(produced, mutability_of, (fp, name, layout)))
            .transpose()?;
        let form = CanonicalForm::derive(
            info,
            left.canonical(),
            right.as_ref().map(|right| right.canonical()),
        )?;
        let flow = info.flow();
        let input_count = 1 + usize::from(right.is_some());
        let output = |mutability| {
            Arc::new(Collection::new(
                info.output_info_fp(),
                info.output_name().to_string(),
                info.output_kv_layout().clone(),
                form,
                mutability,
            ))
        };
        let tx = match (info, right) {
            (
                TransformationInfo::KVToKV {
                    is_row_input,
                    is_row_output,
                    ..
                },
                None,
            ) => {
                let output = output(left.mutability());
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
                let output = output(derived_mutability(
                    [left.mutability(), right.mutability()],
                    [],
                ));
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
                // The left input is the filter (see `CanonicalForm::derive`).
                let output = output(derived_mutability(
                    [right.mutability()],
                    [left.mutability()],
                ));
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
            (TransformationInfo::AggregateToKV { is_row_output, .. }, groups) => {
                // The answers change whenever a group's matches do, as an
                // aggregate over them; a group that comes and goes takes
                // its answer with it, as a join would. Without groups the
                // matches alone decide.
                let output = output(derived_mutability(
                    std::iter::once(aggregate_mutability(left.mutability()))
                        .chain(groups.as_ref().map(|groups| groups.mutability())),
                    [],
                ));
                let input = (left, groups);
                if *is_row_output {
                    Self::AggToRow {
                        input,
                        output,
                        flow,
                    }
                } else {
                    Self::AggToKv {
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
                    input_count
                )));
            }
        };
        produced.insert(info.output_info_fp(), Arc::clone(tx.output()));
        Ok(tx)
    }

    /// Makes every input whose fingerprint is `from` read `to` instead.
    /// The flow keeps its positions, so `to` must lay out the same
    /// columns as the collection it replaces.
    pub(crate) fn swap_input(&mut self, from: u64, to: &Arc<Collection>) {
        let swap = |input: &mut Arc<Collection>| {
            if input.fingerprint() == from {
                *input = Arc::clone(to);
            }
        };
        match self {
            Self::RowToRow { input, .. }
            | Self::RowToKv { input, .. }
            | Self::KvToRow { input, .. }
            | Self::KvToKv { input, .. } => swap(input),
            Self::JnToRow { input, .. }
            | Self::JnToKv { input, .. }
            | Self::NJnToRow { input, .. }
            | Self::NJnToKv { input, .. } => {
                swap(&mut input.0);
                swap(&mut input.1);
            }
            Self::AggToRow {
                input: (matches, groups),
                ..
            }
            | Self::AggToKv {
                input: (matches, groups),
                ..
            } => {
                swap(matches);
                if let Some(groups) = groups {
                    swap(groups);
                }
            }
        }
    }

    /// This transformation with its keyed output held as rows, the key
    /// columns first, under the same fingerprint. A row output is
    /// returned as it is.
    pub(crate) fn unkeyed(&self) -> Self {
        let rows = |output: &Arc<Collection>| Arc::new(output.unkeyed());
        match self {
            Self::RowToKv {
                input,
                output,
                flow,
            } => Self::RowToRow {
                input: Arc::clone(input),
                output: rows(output),
                flow: flow.unkeyed(),
            },
            Self::KvToKv {
                input,
                output,
                flow,
            } => Self::KvToRow {
                input: Arc::clone(input),
                output: rows(output),
                flow: flow.unkeyed(),
            },
            Self::JnToKv {
                input,
                output,
                flow,
            } => Self::JnToRow {
                input: input.clone(),
                output: rows(output),
                flow: flow.unkeyed(),
            },
            Self::NJnToKv {
                input,
                output,
                flow,
            } => Self::NJnToRow {
                input: input.clone(),
                output: rows(output),
                flow: flow.unkeyed(),
            },
            Self::AggToKv {
                input,
                output,
                flow,
            } => Self::AggToRow {
                input: input.clone(),
                output: rows(output),
                flow: flow.unkeyed(),
            },
            Self::RowToRow { .. }
            | Self::KvToRow { .. }
            | Self::JnToRow { .. }
            | Self::NJnToRow { .. }
            | Self::AggToRow { .. } => self.clone(),
        }
    }

    /// The collection an info reads under `layout`, its own view of the
    /// columns: the producer's form and mutability when `produced` knows
    /// `fp`, else the form of the relation named `name` read as rows, with
    /// its mutability from `mutability_of` under `fp`.
    ///
    /// # Errors
    ///
    /// Returns an internal error when `mutability_of` has no value for a
    /// relation read directly. Every relation a rule reads is an EDB or an
    /// IDB of an earlier or the current stratum, so each has one.
    fn input(
        produced: &HashMap<u64, Arc<Collection>>,
        mutability_of: &impl Fn(u64) -> Option<Mutability>,
        (fp, name, layout): (u64, &str, &KeyValueLayout),
    ) -> Result<Arc<Collection>, PlanError> {
        let (form, mutability) = match produced.get(&fp) {
            Some(producer) => (producer.canonical().clone(), producer.mutability()),
            None => {
                let form = CanonicalForm::relation(name, layout.key().len() + layout.value().len());
                let mutability = mutability_of(fp).ok_or_else(|| {
                    PlanError::internal(format!(
                        "relation `{name}` has no mutability, yet a rule reads it"
                    ))
                })?;
                (form, mutability)
            }
        };
        Ok(Arc::new(Collection::new(
            fp,
            name.to_string(),
            layout.clone(),
            form,
            mutability,
        )))
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

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::AggregationOperator;
    use flowlog_parser::DataType;
    use rstest::rstest;

    use super::*;
    use crate::catalog::ArithmeticPos;
    use crate::catalog::AtomArgumentSignature;
    use crate::catalog::AtomSignature;
    use crate::catalog::JoinPredicates;
    use crate::catalog::KvPredicates;
    use crate::planner::ArithmeticArgument;
    use crate::planner::FactorArgument;
    use crate::planner::TransformationArgument;

    fn column(atom: usize, argument: usize) -> ArithmeticPos {
        ArithmeticPos::from_var_signature(AtomArgumentSignature::new(
            AtomSignature::new(true, atom),
            argument,
        ))
    }

    fn layout(key: &[ArithmeticPos], value: &[ArithmeticPos]) -> KeyValueLayout {
        KeyValueLayout::new(key.to_vec(), value.to_vec())
    }

    /// Each test relation's mutability: `s` and `t` are static, `a`
    /// append, `m` and `n` mutable, and any other relation has none.
    fn mutability_of(fp: u64) -> Option<Mutability> {
        [
            ("s", Mutability::Static),
            ("t", Mutability::Static),
            ("a", Mutability::Append),
            ("m", Mutability::Mutable),
            ("n", Mutability::Mutable),
        ]
        .into_iter()
        .find(|(name, _)| compute_fp(name) == fp)
        .map(|(_, mutability)| mutability)
    }

    /// Reads two-column relation `name` directly and arranges it by its
    /// first column, with the second as the value unless `key_only`. The
    /// arrangement is added to `produced`, so a later step can read it.
    fn arranged(
        name: &str,
        key_only: bool,
        produced: &mut HashMap<u64, Arc<Collection>>,
    ) -> Arc<Collection> {
        let value = if key_only { vec![] } else { vec![column(0, 1)] };
        let info = TransformationInfo::kv_to_kv(
            compute_fp(name),
            name.into(),
            format!("arranged {name}"),
            true,
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &value),
            KvPredicates::default(),
        );
        let tx = Transformation::from_info(&info, produced, &mutability_of).expect("arranges");
        Arc::clone(tx.output())
    }

    /// The two-column relation `name` read as rows, with no key.
    fn copied(name: &str, produced: &mut HashMap<u64, Arc<Collection>>) -> Arc<Collection> {
        let all = [column(0, 0), column(0, 1)];
        let info = TransformationInfo::kv_to_kv(
            compute_fp(name),
            name.into(),
            format!("rows of {name}"),
            true,
            layout(&[], &all),
            layout(&[], &all),
            KvPredicates::default(),
        );
        let tx = Transformation::from_info(&info, produced, &mutability_of).expect("copies");
        Arc::clone(tx.output())
    }

    /// The output mutability of `left` joined with `right` on their keys.
    fn joined(left: &str, right: &str) -> Mutability {
        let mut produced = HashMap::new();
        let left = arranged(left, false, &mut produced);
        let right = arranged(right, false, &mut produced);
        let info = TransformationInfo::join_to_kv(
            left.fingerprint(),
            "left".into(),
            right.fingerprint(),
            "right".into(),
            "out".into(),
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(0, 0)], &[column(0, 1), column(1, 1)]),
            JoinPredicates::default(),
        );
        Transformation::from_info(&info, &mut produced, &mutability_of)
            .expect("joins")
            .output()
            .mutability()
    }

    /// The output mutability of `source` rows whose key has no match in
    /// `filter`.
    fn antijoined(source: &str, filter: &str) -> Mutability {
        let mut produced = HashMap::new();
        let filter = arranged(filter, true, &mut produced);
        let source = arranged(source, false, &mut produced);
        let info = TransformationInfo::anti_join_to_kv(
            filter.fingerprint(),
            "filter".into(),
            source.fingerprint(),
            "source".into(),
            "out".into(),
            layout(&[column(0, 0)], &[]),
            layout(&[column(1, 0)], &[column(1, 1)]),
            layout(&[column(1, 0)], &[column(1, 1)]),
        );
        Transformation::from_info(&info, &mut produced, &mutability_of)
            .expect("antijoins")
            .output()
            .mutability()
    }

    /// The output mutability of a count over `matches`'s rows, keyed by
    /// their first column, for the groups of `groups`, or over all of
    /// them, read as rows, for none.
    fn aggregated(matches: &str, groups: Option<&str>) -> Mutability {
        let mut produced = HashMap::new();
        let (matches, key, value) = if groups.is_some() {
            (
                arranged(matches, false, &mut produced),
                vec![column(0, 0)],
                vec![column(0, 1)],
            )
        } else {
            (
                copied(matches, &mut produced),
                vec![],
                vec![column(0, 0), column(0, 1)],
            )
        };
        let groups = groups.map(|groups| {
            let groups = arranged(groups, true, &mut produced);
            (
                groups.fingerprint(),
                "groups".into(),
                layout(&[column(1, 0)], &[]),
            )
        });
        let info = TransformationInfo::aggregate_to_kv(
            matches.fingerprint(),
            "matches".into(),
            groups,
            "out".into(),
            layout(&key, &value),
            layout(&key, &[column(0, 2)]),
            AggregationOperator::Count,
            None,
            DataType::Int32,
            AtomArgumentSignature::new(AtomSignature::new(true, 0), 2),
            JoinPredicates::default(),
        );
        Transformation::from_info(&info, &mut produced, &mutability_of)
            .expect("aggregates")
            .output()
            .mutability()
    }

    /// An aggregate's answers are mutable once its matches can change, as
    /// an aggregate over them is, and once its groups can.
    // Cases: matches relation, groups relation, output mutability.
    #[rstest]
    #[case::static_over_static("s", Some("t"), Mutability::Static)]
    #[case::append_matches("a", Some("t"), Mutability::Mutable)]
    #[case::mutable_groups("s", Some("m"), Mutability::Mutable)]
    #[case::static_without_groups("s", None, Mutability::Static)]
    #[case::append_without_groups("a", None, Mutability::Mutable)]
    fn an_aggregate_is_as_mutable_as_its_matches_and_groups(
        #[case] matches: &str,
        #[case] groups: Option<&str>,
        #[case] expected: Mutability,
    ) {
        assert_eq!(aggregated(matches, groups), expected);
    }

    /// A unary step keeps the mutability of what it reads, whether a
    /// relation read directly or a collection an earlier step produced.
    #[rstest]
    #[case::static_relation("s", Mutability::Static)]
    #[case::mutable_relation("m", Mutability::Mutable)]
    fn unary_step_keeps_its_inputs_mutability(#[case] name: &str, #[case] expected: Mutability) {
        let mut produced = HashMap::new();
        let arrangement = arranged(name, false, &mut produced);
        let info = TransformationInfo::kv_to_kv(
            arrangement.fingerprint(),
            "arranged".into(),
            "keys".into(),
            false,
            layout(&[column(0, 0)], &[column(0, 1)]),
            layout(&[column(0, 0)], &[]),
            KvPredicates::default(),
        );
        let tx = Transformation::from_info(&info, &mut produced, &mutability_of).expect("maps");
        assert_eq!(arrangement.mutability(), expected);
        assert_eq!(tx.output().mutability(), expected);
    }

    /// A join is as mutable as its more mutable side.
    #[rstest]
    #[case::static_static("s", "t", Mutability::Static)]
    #[case::static_mutable("s", "m", Mutability::Mutable)]
    #[case::mutable_static("m", "s", Mutability::Mutable)]
    #[case::mutable_mutable("m", "n", Mutability::Mutable)]
    fn join_takes_the_more_mutable_side(
        #[case] left: &str,
        #[case] right: &str,
        #[case] expected: Mutability,
    ) {
        assert_eq!(joined(left, right), expected);
    }

    /// An antijoin follows the matrix: only a static source over a static
    /// filter stays static.
    #[rstest]
    #[case::static_over_static("s", "t", Mutability::Static)]
    #[case::static_over_mutable("s", "m", Mutability::Mutable)]
    #[case::mutable_over_static("m", "t", Mutability::Mutable)]
    #[case::mutable_over_mutable("m", "n", Mutability::Mutable)]
    fn antijoin_follows_the_matrix(
        #[case] source: &str,
        #[case] filter: &str,
        #[case] expected: Mutability,
    ) {
        assert_eq!(antijoined(source, filter), expected);
    }

    /// A relation read directly without a mutability is an internal error
    /// naming it. Stratum maps cover every relation a rule reads, so a
    /// planned program cannot reach this, and `from_info` is driven directly.
    #[test]
    fn relation_without_a_mutability_is_an_internal_error() {
        let info = TransformationInfo::kv_to_kv(
            compute_fp("x"),
            "x".into(),
            "out".into(),
            true,
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[column(0, 1)]),
            KvPredicates::default(),
        );
        let err = Transformation::from_info(&info, &mut HashMap::new(), &mutability_of)
            .expect_err("no mutability");
        assert!(
            matches!(&err, PlanError::Internal(_)) && err.to_string().contains("`x`"),
            "got {err}"
        );
    }

    /// A plain read of slot `index` on the key (`true`) or value side.
    fn slot(is_key: bool, index: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((is_key, index))),
            rest: vec![],
        }
    }

    /// Held as rows, a keyed output lists its columns keys first under the
    /// same fingerprint, and its flow emits them in that order.
    #[test]
    fn a_keyed_output_held_as_rows_lists_its_keys_first() {
        let info = TransformationInfo::kv_to_kv(
            compute_fp("s"),
            "s".into(),
            "arranged s".into(),
            true,
            layout(&[], &[column(0, 0), column(0, 1)]),
            layout(&[column(0, 0)], &[column(0, 1)]),
            KvPredicates::default(),
        );
        let arranged = Transformation::from_info(&info, &mut HashMap::new(), &mutability_of)
            .expect("arranges");

        let rows = arranged.unkeyed();
        assert!(matches!(rows, Transformation::RowToRow { .. }));
        assert_eq!(rows.output().fingerprint(), arranged.output().fingerprint());
        assert_eq!(rows.output().arity(), (0, 2));
        assert!(rows.flow().key().is_empty());
        assert_eq!(**rows.flow().value(), vec![slot(false, 0), slot(false, 1)]);
    }
}
