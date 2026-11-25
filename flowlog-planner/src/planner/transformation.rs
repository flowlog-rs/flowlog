//! Materialized plan steps: each [`Transformation`] reads one or two
//! [`Collection`]s and produces one, following a [`TransformationFlow`].
//!
//! - `info`: the per-rule description a step is materialized from.
//! - `flow`: how output columns and filters read the input columns.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use flowlog_common::compute_fp;
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

// =============================================================================
// Operation
// =============================================================================

/// What a step does and the shapes it reads and writes: rows, or
/// key-value pairs whose value may be empty.
#[derive(Clone, Copy, Hash, Eq, PartialEq, Debug)]
pub enum Operation {
    /// Filters and projects rows.
    RowToRow,
    /// Arranges rows into key-value pairs.
    RowToKv,
    /// Flattens key-value pairs into rows.
    KvToRow,
    /// Re-keys or re-structures key-value pairs.
    KvToKv,
    /// Joins two arrangements on their keys into rows.
    JoinToRow,
    /// Joins two arrangements on their keys into key-value pairs.
    JoinToKv,
    /// Keeps the right arrangement's pairs whose key the key-only left
    /// arrangement lacks, as rows.
    AntiJoinToRow,
    /// Keeps the right arrangement's pairs whose key the key-only left
    /// arrangement lacks, as key-value pairs.
    AntiJoinToKv,
    /// Answers an aggregate per group over the matches arrangement's rows,
    /// the braces' matches keyed by group: for every group the key-only
    /// groups arrangement holds, or once over all the rows without one;
    /// as rows.
    AggregateToRow,
    /// As [`Self::AggregateToRow`], with the answers as key-value pairs.
    AggregateToKv,
}

impl Operation {
    /// Returns `true` for a map: a filter, projection, or re-keying of one
    /// collection. A join or an aggregate is not one, even an aggregate
    /// without groups: both read their inputs arranged.
    #[must_use]
    pub fn is_map(self) -> bool {
        match self {
            Self::RowToRow | Self::RowToKv | Self::KvToRow | Self::KvToKv => true,
            Self::JoinToRow
            | Self::JoinToKv
            | Self::AntiJoinToRow
            | Self::AntiJoinToKv
            | Self::AggregateToRow
            | Self::AggregateToKv => false,
        }
    }

    /// Returns `true` if the output needs arranging by key, the shape a
    /// join or an aggregate reads. A row output does not, even when its
    /// layout has no key columns either.
    #[must_use]
    pub fn need_arrange(self) -> bool {
        match self {
            Self::RowToKv
            | Self::KvToKv
            | Self::JoinToKv
            | Self::AntiJoinToKv
            | Self::AggregateToKv => true,
            Self::RowToRow
            | Self::KvToRow
            | Self::JoinToRow
            | Self::AntiJoinToRow
            | Self::AggregateToRow => false,
        }
    }

    /// The operation writing the same output as rows; itself when it
    /// writes rows already.
    fn unkeyed(self) -> Self {
        match self {
            Self::RowToKv | Self::RowToRow => Self::RowToRow,
            Self::KvToKv | Self::KvToRow => Self::KvToRow,
            Self::JoinToKv | Self::JoinToRow => Self::JoinToRow,
            Self::AntiJoinToKv | Self::AntiJoinToRow => Self::AntiJoinToRow,
            Self::AggregateToKv | Self::AggregateToRow => Self::AggregateToRow,
        }
    }
}

impl fmt::Display for Operation {
    /// The label plan dumps head a step with, as `[Join -> KV]`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::RowToRow => "[Row -> Row]",
            Self::RowToKv => "[Row -> KV]",
            Self::KvToRow => "[KV -> Row]",
            Self::KvToKv => "[KV -> KV]",
            Self::JoinToRow => "[Join -> Row]",
            Self::JoinToKv => "[Join -> KV]",
            Self::AntiJoinToRow => "[AntiJoin -> Row]",
            Self::AntiJoinToKv => "[AntiJoin -> KV]",
            Self::AggregateToRow => "[Aggregate -> Row]",
            Self::AggregateToKv => "[Aggregate -> KV]",
        })
    }
}

/// The fingerprint of the step doing `operation` on `inputs`, each given
/// by its fingerprint and the layout the step reads it in, left before
/// right, through `flow` into a collection laid out as `output`. It is
/// the one recipe for a step's fingerprint, an info's and a materialized
/// step's alike, so two steps with equal fingerprints compute the same
/// collection in the same shape and either can stand for the other.
#[must_use]
pub(crate) fn step_fingerprint(
    operation: Operation,
    inputs: &[(u64, &KeyValueLayout)],
    flow: &TransformationFlow,
    output: &KeyValueLayout,
) -> u64 {
    compute_fp((operation, inputs, flow, output))
}

// =============================================================================
// Transformation
// =============================================================================

/// One step of a query plan: an [`Operation`] reading one or two
/// collections through a flow into one. A map reads one collection; a
/// join or an antijoin reads two; an aggregate reads its matches, and its
/// groups when it has them.
#[derive(Clone, Hash, Eq, PartialEq, Debug)]
pub struct Transformation {
    operation: Operation,
    /// The one input of a map, the left of a join or an antijoin, or an
    /// aggregate's matches.
    left: Arc<Collection>,
    /// A join's or an antijoin's right, or an aggregate's groups.
    right: Option<Arc<Collection>>,
    output: Arc<Collection>,
    flow: TransformationFlow,
}

// --- Getters ---

impl Transformation {
    /// Returns what this step does and the shapes it reads and writes.
    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }

    /// Returns `true` if this transformation reads one collection.
    #[must_use]
    pub fn is_unary(&self) -> bool {
        self.right.is_none()
    }

    /// Returns `true` for a map (see [`Operation::is_map`]).
    #[must_use]
    pub fn is_map(&self) -> bool {
        self.operation.is_map()
    }

    /// Returns `true` if the output needs arranging by key (see
    /// [`Operation::need_arrange`]).
    #[must_use]
    pub fn need_arrange(&self) -> bool {
        self.operation.need_arrange()
    }

    /// Returns the input collections: the left, and the right of a step
    /// that reads two.
    #[must_use]
    pub fn inputs(&self) -> (&Arc<Collection>, Option<&Arc<Collection>>) {
        (&self.left, self.right.as_ref())
    }

    /// Returns the input collection of a unary transformation.
    ///
    /// # Panics
    ///
    /// Panics on a binary transformation; check [`Self::is_unary`] first.
    #[must_use]
    pub fn unary_input(&self) -> &Arc<Collection> {
        assert!(
            self.right.is_none(),
            "unary_input: {} reads two collections",
            self.operation
        );
        &self.left
    }

    /// Returns the input collections of a binary transformation.
    ///
    /// # Panics
    ///
    /// Panics on a unary transformation; check [`Self::is_unary`] first.
    #[must_use]
    pub fn binary_input(&self) -> (&Arc<Collection>, &Arc<Collection>) {
        let Some(right) = &self.right else {
            panic!("binary_input: {} reads one collection", self.operation)
        };
        (&self.left, right)
    }

    /// Returns the input fingerprints, left before right.
    #[must_use]
    pub fn input_fingerprints(&self) -> Vec<u64> {
        std::iter::once(&self.left)
            .chain(&self.right)
            .map(|input| input.fingerprint())
            .collect()
    }

    /// Returns the output collection.
    #[must_use]
    pub fn output(&self) -> &Arc<Collection> {
        &self.output
    }

    /// Returns the flow from the inputs to the output.
    #[must_use]
    pub fn flow(&self) -> &TransformationFlow {
        &self.flow
    }

    /// Returns the operation label used by the profiler and visualizer,
    /// where a join against a key-only left input is a semijoin.
    #[must_use]
    pub fn profile_operation_name(&self) -> &'static str {
        match self.operation {
            Operation::RowToRow => "Map",
            Operation::RowToKv => "Arrange",
            Operation::KvToRow => "Flatten",
            Operation::KvToKv => "Transform",
            Operation::JoinToRow => {
                if self.left.is_k_only() {
                    "SemiJoin"
                } else {
                    "Join"
                }
            }
            Operation::JoinToKv => {
                if self.left.is_k_only() {
                    "SemiJoinMap"
                } else {
                    "JoinMap"
                }
            }
            Operation::AntiJoinToRow => "AntiJoin",
            Operation::AntiJoinToKv => "AntiJoinMap",
            Operation::AggregateToRow | Operation::AggregateToKv => "Aggregate",
        }
    }
}

// --- Construction ---

impl Transformation {
    /// The map doing `operation` from `input` into `output` through
    /// `flow`.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `operation` is not a map (see
    /// [`Operation::is_map`]).
    pub(crate) fn map(
        operation: Operation,
        input: Arc<Collection>,
        output: Arc<Collection>,
        flow: TransformationFlow,
    ) -> Result<Self, PlanError> {
        if !operation.is_map() {
            return Err(PlanError::internal(format!(
                "map: {operation} is not a map, yet it is to produce {output} from {input} alone"
            )));
        }
        Ok(Self {
            operation,
            left: input,
            right: None,
            output,
            flow,
        })
    }

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
    /// no mutability, or `info` has inputs its operation does not allow.
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
        let operation = info.operation();
        let mutability = match (operation, &right) {
            (
                Operation::RowToRow | Operation::RowToKv | Operation::KvToRow | Operation::KvToKv,
                None,
            ) => left.mutability(),
            (Operation::JoinToRow | Operation::JoinToKv, Some(right)) => {
                derived_mutability([left.mutability(), right.mutability()], [])
            }
            // The left input is the filter (see `CanonicalForm::derive`).
            (Operation::AntiJoinToRow | Operation::AntiJoinToKv, Some(right)) => {
                derived_mutability([right.mutability()], [left.mutability()])
            }
            // The answers change whenever a group's matches do, as an
            // aggregate over them; a group that comes and goes takes its
            // answer with it, as a join would. Without groups the matches
            // alone decide.
            (Operation::AggregateToRow | Operation::AggregateToKv, groups) => derived_mutability(
                std::iter::once(aggregate_mutability(left.mutability()))
                    .chain(groups.as_ref().map(|groups| groups.mutability())),
                [],
            ),
            (
                Operation::RowToRow | Operation::RowToKv | Operation::KvToRow | Operation::KvToKv,
                Some(_),
            )
            | (
                Operation::JoinToRow
                | Operation::JoinToKv
                | Operation::AntiJoinToRow
                | Operation::AntiJoinToKv,
                None,
            ) => {
                return Err(PlanError::internal(format!(
                    "{operation} has {} inputs",
                    1 + usize::from(right.is_some())
                )));
            }
        };
        let output = Arc::new(Collection::new(
            info.output_info_fp(),
            info.output_name().to_string(),
            info.output_kv_layout().clone(),
            form,
            mutability,
        ));
        produced.insert(info.output_info_fp(), Arc::clone(&output));
        Ok(Self {
            operation,
            left,
            right,
            output,
            flow: info.flow(),
        })
    }

    /// Makes every input whose fingerprint is `from` read `to` instead.
    /// The flow keeps its positions, so `to` must lay out the same
    /// columns as the collection it replaces.
    pub(crate) fn swap_input(&mut self, from: u64, to: &Arc<Collection>) {
        let inputs = std::iter::once(&mut self.left).chain(&mut self.right);
        for input in inputs {
            if input.fingerprint() == from {
                *input = Arc::clone(to);
            }
        }
    }

    /// The fingerprint of this step as it stands (see
    /// [`step_fingerprint`]). The output carries it from materialization
    /// on, except while sharing rewrites the step.
    #[must_use]
    pub(crate) fn content_fingerprint(&self) -> u64 {
        let inputs: Vec<(u64, &KeyValueLayout)> = std::iter::once(&self.left)
            .chain(&self.right)
            .map(|input| (input.fingerprint(), input.kv_layout()))
            .collect();
        step_fingerprint(self.operation, &inputs, &self.flow, self.output.kv_layout())
    }

    /// Checks, in debug builds, what `phase` must leave behind in `steps`:
    /// each output carries its step's content fingerprint (see
    /// [`Self::content_fingerprint`]).
    ///
    /// # Panics
    ///
    /// Panics in a debug build on a step whose output does not, naming
    /// `phase` and the step.
    pub(crate) fn debug_assert_named_by_content(steps: &[Self], phase: &str) {
        for step in steps {
            debug_assert_eq!(
                step.output.fingerprint(),
                step.content_fingerprint(),
                "{phase} left a stale output fingerprint on\n{step}"
            );
        }
    }

    /// This transformation reading each input `renamed` maps under its
    /// new fingerprint, and producing its output under
    /// [`Self::content_fingerprint`].
    #[must_use]
    pub(crate) fn fingerprinted(&self, renamed: &HashMap<u64, u64>) -> Self {
        let mut tx = self.clone();
        let inputs = std::iter::once(&mut tx.left).chain(&mut tx.right);
        for input in inputs {
            if let Some(&fingerprint) = renamed.get(&input.fingerprint()) {
                *input = Arc::new(input.renamed(fingerprint));
            }
        }
        tx.output = Arc::new(tx.output.renamed(tx.content_fingerprint()));
        tx
    }

    /// This transformation with its keyed output held as rows, the key
    /// columns first, under the same fingerprint. A row output is
    /// returned as it is.
    pub(crate) fn unkeyed(&self) -> Self {
        if !self.need_arrange() {
            return self.clone();
        }
        Self {
            operation: self.operation.unkeyed(),
            left: Arc::clone(&self.left),
            right: self.right.clone(),
            output: Arc::new(self.output.unkeyed()),
            flow: self.flow.unkeyed(),
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
        writeln!(f, "{}", self.operation)?;
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
    use crate::planner::Constraints;
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

    // Cases: operation, label, (is_map, need_arrange), held as rows.
    #[rstest]
    #[case(Operation::RowToRow, "[Row -> Row]", (true, false), Operation::RowToRow)]
    #[case(Operation::RowToKv, "[Row -> KV]", (true, true), Operation::RowToRow)]
    #[case(Operation::KvToRow, "[KV -> Row]", (true, false), Operation::KvToRow)]
    #[case(Operation::KvToKv, "[KV -> KV]", (true, true), Operation::KvToRow)]
    #[case(Operation::JoinToRow, "[Join -> Row]", (false, false), Operation::JoinToRow)]
    #[case(Operation::JoinToKv, "[Join -> KV]", (false, true), Operation::JoinToRow)]
    #[case(
        Operation::AntiJoinToRow,
        "[AntiJoin -> Row]",
        (false, false),
        Operation::AntiJoinToRow
    )]
    #[case(
        Operation::AntiJoinToKv,
        "[AntiJoin -> KV]",
        (false, true),
        Operation::AntiJoinToRow
    )]
    #[case(
        Operation::AggregateToRow,
        "[Aggregate -> Row]",
        (false, false),
        Operation::AggregateToRow
    )]
    #[case(
        Operation::AggregateToKv,
        "[Aggregate -> KV]",
        (false, true),
        Operation::AggregateToRow
    )]
    fn an_operation_knows_its_label_and_shapes(
        #[case] operation: Operation,
        #[case] label: &str,
        #[case] (is_map, need_arrange): (bool, bool),
        #[case] rows: Operation,
    ) {
        assert_eq!(operation.to_string(), label);
        assert_eq!(operation.is_map(), is_map);
        assert_eq!(operation.need_arrange(), need_arrange);
        assert_eq!(operation.unkeyed(), rows);
    }

    /// A join reads two collections, so `map`, which takes one, refuses
    /// to build it.
    #[test]
    fn a_map_of_an_operation_that_is_not_one_is_an_internal_error() {
        let mut produced = HashMap::new();
        let input = copied("s", &mut produced);
        let flow = TransformationFlow::KVToKV {
            key: Arc::new(vec![]),
            value: Arc::new(vec![slot(false, 0), slot(false, 1)]),
            constraints: Constraints::new(Vec::new(), Vec::new()),
            compares: Vec::new(),
        };

        let err = Transformation::map(Operation::JoinToRow, Arc::clone(&input), input, flow)
            .expect_err("a join is not a map");

        assert!(
            matches!(&err, PlanError::Internal(_)) && err.to_string().contains("[Join -> Row]"),
            "got {err}"
        );
    }

    /// `left` joined with `right` on their keys, both arranged by
    /// [`arranged`] first.
    fn join(left: &str, right: &str) -> Transformation {
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
        Transformation::from_info(&info, &mut produced, &mutability_of).expect("joins")
    }

    /// The same step over the same inputs hashes the same however often
    /// it is built, and another input hashes it apart.
    #[test]
    fn a_content_fingerprint_follows_the_step_and_its_inputs() {
        assert_eq!(
            join("s", "t").content_fingerprint(),
            join("s", "t").content_fingerprint()
        );
        assert_ne!(
            join("s", "t").content_fingerprint(),
            join("s", "a").content_fingerprint()
        );
    }

    /// A step held as rows no longer stands for the keyed one, which a
    /// join still needs, so the two hash apart.
    #[test]
    fn a_step_held_as_rows_hashes_apart_from_the_keyed_one() {
        let keyed = join("s", "t");
        assert_ne!(
            keyed.unkeyed().content_fingerprint(),
            keyed.content_fingerprint()
        );
    }

    /// Only the input `renamed` maps is read under a new fingerprint, and
    /// the output follows it: the step hashes apart from the original.
    #[test]
    fn a_fingerprinted_step_reads_its_renamed_inputs() {
        let original = join("s", "t");
        let [left, right] = original.input_fingerprints()[..] else {
            panic!("a join reads two inputs");
        };

        let step = original.fingerprinted(&HashMap::from([(left, 7)]));

        assert_eq!(step.input_fingerprints(), [7, right]);
        assert_eq!(step.output().fingerprint(), step.content_fingerprint());
        assert_ne!(step.output().fingerprint(), original.content_fingerprint());
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
        assert_eq!(rows.operation(), Operation::RowToRow);
        assert_eq!(rows.output().fingerprint(), arranged.output().fingerprint());
        assert_eq!(rows.output().arity(), (0, 2));
        assert!(rows.flow().key().is_empty());
        assert_eq!(**rows.flow().value(), vec![slot(false, 0), slot(false, 1)]);
    }
}
