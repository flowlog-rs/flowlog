//! The timely-operator count each codegen pattern expands to, per weight
//! and engine. Address prediction multiplies these counts into the operator
//! ranges [`crate::plan::node::Node::operators`] records.
//!
//! The counts are facts about the pinned dependencies, verified against
//! differential-dataflow 0.25 / timely 0.31. A dependency bump that changes
//! an expansion shifts every later address and silently misattributes
//! metrics, so re-verify the primitives below whenever those crates move;
//! an end-to-end profiled run of `example/graph_analysis/reach.dl` shows
//! drift immediately.
//!
//! The primitives are single DD combinators; every pattern after them is a
//! sum of primitives, spelled out in the order its operators are built.

use flowlog_parser::Mutability;

// =============================================================================
// Primitives
// =============================================================================

/// A combinator that builds a single timely operator: a `unary`, Map,
/// Inspect, Concatenate, Join, Probe, a `to_stream` source, or an
/// arrangement's AsCollection.
const ONE: u32 = 1;

/// `.consolidate()`: FlatMap + Consolidate + AsCollection.
const CONSOLIDATE: u32 = 3;

/// `.threshold(...)`: FlatMap + Arrange: Threshold + Threshold + AsCollection.
const THRESHOLD: u32 = 4;

/// `.threshold_semigroup(...)` or `.threshold_total(...)`: FlatMap +
/// Arrange: ThresholdTotal + ThresholdTotal. `first_occurrences` (Map +
/// Arrange: Dedup + Dedup) counts the same.
const THRESHOLD_TOTAL: u32 = 3;

// =============================================================================
// Patterns
// =============================================================================

/// Operators from arranging a collection, by arrangement kind (the
/// `only_key` split in `register_arrangement`):
///
/// - key-only `arrange_by_self()` (2): ArrangeBySelf + AsCollection
/// - key-value `arrange_by_key()` (1): ArrangeByKey
pub(crate) fn arrange(is_key_only: bool) -> u32 {
    if is_key_only { 2 * ONE } else { ONE }
}

/// Operators from `flowlog_dedup` on a collection of weight `mutability`,
/// in a recursive scope when `recursive`. Presence dedups through a
/// total-clock threshold or `first_occurrences` wherever it runs. A signed
/// collection does too outside recursion, but a mutable fixpoint's
/// `Product<u32, u16>` clock is not totally ordered, so there it needs the
/// full `.threshold(...)`.
pub(crate) fn dedup(mutability: Mutability, recursive: bool) -> u32 {
    match (mutability, recursive) {
        (Mutability::Static | Mutability::Append, _) | (Mutability::Mutable, false) => {
            THRESHOLD_TOTAL
        }
        (Mutability::Mutable, true) => THRESHOLD,
    }
}

/// Operators from `flowlog_input_dedup` on an input of weight `mutability`:
/// a presence input dedups; a signed one maps each row to a key, arranges
/// it through the membership latch, and reads the arrangement back as a
/// collection.
pub(crate) fn input_dedup(mutability: Mutability) -> u32 {
    match mutability {
        Mutability::Static | Mutability::Append => dedup(mutability, false),
        Mutability::Mutable => 3 * ONE,
    }
}

/// Operators in `flowlog_antijoin` (excluding arrangement) with the given
/// `filter` and `source` weights, in build order: the source arm, the
/// matched arm through the join, their concatenation, the projection, and
/// the decode.
///
/// - The source arm derefs the source (FlatMap), then encodes it: a
///   presence source as `+1` (FlatMap), a `diff::Mutable` one by a dedup.
/// - The matched arm joins, then encodes the matches at the join's product
///   weight: a presence product as `-1` (FlatMap), a signed one, which
///   either signed input gives, by a dedup and a MapInPlace negation.
/// - The decode dedups at the output weight: the source's under a static
///   filter, `diff::Mutable` under a filter that grows
///   (`AntijoinOutputWeight`).
pub(crate) fn anti_join(filter: Mutability, source: Mutability, recursive: bool) -> u32 {
    let signed = |weight: Mutability| matches!(weight, Mutability::Mutable);
    let positive = ONE
        + if signed(source) {
            dedup(Mutability::Mutable, recursive)
        } else {
            ONE
        };
    let negative = ONE
        + if signed(filter) || signed(source) {
            dedup(Mutability::Mutable, recursive) + ONE
        } else {
            ONE
        };
    let output = match filter {
        Mutability::Static => source,
        Mutability::Append | Mutability::Mutable => Mutability::Mutable,
    };
    let concat_project = 2 * ONE;
    positive + negative + concat_project + dedup(output, recursive)
}

/// Operators from the `diff::Mutable` aggregate, the group-by reduce pipeline
/// through `reduce_abelian`: Map (row chop) + ArrangeByKey + Reduce +
/// AsCollection (merge). The `diff::Append` aggregate mirrors it operator
/// for operator.
pub(crate) const MUTABLE_AGGREGATE: u32 = 4 * ONE;

/// Additional operators for a `diff::Mutable` aggregate with an empty-group
/// default: ToStreamBuilder + Concatenate.
pub(crate) const MUTABLE_AGGREGATE_SEED: u32 = 2 * ONE;

/// Operators from the `diff::Static` aggregate, carrying contributions as
/// weights: Lift + `.threshold_semigroup()` + Lower. Lift emits the optional
/// empty-group contribution without extra operators.
pub(crate) const STATIC_AGGREGATE: u32 = ONE + THRESHOLD_TOTAL + ONE;

/// Operators from the post-leave `diff::Static` aggregate, merging semiring
/// weights collapsed across iterations: `.consolidate()` then a Map back.
pub(crate) const POST_LEAVE_STATIC_AGGREGATE: u32 = CONSOLIDATE + ONE;

/// Operators in `gen_size_inspector`, in build order: the dedup, a lift of a
/// presence relation's weight to an `i32` report weight, the collapse onto
/// one key, its consolidation, the inspect, and an incremental engine's
/// probe.
pub(crate) fn inspect_size(mutability: Mutability, incremental: bool) -> u32 {
    let lift = match mutability {
        Mutability::Static | Mutability::Append => ONE,
        Mutability::Mutable => 0,
    };
    dedup(mutability, false) + lift + ONE + CONSOLIDATE + ONE + probe(incremental)
}

/// Operators in content inspectors (terminal/file), in build order: a
/// mutable relation's consolidation, the inspect, and an incremental
/// engine's probe. Not exercised by the reach fixture, so the names are
/// from codegen, not a run.
pub(crate) fn inspect_content(mutability: Mutability, incremental: bool) -> u32 {
    let consolidate = match mutability {
        Mutability::Static | Mutability::Append => 0,
        Mutability::Mutable => CONSOLIDATE,
    };
    consolidate + ONE + probe(incremental)
}

/// The probe an incremental engine attaches to every report.
fn probe(incremental: bool) -> u32 {
    if incremental { ONE } else { 0 }
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;

    // Cases: filter, source, recursive, operators.
    #[rstest]
    #[case(Mutability::Static, Mutability::Static, false, 9)]
    #[case(Mutability::Static, Mutability::Static, true, 9)]
    #[case(Mutability::Static, Mutability::Append, false, 9)]
    #[case(Mutability::Static, Mutability::Mutable, false, 14)]
    #[case(Mutability::Append, Mutability::Static, false, 9)]
    #[case(Mutability::Append, Mutability::Static, true, 10)]
    #[case(Mutability::Append, Mutability::Append, false, 9)]
    #[case(Mutability::Append, Mutability::Mutable, false, 14)]
    #[case(Mutability::Mutable, Mutability::Static, false, 12)]
    #[case(Mutability::Mutable, Mutability::Static, true, 14)]
    #[case(Mutability::Mutable, Mutability::Append, false, 12)]
    #[case(Mutability::Mutable, Mutability::Mutable, false, 14)]
    #[case(Mutability::Mutable, Mutability::Mutable, true, 17)]
    fn antijoin_operators_follow_its_weights_and_scope(
        #[case] filter: Mutability,
        #[case] source: Mutability,
        #[case] recursive: bool,
        #[case] expected: u32,
    ) {
        assert_eq!(anti_join(filter, source, recursive), expected);
    }

    // Cases: weight, operators.
    #[rstest]
    #[case(Mutability::Static, 3)]
    #[case(Mutability::Append, 3)]
    #[case(Mutability::Mutable, 3)]
    fn input_dedup_operators_follow_the_weight(
        #[case] mutability: Mutability,
        #[case] expected: u32,
    ) {
        assert_eq!(input_dedup(mutability), expected);
    }

    // Cases: weight, recursive, operators.
    #[rstest]
    #[case(Mutability::Static, false, 3)]
    #[case(Mutability::Static, true, 3)]
    #[case(Mutability::Append, false, 3)]
    #[case(Mutability::Append, true, 3)]
    #[case(Mutability::Mutable, false, 3)]
    #[case(Mutability::Mutable, true, 4)]
    fn dedup_operators_follow_its_weight_and_scope(
        #[case] mutability: Mutability,
        #[case] recursive: bool,
        #[case] expected: u32,
    ) {
        assert_eq!(dedup(mutability, recursive), expected);
    }

    // Cases: relation weight, incremental, (size, content) operators.
    #[rstest]
    #[case(Mutability::Static, false, (9, 1))]
    #[case(Mutability::Static, true, (10, 2))]
    #[case(Mutability::Append, true, (10, 2))]
    #[case(Mutability::Mutable, true, (9, 5))]
    fn reports_follow_the_relations_weight_and_engine(
        #[case] mutability: Mutability,
        #[case] incremental: bool,
        #[case] expected: (u32, u32),
    ) {
        assert_eq!(
            (
                inspect_size(mutability, incremental),
                inspect_content(mutability, incremental)
            ),
            expected
        );
    }
}
