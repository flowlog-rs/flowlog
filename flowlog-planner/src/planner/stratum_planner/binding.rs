//! Relation bindings: a stratum's union of a relation's seed and rule
//! results, combined into the bound relation.

use std::sync::Arc;

use flowlog_parser::Lattice;
use flowlog_parser::Mutability;
use flowlog_parser::Relation;

use crate::planner::Collection;
use crate::stratifier::aggregate_mutability;

/// A relation's binding within one stratum: its seed and rule results
/// concatenated, then collapsed into `output`, the relation's rows under
/// its own fingerprint and in declared column order. The variant names
/// how rows collapse.
#[derive(Debug)]
pub enum RelationBinding {
    /// Collapses equal rows.
    Dedup {
        input_seed: Option<Arc<Collection>>,
        input_contributors: Vec<Arc<Collection>>,
        output: Arc<Collection>,
    },
    /// Collapses rows equal outside the lattice column, merging that
    /// column. The merge is idempotent, so equal rows collapse with no
    /// dedup before it.
    Lattice {
        input_seed: Option<Arc<Collection>>,
        input_contributors: Vec<Arc<Collection>>,
        merge_index: usize,
        lattice: Lattice,
        output: Arc<Collection>,
    },
}

impl RelationBinding {
    /// Binds `relation` from `input_seed` and `input_contributors`,
    /// merging when it declares a lattice column and deduping otherwise.
    /// The output takes the most mutable input's mutability; a lattice
    /// merge makes append rows mutable.
    #[must_use]
    pub(super) fn new(
        relation: &Relation,
        input_seed: Option<Arc<Collection>>,
        input_contributors: Vec<Arc<Collection>>,
    ) -> Self {
        let union = input_seed
            .iter()
            .chain(&input_contributors)
            .map(|input| input.mutability())
            .fold(Mutability::Static, Mutability::max);
        if let Some((merge_index, lattice)) = relation.lattice() {
            let output = Collection::from_relation(relation, aggregate_mutability(union));
            Self::Lattice {
                input_seed,
                input_contributors,
                merge_index,
                lattice,
                output: Arc::new(output),
            }
        } else {
            Self::Dedup {
                input_seed,
                input_contributors,
                output: Arc::new(Collection::from_relation(relation, union)),
            }
        }
    }

    /// The relation's earlier binding, from an input declaration or an
    /// earlier stratum, unioned before the rule results. Inside recursion
    /// this is the entered outer binding, not the feedback variable.
    #[must_use]
    pub fn input_seed(&self) -> Option<&Collection> {
        match self {
            Self::Dedup { input_seed, .. } | Self::Lattice { input_seed, .. } => {
                input_seed.as_deref()
            }
        }
    }

    /// Rule results after sharing, in concatenation order. Each is its
    /// producing transformation's own output, already in `output`'s
    /// layout.
    #[must_use]
    pub fn input_contributors(&self) -> &[Arc<Collection>] {
        match self {
            Self::Dedup {
                input_contributors, ..
            }
            | Self::Lattice {
                input_contributors, ..
            } => input_contributors,
        }
    }

    /// The fingerprints of
    /// [`input_contributors`](Self::input_contributors), in order.
    #[must_use = "consume the iterator to read the contributors"]
    pub fn input_contributor_fingerprints(&self) -> impl Iterator<Item = u64> + '_ {
        self.input_contributors()
            .iter()
            .map(|contributor| contributor.fingerprint())
    }

    #[must_use]
    pub fn output(&self) -> &Arc<Collection> {
        match self {
            Self::Dedup { output, .. } | Self::Lattice { output, .. } => output,
        }
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;

    // Cases: declaration fields, input mutability, whether the binding
    // merges a lattice, output mutability.
    #[rstest]
    #[case::static_dedup("a: int32, b: symbol", Mutability::Static, false, Mutability::Static)]
    #[case::append_dedup("a: int32, b: symbol", Mutability::Append, false, Mutability::Append)]
    #[case::mutable_dedup("a: int32, b: symbol", Mutability::Mutable, false, Mutability::Mutable)]
    #[case::static_minimum(
        "a: int32, b: min<int32>",
        Mutability::Static,
        true,
        Mutability::Static
    )]
    #[case::append_minimum(
        "a: int32, b: min<int32>",
        Mutability::Append,
        true,
        Mutability::Mutable
    )]
    #[case::mutable_maximum(
        "a: int32, b: max<int32>",
        Mutability::Mutable,
        true,
        Mutability::Mutable
    )]
    fn a_binding_takes_its_variant_and_output_weight_from_the_declaration(
        #[case] fields: &str,
        #[case] input: Mutability,
        #[case] lattice: bool,
        #[case] output: Mutability,
    ) {
        let program = program(&format!(".decl R({fields})\n.input R\n.output R\n"));
        let relation = &program.relations()[0];
        let contributor = Arc::new(Collection::from_relation(relation, input));
        let binding = RelationBinding::new(relation, None, vec![contributor]);
        assert_eq!(matches!(binding, RelationBinding::Lattice { .. }), lattice);
        assert_eq!(binding.output().mutability(), output);
    }

    // Cases: declaration fields, (merge index, lattice).
    #[rstest]
    #[case::trailing_minimum("a: int32, b: min<int32>", (1, Lattice::Min))]
    #[case::leading_maximum("a: max<int32>, b: int32, c: symbol", (0, Lattice::Max))]
    fn a_lattice_binding_merges_the_declared_lattice_column(
        #[case] fields: &str,
        #[case] expected: (usize, Lattice),
    ) {
        let program = program(&format!(".decl R({fields})\n.input R\n.output R\n"));
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        let RelationBinding::Lattice {
            merge_index,
            lattice,
            ..
        } = binding
        else {
            panic!("a declared lattice column makes a lattice binding");
        };
        assert_eq!((merge_index, lattice), expected);
    }

    #[test]
    fn a_dedup_binds_at_its_most_mutable_input() {
        let program = program(".decl R(a: int32)\n.input R\n.output R\n");
        let relation = &program.relations()[0];
        let seed = Arc::new(Collection::from_relation(relation, Mutability::Static));
        let contributor = Arc::new(Collection::from_relation(relation, Mutability::Append));
        let binding = RelationBinding::new(relation, Some(seed), vec![contributor]);
        assert_eq!(binding.output().mutability(), Mutability::Append);
    }

    #[test]
    fn a_binding_outputs_under_its_relations_fingerprint() {
        let program = program(".decl R(a: int32)\n.input R\n.output R\n");
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        assert_eq!(binding.output().fingerprint(), compute_fp("r"));
    }
}
