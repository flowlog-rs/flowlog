//! Relation bindings: a stratum's union of a relation's seed and rule
//! results, combined into the bound relation.

use std::sync::Arc;

use flowlog_parser::Lattice;
use flowlog_parser::Mutability;
use flowlog_parser::Relation;

use crate::planner::ArithmeticArgument;
use crate::planner::Collection;
use crate::planner::Constraints;
use crate::planner::FactorArgument;
use crate::planner::TransformationArgument;
use crate::planner::TransformationFlow;
use crate::stratifier::aggregate_mutability;

/// A relation's binding within one stratum: its seed and rule results
/// concatenated and collapsed into `collapse`, then mapped by `flow` into
/// `output`, the relation's rows under its own fingerprint. The variant
/// names how rows collapse.
///
/// `collapse` holds the relation within the stratum: every input is in
/// its layout, which a lattice's `merge_index` and a choice's `domain`
/// index, a recursion feeds back and its loop reads, and a static loop's
/// exit merges. Where the relation leaves the stratum, `flow` maps it into
/// `output`, the layout later strata, a later binding's seed, and
/// `.output` see. `collapse` is `output` itself exactly when `flow` is the
/// identity; otherwise a seed, an earlier binding's output, must be
/// brought to the collapse layout before it is bound here.
#[derive(Debug)]
pub enum RelationBinding {
    /// Collapses equal rows.
    Dedup {
        input_seed: Option<Arc<Collection>>,
        input_contributors: Vec<Arc<Collection>>,
        collapse: Arc<Collection>,
        flow: TransformationFlow,
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
        collapse: Arc<Collection>,
        flow: TransformationFlow,
        output: Arc<Collection>,
    },
    /// Collapses rows equal on the `domain` columns, ascending, to the one
    /// with the least other columns; a static loop keeps the least of the
    /// first round with a row. Keeping one row per key absorbs equal rows,
    /// so no dedup runs before it.
    Choice {
        input_seed: Option<Arc<Collection>>,
        input_contributors: Vec<Arc<Collection>>,
        domain: Vec<usize>,
        collapse: Arc<Collection>,
        flow: TransformationFlow,
        output: Arc<Collection>,
    },
}

impl RelationBinding {
    /// Binds `relation` from `input_seed` and `input_contributors`,
    /// merging when it declares a lattice column, choosing when it declares
    /// a choice domain, and deduping otherwise, held as its output in the
    /// declared row, with the identity `flow`. The output takes the most
    /// mutable input's mutability; a lattice merge or a choice makes append
    /// rows mutable.
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
        let flow = identity_row_flow(relation.arity());
        if let Some((merge_index, lattice)) = relation.lattice() {
            let output = Arc::new(Collection::from_relation(
                relation,
                aggregate_mutability(union),
            ));
            Self::Lattice {
                input_seed,
                input_contributors,
                merge_index,
                lattice,
                collapse: Arc::clone(&output),
                flow,
                output,
            }
        } else if let Some(domain) = relation.choice_domain() {
            let output = Arc::new(Collection::from_relation(
                relation,
                aggregate_mutability(union),
            ));
            Self::Choice {
                input_seed,
                input_contributors,
                domain: domain.to_vec(),
                collapse: Arc::clone(&output),
                flow,
                output,
            }
        } else {
            let output = Arc::new(Collection::from_relation(relation, union));
            Self::Dedup {
                input_seed,
                input_contributors,
                collapse: Arc::clone(&output),
                flow,
                output,
            }
        }
    }

    /// The relation's earlier binding, from an input declaration or an
    /// earlier stratum, unioned before the rule results. Inside recursion
    /// this is the entered outer binding, not the feedback variable.
    #[must_use]
    pub fn input_seed(&self) -> Option<&Collection> {
        match self {
            Self::Dedup { input_seed, .. }
            | Self::Lattice { input_seed, .. }
            | Self::Choice { input_seed, .. } => input_seed.as_deref(),
        }
    }

    /// Rule results after sharing, in concatenation order. Each is its
    /// producing transformation's own output, in the collapse layout.
    #[must_use]
    pub fn input_contributors(&self) -> &[Arc<Collection>] {
        match self {
            Self::Dedup {
                input_contributors, ..
            }
            | Self::Lattice {
                input_contributors, ..
            }
            | Self::Choice {
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

    /// The relation's rows within its stratum, as its inputs collapse into
    /// them.
    #[must_use]
    pub fn collapse(&self) -> &Arc<Collection> {
        match self {
            Self::Dedup { collapse, .. }
            | Self::Lattice { collapse, .. }
            | Self::Choice { collapse, .. } => collapse,
        }
    }

    /// The map from a collapsed row to `output`, applied where the relation
    /// leaves its stratum: a `KVToKV` projection over the collapse layout
    /// into `output`'s key and value columns.
    #[must_use]
    pub fn flow(&self) -> &TransformationFlow {
        match self {
            Self::Dedup { flow, .. } | Self::Lattice { flow, .. } | Self::Choice { flow, .. } => {
                flow
            }
        }
    }

    #[must_use]
    pub fn output(&self) -> &Arc<Collection> {
        match self {
            Self::Dedup { output, .. }
            | Self::Lattice { output, .. }
            | Self::Choice { output, .. } => output,
        }
    }
}

/// The projection that copies each of a row's `arity` columns in place.
fn identity_row_flow(arity: usize) -> TransformationFlow {
    let value = (0..arity)
        .map(|index| ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, index))),
            rest: Vec::new(),
        })
        .collect();
    TransformationFlow::KVToKV {
        key: Arc::new(Vec::new()),
        value: Arc::new(value),
        constraints: Constraints::new(Vec::new(), Vec::new()),
        compares: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use flowlog_common::compute_fp;
    use flowlog_parser::test_harness::program;
    use rstest::rstest;

    use super::*;

    // Cases: declaration after its name, input mutability, the binding's
    // variant, output mutability.
    #[rstest]
    #[case::static_dedup(
        "(a: int32, b: symbol)",
        Mutability::Static,
        "dedup",
        Mutability::Static
    )]
    #[case::append_dedup(
        "(a: int32, b: symbol)",
        Mutability::Append,
        "dedup",
        Mutability::Append
    )]
    #[case::mutable_dedup(
        "(a: int32, b: symbol)",
        Mutability::Mutable,
        "dedup",
        Mutability::Mutable
    )]
    #[case::static_minimum(
        "(a: int32, b: min<int32>)",
        Mutability::Static,
        "lattice",
        Mutability::Static
    )]
    #[case::append_minimum(
        "(a: int32, b: min<int32>)",
        Mutability::Append,
        "lattice",
        Mutability::Mutable
    )]
    #[case::mutable_maximum(
        "(a: int32, b: max<int32>)",
        Mutability::Mutable,
        "lattice",
        Mutability::Mutable
    )]
    #[case::static_choice(
        "(a: int32, b: symbol) choice-domain a",
        Mutability::Static,
        "choice",
        Mutability::Static
    )]
    #[case::append_choice(
        "(a: int32, b: symbol) choice-domain a",
        Mutability::Append,
        "choice",
        Mutability::Mutable
    )]
    #[case::mutable_choice(
        "(a: int32, b: symbol) choice-domain a",
        Mutability::Mutable,
        "choice",
        Mutability::Mutable
    )]
    fn a_binding_takes_its_variant_and_output_weight_from_the_declaration(
        #[case] declaration: &str,
        #[case] input: Mutability,
        #[case] variant: &str,
        #[case] output: Mutability,
    ) {
        let program = program(&format!(".decl R{declaration}\n.input R\n.output R\n"));
        let relation = &program.relations()[0];
        let contributor = Arc::new(Collection::from_relation(relation, input));
        let binding = RelationBinding::new(relation, None, vec![contributor]);
        let found = match &binding {
            RelationBinding::Dedup { .. } => "dedup",
            RelationBinding::Lattice { .. } => "lattice",
            RelationBinding::Choice { .. } => "choice",
        };
        assert_eq!((found, binding.output().mutability()), (variant, output));
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

    /// A choice binding holds its domain's columns in ascending order,
    /// however the declaration names them.
    #[test]
    fn a_choice_binding_holds_the_declared_domain() {
        let program = program(
            ".decl R(a: int32, b: symbol, c: int32) choice-domain (c, a)\n.input R\n.output R\n",
        );
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        let RelationBinding::Choice { domain, .. } = binding else {
            panic!("a declared choice domain makes a choice binding");
        };
        assert_eq!(domain, [0, 2]);
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

    /// Column `index` of a row, read in place.
    fn column(index: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, index))),
            rest: Vec::new(),
        }
    }

    // Cases: declaration fields, the flow's value columns.
    #[rstest]
    #[case::dedup("a: int32, b: symbol", vec![column(0), column(1)])]
    #[case::lattice("a: int32, b: min<int32>, c: symbol", vec![column(0), column(1), column(2)])]
    fn a_new_binding_maps_each_collapsed_column_in_place(
        #[case] fields: &str,
        #[case] expected: Vec<ArithmeticArgument>,
    ) {
        let program = program(&format!(".decl R({fields})\n.input R\n.output R\n"));
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        let TransformationFlow::KVToKV {
            key,
            value,
            constraints,
            compares,
        } = binding.flow()
        else {
            panic!("a binding's flow is a projection");
        };
        assert!(key.is_empty());
        assert_eq!(value.as_slice(), expected);
        assert!(constraints.is_empty());
        assert!(compares.is_empty());
    }

    #[test]
    fn a_binding_outputs_under_its_relations_fingerprint() {
        let program = program(".decl R(a: int32)\n.input R\n.output R\n");
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        assert_eq!(binding.output().fingerprint(), compute_fp("r"));
    }

    /// With the identity flow nothing maps the collapsed rows, so the
    /// relation is held within its stratum as its output.
    #[test]
    fn a_new_binding_holds_its_relation_as_its_output() {
        let program = program(".decl R(a: int32)\n.input R\n.output R\n");
        let binding = RelationBinding::new(&program.relations()[0], None, Vec::new());
        assert!(Arc::ptr_eq(binding.collapse(), binding.output()));
    }
}
