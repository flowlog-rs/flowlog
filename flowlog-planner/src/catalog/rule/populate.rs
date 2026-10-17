//! Populates a [`Catalog`]'s signatures, filters, variable-occurrence
//! maps, unused arguments, and superset relationships.

use std::collections::BTreeMap;
use std::collections::BTreeSet;

use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::Constant;
use flowlog_parser::Predicate;

use super::Catalog;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::CatalogError;
use crate::catalog::Filters;

impl Catalog {
    /// Populates an empty metadata cache for the current rule while
    /// preserving the rule's original identity.
    ///
    /// # Errors
    ///
    /// Returns an internal error for inconsistent metadata.
    pub(super) fn populate_all_metadata(&mut self) -> Result<(), CatalogError> {
        self.populate_body_metadata()?;
        self.populate_positive_argument_presence()?;
        self.populate_unused_arguments();
        self.populate_supersets();

        Ok(())
    }

    /// Populates body metadata.
    fn populate_body_metadata(&mut self) -> Result<(), CatalogError> {
        let predicates = self.rule.rhs().to_vec();

        let mut variable_equality_map = BTreeMap::new();
        let mut constant_map = BTreeMap::new();
        let mut placeholders = BTreeSet::new();
        self.populate_positive_atom_metadata(
            &predicates,
            &mut variable_equality_map,
            &mut constant_map,
            &mut placeholders,
        );
        self.populate_negative_atom_metadata(
            &predicates,
            &mut variable_equality_map,
            &mut constant_map,
            &mut placeholders,
        );
        self.populate_comparison_metadata(&predicates);
        self.populate_aggregate_metadata(&predicates);
        self.filters = Filters::new(variable_equality_map, constant_map, placeholders);

        Ok(())
    }

    fn populate_positive_atom_metadata(
        &mut self,
        predicates: &[Predicate],
        variable_equality_map: &mut BTreeMap<AtomArgumentSignature, AtomArgumentSignature>,
        constant_map: &mut BTreeMap<AtomArgumentSignature, Constant>,
        placeholders: &mut BTreeSet<AtomArgumentSignature>,
    ) {
        let atoms = predicates
            .iter()
            .enumerate()
            .filter_map(|(rhs_index, predicate)| match predicate {
                Predicate::PositiveAtom(atom) => Some((rhs_index, atom)),
                Predicate::NegativeAtom(_) | Predicate::Compare(_) | Predicate::Aggregate(_) => {
                    None
                }
            });
        for (positive_atom_index, (rhs_index, atom)) in atoms.enumerate() {
            let atom_signature = AtomSignature::new(true, positive_atom_index);
            let (signatures, variables) = self.populate_atom_argument_metadata(
                atom,
                atom_signature,
                variable_equality_map,
                constant_map,
                placeholders,
            );

            self.positive_atom_body_indices.push(rhs_index);
            self.positive_atom_fingerprints.push(atom.fingerprint());
            self.positive_atom_argument_signatures.push(signatures);
            self.positive_atom_variables.push(variables);
        }
    }

    fn populate_negative_atom_metadata(
        &mut self,
        predicates: &[Predicate],
        variable_equality_map: &mut BTreeMap<AtomArgumentSignature, AtomArgumentSignature>,
        constant_map: &mut BTreeMap<AtomArgumentSignature, Constant>,
        placeholders: &mut BTreeSet<AtomArgumentSignature>,
    ) {
        let atoms = predicates
            .iter()
            .enumerate()
            .filter_map(|(rhs_index, predicate)| match predicate {
                Predicate::PositiveAtom(_) | Predicate::Compare(_) | Predicate::Aggregate(_) => {
                    None
                }
                Predicate::NegativeAtom(atom) => Some((rhs_index, atom)),
            });
        for (negative_atom_index, (rhs_index, atom)) in atoms.enumerate() {
            let atom_signature = AtomSignature::new(false, negative_atom_index);
            let (signatures, variables) = self.populate_atom_argument_metadata(
                atom,
                atom_signature,
                variable_equality_map,
                constant_map,
                placeholders,
            );

            self.negative_atom_body_indices.push(rhs_index);
            self.negative_atom_fingerprints.push(atom.fingerprint());
            self.negative_atom_argument_signatures.push(signatures);
            self.negative_atom_variables.push(variables);
        }
    }

    fn populate_atom_argument_metadata(
        &mut self,
        atom: &Atom,
        atom_signature: AtomSignature,
        variable_equality_map: &mut BTreeMap<AtomArgumentSignature, AtomArgumentSignature>,
        constant_map: &mut BTreeMap<AtomArgumentSignature, Constant>,
        placeholders: &mut BTreeSet<AtomArgumentSignature>,
    ) -> (Vec<AtomArgumentSignature>, BTreeSet<String>) {
        let mut signatures = Vec::with_capacity(atom.arity());
        let mut variables = BTreeSet::new();
        let mut first_occurrences: BTreeMap<&str, AtomArgumentSignature> = BTreeMap::new();

        for (argument_id, argument) in atom.arguments().iter().enumerate() {
            let signature = AtomArgumentSignature::new(atom_signature, argument_id);
            signatures.push(signature);

            match argument {
                AtomArg::Var(variable) => {
                    variables.insert(variable.clone());
                    self.argument_variables.insert(signature, variable.clone());
                    if let Some(first) = first_occurrences.get(variable.as_str()) {
                        variable_equality_map.insert(signature, *first);
                    } else {
                        first_occurrences.insert(variable, signature);
                    }
                }
                AtomArg::Const(constant) => {
                    constant_map.insert(signature, constant.clone());
                }
                AtomArg::Placeholder => {
                    placeholders.insert(signature);
                }
            }
        }

        (signatures, variables)
    }

    fn populate_comparison_metadata(&mut self, predicates: &[Predicate]) {
        self.comparison_predicates = predicates
            .iter()
            .filter_map(|predicate| match predicate {
                Predicate::PositiveAtom(_)
                | Predicate::NegativeAtom(_)
                | Predicate::Aggregate(_) => None,
                Predicate::Compare(comparison) => Some(comparison.clone()),
            })
            .collect();
        self.comparison_variables = self
            .comparison_predicates
            .iter()
            .map(|comparison| comparison.vars_set().into_iter().cloned().collect())
            .collect();
    }

    /// Records each aggregate, its body position, and the variables it
    /// reads from this body. Runs after the positive atom metadata, which
    /// it reads for the body's bindings.
    fn populate_aggregate_metadata(&mut self, predicates: &[Predicate]) {
        let mut bound: BTreeSet<String> = self
            .positive_atom_variables
            .iter()
            .flatten()
            .cloned()
            .collect();
        for (rhs_index, predicate) in predicates.iter().enumerate() {
            let Predicate::Aggregate(aggregate) = predicate else {
                continue;
            };
            bound.extend(aggregate.var().map(String::from));
            self.aggregate_predicates.push(aggregate.clone());
            self.aggregate_body_indices.push(rhs_index);
        }
        self.aggregate_variables = self
            .aggregate_predicates
            .iter()
            .map(|aggregate| {
                aggregate
                    .vars()
                    .into_iter()
                    .filter(|variable| bound.contains(*variable))
                    .map(String::from)
                    .collect()
            })
            .collect();
    }

    /// Records each variable's first binding occurrence per positive atom.
    fn populate_positive_argument_presence(&mut self) -> Result<(), CatalogError> {
        let positive_atom_count = self.positive_atom_argument_signatures.len();
        for (positive_atom_index, signatures) in
            self.positive_atom_argument_signatures.iter().enumerate()
        {
            for signature in signatures {
                // Only primary variable occurrences introduce bindings.
                if self.filters.is_const_or_var_eq_or_placeholder(signature) {
                    continue;
                }
                let Some(variable) = self.argument_variables.get(signature) else {
                    return Err(CatalogError::internal(format!(
                        "argument signature {signature} absent from \
                         signature-to-variable map"
                    )));
                };
                let presence = self
                    .positive_argument_presence
                    .entry(variable.clone())
                    .or_insert_with(|| vec![None; positive_atom_count]);
                if presence[positive_atom_index].is_none() {
                    presence[positive_atom_index] = Some(*signature);
                }
            }
        }
        Ok(())
    }

    /// Records projectable variables used by one body predicate and absent
    /// from the head.
    fn populate_unused_arguments(&mut self) {
        let mut variable_counts: BTreeMap<String, usize> = BTreeMap::new();
        let mut count_variable = |variable: &String| {
            *variable_counts.entry(variable.clone()).or_insert(0) += 1;
        };

        let predicate_variables = self
            .positive_atom_variables
            .iter()
            .chain(&self.negative_atom_variables)
            .chain(&self.comparison_variables)
            .chain(&self.aggregate_variables);
        for variables in predicate_variables {
            variables.iter().for_each(&mut count_variable);
        }

        let head_variables = self.head_variables();
        let atom_signatures = self
            .positive_atom_argument_signatures
            .iter()
            .chain(&self.negative_atom_argument_signatures)
            .flatten();
        for signature in atom_signatures {
            if let Some(variable) = self.argument_variables.get(signature) {
                let used_by_one_predicate = variable_counts.get(variable) == Some(&1);
                let absent_from_head = !head_variables.contains(variable);

                if used_by_one_predicate && absent_from_head {
                    self.unused_arguments_per_atom
                        .entry(*signature.atom_signature())
                        .or_default()
                        .push(*signature);
                }
            }
        }
    }

    /// Records the positive atom variable sets that cover each predicate.
    fn populate_supersets(&mut self) {
        let positive_variable_sets = &self.positive_atom_variables;
        let positive_supersets_of =
            |variables: &BTreeSet<String>, excluded_index: Option<usize>| -> Vec<usize> {
                positive_variable_sets
                    .iter()
                    .enumerate()
                    .filter(|(index, positive_variables)| {
                        Some(*index) != excluded_index && variables.is_subset(positive_variables)
                    })
                    .map(|(index, _)| index)
                    .collect()
            };

        self.positive_supersets = positive_variable_sets
            .iter()
            .enumerate()
            .map(|(index, variables)| positive_supersets_of(variables, Some(index)))
            .collect();

        self.negative_supersets = self
            .negative_atom_variables
            .iter()
            .map(|variables| positive_supersets_of(variables, None))
            .collect();

        self.comparison_supersets = self
            .comparison_variables
            .iter()
            .map(|variables| positive_supersets_of(variables, None))
            .collect();

        self.aggregate_supersets = self
            .aggregate_variables
            .iter()
            .map(|variables| positive_supersets_of(variables, None))
            .collect();
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;

    use super::*;

    fn catalog_rule(source: &str) -> (Result<Catalog, CatalogError>, SourceMap) {
        let mut sm = SourceMap::new();
        let program = flowlog_parser::test_harness::parse(source, &mut sm, &mut Config::default())
            .expect("parse failed");
        let rule = program
            .rules()
            .first()
            .expect("test source produced no rule");
        (Catalog::from_rule(rule, &BTreeSet::new()), sm)
    }

    #[test]
    fn repeated_variable_creates_an_equality_filter() {
        let source = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, x).\n";
        let (result, _) = catalog_rule(source);
        let catalog = result.expect("catalog build failed");
        let signatures = catalog
            .positive_atom_argument_signature(0)
            .expect("positive atom");
        assert_eq!(
            catalog.filters().var_eq_map().get(&signatures[1]),
            Some(&signatures[0])
        );
    }

    #[test]
    fn placeholder_argument_creates_a_placeholder_filter() {
        let source = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out()\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out() :- A(_, 5).\n";
        let (result, _) = catalog_rule(source);
        let catalog = result.expect("catalog build failed");
        let signatures = catalog
            .positive_atom_argument_signature(0)
            .expect("positive atom");
        assert!(catalog.filters().placeholder_set().contains(&signatures[0]));
    }

    #[test]
    fn constant_argument_creates_a_constant_filter() {
        let source = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out()\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out() :- A(_, 5).\n";
        let (result, _) = catalog_rule(source);
        let catalog = result.expect("catalog build failed");
        let signatures = catalog
            .positive_atom_argument_signature(0)
            .expect("positive atom");
        assert_eq!(
            catalog.filters().const_map().get(&signatures[1]),
            Some(&Constant::new(DataType::Int32, "5"))
        );
    }

    #[test]
    fn only_non_head_variable_is_projectable() {
        let source = "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, y).\n";
        let (result, _) = catalog_rule(source);
        let catalog = result.expect("catalog build failed");
        let signatures = catalog
            .positive_atom_argument_signature(0)
            .expect("positive atom");
        let projectable = catalog
            .unused_arguments_per_atom()
            .values()
            .flatten()
            .collect::<BTreeSet<_>>();
        assert!(!projectable.contains(&signatures[0]));
        assert!(projectable.contains(&signatures[1]));
    }

    // --- Aggregates ---

    /// Shared declarations for the aggregate tests.
    const AGGREGATE_DECLS: &str = "\
        .decl A(a: int32, b: int32)\n\
        .decl B(a: int32, b: int32)\n\
        .decl Out(x: int32, y: int32)\n\
        .decl Total(c: int32)\n\
        .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
        .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
        .output Out\n\
        .output Total\n";

    fn aggregate_catalog(rule: &str) -> Catalog {
        let (result, _) = catalog_rule(&format!("{AGGREGATE_DECLS}{rule}\n"));
        result.expect("catalog build failed")
    }

    /// An aggregate's variables are the ones the body binds, outside the
    /// braces or in them; a variable bound only inside is local.
    #[test]
    fn aggregate_variables_are_those_the_body_binds() {
        let catalog = aggregate_catalog("Out(x, c) :- A(x, y), c = count : { B(x, z), z > y }.");
        let expected: BTreeSet<String> = ["x", "y"].map(String::from).into();
        assert_eq!(catalog.aggregate_variables(), [expected]);
        assert_eq!(catalog.aggregate_supersets(), [vec![0]]);
    }

    /// A nested aggregate's variables are its own body's business: the
    /// outer aggregate reads nothing from a rule body that binds nothing.
    #[test]
    fn a_nested_aggregate_leaves_the_outer_variables_empty() {
        let catalog =
            aggregate_catalog("Total(c) :- c = count : { A(x, _), sum z : B(x, z) > 1 }.");
        assert_eq!(catalog.aggregate_variables(), [BTreeSet::new()]);
        assert_eq!(catalog.aggregate_supersets(), [Vec::<usize>::new()]);
    }

    /// An aggregate reading an earlier one's variable is covered only once
    /// an atom carries that variable.
    #[test]
    fn an_aggregate_reading_another_waits_for_its_column() {
        let catalog = aggregate_catalog(
            "Out(x, d) :- A(x, _), c = count : B(x, _), d = sum y + c : B(x, y).",
        );
        let expected: BTreeSet<String> = ["@agg_0", "x"].map(String::from).into();
        assert_eq!(catalog.aggregate_variables()[1], expected);
        assert_eq!(catalog.aggregate_supersets(), [vec![0], Vec::new()]);
    }

    /// A variable an aggregate reads is in use, so its atom column stays.
    #[test]
    fn an_aggregate_keeps_the_columns_it_reads() {
        let catalog = aggregate_catalog("Total(c) :- A(x, y), c = count : B(x, _).");
        let signatures = catalog
            .positive_atom_argument_signature(0)
            .expect("positive atom");
        let projectable = catalog
            .unused_arguments_per_atom()
            .values()
            .flatten()
            .collect::<BTreeSet<_>>();
        assert!(!projectable.contains(&signatures[0]));
        assert!(projectable.contains(&signatures[1]));
    }
}
