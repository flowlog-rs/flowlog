//! Populates a [`Catalog`]'s signatures, filters, variable-occurrence
//! maps, unused arguments, and superset relationships.
//!
//! Range restriction is validated before body metadata is written:
//! variables in negations and comparisons require a positive binding.

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
use crate::catalog::UnsafePredicateKind;

impl Catalog {
    /// Populates an empty metadata cache for the current rule while
    /// preserving the rule's original identity.
    ///
    /// # Errors
    ///
    /// Returns [`CatalogError::UnsafeVariable`] for a variable without a
    /// positive binding, or an internal error for inconsistent metadata.
    pub(super) fn populate_all_metadata(&mut self) -> Result<(), CatalogError> {
        self.populate_body_metadata()?;
        self.populate_positive_argument_presence()?;
        self.populate_unused_arguments();
        self.populate_supersets();

        Ok(())
    }

    /// Validates range restriction, then populates body metadata.
    ///
    /// Leaves body metadata unchanged when validation fails.
    fn populate_body_metadata(&mut self) -> Result<(), CatalogError> {
        self.validate_range_restriction()?;
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
        self.filters = Filters::new(variable_equality_map, constant_map, placeholders);

        Ok(())
    }

    /// Rejects the first variable in source order without a positive
    /// binding.
    fn validate_range_restriction(&self) -> Result<(), CatalogError> {
        let positive_variables = self.rule.positive_body_vars();
        let rule_span = self.rule.span();
        for predicate in self.rule.rhs() {
            match predicate {
                Predicate::PositiveAtom(_) => {}
                Predicate::NegativeAtom(atom) => {
                    for argument in atom.arguments() {
                        if let AtomArg::Var(variable) = argument
                            && !positive_variables.contains(variable.as_str())
                        {
                            return Err(CatalogError::UnsafeVariable {
                                kind: UnsafePredicateKind::Negation,
                                predicate: format!("!{atom}"),
                                predicate_span: atom.span(),
                                rule_span,
                                var: variable.clone(),
                            });
                        }
                    }
                }
                Predicate::Compare(comparison) => {
                    let variables = comparison
                        .left()
                        .vars()
                        .into_iter()
                        .chain(comparison.right().vars());
                    for variable in variables {
                        if !positive_variables.contains(variable.as_str()) {
                            return Err(CatalogError::UnsafeVariable {
                                kind: UnsafePredicateKind::Comparison,
                                predicate: comparison.to_string(),
                                predicate_span: comparison.span(),
                                rule_span,
                                var: variable.clone(),
                            });
                        }
                    }
                }
            }
        }

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
                Predicate::NegativeAtom(_) | Predicate::Compare(_) => None,
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
                Predicate::PositiveAtom(_) | Predicate::Compare(_) => None,
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
                Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => None,
                Predicate::Compare(comparison) => Some(comparison.clone()),
            })
            .collect();
        self.comparison_variables = self
            .comparison_predicates
            .iter()
            .map(|comparison| comparison.vars_set().into_iter().cloned().collect())
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
            .chain(&self.comparison_variables);
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
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_error::SourceMap;
    use flowlog_parser::Constant;
    use flowlog_parser::DataType;
    use tempfile::NamedTempFile;

    use super::*;

    fn catalog_rule(source: &str) -> (Result<Catalog, CatalogError>, SourceMap) {
        let mut tmp = NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        let mut sm = SourceMap::new();
        let program = flowlog_parser::parse(
            &tmp.path().to_string_lossy(),
            &[],
            &mut sm,
            &mut Config::default(),
        )
        .expect("parse failed");
        let rules = program.rules();
        let rule = rules.first().expect("test source produced no rule");
        (Catalog::from_rule(rule), sm)
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

    #[test]
    fn unsafe_variable_in_negation_is_rejected() {
        let src = "\
            .decl Person(id: int32, name: string)\n\
            .decl Blocked(id: int32)\n\
            .decl Safe(name: string)\n\
            .input Person(IO=\"file\", filename=\"Person.csv\", delimiter=\",\")\n\
            .input Blocked(IO=\"file\", filename=\"Blocked.csv\", delimiter=\",\")\n\
            .output Safe\n\
            Safe(name) :- Person(id, name), !Blocked(other).\n";
        let (result, sm) = catalog_rule(src);
        let err = result.expect_err("an unbound negated variable must be rejected");
        let CatalogError::UnsafeVariable {
            kind,
            predicate,
            predicate_span,
            rule_span,
            var,
        } = &err
        else {
            panic!("got {err:?}");
        };
        assert_eq!(*kind, UnsafePredicateKind::Negation);
        assert_eq!(predicate, "!Blocked(other)");
        assert_eq!(sm.snippet(*predicate_span), "Blocked(other)");
        assert_eq!(
            sm.snippet(*rule_span),
            "Safe(name) :- Person(id, name), !Blocked(other).\n"
        );
        assert_eq!(var, "other");
    }

    #[test]
    fn unsafe_variable_in_comparison_is_rejected() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x) :- Edge(x, y), z > 5.\n";
        let (result, sm) = catalog_rule(src);
        let err = result.expect_err("an unbound compared variable must be rejected");
        let CatalogError::UnsafeVariable {
            kind,
            predicate,
            predicate_span,
            rule_span,
            var,
        } = &err
        else {
            panic!("got {err:?}");
        };
        assert_eq!(*kind, UnsafePredicateKind::Comparison);
        assert_eq!(predicate, "z > 5");
        assert_eq!(sm.snippet(*predicate_span), "z > 5");
        assert_eq!(sm.snippet(*rule_span), "Reach(x) :- Edge(x, y), z > 5.\n");
        assert_eq!(var, "z");
    }

    /// UDFs are value-only, so a UDF filter is a comparison
    /// (`is_positive(z) = True`) and an unbound variable inside it is
    /// reported through the comparison predicate.
    #[test]
    fn unsafe_variable_in_fn_call_is_rejected_as_comparison() {
        let src = "\
            .extern fn is_positive(n: int32) -> bool\n\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x) :- Edge(x, y), is_positive(z) = True.\n";
        let (result, sm) = catalog_rule(src);
        let err = result.expect_err("an unbound UDF argument must be rejected");
        let CatalogError::UnsafeVariable {
            kind,
            predicate,
            predicate_span,
            rule_span,
            var,
        } = &err
        else {
            panic!("got {err:?}");
        };
        assert_eq!(*kind, UnsafePredicateKind::Comparison);
        assert_eq!(predicate, "is_positive(z) == True");
        assert_eq!(sm.snippet(*predicate_span), "is_positive(z) = True");
        assert_eq!(
            sm.snippet(*rule_span),
            "Reach(x) :- Edge(x, y), is_positive(z) = True.\n"
        );
        assert_eq!(var, "z");
    }

    #[test]
    fn first_unsafe_comparison_variable_is_reported() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x) :- Edge(x, y), z > w + 5.\n";
        let (result, _) = catalog_rule(src);
        let err = result.expect_err("the first unbound variable must be rejected");
        let CatalogError::UnsafeVariable {
            kind,
            predicate,
            var,
            ..
        } = err
        else {
            panic!("got {err:?}");
        };
        assert_eq!(kind, UnsafePredicateKind::Comparison);
        assert_eq!(predicate, "z > w + 5");
        assert_eq!(var, "z");
    }

    #[test]
    fn bound_variables_in_negation_and_comparison_are_accepted() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Blocked(x: int32)\n\
            .decl Reach(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .input Blocked(IO=\"file\", filename=\"Blocked.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x) :- Edge(x, y), !Blocked(y), y > 5.\n";
        let (result, _) = catalog_rule(src);
        result.expect("bound negated and compared variables should build a catalog");
    }
}
