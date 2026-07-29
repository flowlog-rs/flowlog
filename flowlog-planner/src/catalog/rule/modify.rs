//! Catalog rewrites applied during planning.
//!
//! Each operation changes the rule body for one planning step, then
//! refreshes every derived catalog field through
//! [`Catalog::update_rule`].

use flowlog_parser::Atom;
use flowlog_parser::AtomArg;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Predicate;

use super::Catalog;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::AtomSignature;
use crate::catalog::CatalogError;

impl Catalog {
    /// Replaces an atom's name and fingerprint without changing its
    /// arguments or polarity.
    ///
    /// For example, mapping positive atom `0` to `A_mapped` rewrites
    /// `Out(x) :- A(x).` as `Out(x) :- A_mapped(x).`.
    pub(crate) fn map_modify(
        &mut self,
        atom_signature: AtomSignature,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        let rhs_index = self.rhs_index_from_signature(atom_signature)?;

        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtom(atom) => Predicate::PositiveAtom(Atom::new(
                &new_atom_name,
                atom.arguments().to_vec(),
                new_atom_fingerprint,
            )),
            Predicate::NegativeAtom(atom) => Predicate::NegativeAtom(Atom::new(
                &new_atom_name,
                atom.arguments().to_vec(),
                new_atom_fingerprint,
            )),
            other @ Predicate::Compare(_) => {
                return Err(CatalogError::internal(format!(
                    "map_modify: target predicate at rhs index {rhs_index} is not an atom: {other}"
                )));
            }
        };

        self.update_rule_in_place(rhs_index, new_atom)
    }

    /// Appends named variable arguments to a positive atom and replaces
    /// its name and fingerprint.
    ///
    /// For example, appending `x_next` and renaming the atom to
    /// `A_with_x_next` rewrites `Out(x) :- A(x).` as
    /// `Out(x) :- A_with_x_next(x, x_next).`.
    pub(crate) fn append_arguments_modify(
        &mut self,
        atom_signature: AtomSignature,
        extra_arg_names: Vec<String>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        let rhs_index = self.rhs_index_from_signature(atom_signature)?;

        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtom(atom) => {
                let mut args = atom.arguments().to_vec();
                args.extend(extra_arg_names.into_iter().map(AtomArg::Var));
                Predicate::PositiveAtom(Atom::new(&new_atom_name, args, new_atom_fingerprint))
            }
            other @ (Predicate::NegativeAtom(_) | Predicate::Compare(_)) => {
                return Err(CatalogError::internal(format!(
                    "append_arguments_modify: target predicate at rhs index {rhs_index} \
                     is not a positive atom: {other}"
                )));
            }
        };

        self.update_rule_in_place(rhs_index, new_atom)
    }

    /// Drops the given arguments from an atom and replaces its name and
    /// fingerprint.
    ///
    /// For example, dropping `0.1` and renaming the atom to `A_without_y`
    /// rewrites `Out(x) :- A(x, y, z).` as
    /// `Out(x) :- A_without_y(x, z).`.
    pub(crate) fn projection_modify(
        &mut self,
        atom_signature: AtomSignature,
        arguments_to_delete: Vec<AtomArgumentSignature>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        for arg_sig in &arguments_to_delete {
            if *arg_sig.atom_signature() != atom_signature {
                return Err(CatalogError::internal(format!(
                    "projection_modify: argument signature {arg_sig} does not belong \
                     to target atom {atom_signature}"
                )));
            }
        }

        let rhs_index = self.rhs_index_from_signature(atom_signature)?;

        // Removing from the end keeps earlier indices stable.
        let mut arg_ids_to_delete: Vec<usize> = arguments_to_delete
            .iter()
            .map(|s| s.argument_id())
            .collect();
        arg_ids_to_delete.sort_unstable();
        arg_ids_to_delete.dedup();
        arg_ids_to_delete.reverse();

        let build_projected_atom = |atom: &Atom| -> Result<Atom, CatalogError> {
            for &arg_id in &arg_ids_to_delete {
                if arg_id >= atom.arity() {
                    return Err(CatalogError::internal(format!(
                        "projection_modify: argument id {arg_id} out of bounds for atom \
                         `{}` with arity {}",
                        atom.name(),
                        atom.arity()
                    )));
                }
            }
            let mut new_args = atom.arguments().to_vec();
            for &arg_id in &arg_ids_to_delete {
                new_args.remove(arg_id);
            }
            Ok(Atom::new(&new_atom_name, new_args, new_atom_fingerprint))
        };

        let new_atom = match &self.rule.rhs()[rhs_index] {
            Predicate::PositiveAtom(atom) => Predicate::PositiveAtom(build_projected_atom(atom)?),
            Predicate::NegativeAtom(atom) => Predicate::NegativeAtom(build_projected_atom(atom)?),
            other @ Predicate::Compare(_) => {
                return Err(CatalogError::internal(format!(
                    "projection_modify: target predicate at rhs index {rhs_index} \
                     is not an atom: {other}"
                )));
            }
        };

        self.update_rule_in_place(rhs_index, new_atom)
    }

    /// Replaces a positive atom with its arguments in the requested
    /// order, normally with semijoin keys first.
    ///
    /// For example, using argument order `[0.1, 0.0]` and name
    /// `A_key_first` rewrites `Out(value) :- A(value, key).` as
    /// `Out(value) :- A_key_first(key, value).`.
    pub(crate) fn sip_modify(
        &mut self,
        right_atom_signature: AtomSignature,
        new_argument_list: Vec<AtomArgumentSignature>,
        new_atom_name: String,
        new_atom_fingerprint: u64,
    ) -> Result<(), CatalogError> {
        let rhs_index = self.rhs_index_from_signature(right_atom_signature)?;

        if !matches!(self.rule.rhs()[rhs_index], Predicate::PositiveAtom(_)) {
            return Err(CatalogError::internal(format!(
                "sip_modify: target predicate at rhs index {rhs_index} is not a positive atom: {}",
                self.rule.rhs()[rhs_index]
            )));
        }

        let new_atom_args = self.lookup_arg_vars(&new_argument_list, "sip_modify")?;

        let new_atom = Atom::new(&new_atom_name, new_atom_args, new_atom_fingerprint);
        self.update_rule_in_place(rhs_index, Predicate::PositiveAtom(new_atom))
    }

    /// Removes one left atom and replaces each right atom with a positive
    /// joined atom using its requested arguments, name, and fingerprint.
    ///
    /// For example, joining left atom `0` with right atom `1`, using
    /// arguments `[0.0, 1.1]` and name `A_join_B`, rewrites
    /// `Out(x, z) :- A(x, y), B(y, z).` as
    /// `Out(x, z) :- A_join_B(x, z).`.
    pub(crate) fn join_modify(
        &mut self,
        left_atom_signature: AtomSignature,
        right_atom_signatures: Vec<AtomSignature>,
        new_arguments_list: Vec<Vec<AtomArgumentSignature>>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) -> Result<(), CatalogError> {
        let num_right_atoms = right_atom_signatures.len();
        if new_arguments_list.len() != num_right_atoms
            || new_names.len() != num_right_atoms
            || new_fingerprints.len() != num_right_atoms
        {
            return Err(CatalogError::internal(format!(
                "join_modify: parameter length mismatch: right_atom_signatures={}, \
                 new_arguments_list={}, new_names={}, new_fingerprints={}",
                num_right_atoms,
                new_arguments_list.len(),
                new_names.len(),
                new_fingerprints.len()
            )));
        }

        let left_rhs_index = self.rhs_index_from_signature(left_atom_signature)?;

        match &self.rule.rhs()[left_rhs_index] {
            Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {}
            other @ Predicate::Compare(_) => {
                return Err(CatalogError::internal(format!(
                    "join_modify: left predicate at rhs index {left_rhs_index} \
                     is not an atom: {other}"
                )));
            }
        }

        let right_indices =
            self.validate_atom_rhs_indices(&right_atom_signatures, "join_modify")?;

        let mut new_joined_atoms = Vec::with_capacity(num_right_atoms);
        for i in 0..num_right_atoms {
            let new_atom_args = self.lookup_arg_vars(&new_arguments_list[i], "join_modify")?;
            let new_atom = Atom::new(&new_names[i], new_atom_args, new_fingerprints[i]);
            new_joined_atoms.push(Predicate::PositiveAtom(new_atom));
        }

        self.remove_and_update_rule(left_rhs_index, right_indices, new_joined_atoms)
    }

    /// Removes a comparison and replaces each target atom with a positive
    /// filtered copy using its supplied name and fingerprint.
    ///
    /// For example, atom `0` (`A`) absorbs comparison `0` (`x > 0`)
    /// and is renamed `A_with_x_gt_0`. This rewrites
    /// `Out(x) :- A(x), x > 0.` as `Out(x) :- A_with_x_gt_0(x).`.
    pub(crate) fn comparison_modify(
        &mut self,
        comparison_index: usize,
        right_atom_signatures: Vec<AtomSignature>,
        new_names: Vec<String>,
        new_fingerprints: Vec<u64>,
    ) -> Result<(), CatalogError> {
        let num_atoms = right_atom_signatures.len();
        if new_names.len() != num_atoms || new_fingerprints.len() != num_atoms {
            return Err(CatalogError::internal(format!(
                "comparison_modify: parameter length mismatch: right_atom_signatures={}, \
                 new_names={}, new_fingerprints={}",
                num_atoms,
                new_names.len(),
                new_fingerprints.len()
            )));
        }

        let comparison_rhs_index =
            self.comparison_rhs_index(comparison_index, "comparison_modify")?;

        let right_indices =
            self.validate_atom_rhs_indices(&right_atom_signatures, "comparison_modify")?;

        let new_filtered_atoms = self.build_renamed_atom_copies(
            &right_indices,
            &new_names,
            &new_fingerprints,
            "comparison_modify",
        )?;

        self.remove_and_update_rule(comparison_rhs_index, right_indices, new_filtered_atoms)
    }

    /// Removes comparisons that a planning step has already enforced.
    ///
    /// For example, consuming comparison `0` rewrites
    /// `Out(x) :- A(x), x = 1, x < 10.` as
    /// `Out(x) :- A(x), x < 10.`.
    pub(crate) fn consume_comparisons(
        &mut self,
        comparison_indices: &[usize],
    ) -> Result<(), CatalogError> {
        // Resolve all RHS positions against the current rule, then remove in
        // descending order so removals don't shift the remaining positions;
        // one rule update recomputes the metadata once.
        let mut rhs_indices = comparison_indices
            .iter()
            .map(|&idx| self.comparison_rhs_index(idx, "consume_comparisons"))
            .collect::<Result<Vec<_>, _>>()?;
        rhs_indices.sort_unstable();
        rhs_indices.dedup();

        let mut new_rhs = self.rule.rhs().to_vec();
        for idx in rhs_indices.into_iter().rev() {
            new_rhs.remove(idx);
        }
        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs);
        self.update_rule(&new_rule)
    }

    // --- Helpers ---

    /// Returns the full-body index of the comparison at
    /// `comparison_index` in the catalog's comparison list.
    fn comparison_rhs_index(
        &self,
        comparison_index: usize,
        context: &str,
    ) -> Result<usize, CatalogError> {
        let comparison_predicate = self
            .comparison_predicates
            .get(comparison_index)
            .ok_or_else(|| {
                CatalogError::internal(format!(
                    "{context}: comparison index {comparison_index} out of bounds for length {}",
                    self.comparison_predicates.len()
                ))
            })?;
        self.rule
            .rhs()
            .iter()
            .enumerate()
            .find_map(|(idx, p)| match p {
                Predicate::Compare(expr) if expr == comparison_predicate => Some(idx),
                Predicate::Compare(_) | Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => {
                    None
                }
            })
            .ok_or_else(|| {
                CatalogError::internal(format!(
                    "{context}: comparison predicate at index {comparison_index} \
                     not found in rule RHS"
                ))
            })
    }

    /// Returns the full-body index for each atom signature.
    ///
    /// # Errors
    ///
    /// Returns an internal error if a signature resolves to a non-atom
    /// predicate.
    fn validate_atom_rhs_indices(
        &self,
        signatures: &[AtomSignature],
        context: &str,
    ) -> Result<Vec<usize>, CatalogError> {
        signatures
            .iter()
            .map(|&sig| {
                let idx = self.rhs_index_from_signature(sig)?;
                match &self.rule.rhs()[idx] {
                    Predicate::PositiveAtom(_) | Predicate::NegativeAtom(_) => Ok(idx),
                    other @ Predicate::Compare(_) => Err(CatalogError::internal(format!(
                        "{context}: right predicate at rhs index {idx} is not an atom: {other}"
                    ))),
                }
            })
            .collect()
    }

    /// Returns the variable argument at each signature.
    ///
    /// # Errors
    ///
    /// Returns an internal error if a signature has no variable mapping.
    fn lookup_arg_vars(
        &self,
        signatures: &[AtomArgumentSignature],
        context: &str,
    ) -> Result<Vec<AtomArg>, CatalogError> {
        signatures
            .iter()
            .map(|arg_sig| {
                self.argument_variables
                    .get(arg_sig)
                    .cloned()
                    .map(AtomArg::Var)
                    .ok_or_else(|| {
                        CatalogError::internal(format!(
                            "{context}: argument signature {arg_sig} not found in signature map"
                        ))
                    })
            })
            .collect()
    }

    /// Builds positive copies of the atoms at `indices`, applying the
    /// paired names and fingerprints.
    ///
    /// # Errors
    ///
    /// Returns an internal error if an index points to a non-atom
    /// predicate.
    fn build_renamed_atom_copies(
        &self,
        indices: &[usize],
        new_names: &[String],
        new_fingerprints: &[u64],
        context: &str,
    ) -> Result<Vec<Predicate>, CatalogError> {
        indices
            .iter()
            .enumerate()
            .map(|(i, &atom_idx)| {
                let args = match &self.rule.rhs()[atom_idx] {
                    Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                        atom.arguments().to_vec()
                    }
                    other @ Predicate::Compare(_) => {
                        return Err(CatalogError::internal(format!(
                            "{context}: expected atom predicate at rhs index {atom_idx}, got: {other}"
                        )));
                    }
                };
                let new_atom = Atom::new(&new_names[i], args, new_fingerprints[i]);
                Ok(Predicate::PositiveAtom(new_atom))
            })
            .collect()
    }

    fn update_rule_in_place(
        &mut self,
        global_rhs_idx: usize,
        new_predicate: Predicate,
    ) -> Result<(), CatalogError> {
        let mut new_rhs = self.rule.rhs().to_vec();
        new_rhs[global_rhs_idx] = new_predicate;
        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs);
        self.update_rule(&new_rule)
    }

    fn remove_and_update_rule(
        &mut self,
        global_rhs_index_to_remove: usize,
        global_rhs_indices_to_update: Vec<usize>,
        new_predicates: Vec<Predicate>,
    ) -> Result<(), CatalogError> {
        let mut new_rhs = self.rule.rhs().to_vec();

        for (idx, pred) in global_rhs_indices_to_update.into_iter().zip(new_predicates) {
            new_rhs[idx] = pred;
        }
        new_rhs.remove(global_rhs_index_to_remove);

        let new_rule = FlowLogRule::new(self.rule.head().clone(), new_rhs);
        self.update_rule(&new_rule)
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_common::compute_fp;
    use tempfile::NamedTempFile;

    use super::*;

    fn catalog_for_body(body: &str) -> Catalog {
        let source = format!(
            "\
.decl A(a: int32, b: int32, c: int32)
.decl B(a: int32, b: int32)
.decl Out()
.input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")
.input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")
.output Out
Out() :- {body}.
"
        );
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(source.as_bytes()).expect("write source");
        let mut source_map = SourceMap::new();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut source_map,
            &mut Config::default(),
        )
        .expect("parse rule");
        Catalog::from_rule(program.rules()[0]).expect("build catalog")
    }

    mod map_modify {
        use super::*;

        #[test]
        fn replaces_identity_without_changing_shape_or_polarity() {
            let mut catalog = catalog_for_body("A(x, y, z), !B(x, y)");
            let positive_fp = compute_fp("a_mapped");
            let negative_fp = compute_fp("b_mapped");

            catalog
                .map_modify(AtomSignature::new(true, 0), "A_mapped".into(), positive_fp)
                .expect("map positive atom");
            catalog
                .map_modify(AtomSignature::new(false, 0), "B_mapped".into(), negative_fp)
                .expect("map negative atom");

            assert_eq!(
                catalog.rule().to_string(),
                "out() :- A_mapped(x, y, z), !B_mapped(x, y)."
            );
            assert_eq!(
                catalog
                    .positive_atom_fingerprint(0)
                    .expect("positive fingerprint"),
                positive_fp
            );
            assert_eq!(
                catalog
                    .negative_atom_fingerprint(0)
                    .expect("negative fingerprint"),
                negative_fp
            );
        }
    }

    mod append_arguments_modify {
        use super::*;

        #[test]
        fn appends_arguments_and_refreshes_argument_metadata() {
            let mut catalog = catalog_for_body("A(x, y, z)");
            let new_fp = compute_fp("a_with_x_next");

            catalog
                .append_arguments_modify(
                    AtomSignature::new(true, 0),
                    vec!["x_next".into()],
                    "A_with_x_next".into(),
                    new_fp,
                )
                .expect("append argument");

            assert_eq!(
                catalog.rule().to_string(),
                "out() :- A_with_x_next(x, y, z, x_next)."
            );
            assert_eq!(
                catalog
                    .signature_to_argument_str(&AtomArgumentSignature::new(
                        AtomSignature::new(true, 0),
                        3,
                    ))
                    .expect("appended argument metadata"),
                "x_next"
            );
            assert_eq!(
                catalog
                    .positive_atom_fingerprint(0)
                    .expect("positive fingerprint"),
                new_fp
            );
        }

        #[test]
        fn rejects_negative_atom() {
            let mut catalog = catalog_for_body("A(x, y, z), !B(x, y)");

            let error = catalog
                .append_arguments_modify(
                    AtomSignature::new(false, 0),
                    vec!["extra".into()],
                    "B_with_extra".into(),
                    compute_fp("b_with_extra"),
                )
                .expect_err("negative atom must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: append_arguments_modify: target \
                 predicate at rhs index 1 is not a positive atom: !B(x, y)"
            );
        }
    }

    mod projection_modify {
        use super::*;

        #[test]
        fn drops_arguments_and_reindexes_remaining_metadata() {
            let mut catalog = catalog_for_body("A(x, y, z)");
            let atom = AtomSignature::new(true, 0);

            catalog
                .projection_modify(
                    atom,
                    vec![AtomArgumentSignature::new(atom, 1)],
                    "A_without_y".into(),
                    compute_fp("a_without_y"),
                )
                .expect("project argument");

            assert_eq!(catalog.rule().to_string(), "out() :- A_without_y(x, z).");
            assert_eq!(
                catalog
                    .signature_to_argument_str(&AtomArgumentSignature::new(atom, 1))
                    .expect("reindexed argument metadata"),
                "z"
            );
        }

        #[test]
        fn duplicate_argument_signature_is_removed_once() {
            let mut catalog = catalog_for_body("A(x, y, z)");
            let atom = AtomSignature::new(true, 0);
            let y = AtomArgumentSignature::new(atom, 1);

            catalog
                .projection_modify(
                    atom,
                    vec![y, y],
                    "A_without_y".into(),
                    compute_fp("a_without_y"),
                )
                .expect("project duplicate signature");

            assert_eq!(catalog.rule().to_string(), "out() :- A_without_y(x, z).");
        }

        #[test]
        fn rejects_argument_from_another_atom() {
            let mut catalog = catalog_for_body("A(x, y, z), B(y, z)");

            let error = catalog
                .projection_modify(
                    AtomSignature::new(true, 0),
                    vec![AtomArgumentSignature::new(AtomSignature::new(true, 1), 0)],
                    "A_projected".into(),
                    compute_fp("a_projected"),
                )
                .expect_err("foreign argument signature must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: projection_modify: argument \
                 signature 1.0 does not belong to target atom 0"
            );
        }

        #[test]
        fn rejects_argument_outside_atom_arity() {
            let mut catalog = catalog_for_body("A(x, y, z)");
            let atom = AtomSignature::new(true, 0);

            let error = catalog
                .projection_modify(
                    atom,
                    vec![AtomArgumentSignature::new(atom, 3)],
                    "A_projected".into(),
                    compute_fp("a_projected"),
                )
                .expect_err("out-of-bounds argument must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: projection_modify: argument id 3 \
                 out of bounds for atom `a` with arity 3"
            );
        }
    }

    mod sip_modify {
        use super::*;

        #[test]
        fn reorders_arguments_and_refreshes_identity() {
            let mut catalog = catalog_for_body("A(value, key, rest)");
            let atom = AtomSignature::new(true, 0);
            let new_fp = compute_fp("a_key_first");

            catalog
                .sip_modify(
                    atom,
                    vec![
                        AtomArgumentSignature::new(atom, 1),
                        AtomArgumentSignature::new(atom, 0),
                        AtomArgumentSignature::new(atom, 2),
                    ],
                    "A_key_first".into(),
                    new_fp,
                )
                .expect("reorder arguments");

            assert_eq!(
                catalog.rule().to_string(),
                "out() :- A_key_first(key, value, rest)."
            );
            assert_eq!(
                catalog
                    .positive_atom_fingerprint(0)
                    .expect("positive fingerprint"),
                new_fp
            );
        }

        #[test]
        fn rejects_negative_atom() {
            let mut catalog = catalog_for_body("A(x, y, z), !B(x, y)");

            let error = catalog
                .sip_modify(
                    AtomSignature::new(false, 0),
                    Vec::new(),
                    "B_key_first".into(),
                    compute_fp("b_key_first"),
                )
                .expect_err("negative atom must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: sip_modify: target predicate at rhs \
                 index 1 is not a positive atom: !B(x, y)"
            );
        }

        #[test]
        fn rejects_argument_without_variable_mapping() {
            let mut catalog = catalog_for_body("A(x, y, z)");
            let atom = AtomSignature::new(true, 0);

            let error = catalog
                .sip_modify(
                    atom,
                    vec![AtomArgumentSignature::new(atom, 3)],
                    "A_key_first".into(),
                    compute_fp("a_key_first"),
                )
                .expect_err("unknown argument must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: sip_modify: argument signature 0.3 \
                 not found in signature map"
            );
        }
    }

    mod join_modify {
        use super::*;

        #[test]
        fn replaces_right_atom_for_each_left_polarity() {
            let mut positive = catalog_for_body("A(x, y, z), B(y, w)");
            let a = AtomSignature::new(true, 0);
            let b = AtomSignature::new(true, 1);
            positive
                .join_modify(
                    a,
                    vec![b],
                    vec![vec![
                        AtomArgumentSignature::new(a, 0),
                        AtomArgumentSignature::new(b, 1),
                    ]],
                    vec!["A_join_B".into()],
                    vec![compute_fp("a_join_b")],
                )
                .expect("join positive left atom");
            assert_eq!(positive.rule().to_string(), "out() :- A_join_B(x, w).");

            let mut negative = catalog_for_body("A(x, y, z), !B(x, y)");
            let a = AtomSignature::new(true, 0);
            let b = AtomSignature::new(false, 0);
            negative
                .join_modify(
                    b,
                    vec![a],
                    vec![vec![
                        AtomArgumentSignature::new(b, 1),
                        AtomArgumentSignature::new(a, 2),
                    ]],
                    vec!["A_without_B".into()],
                    vec![compute_fp("a_without_b")],
                )
                .expect("join negative left atom");
            assert_eq!(negative.rule().to_string(), "out() :- A_without_B(y, z).");
        }

        #[test]
        fn rejects_mismatched_parameter_lengths() {
            let mut catalog = catalog_for_body("A(x, y, z), B(y, z)");

            let error = catalog
                .join_modify(
                    AtomSignature::new(true, 0),
                    vec![AtomSignature::new(true, 1)],
                    Vec::new(),
                    vec!["A_join_B".into()],
                    vec![compute_fp("a_join_b")],
                )
                .expect_err("parallel parameters must have matching lengths");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: join_modify: parameter length \
                 mismatch: right_atom_signatures=1, new_arguments_list=0, new_names=1, \
                 new_fingerprints=1"
            );
        }
    }

    mod comparison_modify {
        use super::*;

        #[test]
        fn removes_comparison_and_replaces_target_atom() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0");
            let new_fp = compute_fp("a_with_x_gt_0");

            catalog
                .comparison_modify(
                    0,
                    vec![AtomSignature::new(true, 0)],
                    vec!["A_with_x_gt_0".into()],
                    vec![new_fp],
                )
                .expect("fold comparison");

            assert_eq!(
                catalog.rule().to_string(),
                "out() :- A_with_x_gt_0(x, y, z)."
            );
            assert!(catalog.comparison_supersets().is_empty());
            assert_eq!(
                catalog
                    .positive_atom_fingerprint(0)
                    .expect("positive fingerprint"),
                new_fp
            );
        }

        #[test]
        fn rejects_mismatched_parameter_lengths() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0");

            let error = catalog
                .comparison_modify(
                    0,
                    vec![AtomSignature::new(true, 0)],
                    Vec::new(),
                    vec![compute_fp("a_with_x_gt_0")],
                )
                .expect_err("parallel parameters must have matching lengths");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: comparison_modify: parameter length \
                 mismatch: right_atom_signatures=1, new_names=0, new_fingerprints=1"
            );
        }

        #[test]
        fn rejects_comparison_index_outside_catalog() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0");

            let error = catalog
                .comparison_modify(1, Vec::new(), Vec::new(), Vec::new())
                .expect_err("unknown comparison must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: comparison_modify: comparison index \
                 1 out of bounds for length 1"
            );
        }
    }

    mod consume_comparisons {
        use super::*;

        #[test]
        fn removes_only_requested_comparisons() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0, y < 10");

            catalog
                .consume_comparisons(&[0])
                .expect("consume comparison");

            assert_eq!(catalog.rule().to_string(), "out() :- A(x, y, z), y < 10.");
            assert_eq!(
                catalog
                    .comparison_predicate(0)
                    .expect("remaining comparison")
                    .to_string(),
                "y < 10"
            );
        }

        #[test]
        fn duplicate_index_is_consumed_once() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0, y < 10");

            catalog
                .consume_comparisons(&[0, 0])
                .expect("consume duplicate comparison index");

            assert_eq!(catalog.rule().to_string(), "out() :- A(x, y, z), y < 10.");
        }

        #[test]
        fn rejects_index_outside_catalog() {
            let mut catalog = catalog_for_body("A(x, y, z), x > 0, y < 10");

            let error = catalog
                .consume_comparisons(&[2])
                .expect_err("unknown comparison must be rejected");

            assert_eq!(
                error.to_string(),
                "internal compiler error at stage `catalog`: consume_comparisons: comparison \
                 index 2 out of bounds for length 2"
            );
        }
    }
}
