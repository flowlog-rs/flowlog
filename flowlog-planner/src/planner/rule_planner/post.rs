//! Head alignment: resolves head expressions and emits them in the binding's
//! physical row order before materialization and sharing.

use std::collections::HashMap;

use flowlog_parser::HeadArg;
use flowlog_parser::Variable;
use tracing::trace;

use super::RulePlanner;
use crate::catalog::ArithmeticPos;
use crate::catalog::AtomArgumentSignature;
use crate::catalog::Catalog;
use crate::catalog::KvPredicates;
use crate::planner::KeyValueLayout;
use crate::planner::PlanError;
use crate::planner::TransformationInfo;

// =========================================================================
// Post Processing
// =========================================================================
impl RulePlanner {
    /// Aligns the final output expressions with the binding's row order.
    /// `None` preserves logical head order. Existing producers emit the
    /// selected order directly; a copy rule gets its sole transformation.
    ///
    /// # Errors
    ///
    /// Returns an internal error for an invalid permutation or a head
    /// argument unavailable in the planned body.
    pub(crate) fn post(
        &mut self,
        catalog: &mut Catalog,
        order: Option<&[usize]>,
    ) -> Result<(), PlanError> {
        let name_to_sig = match self.transformation_infos.last() {
            Some(last) => Self::build_name_to_output_signatures_from_last_tx(catalog, last)?,
            None => Self::build_name_to_output_signatures_from_atom(catalog)?,
        };
        let mut output_values =
            Self::resolve_head_arguments(catalog, &name_to_sig, catalog.head_arguments())?;
        if let Some(order) = order {
            let mut sorted = order.to_vec();
            sorted.sort_unstable();
            if !sorted.iter().copied().eq(0..output_values.len()) {
                return Err(PlanError::internal(format!(
                    "post: rule `{}` has binding order {order:?}, expected {} columns",
                    catalog.rule(),
                    output_values.len(),
                )));
            }
            output_values = order
                .iter()
                .map(|&column| output_values[column].clone())
                .collect();
        }
        if let Some(last) = self.transformation_infos.last_mut() {
            last.update_row_output(true);
            last.update_output_key_value_layout(KeyValueLayout::new(Vec::new(), output_values));
            last.refresh_output_fp();
        } else {
            self.create_post_transformation(catalog, output_values)?;
        }
        trace!(
            "Transformations after post:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }

    /// Creates the sole transformation of a rule that only reads an atom.
    fn create_post_transformation(
        &mut self,
        catalog: &Catalog,
        output_values: Vec<ArithmeticPos>,
    ) -> Result<(), PlanError> {
        // A head that reads the atom column for column gets this step too,
        // although it only copies the rows. Codegen emits the copy as an
        // alias of its input, so it costs nothing at run time, while the
        // stratum derives its loop from the transformations alone: which
        // collections enter it, which steps run inside it, and the weight
        // it runs at. A rule without a step would be a read the stratum
        // cannot see, and a loop of such rules would have no body at all.
        let (input_fp, input_kv_layout) = (
            catalog.positive_atom_fingerprint(0)?,
            KeyValueLayout::new(
                Vec::new(),
                catalog
                    .positive_atom_argument_signature(0)?
                    .iter()
                    .map(|&sig| ArithmeticPos::from_var_signature(sig))
                    .collect(),
            ),
        );

        let input_name = catalog.positive_atom_name(0)?.to_string();
        let mut post_tx = TransformationInfo::kv_to_kv(
            input_fp,
            input_name.clone(),
            input_name,
            /*is_row_input*/ true,
            input_kv_layout,
            KeyValueLayout::new(Vec::new(), output_values),
            KvPredicates::default(),
        );
        post_tx.update_row_output(true);
        post_tx.refresh_output_fp();

        self.push_transformation(post_tx, catalog, "Post")?;
        Ok(())
    }
}

// =========================================================================
// Small Utilities (private)
// =========================================================================
impl RulePlanner {
    /// Build a mapping from variable names to their argument signatures as they
    /// appear in the current (last) output layout.
    ///
    /// Respects any reordering performed by previous transformations.
    fn build_name_to_output_signatures_from_last_tx(
        catalog: &Catalog,
        last_tx: &TransformationInfo,
    ) -> Result<HashMap<Variable, AtomArgumentSignature>, PlanError> {
        // Pre-head layouts hold only variable positions; anything else
        // means an earlier planning phase produced a broken layout.
        let as_var = |pos: &ArithmeticPos| -> Result<AtomArgumentSignature, PlanError> {
            pos.init().as_var_signature().copied().ok_or_else(|| {
                PlanError::internal(format!(
                    "post: output layout of rule `{}` holds a non-variable \
                     position before head alignment",
                    catalog.rule()
                ))
            })
        };
        let output_layout = last_tx.output_kv_layout();
        let all_positions = output_layout
            .key()
            .iter()
            .map(as_var)
            .chain(output_layout.value().iter().map(as_var));
        let atom_sigs = catalog.positive_atom_argument_signature(0)?;

        atom_sigs
            .iter()
            .zip(all_positions)
            .map(|(sig, pos)| Ok((catalog.signature_to_variable(sig)?.clone(), pos?)))
            .collect()
    }

    /// Build a mapping from variable names to their argument signatures
    /// straight from the first positive atom, with no transformation
    /// reordering applied.
    fn build_name_to_output_signatures_from_atom(
        catalog: &Catalog,
    ) -> Result<HashMap<Variable, AtomArgumentSignature>, PlanError> {
        let atom_sigs = catalog.positive_atom_argument_signature(0)?;
        atom_sigs
            .iter()
            .map(|sig| Ok((catalog.signature_to_variable(sig)?.clone(), *sig)))
            .collect()
    }

    /// Resolve head arguments (variables and arithmetic expressions, UDF
    /// calls included) into [`ArithmeticPos`].
    fn resolve_head_arguments(
        catalog: &Catalog,
        name_to_sig: &HashMap<Variable, AtomArgumentSignature>,
        head_args: &[HeadArg],
    ) -> Result<Vec<ArithmeticPos>, PlanError> {
        // Unbound head variables are rejected by parser validation; hitting
        // one here means an earlier stage broke that contract.
        let sig_of = |name: &Variable| -> Result<AtomArgumentSignature, PlanError> {
            name_to_sig.get(name).copied().ok_or_else(|| {
                PlanError::internal(format!(
                    "head variable `{name}` of rule `{}` has no binding in the \
                     planned pipeline",
                    catalog.rule()
                ))
            })
        };

        let mut out = Vec::with_capacity(head_args.len());
        for arg in head_args {
            match arg {
                HeadArg::Var(var) => {
                    out.push(ArithmeticPos::from_var_signature(sig_of(var)?));
                }
                HeadArg::Arith(arith) => {
                    let var_sigs: Vec<_> = arith
                        .vars()
                        .iter()
                        .map(|v| sig_of(v))
                        .collect::<Result<_, _>>()?;
                    out.push(ArithmeticPos::from_arithmetic(arith, &var_sigs)?);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use flowlog_parser::Variable;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::rule_planner;

    // Cases: duplicate, missing and out-of-range columns.
    #[rstest]
    #[case(&[0, 0])]
    #[case(&[0])]
    #[case(&[0, 2])]
    fn post_rejects_invalid_binding_orders(#[case] order: &[usize]) {
        let (mut planner, mut catalog) = rule_planner(
            ".decl A(a:int32,b:int32)\n.input A\n\
             .decl Out(a:int32,b:int32)\n.output Out\n\
             Out(a,b) :- A(a,b).\n",
        );
        planner.prepare(&mut catalog).unwrap();
        planner.fuse(catalog.original_atom_fingerprints()).unwrap();
        let error = planner.post(&mut catalog, Some(order)).unwrap_err();
        assert!(matches!(error, PlanError::Internal(_)));
        assert!(error.to_string().contains("binding order"));
    }

    /// `Out(y, x) :- A(x, y).`: post must reorder the final layout so
    /// output values map to head order, not source order. A no-op post
    /// would emit (x, y): every output row swapped, no error.
    #[test]
    fn post_aligns_head_var_order() {
        let (mut planner, mut catalog) = rule_planner(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(y: int32, x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y, x) :- A(x, y).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        planner.post(&mut catalog, None).expect("post");

        let last = planner
            .transformation_infos()
            .last()
            .expect("post must emit/update a transformation");
        let values = last.output_kv_layout().value();
        assert_eq!(values.len(), 2);
        let id0 = values[0].init().as_var_signature().unwrap().argument_id();
        let id1 = values[1].init().as_var_signature().unwrap().argument_id();
        assert_eq!(
            (id0, id1),
            (1, 0),
            "head `Out(y, x)` over `A(x, y)` must map to (arg1=y, arg0=x)"
        );
    }

    /// `Out(x + 1) :- A(x).`: the head carries an arithmetic expression.
    /// Post must preserve the `+ 1` operator, not flatten to bare `x`.
    /// If post dropped the Arith branch, codegen emits `x`: off-by-one
    /// every row with no compiler error.
    /// Post replaces the last output layout with head positions; the names
    /// recorded at planning time must still describe the new columns, and
    /// an arithmetic column has no name.
    #[test]
    fn output_variables_follow_post_alignment() {
        let (mut planner, mut catalog) = rule_planner(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(y: int32, x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y, x + 1) :- A(x, y).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        planner.post(&mut catalog, None).expect("post");

        let last = planner.transformation_infos().last().unwrap();
        assert_eq!(
            last.output_variables(),
            vec![Some(&Variable::from("y")), None]
        );
    }

    #[test]
    fn post_emits_head_arithmetic_expression() {
        let (mut planner, mut catalog) = rule_planner(
            "\
            .decl A(a: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x + 1) :- A(x).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");
        planner
            .fuse(catalog.original_atom_fingerprints())
            .expect("fuse");
        planner.post(&mut catalog, None).expect("post");

        let last = planner
            .transformation_infos()
            .last()
            .expect("post transformation missing");
        let values = last.output_kv_layout().value();
        assert_eq!(values.len(), 1);
        assert!(
            !values[0].rest().is_empty(),
            "head arithmetic `x + 1` collapsed to bare var"
        );
    }
}
