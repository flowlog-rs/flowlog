//! Post processing: aligns a rule pipeline's final output layout to the
//! rule head, resolving head variables and arithmetic into output
//! positions.

use std::collections::HashMap;

use flowlog_parser::HeadArg;
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
    /// Align the final pipeline output to the rule head.
    ///
    /// - If there is no prior transformation, create a single post transformation.
    /// - Otherwise, modify the last transformation in-place to match the head.
    pub(crate) fn post(&mut self, catalog: &mut Catalog) -> Result<(), PlanError> {
        let head_args = catalog.head_arguments();

        // Note: here we always create row output layout for rule heads.
        if self.transformation_infos.is_empty() {
            self.create_post_transformation(catalog, head_args)?;
        } else {
            self.update_last_transformation_layout(catalog, head_args)?;
        }

        trace!(
            "Transformations after post:\n{}",
            self.transformation_infos_dump()
        );
        Ok(())
    }
}

impl RulePlanner {
    /// Update the last transformation's output layout in-place for rule head.
    fn update_last_transformation_layout(
        &mut self,
        catalog: &Catalog,
        head_args: &[HeadArg],
    ) -> Result<(), PlanError> {
        let last_tx = self.transformation_infos.last_mut().ok_or_else(|| {
            PlanError::internal("post: no transformations available before post phase")
        })?;

        let name_to_sig = Self::build_name_to_output_signatures_from_last_tx(catalog, last_tx)?;
        let output_values = Self::resolve_head_arguments(catalog, &name_to_sig, head_args)?;

        let new_layout = KeyValueLayout::new(Vec::new(), output_values);
        last_tx.update_row_output(true);
        last_tx.update_output_key_value_layout(new_layout);
        last_tx.update_output_fake_sig();
        Ok(())
    }

    /// Create a new post transformation when there is no prior transformation.
    fn create_post_transformation(
        &mut self,
        catalog: &Catalog,
        head_args: &[HeadArg],
    ) -> Result<(), PlanError> {
        let name_to_sig = Self::build_name_to_output_signatures_from_atom(catalog)?;
        let output_values = Self::resolve_head_arguments(catalog, &name_to_sig, head_args)?;

        // We default here assume the input layout is all values from the first positive atom.
        // No additional mapping is needed.
        let (input_fake_sig, input_kv_layout) = (
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

        // Post is a layout-only alignment; the name passes through unchanged.
        let input_name = catalog.positive_atom_name(0)?.to_string();
        let mut post_tx = TransformationInfo::kv_to_kv(
            input_fake_sig,
            input_name.clone(),
            input_name,
            true,
            input_kv_layout,
            KeyValueLayout::new(Vec::new(), output_values),
            KvPredicates::default(),
        );
        post_tx.update_row_output(true);
        post_tx.update_output_fake_sig();

        self.transformation_infos.push(post_tx);
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
    ) -> Result<HashMap<String, AtomArgumentSignature>, PlanError> {
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
            .map(|(sig, pos)| Ok((catalog.signature_to_argument_str(sig)?.to_string(), pos?)))
            .collect()
    }

    /// Build a mapping from variable names to their argument signatures
    /// straight from the first positive atom, with no transformation
    /// reordering applied.
    fn build_name_to_output_signatures_from_atom(
        catalog: &Catalog,
    ) -> Result<HashMap<String, AtomArgumentSignature>, PlanError> {
        let atom_sigs = catalog.positive_atom_argument_signature(0)?;
        atom_sigs
            .iter()
            .map(|sig| Ok((catalog.signature_to_argument_str(sig)?.to_string(), *sig)))
            .collect()
    }

    /// Resolve head arguments (variables, arithmetic expressions, aggregation,
    /// UDF) into [`ArithmeticPos`]. Aggregation arguments lower to their inner
    /// arithmetic; the aggregate itself is planned per stratum.
    fn resolve_head_arguments(
        catalog: &Catalog,
        name_to_sig: &HashMap<String, AtomArgumentSignature>,
        head_args: &[HeadArg],
    ) -> Result<Vec<ArithmeticPos>, PlanError> {
        // Unbound head variables are rejected by parser validation; hitting
        // one here means an earlier stage broke that contract.
        let sig_of = |name: &str| -> Result<AtomArgumentSignature, PlanError> {
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
                    out.push(ArithmeticPos::from_var_signature(sig_of(var.as_str())?));
                }
                HeadArg::Arith(arith) => {
                    let var_sigs: Vec<_> = arith
                        .vars()
                        .iter()
                        .map(|v| sig_of(v.as_str()))
                        .collect::<Result<_, _>>()?;
                    out.push(ArithmeticPos::from_arithmetic(arith, &var_sigs)?);
                }
                HeadArg::Aggregation(agg) => {
                    let var_sigs: Vec<_> = agg
                        .vars()
                        .iter()
                        .map(|v| sig_of(v.as_str()))
                        .collect::<Result<_, _>>()?;
                    out.push(ArithmeticPos::from_arithmetic(agg.arithmetic(), &var_sigs)?);
                }
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;

    /// `Out(y, x) :- A(x, y).`: post must reorder the final layout so
    /// output values map to head order, not source order. A no-op post
    /// would emit (x, y): every output row swapped, no error.
    #[test]
    fn post_aligns_head_var_order() {
        let (mut planner, mut catalog) = test_setup(
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
        planner.post(&mut catalog).expect("post");

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
    #[test]
    fn post_emits_head_arithmetic_expression() {
        let (mut planner, mut catalog) = test_setup(
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
        planner.post(&mut catalog).expect("post");

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
