//! Post processing to align the final output to the rule head for rule planning.
//!
//! This includes handling variables, arithmetic expressions.
//!
//! For aggregation in the head, we handled it at stratum planning phase.

use std::collections::HashMap;
use tracing::trace;

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{ArithmeticPos, AtomArgumentSignature, Catalog};
use parser::HeadArg;

// =========================================================================
// Post Processing
// =========================================================================
impl RulePlanner {
    /// Align the final pipeline output to the rule head.
    ///
    /// - If there is no prior transformation, create a single post transformation.
    /// - Otherwise, modify the last transformation in-place to match the head.
    pub fn post(&mut self, catalog: &mut Catalog) {
        let head_args = catalog.head_arguments();

        if self.needs_post_transformation() {
            self.create_post_transformation(catalog, head_args);
        } else {
            self.update_last_transformation_layout(catalog, head_args);
        }

        trace!(
            "Transformations after post:\n{:?}",
            self.transformation_infos
        );
    }
}

impl RulePlanner {
    /// Whether we need to append a post transformation (true when the pipeline is empty).
    #[inline]
    fn needs_post_transformation(&self) -> bool {
        self.transformation_infos.is_empty()
    }

    /// Update the last transformation's output layout in-place for rule head.
    fn update_last_transformation_layout(&mut self, catalog: &Catalog, head_args: &[HeadArg]) {
        let last_tx = self
            .transformation_infos
            .last_mut()
            .expect("No transformations available before post phase");

        let name_to_sig = Self::build_name_to_output_signatures_from_last_tx(catalog, last_tx);
        let output_values = Self::resolve_head_arguments(&name_to_sig, head_args);

        let new_layout = KeyValueLayout::new(Vec::new(), output_values);
        last_tx.update_output_key_value_layout(new_layout);
        last_tx.update_output_fake_sig();
    }

    /// Create a new post transformation when there is no prior transformation.
    fn create_post_transformation(&mut self, catalog: &Catalog, head_args: &[HeadArg]) {
        let name_to_sig = Self::build_name_to_output_signatures_from_atom(catalog);
        let output_values = Self::resolve_head_arguments(&name_to_sig, head_args);

        let (input_fake_sig, input_kv_layout) = (
            catalog.positive_atom_fingerprint(0),
            KeyValueLayout::new(
                Vec::new(),
                catalog
                    .positive_atom_argument_signature(0)
                    .iter()
                    .map(|&sig| ArithmeticPos::from_var_signature(sig))
                    .collect(),
            ),
        );

        let post_tx = TransformationInfo::kv_to_kv(
            input_fake_sig,
            input_kv_layout,
            KeyValueLayout::new(Vec::new(), output_values),
            Vec::new(), // no const constraints
            Vec::new(), // no var constraints
            Vec::new(), // no comparisons
        );

        self.transformation_infos.push(post_tx);
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
    ) -> HashMap<String, AtomArgumentSignature> {
        let output_layout = last_tx.output_kv_layout();
        let all_positions = output_layout
            .key()
            .iter()
            .map(|pos| *pos.init().signature().unwrap())
            .chain(
                output_layout
                    .value()
                    .iter()
                    .map(|pos| *pos.init().signature().unwrap()),
            );
        let atom_sigs = catalog.positive_atom_argument_signature(0);

        atom_sigs
            .iter()
            .zip(all_positions)
            .map(|(sig, pos)| (catalog.signature_to_argument_str(sig).clone(), pos))
            .collect()
    }

    /// Build a mapping from variable names to their argument signatures from the first positive atom.
    /// Used when the pipeline is empty.
    fn build_name_to_output_signatures_from_atom(
        catalog: &Catalog,
    ) -> HashMap<String, AtomArgumentSignature> {
        let atom_sigs = catalog.positive_atom_argument_signature(0);
        atom_sigs
            .iter()
            .map(|sig| (catalog.signature_to_argument_str(sig).clone(), *sig))
            .collect()
    }

    /// Resolve head arguments (variables, arithmetic expressions, aggregation) into ArithmeticPos.
    ///
    /// Notice we handled aggregation at stratum planning phase, so here we only need to
    /// convert aggregation arguments to arithmetic expressions.
    fn resolve_head_arguments(
        name_to_sig: &HashMap<String, AtomArgumentSignature>,
        head_args: &[HeadArg],
    ) -> Vec<ArithmeticPos> {
        let sig_of = |name: &str| -> AtomArgumentSignature {
            name_to_sig
                .get(name)
                .copied()
                .unwrap_or_else(|| panic!("Unknown head variable '{}' in post()", name))
        };

        head_args
            .iter()
            .map(|arg| match arg {
                HeadArg::Var(var) => ArithmeticPos::from_var_signature(sig_of(var.as_str())),
                HeadArg::Arith(arith) => {
                    let var_sigs: Vec<_> =
                        arith.vars().iter().map(|v| sig_of(v.as_str())).collect();
                    ArithmeticPos::from_arithmetic(arith, &var_sigs)
                }
                HeadArg::Aggregation(agg) => {
                    let var_sigs: Vec<_> = agg.vars().iter().map(|v| sig_of(v.as_str())).collect();
                    ArithmeticPos::from_arithmetic(agg.arithmetic(), &var_sigs)
                }
            })
            .collect()
    }
}
