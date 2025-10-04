use catalog::{ArithmeticPos, AtomArgumentSignature, Catalog};
use parser::HeadArg;
use std::collections::HashMap;

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};

impl RulePlanner {
    /// Apply post-processing to align the final transformation output with the head arguments.
    /// Optimizes by skipping transformation creation when the head consists solely of variables.
    pub fn post(&mut self, catalog: &mut Catalog) {
        let head_args = catalog.head_arguments();

        if self.is_post_transformation_needed(head_args) {
            self.create_post_transformation(catalog, head_args);
        } else {
            self.update_last_transformation_layout(catalog, head_args);
        }
    }

    /// Check if we need to create a post transformation or can optimize by updating in-place.
    fn is_post_transformation_needed(&self, head_args: &[HeadArg]) -> bool {
        // If head contains arithmetic or aggregation, we need a post transformation
        !head_args.iter().all(|arg| matches!(arg, HeadArg::Var(_)))
    }

    /// Update the last transformation's output layout in-place for all-variable heads.
    fn update_last_transformation_layout(&mut self, catalog: &Catalog, head_args: &[HeadArg]) {
        let last_tx = self
            .transformation_infos
            .last_mut()
            .expect("No transformations available before post phase");

        let name_to_pos = Self::build_name_to_position_mapping(catalog, last_tx);
        let output_values = Self::resolve_head_variables(&name_to_pos, head_args);

        let new_layout = KeyValueLayout::new(Vec::new(), output_values);
        last_tx.update_output_key_value_layout(new_layout);
        last_tx.update_output_fake_sig();
    }

    /// Create a new post transformation for heads with arithmetic/aggregation.
    fn create_post_transformation(&mut self, catalog: &Catalog, head_args: &[HeadArg]) {
        let name_to_sig = Self::build_name_to_signature_mapping(catalog);
        let output_values = Self::resolve_head_arguments(&name_to_sig, head_args);

        let last_tx = self
            .transformation_infos
            .last()
            .expect("No transformations available before post phase");

        let post_tx = TransformationInfo::kv_to_kv(
            last_tx.output_info_fp(),
            last_tx.output_kv_layout().clone(),
            KeyValueLayout::new(Vec::new(), output_values),
            vec![], // no const constraints
            vec![], // no var constraints
            vec![], // no comparisons
        );

        self.transformation_infos.push(post_tx);
    }

    /// Build a mapping from variable names to their positions in the current output layout.
    /// Used for all-variable head optimization.
    fn build_name_to_position_mapping(
        catalog: &Catalog,
        last_tx: &TransformationInfo,
    ) -> HashMap<String, ArithmeticPos> {
        let mut name_to_pos = HashMap::new();
        let output_layout = last_tx.output_kv_layout();
        let all_positions = output_layout
            .key()
            .iter()
            .chain(output_layout.value().iter());
        let atom_sigs = catalog.positive_atom_argument_signature(0);

        for (sig, pos) in atom_sigs.iter().zip(all_positions) {
            let name = catalog.signature_to_argument_str(sig);
            name_to_pos.insert(name.clone(), pos.clone());
        }

        name_to_pos
    }

    /// Build a mapping from variable names to their argument signatures.
    /// Used for general head argument resolution.
    fn build_name_to_signature_mapping(
        catalog: &Catalog,
    ) -> HashMap<String, AtomArgumentSignature> {
        let pos0_sigs = catalog.positive_atom_argument_signature(0);
        let mut name_to_sig = HashMap::with_capacity(pos0_sigs.len());

        for sig in pos0_sigs.iter() {
            let name = catalog.signature_to_argument_str(sig);
            name_to_sig.insert(name.clone(), *sig);
        }

        name_to_sig
    }

    /// Resolve head variables to their corresponding ArithmeticPos (for all-variable heads).
    fn resolve_head_variables(
        name_to_pos: &HashMap<String, ArithmeticPos>,
        head_args: &[HeadArg],
    ) -> Vec<ArithmeticPos> {
        head_args
            .iter()
            .map(|arg| match arg {
                HeadArg::Var(var) => name_to_pos
                    .get(var)
                    .cloned()
                    .unwrap_or_else(|| panic!("Unknown head variable '{}' in post()", var)),
                _ => panic!("Expected only variables in all-variable head"),
            })
            .collect()
    }

    /// Resolve head arguments (variables, arithmetic, aggregation) to ArithmeticPos.
    fn resolve_head_arguments(
        name_to_sig: &HashMap<String, AtomArgumentSignature>,
        head_args: &[HeadArg],
    ) -> Vec<ArithmeticPos> {
        let resolve_var = |var: &str| -> AtomArgumentSignature {
            name_to_sig
                .get(var)
                .copied()
                .unwrap_or_else(|| panic!("Unknown head variable '{}' in post()", var))
        };

        head_args
            .iter()
            .map(|arg| match arg {
                HeadArg::Var(var) => ArithmeticPos::from_var_signature(resolve_var(var.as_str())),
                HeadArg::Arith(arith) => {
                    let var_sigs: Vec<_> = arith
                        .vars()
                        .iter()
                        .map(|v| resolve_var(v.as_str()))
                        .collect();
                    ArithmeticPos::from_arithmetic(arith, &var_sigs)
                }
                HeadArg::Aggregation(agg) => {
                    let var_sigs: Vec<_> =
                        agg.vars().iter().map(|v| resolve_var(v.as_str())).collect();
                    ArithmeticPos::from_arithmetic(agg.arithmetic(), &var_sigs)
                }
            })
            .collect()
    }
}
