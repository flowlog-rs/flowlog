//! Prepare logic for rule planning.
//!
//! This module implements the prepare phase of rule planning, focusing on applying
//! local filters (variable equality, constant equality, and placeholders) to simplify
//! the rule before the core planning phase.
//!
//! It is socalled "alpha-elimination" because it eliminates variables by applying
//! constraints, similar GYO algorithm to determine acyclicity.
//!
//! Formally, this phase can be found at Wang, Qichen, et al. "Yannakakis+: Practical
//! Acyclic Query Evaluation with Theoretical Guarantees." Proceedings of the ACM on
//! Management of Data 3.3 (2025): 1-28, as part of algorithm 1.

use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{AtomArgumentSignature, Catalog};
use parser::ConstType;
use tracing::trace;

// =========================================================================
// Prepare Planning
// =========================================================================
impl RulePlanner {
    /// Run the prepare phase for a single rule.
    ///
    /// This repeatedly applies (in order):
    /// 1) Local filters (var=var, var=const, placeholder)
    /// 2) (Anti-)semijoins and comparison pushdown
    /// 3) Projection that removes unused arguments
    ///
    /// The loop stops when a full iteration makes no changes.
    pub fn prepare(&mut self, catalog: &mut Catalog) {
        let mut step = 0;

        loop {
            // 1) Try filters first to maximize early pruning and simplify later steps.
            if self.apply_filter(catalog) {
                step += 1;
                trace!("Prepare Step {}: filter applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Then (anti-)semijoins and comparison pushdown.
            if self.apply_semijoin(catalog) {
                step += 1;
                trace!("Prepare Step {}: semijoin applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 3) Finally, remove any arguments that no longer contribute to outputs.
            if self.remove_unused_arguments(catalog) {
                step += 1;
                trace!("Prepare Step {}: unused arguments removed", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Nothing else to do in this phase.
            break;
        }
    }
}

// =========================================================================
// Filter pass (private)
// =========================================================================
impl RulePlanner {
    /// Try to apply any available filter in priority order.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> bool {
        // (1) var == var
        if let Some((&left, &right)) = catalog.filters().var_eq_map().iter().next() {
            trace!(
                "Variables equality filter:\n  Atom: {}\n  Arguments: {} == {}",
                catalog.rhs_index_from_signature(*left.atom_signature()),
                left,
                right
            );
            return self.apply_var_equality_filter(catalog, left, right);
        }

        // (2) var == const
        if let Some((&var_sig, const_val)) = catalog.filters().const_map().iter().next() {
            trace!(
                "Constant equality filter:\n  Atom: {}\n  Arguments: {} == {}",
                catalog.rhs_index_from_signature(*var_sig.atom_signature()),
                var_sig,
                const_val
            );
            return self.apply_const_equality_filter(catalog, var_sig, const_val.clone());
        }

        // (3) placeholder
        if let Some(&var_sig) = catalog.filters().placeholder_set().iter().next() {
            trace!(
                "Placeholder filter:\n  Atom: {}\n  Arguments: {}",
                catalog.rhs_index_from_signature(*var_sig.atom_signature()),
                var_sig
            );
            return self.apply_placeholder_filter(catalog, var_sig);
        }

        false
    }

    /// Apply a variable equality filter (var1 == var2) by filtering and projecting
    /// away one of the equal variables.
    fn apply_var_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        left: AtomArgumentSignature,
        right: AtomArgumentSignature,
    ) -> bool {
        let current_transformation_index = self.transformation_infos.len();

        // Canonicalize the kept/dropped order by argument id.
        let (left_sig, right_sig) = if left.argument_id() <= right.argument_id() {
            (left, right)
        } else {
            (right, left)
        };

        // The kept variable is left_sig, the dropped variable is right_sig.
        let atom_signature = left_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        );

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        // We drop right_sig from the output payload.
        let out_vals = Self::out_values_excluding(args, right_sig);
        trace!("Output values after dropping {}: {:?}", right_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![],                      // no const-eq
            vec![(left_sig, right_sig)], // var-eq
            vec![],                      // no comparisons
        );

        // Generate descriptive name
        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Var-eq transformation:\n     {}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![right_sig], new_name, new_fp);

        // Store layout information
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Apply a constant equality filter (var == const) and project away the filtered column.
    fn apply_const_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
        const_val: ConstType,
    ) -> bool {
        let current_transformation_index = self.transformation_infos.len();

        // The variable to be dropped is var_sig.
        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        );

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![(var_sig, const_val)], // const-eq
            vec![],                     // no var-eq
            vec![],                     // no comparisons
        );

        // Generate descriptive name
        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Const-eq transformation:\n     {}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        // Store layout information
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Apply a placeholder filter and project away its column.
    fn apply_placeholder_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
    ) -> bool {
        let current_transformation_index = self.transformation_infos.len();

        // The variable to be dropped is var_sig.
        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        );

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![], // no const-eq
            vec![], // no var-eq
            vec![], // no comparisons
        );

        // Generate descriptive name
        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Placeholder transformation:\n      {}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        // Store layout information
        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }
}
