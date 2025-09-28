use super::RulePlanner;
use crate::{transformation::KeyValueLayout, TransformationInfo};
use catalog::{AtomArgumentSignature, Catalog};
use parser::ConstType;
use tracing::trace;

impl RulePlanner {
    // =========================================================================
    // Public API
    // =========================================================================

    /// Run the alpha-elimination prepare phase for a single rule.
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

    // =========================================================================
    // Filter pass (private)
    // =========================================================================

    /// Try to apply any available filter in priority order.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> bool {
        // (1) var == var
        if let Some((&a, &b)) = catalog.filters().var_eq_map().iter().next() {
            return self.apply_var_equality_filter(catalog, a, b);
        }

        // (2) var == const
        if let Some((&var_sig, const_val)) = catalog.filters().const_map().iter().next() {
            return self.apply_const_equality_filter(catalog, var_sig, const_val.clone());
        }

        // (3) placeholder
        if let Some(&var_sig) = catalog.filters().placeholder_set().iter().next() {
            return self.apply_placeholder_filter(catalog, var_sig);
        }

        false
    }

    /// Apply a variable equality filter (var1 == var2) by filtering and projecting
    /// away one of the equal variables.
    fn apply_var_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        a: AtomArgumentSignature,
        b: AtomArgumentSignature,
    ) -> bool {
        trace!(
            "Variables equality filter:\n  Atom: {}\n  Arguments: {} == {}",
            catalog.rhs_index_from_signature(*a.atom_signature()),
            a,
            b
        );

        // Canonicalize the kept/dropped order by signature then arg-id.
        let (var1_sig, var2_sig) =
            if (*a.atom_signature(), a.argument_id()) <= (*b.atom_signature(), b.argument_id()) {
                (a, b)
            } else {
                (b, a)
            };

        let atom_signature = var1_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        // We drop var2_sig from the output payload.
        let out_vals = Self::out_values_excluding(args, var2_sig);
        trace!("Output values after dropping {}: {:?}", var2_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![],                     // no const-eq
            vec![(var1_sig, var2_sig)], // var-eq
            vec![],                     // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Var-eq transformation:\n     {}", tx);

        catalog.projection_modify(*atom_signature, vec![var2_sig], new_name, new_fp);

        // Cache new layout and record the transformation.
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
        trace!(
            "Constant equality filter:\n  Atom: {}\n  Arguments: {} == {}",
            catalog.rhs_index_from_signature(*var_sig.atom_signature()),
            var_sig,
            const_val
        );

        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![(var_sig, const_val)], // const-eq
            vec![],                     // no var-eq
            vec![],                     // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Const-eq transformation:\n     {}", tx);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }

    /// Apply a "placeholder" filter and project away its column.
    fn apply_placeholder_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
    ) -> bool {
        trace!(
            "Placeholder filter:\n  Atom: {}\n  Arguments: {}",
            catalog.rhs_index_from_signature(*var_sig.atom_signature()),
            var_sig
        );

        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, atom_id) = catalog.resolve_atom(atom_signature);

        let (in_keys, in_vals) = self.get_or_init_row_kv_layout(atom_fp, args);

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            KeyValueLayout::new(in_keys, in_vals),
            KeyValueLayout::new(Vec::new(), out_vals.clone()),
            vec![], // no const-eq
            vec![], // no var-eq
            vec![], // no comparisons
        );

        let new_name = Self::projection_name(atom_signature, atom_id);
        let new_fp = tx.output_info_fp();

        trace!("Placeholder transformation:\n      {}", tx);

        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp);

        self.kv_layouts.insert(new_fp, 0);
        self.transformation_infos.push(tx);

        true
    }
}
