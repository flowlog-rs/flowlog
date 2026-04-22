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
use crate::catalog::{ArithmeticPos, AtomArgumentSignature, Catalog, KvPredicates};
use crate::parser::ConstType;
use crate::planner::PlanError;
use crate::planner::{KeyValueLayout, TransformationInfo};
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
    pub(crate) fn prepare(&mut self, catalog: &mut Catalog) -> Result<(), PlanError> {
        let mut step = 0;

        loop {
            // 1) Try filters first to maximize early pruning and simplify later steps.
            if self.apply_filter(catalog)? {
                step += 1;
                trace!("Prepare Step {}: filter applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 2) Then (anti-)semijoins and comparison pushdown.
            if self.apply_semijoin(catalog)? {
                step += 1;
                trace!("Prepare Step {}: semijoin applied", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // 3) Finally, remove any arguments that no longer contribute to outputs.
            if self.remove_unused_arguments(catalog)? {
                step += 1;
                trace!("Prepare Step {}: unused arguments removed", step);
                trace!("Catalog:\n{}", catalog);
                trace!("{}", "-".repeat(60));
                continue;
            }

            // Nothing else to do in this phase.
            break;
        }
        Ok(())
    }
}

// =========================================================================
// Filter pass (private)
// =========================================================================
impl RulePlanner {
    /// Try to apply any available filter in priority order.
    fn apply_filter(&mut self, catalog: &mut Catalog) -> Result<bool, PlanError> {
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

        Ok(false)
    }

    /// Apply a variable equality filter (var1 == var2) by filtering and projecting
    /// away one of the equal variables.
    fn apply_var_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        left: AtomArgumentSignature,
        right: AtomArgumentSignature,
    ) -> Result<bool, PlanError> {
        let current_transformation_index = self.transformation_infos.len();

        // Canonicalize the kept/dropped order by argument id.
        let (left_sig, right_sig) = if left.argument_id() <= right.argument_id() {
            (left, right)
        } else {
            (right, left)
        };

        // The kept variable is left_sig, the dropped variable is right_sig.
        let atom_signature = left_sig.atom_signature();
        let (args, atom_fp, _atom_id, input_name) = catalog.resolve_atom(atom_signature)?;
        let input_name = input_name.to_string();

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        )?;

        // Get current values for this atom
        let in_vals = args
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect::<Vec<_>>();

        // We drop right_sig from the output payload.
        let out_vals = Self::out_values_excluding(args, right_sig);
        trace!("Output values after dropping {}: {:?}", right_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let kept_attrs = Self::attrs_from_positions(&out_vals, catalog);
        let new_name = Self::proj_name(&input_name, &kept_attrs);
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            input_name,
            new_name.clone(),
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(vec![], in_vals),
            KeyValueLayout::new(vec![], out_vals.clone()),
            KvPredicates {
                var_eq: vec![(left_sig, right_sig)],
                ..Default::default()
            },
        );

        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Var-eq transformation:\n{}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![right_sig], new_name, new_fp)?;

        // Store the transformation info
        self.transformation_infos.push(tx);

        Ok(true)
    }

    /// Apply a constant equality filter (var == const) and project away the filtered column.
    fn apply_const_equality_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
        const_val: ConstType,
    ) -> Result<bool, PlanError> {
        let current_transformation_index = self.transformation_infos.len();

        // The variable to be dropped is var_sig.
        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, _atom_id, input_name) = catalog.resolve_atom(atom_signature)?;
        let input_name = input_name.to_string();

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        )?;

        // Get current values for this atom
        let in_vals = args
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect::<Vec<_>>();

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let kept_attrs = Self::attrs_from_positions(&out_vals, catalog);
        let new_name = Self::proj_name(&input_name, &kept_attrs);
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            input_name,
            new_name.clone(),
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(vec![], in_vals),
            KeyValueLayout::new(vec![], out_vals.clone()),
            KvPredicates {
                const_eq: vec![(var_sig, const_val)],
                ..Default::default()
            },
        );

        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Const-eq transformation:\n{}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp)?;

        // Store the transformation info
        self.transformation_infos.push(tx);

        Ok(true)
    }

    /// Apply a placeholder filter and project away its column.
    fn apply_placeholder_filter(
        &mut self,
        catalog: &mut Catalog,
        var_sig: AtomArgumentSignature,
    ) -> Result<bool, PlanError> {
        let current_transformation_index = self.transformation_infos.len();

        // The variable to be dropped is var_sig.
        let atom_signature = var_sig.atom_signature();
        let (args, atom_fp, _atom_id, input_name) = catalog.resolve_atom(atom_signature)?;
        let input_name = input_name.to_string();

        // Update consumer transformation index for the atom
        self.insert_consumer(
            catalog.original_atom_fingerprints(),
            atom_fp,
            current_transformation_index,
        )?;

        // Get current values for this atom
        let in_vals = args
            .iter()
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect::<Vec<_>>();

        let out_vals = Self::out_values_excluding(args, var_sig);
        trace!("Output values after dropping {}: {:?}", var_sig, out_vals);

        // Build a Key-Value to Key-Value info.
        let kept_attrs = Self::attrs_from_positions(&out_vals, catalog);
        let new_name = Self::proj_name(&input_name, &kept_attrs);
        let tx = TransformationInfo::kv_to_kv(
            atom_fp,
            input_name,
            new_name.clone(),
            catalog.original_atom_fingerprints().contains(&atom_fp),
            KeyValueLayout::new(vec![], in_vals),
            KeyValueLayout::new(vec![], out_vals.clone()),
            KvPredicates::default(),
        );

        let new_fp = tx.output_info_fp();

        // Update producer transformation index
        self.insert_producer(new_fp, current_transformation_index);

        trace!("Placeholder transformation:\n{}", tx);

        // Update catalog with the projection modification
        catalog.projection_modify(*atom_signature, vec![var_sig], new_name, new_fp)?;

        // Store the layout information
        self.transformation_infos.push(tx);

        Ok(true)
    }

    /// Build output expressions excluding a specific argument signature.
    #[inline]
    fn out_values_excluding(
        args: &[AtomArgumentSignature],
        drop_sig: AtomArgumentSignature,
    ) -> Vec<ArithmeticPos> {
        args.iter()
            .filter(|&sig| *sig != drop_sig)
            .map(|&sig| ArithmeticPos::from_var_signature(sig))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::super::common::test_setup;
    use super::*;

    /// `A(x, x)` — var_eq canonicalization must keep the lower-argument-id
    /// slot (arg 0) and drop the higher (arg 1). If the `<=` flipped, the
    /// wrong slot survives and downstream indexes go stale silently.
    #[test]
    fn prepare_var_eq_drops_canonical_slot() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, x).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        let tx = planner
            .transformation_infos()
            .iter()
            .find(|t| !t.kv_predicates().var_eq.is_empty())
            .expect("var_eq transformation missing");
        let (kept, dropped) = &tx.kv_predicates().var_eq[0];
        assert_eq!(kept.argument_id(), 0, "kept sig must be arg 0");
        assert_eq!(dropped.argument_id(), 1, "dropped sig must be arg 1");
        assert_eq!(
            tx.output_kv_layout().value().len(),
            1,
            "one slot must be dropped after var_eq projection"
        );

        // Post-state: the filter must be consumed from the catalog, not
        // just "processed". A broken projection_modify that left var_eq_map
        // populated would cause prepare to loop forever or re-emit the
        // same filter.
        assert!(
            catalog.filters().var_eq_map().is_empty(),
            "var_eq filter must be consumed from catalog"
        );
    }

    /// `A(x, 5)` — const_eq must route the literal verbatim (same value,
    /// same variant). A bug that swapped or truncated the constant would
    /// silently generate wrong filters at codegen.
    #[test]
    fn prepare_const_eq_preserves_constant_value() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, 5).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        let tx = planner
            .transformation_infos()
            .iter()
            .find(|t| !t.kv_predicates().const_eq.is_empty())
            .expect("const_eq transformation missing");
        assert_eq!(tx.kv_predicates().const_eq[0].1, ConstType::Int(5));
        assert_eq!(tx.output_kv_layout().value().len(), 1);

        assert!(
            catalog.filters().const_map().is_empty(),
            "const_eq filter must be consumed from catalog"
        );
    }

    /// `A(_, y)` — placeholder filter runs via a distinct branch. It must
    /// drop the slot WITHOUT adding anything to var_eq or const_eq. A
    /// refactor that merged placeholder handling into one of the other
    /// filter branches would miscategorize the predicate.
    #[test]
    fn prepare_placeholder_filter_runs_without_eq_predicate() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32)\n\
            .decl Out(y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y) :- A(_, y).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        let tx = planner
            .transformation_infos()
            .iter()
            .find(|t| t.kv_predicates().is_empty() && t.output_kv_layout().value().len() == 1)
            .expect("placeholder transformation missing");
        assert!(tx.kv_predicates().var_eq.is_empty());
        assert!(tx.kv_predicates().const_eq.is_empty());

        assert!(
            catalog.filters().placeholder_set().is_empty(),
            "placeholder must be consumed from catalog"
        );
    }

    /// `A(x, x, 5)` triggers BOTH var_eq and const_eq. The fixed-point
    /// `loop { if ... apply_filter ... continue; ... }` must keep going
    /// until every filter has fired. A break-instead-of-continue bug
    /// would apply exactly one filter and stop.
    #[test]
    fn prepare_fixpoint_applies_all_filters() {
        let (mut planner, mut catalog) = test_setup(
            "\
            .decl A(a: int32, b: int32, c: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, x, 5).\n",
        );
        planner.prepare(&mut catalog).expect("prepare");

        let txs = planner.transformation_infos();
        let var_eq_hits = txs
            .iter()
            .filter(|t| !t.kv_predicates().var_eq.is_empty())
            .count();
        let const_eq_hits = txs
            .iter()
            .filter(|t| !t.kv_predicates().const_eq.is_empty())
            .count();
        assert_eq!(var_eq_hits, 1, "var_eq filter must fire once");
        assert_eq!(const_eq_hits, 1, "const_eq filter must fire once");

        assert!(
            catalog.filters().is_empty(),
            "all filters must be consumed after fixed-point prepare"
        );
        assert!(catalog.is_planned());
    }
}
