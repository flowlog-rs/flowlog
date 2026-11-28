//! The loop of an e-graph stratum: the recursive stratum that derives the
//! tables of the program's e-graph, run as equality saturation.
//!
//! The plan holds the stratum as a static loop, and the loop runs once at
//! the static loop's time. Its tables, though, are the rebuild's exact
//! changes, so they feed back at the signed weight: a merge retracts the
//! rows it makes stale. Each round, the rule bodies read the canonical
//! tables of the round before, as in any loop, and every table's seed and
//! rule results go to one [`rebuild`](flowlog_runtime::eqsat::rebuild),
//! whose changes split back into the tables. What leaves is each table's
//! final rows, as a set.

use std::collections::HashSet;

use flowlog_parser::Mutability;
use flowlog_parser::Relation;
use flowlog_planner::planner::RelationBinding;
use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use proc_macro2::Ident;
use proc_macro2::Literal;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use super::recursive::Recursion;
use crate::Codegen;
use crate::CodegenError;
use crate::ty::data::internal_kv_tokens;

impl Codegen {
    /// Returns `true` if `stratum` derives the tables of the program's
    /// e-graph. The planner puts every table's rules in one stratum.
    pub(super) fn is_egraph(&self, stratum: &StratumPlanner) -> bool {
        stratum.relation_bindings().keys().any(|&fp| {
            self.program
                .relation_by_fingerprint(fp)
                .is_some_and(Relation::in_egraph)
        })
    }

    /// Returns the e-graph loop of `stratum`, `let <tables> =
    /// scope.scoped(|inner| { ... });`, given the bindings `leaving` it,
    /// and records each leaving table as static.
    ///
    /// # Errors
    ///
    /// Returns an internal error when profiling, which the parser rejects
    /// for an e-graph as the profiler has no plan for the rebuild's
    /// operators, or when a part of the loop is missing.
    pub(super) fn gen_egraph(
        &mut self,
        stratum: &StratumPlanner,
        leaving: &[&RelationBinding],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        if plan_graph.is_some() {
            return Err(CodegenError::internal(
                "profiling does not support an e-graph loop yet",
            ));
        }
        let (inner_time, step) = self.inner_time_tokens(Mutability::Static)?;
        let mut recursion = Recursion::new(Mutability::Mutable);
        let enter_stmts = self.gen_enter(stratum, &mut recursion, plan_graph);
        let variable_stmts = self.gen_variables(stratum, &mut recursion, &step, plan_graph)?;
        let body_stmts = self.gen_bodies(stratum, &mut recursion, plan_graph)?;
        let rebuild_stmts = self.gen_rebuild(stratum, leaving, &mut recursion)?;
        let set_stmts = self.gen_egraph_feedback(stratum, &recursion)?;
        let (leave_pattern, leave_expr) = self.gen_egraph_leave(leaving, &recursion)?;
        for binding in leaving {
            self.global_fp_to_mutability
                .insert(binding.output().fingerprint(), Mutability::Static);
        }
        let mut export_stmts = Vec::new();
        for binding in leaving {
            export_stmts.push(self.gen_binding_export(binding, plan_graph)?);
        }

        let body = quote! {
            |inner| {
                use ::flowlog_runtime::differential_dataflow::operators::iterate::Variable;
                #(#enter_stmts)*
                #(#variable_stmts)*
                #(#body_stmts)*
                #(#rebuild_stmts)*
                #(#set_stmts)*
                #leave_expr
            }
        };
        Ok(quote! {
            let #leave_pattern = scope.scoped::<#inner_time, _, _>("EqualitySaturation", #body);
            #(#export_stmts)*
        })
    }

    /// Returns the rebuild of every table of `stratum` from its seed and
    /// rule results, `let egraph_changes = rebuild(...)`, then `next_X`,
    /// the changes to table `X`, for each table the loop feeds back or
    /// `leaving` takes, which it records in `recursion`. A table is its
    /// binding's place in fingerprint order.
    fn gen_rebuild(
        &self,
        stratum: &StratumPlanner,
        leaving: &[&RelationBinding],
        recursion: &mut Recursion,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let string_intern = self.config.str_intern_enabled();
        let changes = format_ident!("egraph_changes");
        let read: HashSet<u64> = stratum
            .recursion_feedback_collections()
            .iter()
            .map(|collection| collection.fingerprint())
            .chain(
                leaving
                    .iter()
                    .map(|binding| binding.collapse().fingerprint()),
            )
            .collect();
        let mut schemas = Vec::new();
        let mut rows = Vec::new();
        let mut tables = Vec::new();
        for (index, (&idb_fp, binding)) in stratum.relation_bindings().iter().enumerate() {
            let relation = self
                .program
                .relation_by_fingerprint(idb_fp)
                .ok_or_else(|| {
                    CodegenError::internal(format!(
                        "e-graph table 0x{idb_fp:016x} has no declaration"
                    ))
                })?;
            let table = u16::try_from(index)
                .map(Literal::u16_unsuffixed)
                .map_err(|_| CodegenError::internal("an e-graph has too many tables"))?;
            schemas.push(schema_tokens(relation)?);
            let parts = Self::recursive_parts(
                recursion,
                binding.input_seed(),
                binding.input_contributors(),
            )?;
            for (part, _) in parts {
                rows.push(quote! { ::flowlog_runtime::eqsat::rows(#part.clone(), #table) });
            }
            let collapse_fp = binding.collapse().fingerprint();
            if read.contains(&collapse_fp) {
                let next = format_ident!("next_{collapse_fp}");
                let tuple = internal_kv_tokens(self.find_global_type(collapse_fp)?, string_intern);
                tables.push(quote! {
                    let #next = ::flowlog_runtime::eqsat::table::<#tuple>(#changes.clone(), #table);
                });
                recursion.bindings.insert(collapse_fp, next);
            }
        }
        let Some((first, rest)) = rows.split_first() else {
            return Err(CodegenError::internal("an e-graph loop asserts no rows"));
        };
        let rows = if rest.is_empty() {
            quote! { #first }
        } else {
            quote! { #first.concatenate([ #( #rest ),* ]) }
        };
        let mut stmts = vec![quote! {
            let #changes = ::flowlog_runtime::eqsat::rebuild(#rows, vec![ #( #schemas ),* ]);
        }];
        stmts.extend(tables);
        Ok(stmts)
    }

    /// Returns `recursive_X_var.set(next_X);` for each feedback table of
    /// `stratum`, limited to the rounds `.run` asks for when it does.
    fn gen_egraph_feedback(
        &self,
        stratum: &StratumPlanner,
        recursion: &Recursion,
    ) -> Result<Vec<TokenStream>, CodegenError> {
        let rounds = self
            .program
            .run_rounds()
            .map(|rounds| {
                u16::try_from(rounds)
                    .map(Literal::u16_unsuffixed)
                    .map_err(|_| {
                        CodegenError::internal(format!("`.run {rounds}` passed validation"))
                    })
            })
            .transpose()?;
        let mut stmts = Vec::new();
        for collection in stratum.recursion_feedback_collections() {
            let fp = collection.fingerprint();
            let next = recursion.binding(fp)?;
            let variable = recursion.variables.get(&fp).ok_or_else(|| {
                CodegenError::internal(format!("feedback table 0x{fp:016x} has no variable"))
            })?;
            let var_name = format_ident!("{}_var", variable);
            let fed = match &rounds {
                Some(rounds) => quote! { ::flowlog_runtime::eqsat::limit(#next.clone(), #rounds) },
                None => quote! { #next.clone() },
            };
            stmts.push(quote! { #var_name.set(#fed); });
        }
        Ok(stmts)
    }

    /// Returns the pattern binding the tables that leave the loop, the
    /// collapses of the `leaving` bindings, and the expression leaving
    /// them, each as the set of its final rows.
    fn gen_egraph_leave(
        &self,
        leaving: &[&RelationBinding],
        recursion: &Recursion,
    ) -> Result<(TokenStream, TokenStream), CodegenError> {
        let mut targets: Vec<Ident> = Vec::with_capacity(leaving.len());
        let mut exprs = Vec::with_capacity(leaving.len());
        for binding in leaving {
            let collapse_fp = binding.collapse().fingerprint();
            targets.push(self.find_global_ident(collapse_fp));
            let next = recursion.binding(collapse_fp)?;
            exprs.push(quote! { ::flowlog_runtime::eqsat::finish(#next.leave(scope)) });
        }
        Ok(match (targets.as_slice(), exprs.as_slice()) {
            ([target], [expr]) => (quote! { #target }, quote! { #expr }),
            _ => (quote! { ( #(#targets),* ) }, quote! { ( #(#exprs),* ) }),
        })
    }
}

/// Returns how the rebuild reads `relation`'s rows: as unions for an
/// `eqrel` relation, otherwise as a table keyed by every column but its
/// union column, if any, whose id columns hold e-class ids.
fn schema_tokens(relation: &Relation) -> Result<TokenStream, CodegenError> {
    if relation.is_eqrel() {
        return Ok(quote! { ::flowlog_runtime::eqsat::Schema::eqrel() });
    }
    let narrow = |value: usize| {
        u8::try_from(value).map_err(|_| {
            CodegenError::internal(format!(
                "e-graph table `{}` passed validation with {value} columns",
                relation.raw_name()
            ))
        })
    };
    let arity = Literal::u8_unsuffixed(narrow(relation.arity())?);
    let mut ids = 0_u8;
    for &column in relation.id_columns() {
        let bit = u32::try_from(column)
            .ok()
            .and_then(|column| 1_u8.checked_shl(column))
            .ok_or_else(|| {
                CodegenError::internal(format!(
                    "e-graph table `{}` has an id column at {column}",
                    relation.raw_name()
                ))
            })?;
        ids |= bit;
    }
    let ids = Literal::u8_unsuffixed(ids);
    let value = match relation.union_column() {
        Some(column) => {
            let column = Literal::u8_unsuffixed(narrow(column)?);
            quote! { Some(#column) }
        }
        None => quote! { None },
    };
    Ok(quote! { ::flowlog_runtime::eqsat::Schema::table(#arity, #value, #ids) })
}
