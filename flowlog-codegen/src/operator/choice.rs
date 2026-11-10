//! Choice merges: a relation with a choice domain keeps one row per value
//! of the domain's columns. [`Codegen::gen_choice_merge`] wraps the union
//! of the rows its rules derive where a stratum binds that union, outside
//! or inside a loop, as the binding's collapse holds them, the layout its
//! domain indexes; [`Codegen::gen_choice`] also wraps the rows an input
//! loads, in the relation's own layout, so the domain holds of them too.

use flowlog_parser::Mutability;
use flowlog_parser::Relation;
use flowlog_planner::planner::RelationBinding;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::expr::aggregation::choice_merge;
use crate::expr::aggregation::choice_split;

impl Codegen {
    /// Returns `rows`, `binding`'s union in its collapse layout, merged to
    /// one row per value of its domain's columns.
    ///
    /// Duplicate rows do not change a choice, so the input need not be
    /// deduped first.
    ///
    /// # Errors
    ///
    /// Returns an internal error if `binding` is not a choice binding, its
    /// collapse has no recorded type, or its domain names a column outside
    /// the row.
    pub(crate) fn gen_choice_merge(
        &self,
        binding: &RelationBinding,
        rows: TokenStream,
    ) -> Result<TokenStream, CodegenError> {
        let name = self.display_name(binding.output().fingerprint());
        let RelationBinding::Choice { domain, .. } = binding else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no choice domain"
            )));
        };
        let (_, row) = self.find_global_type(binding.collapse().fingerprint())?;
        if let Some(outside) = domain.iter().find(|&&column| column >= row.len()) {
            return Err(CodegenError::internal(format!(
                "choice of `{name}` reads column {outside} of a {}-column row",
                row.len(),
            )));
        }
        Ok(choice_call(&name, row.len(), domain, rows))
    }

    /// Returns `rows`, `relation`'s loaded rows of weight `rows_mutability`
    /// in its own layout, merged to one row per value of its choice
    /// `domain`. Records the step under `output`.
    pub(crate) fn gen_choice(
        &self,
        relation: &Relation,
        domain: &[usize],
        rows: TokenStream,
        rows_mutability: Mutability,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> TokenStream {
        let name = self.display_name(relation.fingerprint());
        // The rows feeding the choice are recorded under the output's name
        // too, so the choice reads from them.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.choice_operator(
                name.clone(),
                output.to_string(),
                output.to_string(),
                relation.fingerprint(),
                rows_mutability,
            );
        });
        choice_call(&name, relation.arity(), domain, rows)
    }
}

/// Returns `flowlog_choice(<rows>, ..)` over rows of `arity` columns, keyed
/// by the `domain` columns, named for relation `name`.
fn choice_call(name: &str, arity: usize, domain: &[usize], rows: TokenStream) -> TokenStream {
    let op_name = format!("Choice: {name}");
    let split = choice_split(arity, domain);
    let merge = choice_merge(arity, domain);
    quote! {
        ::flowlog_runtime::operators::flowlog_choice(#rows, #op_name, #split, #merge)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ProgramPlanner;

    use super::*;
    use crate::test_harness::codegen;

    /// A code generator over `Tree`, a choice over the static `Cand`, and
    /// `Tree`'s binding rebuilt to collapse into its rule result, typed
    /// `row`, and choose by the columns `domain`.
    fn tree(domain: Vec<usize>, row: Vec<DataType>) -> (Codegen, RelationBinding) {
        let mut codegen = codegen(
            ".decl Cand(u: int32, v: int32)\n.input Cand\n\
             .decl Tree(u: int32, v: int32) choice-domain v\nTree(u, v) :- Cand(u, v).\n\
             .output Tree\n",
        );
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let Some(RelationBinding::Choice {
            input_seed,
            input_contributors,
            flow,
            output,
            ..
        }) = plan
            .strata()
            .iter()
            .find_map(|stratum| stratum.relation_bindings().get(&compute_fp("tree")))
        else {
            panic!("Tree chooses by its domain");
        };
        let collapse = Arc::clone(&input_contributors[0]);
        codegen
            .global_fp_to_type
            .insert(collapse.fingerprint(), (Vec::new(), row));
        let binding = RelationBinding::Choice {
            input_seed: input_seed.clone(),
            input_contributors: input_contributors.clone(),
            domain,
            collapse,
            flow: flow.clone(),
            output: Arc::clone(output),
        };
        (codegen, binding)
    }

    /// The choice splits the domain's columns out of each row as the key
    /// and puts each column back in place.
    #[test]
    fn a_choice_keys_its_rows_by_the_domain() {
        let (codegen, binding) = tree(vec![1], vec![DataType::Int32, DataType::Int32]);
        let tokens = codegen
            .gen_choice_merge(&binding, quote! { rows })
            .expect("column 1 is in Tree's row");
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_choice(
                rows,
                "Choice: Tree",
                |(x0, x1)| ((x1,), (x0,)),
                |(k0,), (v0,)| (v0, k0)
            )
        };
        assert_eq!(tokens.to_string(), expected.to_string());
    }

    #[test]
    fn a_dedup_binding_has_no_choice() {
        let (codegen, binding) = tree(vec![1], vec![DataType::Int32, DataType::Int32]);
        let RelationBinding::Choice {
            input_seed,
            input_contributors,
            collapse,
            flow,
            output,
            ..
        } = binding
        else {
            panic!("tree builds a choice binding");
        };
        let dedup = RelationBinding::Dedup {
            input_seed,
            input_contributors,
            collapse,
            flow,
            output,
        };
        let error = codegen
            .gen_choice_merge(&dedup, quote! { rows })
            .expect_err("a dedup binding has no domain");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("relation `Tree` has no choice domain"),
            "{error}"
        );
    }

    #[test]
    fn a_domain_column_outside_the_row_is_an_internal_error() {
        let (codegen, binding) = tree(vec![2], vec![DataType::Int32, DataType::Int32]);
        let error = codegen
            .gen_choice_merge(&binding, quote! { rows })
            .expect_err("Tree has two columns");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("choice of `Tree` reads column 2 of a 2-column row"),
            "{error}"
        );
    }
}
