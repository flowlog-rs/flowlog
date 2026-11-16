//! Relation-binding lowering: unions rule results with an earlier binding,
//! applies the planned collapse and flow, and records the runtime steps.

use flowlog_parser::Mutability;
use flowlog_planner::planner::RelationBinding;
use flowlog_planner::planner::TransformationFlow;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use super::map::flat_map_body_tokens;
use super::map::is_identity_row_projection;
use crate::Codegen;
use crate::CodegenError;
use crate::expr::param::row_params;
use crate::internal_tuple_tokens;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the bound relation's rows: `binding`'s collapse applied to
    /// the union of `parts`, each a producer's rows with their weight in
    /// the collapse layout, then mapped by `binding`'s flow. The union
    /// lifts every part to the most mutable part's weight. Records the
    /// runtime steps under `output`, inside a loop when `recursive` is
    /// true.
    ///
    /// # Errors
    ///
    /// Returns an internal error for no parts, a lattice merge index
    /// outside the row, or a flow [`Self::gen_binding_flow`] rejects.
    pub(crate) fn gen_relation_binding(
        &mut self,
        parts: &[(Ident, Mutability)],
        binding: &RelationBinding,
        output: &Ident,
        recursive: bool,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let idb_fp = binding.output().fingerprint();
        let name = self.display_name(idb_fp);
        let Some((first, rest)) = parts.split_first() else {
            return Err(CodegenError::internal(format!(
                "relation `{name}` has no collections to union"
            )));
        };
        let dedup = match binding {
            RelationBinding::Dedup { .. } => true,
            RelationBinding::Lattice { .. } => false,
        };
        let mutability = rest
            .iter()
            .map(|(_, part)| *part)
            .fold(first.1, Mutability::max);
        // The lift names its target weight before concatenation or dedup
        // would otherwise fix it by type inference.
        let lift_name = format!("Lift: {name}");
        let weight = weight_tokens(mutability);
        let input_rows = |(collection, part): &(Ident, Mutability)| {
            let mut rows = quote! { #collection.clone() };
            if *part < mutability {
                rows = quote! {
                    ::flowlog_runtime::operators::flowlog_lift::<#weight, _, _, _>(
                        #rows, #lift_name,
                    )
                };
            }
            rows
        };
        let first_rows = input_rows(first);
        let tail: Vec<TokenStream> = rest.iter().map(input_rows).collect();
        let union = if tail.is_empty() {
            first_rows
        } else {
            quote! { #first_rows.concatenate([ #( #tail ),* ]) }
        };

        with_plan_graph(plan_graph, |plan_graph| {
            let lifts: u32 = parts
                .iter()
                .map(|(_, part)| u32::from(*part < mutability))
                .sum();
            plan_graph.union_operator(
                name.to_string(),
                parts.iter().map(|(id, _)| id.to_string()).collect(),
                output.to_string(),
                lifts,
                u32::from(!tail.is_empty()),
                dedup.then_some((mutability, recursive)),
            );
        });

        let collapsed = match binding {
            RelationBinding::Dedup { .. } => {
                quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) }
            }
            RelationBinding::Lattice {
                merge_index,
                lattice,
                ..
            } => {
                // The union feeding the merge is recorded under the output's
                // name too, so the merge reads from it.
                with_plan_graph(plan_graph, |plan_graph| {
                    plan_graph.lattice_operator(
                        name.to_string(),
                        output.to_string(),
                        output.to_string(),
                        idb_fp,
                        mutability,
                    );
                });
                self.gen_lattice_merge(idb_fp, union, *merge_index, *lattice)?
            }
        };
        self.gen_binding_flow(binding, collapsed, output, plan_graph)
    }

    /// Returns `collapsed` mapped by `binding`'s flow into its output:
    /// unchanged for the identity, else a row map recorded under `output`,
    /// whose types it registers as the relation's.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the binding has no input, or its flow
    /// is not a projection into rows: a keyed output would need its
    /// arrangement registered for the loop's feedback and leave, which no
    /// plan asks for yet.
    fn gen_binding_flow(
        &mut self,
        binding: &RelationBinding,
        collapsed: TokenStream,
        output: &Ident,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let idb_fp = binding.output().fingerprint();
        let name = self.display_name(idb_fp);
        let flow = binding.flow();
        let (key, value, constraints, compares) = match flow {
            TransformationFlow::KVToKV {
                key,
                value,
                constraints,
                compares,
            } if key.is_empty() && constraints.is_empty() && compares.is_empty() => {
                (key, value, constraints, compares)
            }
            _ => {
                return Err(CodegenError::internal(format!(
                    "relation `{name}` binds through {flow:?}, where a projection into rows \
                     is expected"
                )));
            }
        };
        // Every input holds the collapse layout, so any one types the
        // collapsed row.
        let collapse = binding
            .input_seed()
            .or_else(|| binding.input_contributors().first().map(AsRef::as_ref))
            .ok_or_else(|| {
                CodegenError::internal(format!("relation `{name}` has no input to bind"))
            })?;
        let arity = collapse.arity().1;
        if is_identity_row_projection(value, arity) {
            return Ok(collapsed);
        }

        let si = self.config.str_intern_enabled();
        let input_type = self.find_global_type(collapse.fingerprint())?.clone();
        let row_ty = internal_tuple_tokens(&input_type.1, si);
        let (row_pat, row_fields) = row_params(arity, key, value, compares, constraints);
        let out_val = self.row_projection(value, &row_fields, si, &input_type)?;
        self.record_output_type(collapse.fingerprint(), None, idb_fp, flow)?;
        let literal = format!("Map: {name}");
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.map_join_operator(
                format!("{name}: map"),
                vec![output.to_string()],
                output.to_string(),
                idb_fp,
            );
        });
        let body = flat_map_body_tokens(None, out_val);
        Ok(quote! {
            ::flowlog_runtime::operators::flowlog_map(
                #collapsed,
                #literal,
                |#row_pat: #row_ty, t, d| { #body },
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ArithmeticArgument;
    use flowlog_planner::planner::Constraints;
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_planner::planner::TransformationArgument;
    use proc_macro2::Span;
    use rstest::rstest;

    use super::*;
    use crate::test_harness::codegen;

    /// A static input `R` and a static output `S` derived from it.
    const PROGRAM: &str =
        ".decl R(a: int32)\n.input R\n.decl S(a: int32)\nS(a) :- R(a).\n.output S\n";

    /// As [`PROGRAM`], with `S` a lattice relation.
    const LATTICE_PROGRAM: &str = ".decl R(a: int32, b: int32)\n.input R\n\
        .decl S(a: int32, b: min<int32>)\nS(a, b) :- R(a, b).\n.output S\n";

    fn ident(name: &str) -> Ident {
        Ident::new(name, Span::call_site())
    }

    fn part(name: &str, mutability: Mutability) -> (Ident, Mutability) {
        (ident(name), mutability)
    }

    /// The binding `plan` gives relation `name`.
    fn binding_of<'p>(plan: &'p ProgramPlanner, name: &str) -> &'p RelationBinding {
        plan.strata()
            .iter()
            .find_map(|stratum| stratum.relation_bindings().get(&compute_fp(name)))
            .expect("the relation is bound")
    }

    /// `S` unions its parts in order at its most mutable part's weight,
    /// lifting only the parts below it.
    // Cases: parts with their weight, expected.
    #[rstest]
    #[case::one_static_part(
        vec![("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone()) },
    )]
    #[case::static_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(t_1.clone().concatenate([t_2.clone()])) },
    )]
    #[case::mixed_parts(
        vec![("t_1", Mutability::Static), ("t_2", Mutability::Mutable)],
        quote! {
            ::flowlog_runtime::operators::flowlog_dedup(
                ::flowlog_runtime::operators::flowlog_lift::<::flowlog_runtime::diff::Mutable, _, _, _>(
                    t_1.clone(), "Lift: S",
                )
                .concatenate([t_2.clone()])
            )
        },
    )]
    #[case::earlier_binding_first(
        vec![("earlier", Mutability::Static), ("t_1", Mutability::Static)],
        quote! { ::flowlog_runtime::operators::flowlog_dedup(earlier.clone().concatenate([t_1.clone()])) },
    )]
    fn a_relation_lifts_its_parts_to_the_most_mutable_weight(
        #[case] parts: Vec<(&str, Mutability)>,
        #[case] expected: TokenStream,
    ) {
        let mut codegen = codegen(PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts: Vec<(Ident, Mutability)> = parts.into_iter().map(|(p, m)| part(p, m)).collect();
        let deduped = codegen
            .gen_relation_binding(
                &parts,
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        assert_eq!(deduped.to_string(), expected.to_string());
    }

    const THREE_COLUMNS: &str = ".decl R(a: int32, b: symbol, c: int64)\n.input R\n\
         .decl S(a: int32, b: symbol, c: int64)\nS(a,b,c) :- R(a,b,c).\n.output S\n";

    // Cases: second input weight, recursive, union range, next address.
    #[rstest]
    #[case::same_weight(
        Mutability::Static, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4]],
        vec![0, 5],
    )]
    #[case::lift(
        Mutability::Mutable, false,
        vec![vec![0, 1], vec![0, 2], vec![0, 3], vec![0, 4], vec![0, 5]],
        vec![0, 6],
    )]
    #[case::recursive_lift(
        Mutability::Mutable, true,
        vec![vec![0, 1, 1], vec![0, 1, 2], vec![0, 1, 3], vec![0, 1, 4],
             vec![0, 1, 5], vec![0, 1, 6]],
        vec![0, 1, 7],
    )]
    fn profiled_binding_ranges_include_only_the_emitted_operators(
        #[case] right_weight: Mutability,
        #[case] recursive: bool,
        #[case] expected_range: Vec<Vec<u32>>,
        #[case] expected_next: Vec<u32>,
    ) {
        let mut codegen = codegen(THREE_COLUMNS);
        codegen.config.profile = true;
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts = [
            part("left", Mutability::Static),
            part("right", right_weight),
        ];
        let mut graph = PlanGraph::new(false);
        graph.enter_scope();
        if recursive {
            graph.enter_scope();
        }
        let mut graph = Some(graph);
        let rows = codegen
            .gen_relation_binding(&parts, binding, &ident("out"), recursive, &mut graph)
            .unwrap();
        syn::parse2::<syn::Expr>(rows).unwrap();
        let graph = graph.as_mut().unwrap();
        graph.map_join_operator(
            "following".into(),
            vec!["out".into()],
            "after".into(),
            0x123,
        );
        let nodes = graph.nodes();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].name, "S: concat & dedup");
        let range: Vec<Vec<u32>> = nodes[0]
            .operators
            .iter()
            .map(|address| address.0.clone())
            .collect();
        assert_eq!(range, expected_range);
        assert_eq!(nodes[1].operators.len(), 1);
        assert_eq!(nodes[1].operators[0].0, expected_next);
        assert_eq!(nodes[1].parents, [nodes[0].id]);
    }

    #[test]
    fn a_lattice_binding_merges_without_a_whole_row_dedup() {
        let mut codegen = codegen(LATTICE_PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let parts = [
            part("t_1", Mutability::Static),
            part("t_2", Mutability::Static),
        ];
        let rows = codegen
            .gen_relation_binding(
                &parts,
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect("parts to union");
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                t_1.clone().concatenate([t_2.clone()]),
                "Merge: S",
                ::flowlog_runtime::operators::Min,
                |(x0, x1)| ((x0,), x1),
                |(k0,), v: i32| (k0, v),
            )
        };
        assert_eq!(rows.to_string(), expected.to_string());
    }

    #[test]
    fn a_lattice_binding_merges_its_merge_index_in_place() {
        let mut codegen = codegen(
            r#"
            .decl E(a: int32, b: int32, c: int32)
            .input E
            .decl R(a: int32, b: min<int32>, c: int32)
            .output R
            R(a,b,c) :- E(a,b,c).
        "#,
        );
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "r");
        let rows = codegen
            .gen_relation_binding(
                &[part("rows", Mutability::Static)],
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .unwrap();
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_lattice(
                rows.clone(), "Merge: R", ::flowlog_runtime::operators::Min,
                |(x0, x1, x2)| ((x0, x2), x1),
                |(k0, k1), v: i32| (k0, v, k1),
            )
        };
        assert_eq!(rows.to_string(), expected.to_string());
    }

    /// A static input `R` and a static output `S` of two columns.
    const TWO_COLUMNS: &str = ".decl R(a: int32, b: int64)\n.input R\n\
         .decl S(a: int32, b: int64)\nS(a, b) :- R(a, b).\n.output S\n";

    /// Column `index` of the collapsed row.
    fn column(index: usize) -> ArithmeticArgument {
        ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, index))),
            rest: Vec::new(),
        }
    }

    /// The projection of the collapsed row onto `key` and `value`.
    fn projection(
        key: Vec<ArithmeticArgument>,
        value: Vec<ArithmeticArgument>,
    ) -> TransformationFlow {
        TransformationFlow::KVToKV {
            key: Arc::new(key),
            value: Arc::new(value),
            constraints: Constraints::new(Vec::new(), Vec::new()),
            compares: Vec::new(),
        }
    }

    /// `S`'s binding in `plan`, rebuilt to map its collapsed row by `flow`;
    /// the planner builds only the identity so far.
    fn s_binding_with(plan: &ProgramPlanner, flow: TransformationFlow) -> RelationBinding {
        let RelationBinding::Dedup {
            input_seed,
            input_contributors,
            output,
            ..
        } = binding_of(plan, "s")
        else {
            panic!("S has no lattice column, so it dedups");
        };
        RelationBinding::Dedup {
            input_seed: input_seed.clone(),
            input_contributors: input_contributors.clone(),
            flow,
            output: Arc::clone(output),
        }
    }

    /// A flow that reorders the collapsed row maps it after the dedup, and
    /// the relation is then held, and typed, in the flow's order.
    #[test]
    fn a_reordering_flow_maps_the_collapsed_rows_into_the_relations_order() {
        let mut codegen = codegen(TWO_COLUMNS);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = s_binding_with(&plan, projection(Vec::new(), vec![column(1), column(0)]));
        let head = binding.input_contributors()[0].fingerprint();
        codegen
            .global_fp_to_type
            .insert(head, (Vec::new(), vec![DataType::Int32, DataType::Int64]));
        let rows = codegen
            .gen_relation_binding(
                &[part("rows", Mutability::Static)],
                &binding,
                &ident("s"),
                /*recursive*/ false,
                &mut None,
            )
            .unwrap();
        let expected = quote! {
            ::flowlog_runtime::operators::flowlog_map(
                ::flowlog_runtime::operators::flowlog_dedup(rows.clone()),
                "Map: S",
                |(x0, x1): (i32, i64), t, d| { std::iter::once(((x1, x0), t, d)) },
            )
        };
        assert_eq!(rows.to_string(), expected.to_string());
        assert_eq!(
            codegen.global_fp_to_type[&compute_fp("s")],
            (Vec::new(), vec![DataType::Int64, DataType::Int32])
        );
    }

    /// A keyed output would need an arrangement the loop's feedback and
    /// leave could read, which no plan builds yet.
    #[test]
    fn a_flow_into_a_keyed_output_is_an_internal_error() {
        let mut codegen = codegen(TWO_COLUMNS);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = s_binding_with(&plan, projection(vec![column(0)], vec![column(1)]));
        let error = codegen
            .gen_relation_binding(
                &[part("rows", Mutability::Static)],
                &binding,
                &ident("s"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("a keyed output is not lowered");
        assert!(matches!(error, CodegenError::Internal(_)), "{error}");
    }

    /// A relation with no part has nothing to union: a planner bug, not an
    /// empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let mut codegen = codegen(PROGRAM);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let binding = binding_of(&plan, "s");
        let error = codegen
            .gen_relation_binding(
                &[],
                binding,
                &ident("r"),
                /*recursive*/ false,
                &mut None,
            )
            .expect_err("nothing to union");
        assert!(matches!(error, CodegenError::Internal(_)));
    }
}
