//! Relation-binding lowering: unions rule results with an earlier binding,
//! applies the planned operation, and records the runtime steps; then,
//! where the relation leaves its stratum, maps what it collapsed into by
//! the binding's flow into the relation's own layout.

use std::collections::HashMap;
use std::mem;

use flowlog_parser::Mutability;
use flowlog_planner::planner::RelationBinding;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;
use syn::LitStr;

use super::Operator;
use crate::Codegen;
use crate::CodegenError;
use crate::ty::diff::weight_tokens;

impl Codegen {
    /// Returns the bound relation's rows as `binding`'s collapse holds
    /// them: its operation applied to the union of `parts`, each a
    /// producer's rows with their weight, already in the collapse layout.
    /// The union lifts every part to the most mutable part's weight.
    /// Records the runtime steps under `output`, inside a loop when
    /// `recursive` is true.
    ///
    /// # Errors
    ///
    /// Returns an internal error for no parts, or for a lattice binding
    /// whose collapse has no recorded type or whose merge index is outside
    /// the row.
    pub(crate) fn gen_relation_binding(
        &self,
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

        match binding {
            RelationBinding::Dedup { .. } => {
                Ok(quote! { ::flowlog_runtime::operators::flowlog_dedup(#union) })
            }
            RelationBinding::Lattice { .. } => {
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
                self.gen_lattice_merge(binding, union)
            }
        }
    }

    /// Returns the statements binding `binding`'s relation, as it leaves
    /// its stratum, outside any loop: nothing when the binding holds it as
    /// its output; otherwise its flow lowered as any map from the collapsed
    /// rows into the output, and for a keyed output the arrangement,
    /// recorded in the program-wide outer-scope cache for the joins that
    /// read it. A mapped relation is registered with the flow's type, which
    /// every reader past this point sees. Either way an earlier binding's
    /// arrangement of the relation is dropped from that cache, as it no
    /// longer holds the relation's rows.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the collapse has no recorded type, or
    /// the flow does not produce the output's key and value columns.
    pub(crate) fn gen_binding_export(
        &mut self,
        binding: &RelationBinding,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<TokenStream, CodegenError> {
        let (collapse, output) = (binding.collapse(), binding.output());
        self.outer_fp_to_arrangement.remove(&output.fingerprint());
        if collapse.fingerprint() == output.fingerprint() {
            return Ok(TokenStream::new());
        }
        let name = self.display_name(output.fingerprint());
        let flow = binding.flow();
        let (keys, values) = output.arity();
        if flow.key().len() != keys || flow.value().len() != values {
            return Err(CodegenError::internal(format!(
                "relation `{name}` binds through `{flow}`, which does not produce its \
                 {keys} key and {values} value columns"
            )));
        }
        let local_fp_to_ident = HashMap::from([
            (
                collapse.fingerprint(),
                self.find_global_ident(collapse.fingerprint()),
            ),
            (
                output.fingerprint(),
                self.find_global_ident(output.fingerprint()),
            ),
        ]);
        let label = format!("Map: {name}");
        let operator = Operator {
            literal: LitStr::new(&label, Span::call_site()),
            arrange: (keys > 0).then(|| format!("Arrange: {output}")),
            name: label,
            recursive: false,
            si: self.config.str_intern_enabled(),
            local_fp_to_ident: &local_fp_to_ident,
        };
        let stmts = if keys > 0 {
            let mut outer_fp_to_arrangement = mem::take(&mut self.outer_fp_to_arrangement);
            let stmts = self.gen_row_to_kv(
                operator,
                collapse,
                output,
                flow,
                &mut outer_fp_to_arrangement,
                plan_graph,
            );
            self.outer_fp_to_arrangement = outer_fp_to_arrangement;
            stmts?
        } else {
            self.gen_row_to_row(operator, collapse, output, flow, plan_graph)?
        };
        self.record_output_type(collapse.fingerprint(), None, output.fingerprint(), flow)?;
        Ok(stmts)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use flowlog_common::compute_fp;
    use flowlog_parser::DataType;
    use flowlog_planner::planner::ArithmeticArgument;
    use flowlog_planner::planner::Collection;
    use flowlog_planner::planner::Constraints;
    use flowlog_planner::planner::FactorArgument;
    use flowlog_planner::planner::ProgramPlanner;
    use flowlog_planner::planner::Transformation;
    use flowlog_planner::planner::TransformationArgument;
    use flowlog_planner::planner::TransformationFlow;
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
        let codegen = codegen(PROGRAM);
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
        let codegen = codegen(LATTICE_PROGRAM);
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
        let codegen = codegen(
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

    /// As [`TWO_COLUMNS`], with `S` joining `R` with `T` on its first column.
    const JOINED: &str = ".decl R(a: int32, b: int64)\n.input R\n\
         .decl T(a: int32)\n.input T\n\
         .decl S(a: int32, b: int64)\nS(a, b) :- R(a, b), T(a).\n.output S\n";

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

    /// `S`'s binding in `plan`, rebuilt to collapse into its first rule
    /// result, typed `(i32, i64)`, and map that by `flow` into `output`, or
    /// its planned output; the planner builds no such binding yet.
    fn s_binding_with(
        codegen: &mut Codegen,
        plan: &ProgramPlanner,
        output: Option<Arc<Collection>>,
        flow: TransformationFlow,
    ) -> RelationBinding {
        let RelationBinding::Dedup {
            input_seed,
            input_contributors,
            output: planned,
            ..
        } = binding_of(plan, "s")
        else {
            panic!("S has no lattice column, so it dedups");
        };
        let collapse = Arc::clone(&input_contributors[0]);
        codegen.global_fp_to_type.insert(
            collapse.fingerprint(),
            (Vec::new(), vec![DataType::Int32, DataType::Int64]),
        );
        RelationBinding::Dedup {
            input_seed: input_seed.clone(),
            input_contributors: input_contributors.clone(),
            collapse,
            flow,
            output: output.unwrap_or_else(|| Arc::clone(planned)),
        }
    }

    /// `S`'s binding with its columns swapped on the way out.
    fn swapped_s(codegen: &mut Codegen) -> RelationBinding {
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let swap = projection(Vec::new(), vec![column(1), column(0)]);
        s_binding_with(codegen, &plan, None, swap)
    }

    /// A binding that holds its relation as its output leaves nothing to
    /// map.
    #[test]
    fn a_binding_held_as_its_output_exports_nothing() {
        let mut codegen = codegen(TWO_COLUMNS);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let stmts = codegen
            .gen_binding_export(binding_of(&plan, "s"), &mut None)
            .unwrap();
        assert!(stmts.is_empty(), "{stmts}");
    }

    /// The relation's rows change at its binding, so a later join must
    /// not read an arrangement an earlier binding left.
    #[test]
    fn an_export_drops_the_relations_earlier_arrangement() {
        let mut codegen = codegen(TWO_COLUMNS);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        codegen
            .outer_fp_to_arrangement
            .insert(compute_fp("s"), ident("earlier_arr"));
        codegen
            .gen_binding_export(binding_of(&plan, "s"), &mut None)
            .unwrap();
        assert!(codegen.outer_fp_to_arrangement.is_empty());
    }

    /// A reordering flow maps the collapsed rows into the relation's
    /// layout, with no arrangement for a relation held as rows.
    #[test]
    fn a_reordering_flow_maps_the_collapsed_rows_into_the_relations_layout() {
        let mut codegen = codegen(TWO_COLUMNS);
        let binding = swapped_s(&mut codegen);
        let stmts = codegen.gen_binding_export(&binding, &mut None).unwrap();
        let expected = quote! {
            let rel_1_s = ::flowlog_runtime::operators::flowlog_map(
                t_8252008281719007985.clone(),
                "Map: S",
                |(x0, x1): (i32, i64), t, d| { std::iter::once(((x1, x0), t, d)) },
            );
        };
        assert_eq!(stmts.to_string(), expected.to_string());
        assert!(codegen.outer_fp_to_arrangement.is_empty());
    }

    /// Past the export the relation is typed by the flow over its
    /// collapsed rows, not by its declaration.
    #[test]
    fn an_export_retypes_the_relation_by_its_flow() {
        let mut codegen = codegen(TWO_COLUMNS);
        let binding = swapped_s(&mut codegen);
        codegen.gen_binding_export(&binding, &mut None).unwrap();
        assert_eq!(
            codegen.global_fp_to_type[&compute_fp("s")],
            (Vec::new(), vec![DataType::Int64, DataType::Int32])
        );
    }

    /// A keyed relation is the flow's keyed rows and their arrangement,
    /// which the joins reading the relation find in the outer-scope cache
    /// under its fingerprint.
    #[test]
    fn a_keyed_flow_arranges_the_relation() {
        let mut codegen = codegen(JOINED);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        // The planner keys no relation yet, so the export borrows the keyed
        // collection `R` is arranged as for the join, whose name labels the
        // map.
        let keyed = plan
            .strata()
            .iter()
            .flat_map(|stratum| stratum.non_recursive_transformations())
            .map(Transformation::output)
            .find(|output| output.arity() == (1, 1))
            .map(Arc::clone)
            .expect("R is arranged by its first column for the join");
        let key_first = projection(vec![column(0)], vec![column(1)]);
        let binding = s_binding_with(&mut codegen, &plan, Some(Arc::clone(&keyed)), key_first);
        let stmts = codegen.gen_binding_export(&binding, &mut None).unwrap();
        let expected = quote! {
            let t_16465958648337079827 = ::flowlog_runtime::operators::flowlog_map(
                t_12756016818741617700.clone(),
                "Map: t_16465958648337079827",
                |(x0, x1): (i32, i64), t, d| { std::iter::once((((x0,), (x1,)), t, d)) },
            );
            let t_16465958648337079827_arr = ::flowlog_runtime::operators::flowlog_arrange(
                t_16465958648337079827.clone(),
                "Arrange: r [0xe482d62678671e13], key:(0.0), value:(0.1)"
            );
        };
        assert_eq!(stmts.to_string(), expected.to_string());
        assert_eq!(
            codegen.outer_fp_to_arrangement[&keyed.fingerprint()],
            ident("t_16465958648337079827_arr")
        );
    }

    /// A flow must produce the relation's key and value columns; one that
    /// keys a relation held as rows is a planner bug.
    #[test]
    fn a_flow_off_the_relations_shape_is_an_internal_error() {
        let mut codegen = codegen(TWO_COLUMNS);
        let plan = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let key_first = projection(vec![column(0)], vec![column(1)]);
        let binding = s_binding_with(&mut codegen, &plan, None, key_first);
        let error = codegen
            .gen_binding_export(&binding, &mut None)
            .expect_err("S is held as rows");
        assert!(matches!(&error, CodegenError::Internal(_)));
        assert!(
            error
                .to_string()
                .contains("which does not produce its 0 key and 2 value columns"),
            "{error}"
        );
    }

    /// A relation with no part has nothing to union: a planner bug, not an
    /// empty collection.
    #[test]
    fn a_relation_with_no_part_is_an_internal_error() {
        let codegen = codegen(PROGRAM);
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
