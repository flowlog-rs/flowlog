//! The scope an incremental engine nests at `time::Epoch`: the static
//! bindings it enters, the non-static inputs, the non-static part of every
//! stratum, then the non-static outputs' inspectors.

use std::collections::HashSet;
use std::iter;

use flowlog_planner::planner::StratumPlanner;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::try_with_plan_graph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Codegen;
use crate::CodegenError;
use crate::io::input::Input;
use crate::tuple_tokens;
use crate::ty::time::Clock;

impl Codegen {
    /// Returns the scope an incremental engine nests at `time::Epoch`, as
    /// the statement `let (<handles>, probe) = scope.scoped(..);`, and the
    /// handles it binds. The scope enters the static bindings it reads,
    /// then holds the non-static inputs, the non-static part of every
    /// stratum of `strata`, and the non-static outputs' inspectors, which
    /// `probe` probes. Every operator of them is addressed inside it.
    ///
    /// Requires every static part generated: the scope reads them.
    pub(super) fn gen_epoch_scope(
        &mut self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<(TokenStream, Vec<Ident>), CodegenError> {
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.enter_scope();
        });
        let enters = self.gen_epoch_enters(strata, plan_graph);
        let Input {
            declarations: inputs,
            handles,
        } = self.gen_input(Clock::Epoch, plan_graph);
        let strata = self.gen_non_static_strata(strata, plan_graph)?;
        let inspectors = self.gen_inspectors(Clock::Epoch, plan_graph)?;
        try_with_plan_graph(plan_graph, |plan_graph| plan_graph.leave_scope())
            .map_err(|e| CodegenError::internal(format!("recording `Epoch` scope exit: {e}")))?;

        // The closure returns the tuple its caller binds, both spelled the
        // same. The probe names its time, as the scope may have no output
        // to infer it from.
        let epoch = Clock::Epoch.time_tokens();
        let returned = tuple_tokens(
            handles
                .iter()
                .map(|handle| quote! { #handle })
                .chain(iter::once(quote! { probe })),
        );
        let scoped = quote! {
            let #returned = scope.scoped::<#epoch, _, _>("Incremental", |scope| {
                #(#enters)*
                #inputs
                #(#strata)*
                let probe = ProbeHandle::<#epoch>::new();
                #(#inspectors)*
                #returned
            });
        };
        Ok((scoped, handles))
    }

    /// Returns `let b = b.enter(scope);` for each binding of the `Once`
    /// scope that the non-static part of `strata` reads, once each, in the
    /// order of its first reader: the top of an incremental engine's
    /// `Epoch` scope, where the entered binding then shadows the static one
    /// for every reader. An entered arrangement shares the static one's
    /// trace, so the static part is computed and arranged once, however
    /// many epochs read it.
    ///
    /// Requires every static part generated: an arrangement enters under
    /// the name the outer cache holds for it.
    fn gen_epoch_enters(
        &self,
        strata: &[StratumPlanner],
        plan_graph: &mut Option<PlanGraph>,
    ) -> Vec<TokenStream> {
        // A vector keeps the order of first readers; the set tells a
        // binding already entered without scanning it.
        let mut bindings: Vec<Ident> = Vec::new();
        let mut entered: HashSet<Ident> = HashSet::new();
        let entering = strata
            .iter()
            .flat_map(StratumPlanner::non_static_enter_collections);
        for collection in entering {
            let binding = self.find_entering_ident(collection.fingerprint());
            if entered.insert(binding.clone()) {
                bindings.push(binding);
            }
        }
        // The entered bindings are what the scope takes in from outside, so
        // their nodes lead its input block.
        with_plan_graph(plan_graph, |plan_graph| {
            plan_graph.update_input_block();
        });
        let mut enters = Vec::with_capacity(bindings.len());
        for binding in bindings {
            with_plan_graph(plan_graph, |plan_graph| {
                plan_graph.enter_operator(binding.to_string(), binding.to_string());
            });
            enters.push(quote! { let #binding = #binding.enter(scope); });
        }
        enters
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use flowlog_planner::planner::ProgramPlanner;
    use proc_macro2::Delimiter;
    use proc_macro2::TokenStream;
    use proc_macro2::TokenTree;
    use quote::quote;

    use crate::test_harness::codegen;

    /// Static links and their closure, which two rules join with mutable
    /// edges.
    const MIXED_PROGRAM: &str = "
        .decl Link(x: int32, y: int32)
        .input Link
        .decl Path(x: int32, y: int32)
        Path(x, y) :- Link(x, y).
        Path(x, z) :- Path(x, y), Link(y, z).
        .output Path
        .decl Edge(x: int32, y: int32) mutable
        .input Edge
        .decl Hop(x: int32, z: int32)
        Hop(x, z) :- Edge(x, y), Path(y, z).
        .output Hop
        .decl Back(x: int32, z: int32)
        Back(x, z) :- Edge(y, x), Path(y, z).
        .output Back
    ";

    /// An incremental engine computes its static part once, in the
    /// dataflow's own scope: the links, and their closure, which loops at
    /// `OnceLoop`. It nests the non-static part in a scope at `Epoch`,
    /// which each static binding read there enters once: the closure's
    /// arrangement, though both rules join it. The closure's output never
    /// enters: the dataflow's own scope inspects it after the `Epoch` one,
    /// probed by `once_probe`, and the `Epoch` scope the rest, by `probe`.
    #[test]
    fn an_incremental_engine_computes_its_static_part_once() {
        let mut codegen = codegen(MIXED_PROGRAM);
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut None)
            .expect("generates")
            .dataflow
            .to_string();
        let (once, rest) = dataflow
            .split_once("\"Incremental\"")
            .expect("an incremental engine nests a scope");
        let (epoch, once_outputs) = rest
            .split_once("let once_probe")
            .expect("and probes its static outputs after it");
        let path = quote! { let rel_1_path = rel_1_path.enter(scope); }.to_string();
        let probe = quote! { .probe_with(&probe) }.to_string();
        let once_probe = quote! { .probe_with(&once_probe) }.to_string();

        assert!(once.contains("diff :: Static >"), "{once}");
        assert!(once.contains("time :: OnceLoop"), "{once}");
        assert!(!once.contains("diff :: Mutable"), "{once}");
        assert!(epoch.contains("diff :: Mutable >"), "{epoch}");
        assert!(!epoch.contains("time :: OnceLoop"), "{epoch}");
        assert_eq!(epoch.matches(". enter (scope)").count(), 1, "{epoch}");
        assert!(!epoch.contains(&path), "{epoch}");
        assert!(epoch.contains(&probe), "{epoch}");
        assert!(!epoch.contains(&once_probe), "{epoch}");
        assert!(once_outputs.contains(&once_probe), "{once_outputs}");
        assert!(!once_outputs.contains(&probe), "{once_outputs}");
    }

    /// [`MIXED_PROGRAM`]'s closure read twice more at `Epoch`: by a loop,
    /// through the arrangement its joins read, and by a union, as rows.
    const LOOP_AND_UNION: &str = "
        .decl Reach(x: int32, y: int32)
        Reach(x, y) :- Edge(x, y).
        Reach(x, z) :- Reach(x, y), Path(y, z).
        .output Reach
        .decl Both(x: int32, y: int32)
        Both(x, y) :- Path(x, y).
        Both(x, y) :- Edge(x, y).
        .output Both
    ";

    /// Returns the body of the closure that `tokens` run in the scope
    /// nested at `Epoch`, if they nest one.
    fn epoch_body(tokens: TokenStream) -> Option<TokenStream> {
        let mut trees = tokens.into_iter();
        while let Some(tree) = trees.next() {
            match tree {
                TokenTree::Literal(name) if name.to_string() == "\"Incremental\"" => {
                    return trees.find_map(|tree| match tree {
                        TokenTree::Group(body) if body.delimiter() == Delimiter::Brace => {
                            Some(body.stream())
                        }
                        _ => None,
                    });
                }
                TokenTree::Group(group) => {
                    if let Some(body) = epoch_body(group.stream()) {
                        return Some(body);
                    }
                }
                _ => {}
            }
        }
        None
    }

    /// The `Epoch` scope opens by entering each static binding it reads,
    /// once, in a statement of its own body, so the entered binding shadows
    /// the static one for every reader there: two joins and a loop share
    /// the closure's arrangement, and a union enters rows.
    #[test]
    fn the_epoch_scope_opens_by_entering_each_static_binding_once() {
        let mut codegen = codegen(&format!("{MIXED_PROGRAM}{LOOP_AND_UNION}"));
        let planner = ProgramPlanner::from_program(&codegen.program, &mut None).expect("plans");
        let dataflow = codegen
            .generate(&planner, &mut None)
            .expect("generates")
            .dataflow;
        let body: Vec<TokenTree> = epoch_body(dataflow)
            .expect("an incremental engine nests a scope")
            .into_iter()
            .collect();

        let mut entered = HashSet::new();
        let mut opened = true;
        let statements =
            body.split(|tree| matches!(tree, TokenTree::Punct(punct) if punct.as_char() == ';'));
        for statement in statements {
            let statement = statement
                .iter()
                .cloned()
                .collect::<TokenStream>()
                .to_string();
            match statement
                .strip_prefix("let ")
                .and_then(|statement| statement.split_once(" = "))
            {
                Some((binding, read)) if read == format!("{binding} . enter (scope)") => {
                    assert!(opened, "{binding} enters after the scope's own work");
                    assert!(
                        entered.insert(binding.to_string()),
                        "{binding} enters twice"
                    );
                }
                _ => {
                    opened = false;
                    assert!(!statement.contains(". enter (scope)"), "{statement}");
                }
            }
        }
        assert!(entered.iter().any(|b| b.ends_with("_arr")), "{entered:?}");
        assert!(entered.iter().any(|b| !b.ends_with("_arr")), "{entered:?}");
    }
}
