//! Semantic validation: the final pipeline stage, rejecting rules and
//! declarations that parse and type-check but are semantically broken. Each
//! check's rule is documented on the [`ParseError`] variant it raises:
//!
//! - an aggregate's braces read a relation or a variable of the rule;
//! - every variable an aggregate reads is bound: one it shares with the
//!   rest of the rule by a body around it, a local one by its own body,
//!   and the target reads at least one local;
//! - every head variable is bound;
//! - every variable of a negated atom or a comparison is bound (range
//!   restriction);
//! - only an EDB declares a mutability.
//!
//! Covers every rule, loop-block rules included. The variable checks rely
//! on `assemble`'s substitution pass having already eliminated `x = expr`
//! assignments: after it, positive body atoms and aggregates are the only
//! binders left.

use std::collections::HashSet;

use crate::ast::FlowLogRule;
use crate::ast::Predicate;
use crate::ast::Variable;
use crate::error::ParseError;
use crate::error::UnsafePredicateKind;
use crate::program::Program;

/// Rejects semantically broken rules and declarations; see the module docs
/// for the checks.
pub(crate) fn validate(program: &Program) -> Result<(), ParseError> {
    for rule in program.rules() {
        // Aggregates first: a head variable bound only inside one is better
        // explained as a shared variable than as an unbound one.
        let head_vars: HashSet<&Variable> = rule
            .head()
            .head_arguments()
            .iter()
            .flat_map(|arg| arg.vars())
            .collect();
        check_aggregate_vars_bound(rule, rule.rhs(), &head_vars, &rule.bound_vars())?;
        check_head_vars_bound(rule)?;
        check_body_vars_bound(rule)?;
    }
    check_mutability_on_edbs_only(program)?;
    Ok(())
}

/// Checks each aggregate in `body`, nested ones included: its braces read
/// a relation or a variable, and its variables are bound. One the
/// aggregate shares with the rest of the rule is fixed outside it, so a
/// body around it must bind it: the rule's or an enclosing aggregate's.
/// One local to the aggregate must be bound by its own body. The target
/// must read a local variable, else it has one value per group.
///
/// `outside_body` holds the variables the rule mentions outside `body`, and
/// `enclosing` those the bodies around the aggregates bind, `body` included.
fn check_aggregate_vars_bound(
    rule: &FlowLogRule,
    body: &[Predicate],
    outside_body: &HashSet<&Variable>,
    enclosing: &HashSet<&Variable>,
) -> Result<(), ParseError> {
    for (i, predicate) in body.iter().enumerate() {
        let Predicate::Aggregate(aggregate) = predicate else {
            continue;
        };
        if aggregate.body_atoms().is_empty() && aggregate.vars().is_empty() {
            return Err(ParseError::EmptyAggregateBraces {
                span: aggregate.span(),
                rule_span: rule.span(),
            });
        }
        let mut outside = outside_body.clone();
        for (j, other) in body.iter().enumerate() {
            if j == i {
                continue;
            }
            predicate_vars(other, &mut outside);
            if let Predicate::Aggregate(other) = other {
                outside.extend(other.var());
            }
        }
        // This aggregate's own variables: a nested aggregate's are checked
        // with it.
        let mut inside = HashSet::new();
        if let Some(target) = aggregate.target() {
            inside.extend(target.vars());
        }
        for inner in aggregate.body() {
            if !matches!(inner, Predicate::Aggregate(_)) {
                predicate_vars(inner, &mut inside);
            }
        }
        let own = aggregate.bound_vars();

        // Sorted so the variable reported does not depend on hash order.
        let mut inside: Vec<&Variable> = inside.into_iter().collect();
        inside.sort_unstable();
        for var in inside {
            if outside.contains(var) {
                if !enclosing.contains(var) {
                    return Err(ParseError::UngroundedAggregateVariable {
                        span: aggregate.span(),
                        rule_span: rule.span(),
                        var: var.clone(),
                    });
                }
            } else if !own.contains(var) {
                return Err(ParseError::UnboundAggregateVariable {
                    span: aggregate.span(),
                    rule_span: rule.span(),
                    var: var.clone(),
                });
            }
        }

        if let Some(target) = aggregate.target() {
            let vars = target.vars();
            if !vars.is_empty() && vars.iter().all(|v| enclosing.contains(*v)) {
                return Err(ParseError::FixedAggregateTarget {
                    span: aggregate.span(),
                    rule_span: rule.span(),
                    target: target.to_string(),
                });
            }
        }

        // The target sits outside the nested aggregates' braces, so a
        // variable it reads is one they share.
        if let Some(target) = aggregate.target() {
            outside.extend(target.vars());
        }
        let mut nested_enclosing = enclosing.clone();
        nested_enclosing.extend(own);
        check_aggregate_vars_bound(rule, aggregate.body(), &outside, &nested_enclosing)?;
    }
    Ok(())
}

/// Adds every variable `predicate` mentions to `out`; for an aggregate, its
/// target's and body's, not the variable it binds.
fn predicate_vars<'a>(predicate: &'a Predicate, out: &mut HashSet<&'a Variable>) {
    match predicate {
        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => out.extend(atom.vars()),
        Predicate::Compare(cmp) => out.extend(cmp.vars_set()),
        Predicate::Aggregate(aggregate) => out.extend(aggregate.vars()),
    }
}

/// Checks every head variable against the rule's grounded set
/// ([`FlowLogRule::bound_vars`]).
fn check_head_vars_bound(rule: &FlowLogRule) -> Result<(), ParseError> {
    let bound = rule.bound_vars();
    for arg in rule.head().head_arguments() {
        for var in arg.vars() {
            if !bound.contains(var) {
                return Err(ParseError::UnknownHeadVariable {
                    head_span: rule.head().span(),
                    rule_span: rule.span(),
                    var: var.clone(),
                });
            }
        }
    }
    Ok(())
}

/// Checks every variable of a negated atom or a comparison against the
/// rule's grounded set ([`FlowLogRule::bound_vars`]), reporting the first
/// unbound one in source order: Datalog's range restriction. Variables inside
/// an aggregate are [`check_aggregate_vars_bound`]'s.
fn check_body_vars_bound(rule: &FlowLogRule) -> Result<(), ParseError> {
    let bound = rule.bound_vars();
    for predicate in rule.rhs() {
        match predicate {
            Predicate::PositiveAtom(_) | Predicate::Aggregate(_) => {}
            Predicate::NegativeAtom(atom) => {
                if let Some(var) = atom.vars().find(|v| !bound.contains(v)) {
                    return Err(ParseError::UnsafeVariable {
                        kind: UnsafePredicateKind::Negation,
                        predicate: format!("!{atom}"),
                        predicate_span: atom.span(),
                        rule_span: rule.span(),
                        var: var.clone(),
                    });
                }
            }
            Predicate::Compare(cmp) => {
                let mut vars = cmp.left().vars().into_iter().chain(cmp.right().vars());
                if let Some(var) = vars.find(|v| !bound.contains(*v)) {
                    return Err(ParseError::UnsafeVariable {
                        kind: UnsafePredicateKind::Comparison,
                        predicate: cmp.to_string(),
                        predicate_span: cmp.span(),
                        rule_span: rule.span(),
                        var: var.clone(),
                    });
                }
            }
        }
    }
    Ok(())
}

/// Rejects a declared mutability on a relation that is not an EDB. Runs
/// after prune, so an orphan it materialized counts as an EDB.
fn check_mutability_on_edbs_only(program: &Program) -> Result<(), ParseError> {
    for rel in program.relations() {
        if let Some(mutability) = rel.mutability()
            && !program.is_edb_relation(rel)
        {
            return Err(ParseError::MutabilityOnDerivedRelation {
                span: rel.span(),
                name: rel.raw_name().to_string(),
                mutability,
            });
        }
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::assert_err;
    use crate::test_harness::pruned;

    /// Drive `validate` on the pruned program, the rung below this stage.
    fn validated(src: &str) -> Result<(), ParseError> {
        validate(&pruned(src).expect("earlier stages should accept"))
    }

    // --- Aggregate variables ---

    /// Shared declarations for the aggregate variable tests.
    const AGGREGATE_DECLS: &str = "\
        .decl Node(x: int32)\n\
        .decl Edge(x: int32, y: int32)\n\
        .decl Deg(x: int32, c: int32)\n\
        .decl Total(c: int32)\n\
        .input Node(IO=\"file\", filename=\"Node.csv\", delimiter=\",\")\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Deg\n\
        .output Total\n";

    // Cases: rule.
    #[rstest]
    #[case("Deg(x, c) :- Node(x), c = count : { Edge(x, _) }.")]
    #[case("Deg(x, count : Edge(x, _)) :- Node(x).")]
    #[case("Deg(x, s) :- Node(x), s = sum y : { Edge(x, y), y > 0 }.")]
    #[case("Deg(x, d) :- Node(x), c = count : Edge(x, _), d = sum y + c : { Edge(x, y) }.")]
    #[case("Total(c) :- c = count : { Edge(_, _) }.")]
    #[case("Total(c) :- c = count : { Node(x), count : Edge(x, y) > 1 }.")]
    #[case("Deg(x, s) :- Node(x), s = sum count : Edge(y, _) : Edge(x, y).")]
    #[case("Deg(x, c) :- Node(x), c = count : { Edge(x, y), s = sum z : Edge(y, z), s > 0 }.")]
    #[case("Deg(x, s) :- Edge(x, y), s = sum z + y : Edge(x, z).")]
    #[case("Deg(x, s) :- Node(x), s = sum 1 : Edge(x, _).")]
    #[case("Deg(x, c) :- Node(x), c = count : { x > 3 }.")]
    fn aggregates_with_bound_variables_are_accepted(#[case] rule: &str) {
        validated(&format!("{AGGREGATE_DECLS}{rule}\n")).unwrap();
    }

    /// Braces reading no relation and no variable of the rule match
    /// nothing, so there is nothing to aggregate over.
    // Cases: rule.
    #[rstest]
    #[case("Total(c) :- c = count : { 1 < 2 }.")]
    #[case("Total(c) :- c = sum 5 : { 1 < 2 }.")]
    #[case("Total(c) :- c = count : { count : { 1 < 2 } > 0 }.")]
    fn braces_reading_nothing_are_rejected(#[case] rule: &str) {
        assert_err!(
            validated(&format!("{AGGREGATE_DECLS}{rule}\n")),
            ParseError::EmptyAggregateBraces { .. }
        );
    }

    /// A variable the aggregate shares with the rest of the rule selects
    /// its group, so a body around the aggregate must bind it.
    // Cases: rule, var.
    #[rstest]
    #[case("Deg(x, c) :- c = count : { Edge(x, _) }.", "x")]
    #[case("Total(c) :- c = count : { Edge(x, _) }, x > 1.", "x")]
    #[case(
        "Deg(x, a) :- Node(x), a = count : Edge(x, y), b = count : Edge(y, x), a = b.",
        "y"
    )]
    #[case(
        "Total(c) :- c = count : { Node(x), count : Edge(x, y) > 1, count : Edge(y, x) > 1 }.",
        "y"
    )]
    #[case(
        "Total(c) :- c = count : { Node(x), count : Edge(x, y) > 1 }, y > 0.",
        "y"
    )]
    fn a_shared_variable_bound_only_inside_is_rejected(#[case] rule: &str, #[case] expected: &str) {
        assert_err!(
            validated(&format!("{AGGREGATE_DECLS}{rule}\n")),
            ParseError::UngroundedAggregateVariable { var, .. } if var == expected
        );
    }

    /// A target whose every variable is fixed outside the aggregate has one
    /// value per group, so there is nothing to aggregate over.
    // Cases: rule, target.
    #[rstest]
    #[case("Deg(x, s) :- Edge(x, y), s = sum y : Edge(x, y).", "y")]
    #[case("Deg(x, s) :- Edge(x, y), s = sum y : Edge(x, _).", "y")]
    #[case("Deg(x, s) :- Edge(x, y), s = sum y * 2 : Edge(x, y).", "y * 2")]
    #[case("Total(c) :- c = count : { Edge(x, y), sum y : Edge(x, y) > 0 }.", "y")]
    fn a_target_fixed_outside_the_aggregate_is_rejected(
        #[case] rule: &str,
        #[case] expected: &str,
    ) {
        assert_err!(
            validated(&format!("{AGGREGATE_DECLS}{rule}\n")),
            ParseError::FixedAggregateTarget { target, .. } if target == expected
        );
    }

    /// A variable local to the aggregate takes its values from the
    /// aggregate's own body, so it must bind it.
    // Cases: rule, var.
    #[rstest]
    #[case("Deg(x, s) :- Node(x), s = sum y : { Edge(x, _) }.", "y")]
    #[case("Deg(x, c) :- Node(x), c = count : { Edge(x, _), z > 1 }.", "z")]
    #[case("Total(c) :- c = count : { Node(x), count : Edge(x, y) > y }.", "y")]
    #[case("Total(c) :- c = count : { Node(x), sum z : Edge(x, _) > 1 }.", "z")]
    fn a_local_variable_no_atom_binds_is_rejected(#[case] rule: &str, #[case] expected: &str) {
        assert_err!(
            validated(&format!("{AGGREGATE_DECLS}{rule}\n")),
            ParseError::UnboundAggregateVariable { var, .. } if var == expected
        );
    }

    // --- Head variables ---

    #[test]
    fn head_variable_not_bound_in_body_is_rejected() {
        let src = "\
            .decl Person(id: int32, name: string)\n\
            .decl Greeting(name: string, salutation: string)\n\
            .input Person(IO=\"file\", filename=\"Person.csv\", delimiter=\",\")\n\
            .output Greeting\n\
            Greeting(name, salutation) :- Person(id, name).\n";
        assert_err!(
            validated(src),
            ParseError::UnknownHeadVariable { var, .. } if var == "salutation"
        );
    }

    #[test]
    fn head_variable_bound_only_under_negation_is_rejected() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Edge(y, z), !Edge(x, y).\n";
        assert_err!(
            validated(src),
            ParseError::UnknownHeadVariable { var, .. } if var == "x"
        );
    }

    #[test]
    fn bound_head_variables_are_accepted() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y, x) :- Edge(x, y).\n";
        validated(src).expect("bound head variables should validate");
    }

    // --- Body variables ---

    /// Shared declarations for the body variable tests.
    const BODY_DECLS: &str = "\
        .decl Person(id: int32, name: string)\n\
        .decl Blocked(id: int32)\n\
        .decl Edge(x: int32, y: int32)\n\
        .decl Safe(name: string)\n\
        .decl Reach(x: int32)\n\
        .extern fn is_positive(n: int32) -> bool\n\
        .input Person(IO=\"file\", filename=\"Person.csv\", delimiter=\",\")\n\
        .input Blocked(IO=\"file\", filename=\"Blocked.csv\", delimiter=\",\")\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Safe\n\
        .output Reach\n";

    /// A variable of a negated atom or a comparison, a UDF filter
    /// included, must be bound by a positive atom; the first unbound one in
    /// source order is reported, with the predicate it sits in.
    // Cases: rule, kind, predicate, var.
    #[rstest]
    #[case(
        "Safe(name) :- Person(id, name), !Blocked(other).",
        UnsafePredicateKind::Negation,
        "!Blocked(other)",
        "other"
    )]
    #[case(
        "Reach(x) :- Edge(x, y), z > 5.",
        UnsafePredicateKind::Comparison,
        "z > 5",
        "z"
    )]
    #[case(
        "Reach(x) :- Edge(x, y), is_positive(z) = True.",
        UnsafePredicateKind::Comparison,
        "is_positive(z) = True",
        "z"
    )]
    #[case(
        "Reach(x) :- Edge(x, y), z > w + 5.",
        UnsafePredicateKind::Comparison,
        "z > w + 5",
        "z"
    )]
    fn an_unbound_variable_in_a_body_predicate_is_rejected(
        #[case] rule: &str,
        #[case] expected_kind: UnsafePredicateKind,
        #[case] expected_predicate: &str,
        #[case] expected_var: &str,
    ) {
        assert_err!(
            validated(&format!("{BODY_DECLS}{rule}\n")),
            ParseError::UnsafeVariable { kind, predicate, var, .. }
                if *kind == expected_kind && predicate == expected_predicate && var == expected_var
        );
    }

    /// Positive atoms and aggregates both bind: a negated atom or a
    /// comparison may read either.
    // Cases: rule.
    #[rstest]
    #[case("Reach(x) :- Edge(x, y), !Blocked(y), y > 5.")]
    #[case("Reach(x) :- Edge(x, _), c = count : Edge(x, _), c > 2.")]
    fn variables_bound_by_atoms_or_aggregates_are_safe(#[case] rule: &str) {
        validated(&format!("{BODY_DECLS}{rule}\n")).unwrap();
    }

    // --- Mutability ---

    /// Only an EDB may declare how it changes; a relation that only rules
    /// derive has its mutability inferred.
    #[test]
    fn mutability_on_a_derived_relation_is_rejected() {
        assert_err!(
            validated(
                ".decl E(x: number) .input E .decl R(x: number) mutable .output R R(x) :- E(x).",
            ),
            ParseError::MutabilityOnDerivedRelation { name, .. } if name == "R"
        );
    }

    /// A relation with both an `.input` and rules is an EDB, so it may
    /// declare a mutability.
    #[test]
    fn mutability_on_an_input_with_rules_is_accepted() {
        validated(
            ".decl E(x: number) mutable .input E .decl S(x: number) .input S \
             .output E E(x) :- S(x).",
        )
        .unwrap();
    }
}
