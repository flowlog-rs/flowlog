//! Semantic validation: the final pipeline stage, rejecting rules and
//! declarations that parse and type-check but are semantically broken. Each
//! check's rule is documented on the [`ParseError`] variant it raises:
//!
//! - every variable an aggregate reads is bound: one it shares with the
//!   rest of the rule outside it, a local one by its own atoms;
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

use crate::ast::AtomArg;
use crate::ast::FlowLogRule;
use crate::ast::Predicate;
use crate::error::ParseError;
use crate::error::UnsafePredicateKind;
use crate::program::Program;

/// Rejects semantically broken rules and declarations; see the module docs
/// for the checks.
pub(crate) fn validate(program: &Program) -> Result<(), ParseError> {
    for rule in program.rules() {
        // Aggregates first: a head variable bound only inside one is better
        // explained as a shared variable than as an unbound one.
        check_aggregate_vars_bound(rule)?;
        check_head_vars_bound(rule)?;
        check_body_vars_bound(rule)?;
    }
    check_mutability_on_edbs_only(program)?;
    Ok(())
}

/// Checks each aggregate's variables. One the aggregate shares with the
/// rest of the rule is fixed outside it, so the rule must bind it there
/// ([`FlowLogRule::bound_vars`]); one local to the aggregate must be bound by
/// an atom of the aggregate's body.
fn check_aggregate_vars_bound(rule: &FlowLogRule) -> Result<(), ParseError> {
    let bound = rule.bound_vars();
    for (i, predicate) in rule.rhs().iter().enumerate() {
        let Predicate::Aggregate(aggregate) = predicate else {
            continue;
        };
        let mut outside = HashSet::new();
        for arg in rule.head().head_arguments() {
            outside.extend(arg.vars().into_iter().map(String::as_str));
        }
        for (j, other) in rule.rhs().iter().enumerate() {
            if j == i {
                continue;
            }
            predicate_vars(other, &mut outside);
            if let Predicate::Aggregate(other) = other {
                outside.extend(other.var());
            }
        }
        let mut inside = HashSet::new();
        predicate_vars(predicate, &mut inside);
        let mut body_bound = HashSet::new();
        for inner in aggregate.body() {
            if let Predicate::PositiveAtom(atom) = inner {
                body_bound.extend(atom_vars(atom.arguments()));
            }
        }

        // Sorted so the variable reported does not depend on hash order.
        let mut inside: Vec<&str> = inside.into_iter().collect();
        inside.sort_unstable();
        for var in inside {
            if outside.contains(var) {
                if !bound.contains(var) {
                    return Err(ParseError::UngroundedAggregateVariable {
                        span: aggregate.span(),
                        rule_span: rule.span(),
                        var: var.to_string(),
                    });
                }
            } else if !body_bound.contains(var) {
                return Err(ParseError::UnboundAggregateVariable {
                    span: aggregate.span(),
                    rule_span: rule.span(),
                    var: var.to_string(),
                });
            }
        }
    }
    Ok(())
}

/// Adds every variable `predicate` mentions to `out`; for an aggregate, its
/// target's and body's, not the variable it binds.
fn predicate_vars<'a>(predicate: &'a Predicate, out: &mut HashSet<&'a str>) {
    match predicate {
        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
            out.extend(atom_vars(atom.arguments()));
        }
        Predicate::Compare(cmp) => out.extend(cmp.vars_set().into_iter().map(String::as_str)),
        Predicate::Aggregate(aggregate) => {
            if let Some(target) = aggregate.target() {
                out.extend(target.vars().into_iter().map(String::as_str));
            }
            for inner in aggregate.body() {
                predicate_vars(inner, out);
            }
        }
    }
}

fn atom_vars(arguments: &[AtomArg]) -> impl Iterator<Item = &str> {
    arguments.iter().filter_map(|arg| match arg {
        AtomArg::Var(v) => Some(v.as_str()),
        AtomArg::Const(_) | AtomArg::Placeholder => None,
    })
}

/// Checks every head variable against the rule's grounded set
/// ([`FlowLogRule::bound_vars`]).
fn check_head_vars_bound(rule: &FlowLogRule) -> Result<(), ParseError> {
    let bound = rule.bound_vars();
    for arg in rule.head().head_arguments() {
        for var in arg.vars() {
            if !bound.contains(var.as_str()) {
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
                if let Some(var) = atom_vars(atom.arguments()).find(|v| !bound.contains(v)) {
                    return Err(ParseError::UnsafeVariable {
                        kind: UnsafePredicateKind::Negation,
                        predicate: format!("!{atom}"),
                        predicate_span: atom.span(),
                        rule_span: rule.span(),
                        var: var.to_string(),
                    });
                }
            }
            Predicate::Compare(cmp) => {
                let mut vars = cmp.left().vars().into_iter().chain(cmp.right().vars());
                if let Some(var) = vars.find(|v| !bound.contains(v.as_str())) {
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
    fn aggregates_with_bound_variables_are_accepted(#[case] rule: &str) {
        validated(&format!("{AGGREGATE_DECLS}{rule}\n")).unwrap();
    }

    /// A variable the aggregate shares with the rest of the rule selects
    /// its group, so the rule must bind it outside the aggregate.
    // Cases: rule, var.
    #[rstest]
    #[case("Deg(x, c) :- c = count : { Edge(x, _) }.", "x")]
    #[case("Total(c) :- c = count : { Edge(x, _) }, x > 1.", "x")]
    #[case(
        "Deg(x, a) :- Node(x), a = count : Edge(x, y), b = count : Edge(y, x), a = b.",
        "y"
    )]
    fn a_shared_variable_bound_only_inside_is_rejected(#[case] rule: &str, #[case] expected: &str) {
        assert_err!(
            validated(&format!("{AGGREGATE_DECLS}{rule}\n")),
            ParseError::UngroundedAggregateVariable { var, .. } if var == expected
        );
    }

    /// A variable local to the aggregate takes its values from the
    /// aggregate's atoms, so one of them must bind it.
    // Cases: rule, var.
    #[rstest]
    #[case("Deg(x, s) :- Node(x), s = sum y : { Edge(x, _) }.", "y")]
    #[case("Deg(x, c) :- Node(x), c = count : { Edge(x, _), z > 1 }.", "z")]
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
