//! Rule-shape validation: the final pipeline stage, rejecting rules that
//! parse and type-check but are semantically broken. Each check's rule is
//! documented on the [`ParseError`] variant it raises.
//!
//! Covers every rule, loop-block rules included. The head-variable check
//! relies on `assemble`'s substitution pass having already eliminated
//! `x = expr` assignments: after it, positive body atoms are the only
//! binders left.

use crate::ast::FlowLogRule;
use crate::ast::HeadArg;
use crate::error::ParseError;
use crate::program::Program;

/// Reject semantically broken rules; see the module docs for the checks.
pub(crate) fn validate(program: &Program) -> Result<(), ParseError> {
    for rule in program.rules() {
        check_head_variables_bound(rule)?;
        check_aggregation_count(rule)?;
    }
    Ok(())
}

/// Checks every head variable against the rule's grounded set
/// ([`FlowLogRule::positive_body_vars`]).
fn check_head_variables_bound(rule: &FlowLogRule) -> Result<(), ParseError> {
    let bound = rule.positive_body_vars();
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

/// Rejects a rule head with more than one aggregation argument.
fn check_aggregation_count(rule: &FlowLogRule) -> Result<(), ParseError> {
    let head = rule.head();
    let count = head
        .head_arguments()
        .iter()
        .filter(|arg| matches!(arg, HeadArg::Aggregation(_)))
        .count();
    if count > 1 {
        return Err(ParseError::MultipleAggregationsInHead {
            head_span: head.span(),
            rule_span: rule.span(),
            rel: head.raw_name().to_string(),
            count,
        });
    }
    Ok(())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::assert_err;
    use crate::test_util::pruned;

    /// Drive `validate` on the pruned program, the rung below this stage.
    fn validated(src: &str) -> Result<(), ParseError> {
        validate(&pruned(src).expect("earlier stages should accept"))
    }

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
    fn head_variable_unbound_inside_aggregation_is_rejected() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(x)) :- Orders(id, amount).\n";
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

    #[test]
    fn two_aggregations_in_one_head_are_rejected() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(s: int32, c: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount), count(id)) :- Orders(id, amount).\n";
        assert_err!(
            validated(src),
            ParseError::MultipleAggregationsInHead { rel, count: 2, .. } if rel == "Totals"
        );
    }

    #[test]
    fn single_aggregation_is_accepted() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n";
        validated(src).expect("a single aggregation should validate");
    }

    #[test]
    fn aggregation_compatibility_across_rules_is_deferred() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n\
            Totals(count(id)) :- Orders(id, amount).\n";
        validated(src).expect("cross-rule aggregation checks belong to planning");
    }

    #[test]
    fn same_aggregation_across_rules_is_accepted() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Refunds(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .input Refunds(IO=\"file\", filename=\"Refunds.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n\
            Totals(sum(amount)) :- Refunds(id, amount).\n";
        validated(src).expect("agreeing aggregations should validate");
    }

    #[test]
    fn aggregated_and_plain_rules_for_same_relation_are_accepted() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n\
            Totals(amount) :- Orders(id, amount).\n";
        validated(src).expect("mixed aggregated and plain rules should validate");
    }
}
