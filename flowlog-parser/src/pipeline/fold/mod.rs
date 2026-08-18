//! Constant folding: a post-typecheck optimization pass.
//!
//! Runs after [`check_program`](crate::check_program) (literals pinned,
//! `as()` casts stripped) and before [`prune`](crate::prune). It collapses
//! fully-constant expressions to a single literal so the generated dataflow
//! doesn't recompute them per row, and eliminates rules a constant makes
//! dead. Layered over a value core:
//!
//! - [`eval`]: the pure value and comparison evaluators (correctness core).
//! - [`expr`]: value-fold one expression's constant subtrees.
//! - [`rule`]: fold and classify one rule (keep, eliminate, or make a fact).
//! - `mod`: the driver that walks the program and applies the fold per rule.

mod eval;
mod expr;
mod rule;

use self::rule::Disposition;
use self::rule::classify;
use self::rule::fold_rule;
use crate::Program;
use crate::error::ParseError;

/// Fold every constant expression in `program`, eliminate rules a constant
/// makes dead, and convert emptied ground rules to inline facts. An emptied
/// rule whose head is not constant (`1 / 0`, a string builtin) is rejected as
/// [`ParseError::GroundRuleNotConst`]. Runs after
/// [`check_program`](crate::check_program); assumes all literals are already
/// concrete (not the polymorphic `Int`/`Float` placeholders). Elimination can
/// strand now-dead relations; [`prune`](crate::prune) cleans those up.
pub fn fold_constants(program: &mut Program) -> Result<(), ParseError> {
    // Rebuild rather than `retain`: classifying is fallible, and a rule can
    // leave as a fact rather than a rule, so this is a fallible filter-map.
    // Taking the list also releases the borrow, letting each fact go straight
    // to `facts_mut` instead of through a staging vector.
    let mut kept = Vec::with_capacity(program.rules().len());
    for mut rule in std::mem::take(program.rules_mut()) {
        fold_rule(&mut rule);
        match classify(&rule)? {
            Disposition::Keep => kept.push(rule),
            Disposition::Remove => {}
            Disposition::ToFact(name, fact) => {
                program.facts_mut().entry(name).or_default().push(fact);
            }
        }
    }
    *program.rules_mut() = kept;
    Ok(())
}

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use crate::Constant;
    use crate::DataType;
    use crate::Factor;
    use crate::HeadArg;
    use crate::ParseError;
    use crate::Predicate;
    use crate::assert_err;
    use crate::test_util::folded;

    /// A constant comparison operand `x > 2 + 3` collapses to the single pinned
    /// literal `5`: the core "pre-compute constant predicate operand" win.
    #[test]
    fn comparison_operand_folds_to_single_literal() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(x: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x), x > 2 + 3.\n";
        let program = folded(src).expect("program type-checks");
        let rule = &program.rules()[0];
        let cmp = match &rule.rhs()[1] {
            Predicate::Compare(c) => c,
            other => panic!("expected comparison, got {other:?}"),
        };
        assert!(
            cmp.right().rest().is_empty(),
            "`2 + 3` should collapse to a lone constant"
        );
        match cmp.right().init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int32, "5")),
            other => panic!("expected Const(5), got {other:?}"),
        }
    }

    /// A fully-constant head argument `10 * 2` folds to `20` at the declared width.
    #[test]
    fn constant_head_arg_folds() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(v: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(10 * 2) :- Item(x).\n";
        let program = folded(src).expect("program type-checks");
        let rule = &program.rules()[0];
        let head = match &rule.head().head_arguments()[0] {
            HeadArg::Arith(a) => a,
            other => panic!("expected arith head arg, got {other:?}"),
        };
        assert!(head.rest().is_empty(), "head arg should be a lone const");
        match head.init() {
            Factor::Const(c) => assert_eq!(c, &Constant::new(DataType::Int32, "20")),
            other => panic!("expected Const(20), got {other:?}"),
        }
    }

    /// An expression that overflows its pinned width is left unfolded so the
    /// generated (wrapping) release code computes the identical value.
    #[test]
    fn overflowing_expression_is_left_unfolded() {
        let src = "\
            .decl Item(x: int8)\n\
            .decl Out(x: int8)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x), x > 100 + 100.\n";
        let program = folded(src).expect("program type-checks");
        let rule = &program.rules()[0];
        let cmp = match &rule.rhs()[1] {
            Predicate::Compare(c) => c,
            other => panic!("expected comparison, got {other:?}"),
        };
        assert!(
            !cmp.right().rest().is_empty(),
            "100 + 100 overflows int8 and must be left unfolded"
        );
    }

    /// An always-true constant comparison is dropped when a positive atom
    /// remains, so the generated dataflow doesn't run a per-row no-op filter.
    #[test]
    fn always_true_compare_is_dropped() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(x: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x), 1 < 2.\n";
        let program = folded(src).expect("program type-checks");
        let rule = &program.rules()[0];
        assert_eq!(
            rule.rhs().len(),
            1,
            "the always-true `1 < 2` should be dropped"
        );
        assert!(
            matches!(&rule.rhs()[0], Predicate::PositiveAtom(_)),
            "the surviving predicate should be the Item atom"
        );
    }

    /// An always-FALSE rule is eliminated: its body can never hold, so it
    /// contributes nothing and is dropped entirely.
    #[test]
    fn always_false_rule_is_removed() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(x: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- Item(x).\n\
            Out(x) :- Item(x), 1 > 2.\n";
        let program = folded(src).expect("program type-checks");
        assert_eq!(
            program.rules().len(),
            1,
            "the always-false second rule should be eliminated"
        );
        assert_eq!(
            program.rules()[0].rhs().len(),
            1,
            "the surviving rule keeps just its Item atom"
        );
    }

    /// An always-TRUE sole rule with an all-constant head becomes an inline fact
    /// (which also fixes the pre-existing 0-positive-atom planner panic).
    #[test]
    fn always_true_sole_rule_becomes_fact() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(v: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(5) :- 1 < 2.\n";
        let program = folded(src).expect("program type-checks");
        assert!(
            program.rules().is_empty(),
            "the always-true-sole rule should be converted away"
        );
        let facts = program.facts().get("out").expect("Out should have a fact");
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].columns, vec![Constant::new(DataType::Int32, "5")]);
    }

    /// An expression with a variable is never folded.
    #[test]
    fn variable_expression_not_folded() {
        let src = "\
            .decl Item(x: int32)\n\
            .decl Out(x: int32)\n\
            .input Item(IO=\"file\", filename=\"Item.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x + 5) :- Item(x).\n";
        let program = folded(src).expect("program type-checks");
        let rule = &program.rules()[0];
        let head = match &rule.head().head_arguments()[0] {
            HeadArg::Arith(a) => a,
            other => panic!("expected arith head arg, got {other:?}"),
        };
        assert!(!head.rest().is_empty(), "`x + 5` must stay unfolded");
    }

    /// An all-assignment rule (`P(x) :- x = 1 + 2.`) reaches fold with an empty
    /// body after substitution; fold folds its ground head and materializes the
    /// rule as a fact, at the head column's pinned width.
    #[test]
    fn assignment_emptied_rule_folds_to_a_fact() {
        let program = folded(
            "\
            .decl P(v: int32)\n\
            .output P\n\
            P(x) :- x = 1 + 2.\n",
        )
        .expect("program type-checks");
        let facts = program.facts().get("p").expect("P should have a fact");
        assert_eq!(facts[0].columns, vec![Constant::new(DataType::Int32, "3")]);
    }

    /// After substitution empties the body, a head that does not fold to a constant
    /// (division by zero, a string builtin) has no runtime value and is rejected
    /// here rather than reaching the planner.
    #[rstest]
    #[case::division_by_zero(".decl P(v: int32)\n.output P\nP(x) :- x = 1 / 0.\n")]
    #[case::unfoldable_builtin(".decl P(s: symbol)\n.output P\nP(s) :- s = cat(\"a\", \"b\").\n")]
    #[case::unbound_head_var(".decl P(v: int32)\n.output P\nP(x) :- y = 1.\n")]
    fn assignment_emptied_rule_with_non_constant_head_is_rejected(#[case] src: &str) {
        assert_err!(folded(src), ParseError::GroundRuleNotConst { .. });
    }
}
