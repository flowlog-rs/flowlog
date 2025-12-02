//! Rule structures for FlowLog Datalog programs.
//!
//! A rule is `head :- p1, p2, ..., pn.`
//! - Head: a single derived relation
//! - Body: predicates that must all be satisfied
//! - Optional planning flag (via `.plan`) for optimization

use super::{Factor, Head, HeadArg, Predicate};
use crate::primitive::ConstType;
use crate::{Lexeme, Rule};
use pest::iterators::Pair;
use std::fmt;

/// A complete FlowLog rule.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FlowLogRule {
    head: Head,
    rhs: Vec<Predicate>,
    is_planning: bool,
}

impl fmt::Display for FlowLogRule {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} :- {}.",
            self.head,
            self.rhs
                .iter()
                .map(|p| p.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl FlowLogRule {
    /// Construct a rule.
    #[must_use]
    pub fn new(head: Head, rhs: Vec<Predicate>, is_planning: bool) -> Self {
        Self {
            head,
            rhs,
            is_planning,
        }
    }

    /// Rule head.
    #[must_use]
    #[inline]
    pub fn head(&self) -> &Head {
        &self.head
    }

    /// Rule body (right-hand side predicates).
    #[must_use]
    #[inline]
    pub fn rhs(&self) -> &[Predicate] {
        &self.rhs
    }

    /// Whether planning optimization is enabled.
    #[must_use]
    #[inline]
    pub fn is_planning(&self) -> bool {
        self.is_planning
    }

    /// Whether any body predicate is a boolean literal.
    #[must_use]
    #[inline]
    pub fn is_boolean(&self) -> bool {
        self.rhs.iter().any(|p| p.is_boolean())
    }

    /// Indexed access to a body predicate (panics if out of bounds).
    #[inline]
    pub fn get(&self, i: usize) -> &Predicate {
        &self.rhs[i]
    }

    /// Extract constants from a boolean rule's head.
    ///
    /// Panics if any head argument is not a simple constant.
    #[must_use]
    pub fn extract_constants_from_head(&self) -> Vec<ConstType> {
        let mut out = Vec::new();
        for arg in self.head.head_arguments() {
            match arg {
                HeadArg::Var(_) | HeadArg::Aggregation(_) => {
                    panic!("Boolean rule head must contain only constants: {self}")
                }
                HeadArg::Arith(arith) => {
                    if arith.is_const() {
                        if let Factor::Const(c) = arith.init() {
                            out.push(c.clone());
                        } else {
                            // Defensive (shouldn't happen if is_const() is true).
                            panic!("Boolean rule head must contain only constants: {self}");
                        }
                    } else {
                        panic!("Boolean rule head must contain only constants: {self}");
                    }
                }
            }
        }
        out
    }
}

impl Lexeme for FlowLogRule {
    /// Parse `head ~ ":-" ~ predicates ~ optimize? ~ "."`
    fn from_parsed_rule(parsed_rule: Pair<Rule>) -> Self {
        let mut inner = parsed_rule.into_inner();

        let head = Head::from_parsed_rule(inner.next().expect("Missing rule head"));

        // predicates
        let predicates_rule = inner.next().expect("Missing rule predicates");
        let rhs: Vec<Predicate> = predicates_rule
            .into_inner()
            .map(Predicate::from_parsed_rule)
            .collect();

        // optional optimize marker
        let is_planning = inner.next().is_some();

        Self::new(head, rhs, is_planning)
    }
}

#[cfg(test)]
mod tests {
    use super::super::ArithmeticOperator;
    use super::*;
    use crate::logic::{Arithmetic, Atom, AtomArg, ComparisonExpr, ComparisonOperator, Factor};
    use crate::primitive::ConstType;

    // Helpers
    fn var_arg(name: &str) -> AtomArg {
        AtomArg::Var(name.into())
    }
    fn atom(name: &str, args: Vec<AtomArg>) -> Atom {
        Atom::new(name, args, 0)
    }
    fn head_var(name: &str) -> HeadArg {
        HeadArg::Var(name.into())
    }
    fn head_const(v: ConstType) -> HeadArg {
        HeadArg::Arith(Arithmetic::new(Factor::Const(v), vec![]))
    }
    fn head_named(name: &str, args: Vec<HeadArg>) -> Head {
        Head::new(name.into(), args)
    }

    fn pos_pred(name: &str, args: Vec<AtomArg>) -> Predicate {
        Predicate::PositiveAtomPredicate(atom(name, args))
    }
    fn neg_pred(name: &str, args: Vec<AtomArg>) -> Predicate {
        Predicate::NegativeAtomPredicate(atom(name, args))
    }
    fn bool_pred(v: bool) -> Predicate {
        Predicate::BoolPredicate(v)
    }

    fn cmp_pred() -> Predicate {
        let l = Arithmetic::new(Factor::Var("X".into()), vec![]);
        let r = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
        let c = ComparisonExpr::new(l, ComparisonOperator::GreaterThan, r);
        Predicate::ComparePredicate(c)
    }

    #[test]
    fn create_and_access() {
        let head = head_named("result", vec![head_var("X")]);
        let body = vec![pos_pred("input", vec![var_arg("X")])];
        let r = FlowLogRule::new(head, body, false);

        assert!(!r.is_planning());
        assert!(!r.is_boolean());
        assert_eq!(r.head().name(), "result");
        assert_eq!(r.rhs().len(), 1);
    }

    #[test]
    fn planning_flag() {
        let head = head_named("optimized", vec![head_var("Y")]);
        let body = vec![pos_pred("source", vec![var_arg("Y")])];
        let r = FlowLogRule::new(head, body, true);
        assert!(r.is_planning());
    }

    #[test]
    fn boolean_detection() {
        let head = head_named("fact", vec![head_const(ConstType::Integer(42))]);
        let body = vec![bool_pred(true)];
        let r = FlowLogRule::new(head, body, false);
        assert!(r.is_boolean());
    }

    #[test]
    fn get_by_index() {
        let head = head_named("multi", vec![head_var("X")]);
        let body = vec![
            pos_pred("first", vec![var_arg("X")]),
            cmp_pred(),
            bool_pred(false),
        ];
        let r = FlowLogRule::new(head, body, false);

        match r.get(0) {
            Predicate::PositiveAtomPredicate(a) => assert_eq!(a.name(), "first"),
            _ => panic!("expected positive atom"),
        }
        matches!(r.get(1), Predicate::ComparePredicate(_));
        matches!(r.get(2), Predicate::BoolPredicate(false));
    }

    #[test]
    fn display_formats() {
        let head = head_named("result", vec![head_var("X")]);
        let body = vec![pos_pred("input", vec![var_arg("X")])];
        let r = FlowLogRule::new(head, body, false);
        assert!(r.to_string().starts_with("result(X) :- input(X)"));

        let head2 = head_named("complex", vec![head_var("A"), head_var("B")]);
        let body2 = vec![
            pos_pred("rel1", vec![var_arg("A")]),
            neg_pred("rel2", vec![var_arg("B")]),
            bool_pred(true),
        ];
        let r2 = FlowLogRule::new(head2, body2, false);
        let s2 = r2.to_string();
        assert!(s2.starts_with("complex(A, B) :- rel1(A)"));
        assert!(s2.contains("!rel2(B)"));
        assert!(s2.contains("true"));
        assert!(s2.ends_with('.'));

        let head3 = head_named("filtered", vec![head_var("X")]);
        let body3 = vec![pos_pred("data", vec![var_arg("X")]), {
            let l = Arithmetic::new(Factor::Var("X".into()), vec![]);
            let r = Arithmetic::new(Factor::Const(ConstType::Integer(5)), vec![]);
            Predicate::ComparePredicate(ComparisonExpr::new(l, ComparisonOperator::GreaterThan, r))
        }];
        let r3 = FlowLogRule::new(head3, body3, false);
        let s3 = r3.to_string();
        assert!(s3.starts_with("filtered(X) :- data(X)"));
        assert!(s3.contains(", X > 5"));
        assert!(s3.ends_with('.'));
    }

    #[test]
    fn extract_constants() {
        let head = head_named(
            "facts",
            vec![
                head_const(ConstType::Integer(42)),
                head_const(ConstType::Text("hello".into())),
            ],
        );
        let r = FlowLogRule::new(head, vec![bool_pred(true)], false);

        let c = r.extract_constants_from_head();
        assert_eq!(c.len(), 2);
        assert_eq!(c[0], ConstType::Integer(42));
        assert_eq!(c[1], ConstType::Text("hello".into()));
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn extract_constants_panics_on_var() {
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Integer(1)), head_var("X")],
        );
        let r = FlowLogRule::new(head, vec![bool_pred(true)], false);
        let _ = r.extract_constants_from_head();
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn extract_constants_panics_on_aggregation() {
        use crate::logic::{Aggregation, AggregationOperator};
        let arith = Arithmetic::new(Factor::Var("X".into()), vec![]);
        let agg = Aggregation::new(AggregationOperator::Sum, arith);
        let head = head_named(
            "invalid",
            vec![head_const(ConstType::Integer(1)), HeadArg::Aggregation(agg)],
        );
        let r = FlowLogRule::new(head, vec![bool_pred(true)], false);
        let _ = r.extract_constants_from_head();
    }

    #[test]
    #[should_panic(expected = "Boolean rule head must contain only constants")]
    fn extract_constants_panics_on_complex_arith() {
        let complex = Arithmetic::new(
            Factor::Var("X".into()),
            vec![(
                ArithmeticOperator::Plus,
                Factor::Const(ConstType::Integer(1)),
            )],
        );
        let head = head_named("invalid", vec![HeadArg::Arith(complex)]);
        let r = FlowLogRule::new(head, vec![bool_pred(true)], false);
        let _ = r.extract_constants_from_head();
    }

    #[test]
    fn clone_hash_eq() {
        let head = head_named("test", vec![head_var("X")]);
        let body = vec![pos_pred("input", vec![var_arg("X")])];
        let r = FlowLogRule::new(head, body, false);
        let c = r.clone();
        assert_eq!(r, c);

        use std::collections::HashSet;
        let mut set = HashSet::new();
        set.insert(r.clone());
        set.insert(c);
        assert_eq!(set.len(), 1);
    }

    #[test]
    fn complex_rule_example() {
        // derived(Person, Age) :- person(Person, Age), Age > 18, !blocked(Person).
        let head = head_named("derived", vec![head_var("Person"), head_var("Age")]);
        let left = Arithmetic::new(Factor::Var("Age".into()), vec![]);
        let right = Arithmetic::new(Factor::Const(ConstType::Integer(18)), vec![]);
        let age_gt = ComparisonExpr::new(left, ComparisonOperator::GreaterThan, right);
        let body = vec![
            pos_pred("person", vec![var_arg("Person"), var_arg("Age")]),
            Predicate::ComparePredicate(age_gt),
            neg_pred("blocked", vec![var_arg("Person")]),
        ];
        let r = FlowLogRule::new(head, body, false);

        assert_eq!(r.head().name(), "derived");
        assert_eq!(r.head().arity(), 2);
        assert_eq!(r.rhs().len(), 3);
        assert!(!r.is_boolean());
        assert!(!r.is_planning());
        let sr = r.to_string();
        assert!(sr.starts_with("derived(Person, Age) :- person(Person, Age)"));
        assert!(sr.contains("Age > 18"));
        assert!(sr.contains("!blocked(Person)"));
        assert!(sr.ends_with('.'));
    }
}
