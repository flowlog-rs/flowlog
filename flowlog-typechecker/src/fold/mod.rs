//! Constant folding — a post-typecheck optimization pass.
//!
//! Runs after [`check_program`](crate::check_program) (literals pinned, `as()`
//! casts stripped) and before the planner. It collapses fully-constant
//! expressions to a single literal so the generated dataflow doesn't recompute
//! them per row, and eliminates rules a constant makes dead. Organized as
//! bricks over a value core, like Pass 1:
//!
//! - [`eval`]  — the pure value/comparison evaluators (correctness core).
//! - [`expr`]  — value-fold one expression's constant subtrees.
//! - [`rule`]  — fold + classify one rule (keep / eliminate / → fact).
//! - `mod`     — this driver: walk the program and apply per rule, then
//!   normalize (parsing no longer does that).

mod eval;
mod expr;
mod rule;

use flowlog_parser::InlineFact;
use flowlog_parser::Program;
use flowlog_parser::Segment;

use crate::fold::rule::Disposition;
use crate::fold::rule::classify;
use crate::fold::rule::fold_rule;

/// Fold every constant expression in `program`, eliminate rules a constant
/// makes dead, then normalize. Runs after
/// [`check_program`](crate::check_program); assumes all literals are already
/// concrete (not the polymorphic `Int`/`Float` placeholders).
pub fn fold_constants(program: &mut Program) {
    let mut new_facts: Vec<(String, InlineFact)> = Vec::new();

    for segment in program.segments_mut() {
        match segment {
            Segment::Plain(rules) => {
                let taken = std::mem::take(rules);
                let mut kept = Vec::with_capacity(taken.len());
                for mut rule in taken {
                    fold_rule(&mut rule);
                    match classify(&rule) {
                        Disposition::Keep => kept.push(rule),
                        Disposition::Remove => {}
                        Disposition::ToFact(name, fact) => new_facts.push((name, fact)),
                    }
                }
                *rules = kept;
            }
            // v1 restricts elimination to plain segments: inside loop/fixpoint
            // blocks we value-fold and predicate-drop but never remove a rule
            // (removing a recursive rule could change fixpoint semantics).
            Segment::Loop(block) | Segment::Fixpoint(block) => {
                for rule in block.rules_mut() {
                    fold_rule(rule);
                }
            }
        }
    }

    for (name, fact) in new_facts {
        program.facts_mut().entry(name).or_default().push(fact);
    }

    // Normalize once, here — parsing no longer does it. Also cleans up
    // whatever the rule elimination above just made dead.
    program.normalize();
}
