//! Rule- and program-level helpers for this crate's tests: a catalog, a
//! rule planner, or a whole plan from a source string.

use flowlog_parser::test_harness::program;
use flowlog_parser::test_harness::rule;

use crate::catalog::Catalog;
use crate::planner::ProgramPlanner;
use crate::planner::RulePlanner;

/// Returns the catalog of the first rule of `source`.
///
/// # Panics
///
/// Panics when the program or the catalog is rejected.
pub(crate) fn catalog(source: &str) -> Catalog {
    Catalog::from_rule(&rule(source)).unwrap_or_else(|e| panic!("catalog of {source:?}: {e:?}"))
}

/// Returns the planner and catalog of the first rule of `source`, as a
/// non-recursive stratum plans it.
///
/// # Panics
///
/// Panics as [`catalog`] does.
pub(crate) fn rule_planner(source: &str) -> (RulePlanner, Catalog) {
    recursive_rule_planner(source, &[])
}

/// [`rule_planner`] for a rule in a recursive stratum: the body atoms whose
/// relation is named in `recursive` are marked as feeding the fixpoint.
pub(crate) fn recursive_rule_planner(source: &str, recursive: &[&str]) -> (RulePlanner, Catalog) {
    let rule = rule(source);
    let catalog =
        Catalog::from_rule(&rule).unwrap_or_else(|e| panic!("catalog of {source:?}: {e:?}"));
    let recursive_fps: Vec<u64> = (0..catalog.positive_atom_number())
        .filter(|&index| recursive.contains(&catalog.positive_atom_name(index).unwrap()))
        .map(|index| catalog.positive_atom_fingerprint(index).unwrap())
        .collect();
    (RulePlanner::new(rule, &recursive_fps), catalog)
}

/// Plans `source` end to end. Parsing is the smallest entry that yields
/// planned strata, so tests of the stratum-level passes drive them from
/// source too.
///
/// # Panics
///
/// Panics when the program is rejected or fails to plan.
pub(crate) fn program_planner(source: &str) -> ProgramPlanner {
    ProgramPlanner::from_program(&program(source), &mut None)
        .unwrap_or_else(|e| panic!("plan of {source:?}: {e:?}"))
}
