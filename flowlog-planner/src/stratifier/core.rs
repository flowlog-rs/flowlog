//! Builds ordered strata and derives their relation metadata.
//!
//! Rules are ordered through dependency components; a component with a
//! cycle becomes a recursive stratum.

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::FlowLogRule;
use flowlog_parser::Mutability;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use flowlog_parser::Relation;
use itertools::Itertools;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::planner::PlanError;
use crate::stratifier::dependency_graph::DependencyGraph;
use crate::stratifier::scc;

// =============================================================================
// Stratum
// =============================================================================

/// Rules evaluated together and the metadata needed to plan them.
#[derive(Debug, Clone)]
pub(crate) struct Stratum {
    rule_ids: Vec<usize>,
    is_recursive: bool,
    recursive_relations: Vec<u64>,
    leave_relations: Vec<u64>,
    available_relations: HashSet<u64>,
    mutabilities: BTreeMap<u64, Mutability>,
}

impl Stratum {
    fn new(rule_ids: Vec<usize>, is_recursive: bool) -> Self {
        Self {
            rule_ids,
            is_recursive,
            recursive_relations: Vec::new(),
            leave_relations: Vec::new(),
            available_relations: HashSet::new(),
            mutabilities: BTreeMap::new(),
        }
    }

    /// Returns global source-order rule IDs evaluated as a unit.
    #[must_use]
    pub(crate) fn rule_ids(&self) -> &[usize] {
        &self.rule_ids
    }

    /// Returns `true` if this stratum is recursive.
    #[must_use]
    pub(crate) fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    /// Returns the relations that feed back into this stratum's fixpoint.
    ///
    /// The fingerprints are sorted. The slice is empty for a non-recursive
    /// stratum.
    #[must_use]
    pub(crate) fn recursive_relations(&self) -> &[u64] {
        &self.recursive_relations
    }

    /// Returns sorted relation fingerprints retained after this stratum.
    ///
    /// A head is retained when a later stratum consumes it or it is an IDB
    /// output.
    #[must_use]
    pub(crate) fn leave_relations(&self) -> &[u64] {
        &self.leave_relations
    }

    /// Returns EDBs and retained relations from all preceding strata.
    #[must_use]
    pub(crate) fn available_relations(&self) -> &HashSet<u64> {
        &self.available_relations
    }

    /// Returns the mutability of a relation this stratum reads or produces,
    /// or `None` for a relation it does not touch.
    ///
    /// Every such relation has one value within the stratum. A relation it
    /// reads has its final value: a rule reading a relation runs after every
    /// rule producing it. A relation whose rules span strata may still have
    /// a different value in each stratum producing it: a later stratum
    /// includes what the earlier ones produced, so its value is never lower.
    #[must_use]
    pub(crate) fn mutability(&self, relation_fp: u64) -> Option<Mutability> {
        self.mutabilities.get(&relation_fp).copied()
    }

    /// Returns the mutability of every relation this stratum reads or
    /// produces, keyed by fingerprint; see [`Self::mutability`].
    #[must_use]
    pub(crate) fn mutabilities(&self) -> &BTreeMap<u64, Mutability> {
        &self.mutabilities
    }
}

// =============================================================================
// Stratifier
// =============================================================================

/// Ordered evaluation strata for a program.
///
/// Every rule ID in `strata` indexes `program`.
#[derive(Debug, Clone)]
pub(crate) struct Stratifier {
    program: Program,
    strata: Vec<Stratum>,
}

impl Stratifier {
    /// Returns the strata in evaluation order.
    #[must_use]
    pub(crate) fn strata(&self) -> &[Stratum] {
        &self.strata
    }

    /// Returns a program's strata in evaluation order.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::AggregateInRecursion`] when an aggregate reads a
    /// relation of its own recursion, and an internal [`PlanError`] when a
    /// rule reads a relation that has no mutability, which parsing and
    /// stratum order rule out.
    pub(crate) fn from_program(program: &Program) -> Result<Self, PlanError> {
        // A `.init` splices its instance's rules in at the position the
        // `.init` held, so a relation may be defined by a later instance than
        // the one referencing it. Stratifying the whole program as one SCC
        // problem makes instance order irrelevant, matching Souffle's global
        // stratification.
        let mut strata = Self::stratify(program.rules());

        // SCC traversal order is incidental; global rule IDs preserve source
        // order for downstream plans and diagnostics.
        for stratum in &mut strata {
            stratum.rule_ids.sort_unstable();
        }

        let mut instance = Self {
            program: program.clone(),
            strata,
        };

        instance.reject_aggregates_in_recursion()?;
        instance.build_stratum_metadata();
        instance.assign_mutabilities()?;

        debug!("\n{}", instance);
        info!(
            "Successfully stratified program: produced {} strata ({} recursive)",
            instance.strata.len(),
            instance.strata.iter().filter(|s| s.is_recursive).count()
        );

        Ok(instance)
    }

    /// Returns ordered strata for `rules`, whose indices are the global
    /// source-order rule IDs.
    fn stratify(rules: &[FlowLogRule]) -> Vec<Stratum> {
        if rules.is_empty() {
            return Vec::new();
        }

        let dep_graph = DependencyGraph::from_rules(rules);
        let components = scc::compute_sccs(&dep_graph);

        Self::warn_negation_edges(&dep_graph, rules, &components);

        scc::merge_strata(components, &dep_graph)
            .into_iter()
            .map(|component| Stratum::new(component.rule_ids().to_vec(), component.is_recursive()))
            .collect()
    }

    // --- Negation warnings ---

    /// Warns for negative dependency edges that close a recursive cycle.
    fn warn_negation_edges(
        dep_graph: &DependencyGraph,
        rules: &[FlowLogRule],
        components: &[scc::Component],
    ) {
        for &(src, dst) in dep_graph.negative_edges() {
            if !scc::is_recursive_edge(components, src, dst) {
                continue;
            }
            let source_rule = &rules[src];
            if src == dst {
                warn!(
                    "Negation in recursive stratum (rule {} negates itself): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}",
                    src, src, source_rule
                );
            } else {
                let target_rule = &rules[dst];
                warn!(
                    "Negation in recursive stratum (rule {} negates rule {}): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}\n  Rule {}: {}",
                    src, dst, src, source_rule, dst, target_rule
                );
            }
        }
    }

    // --- Aggregates in recursion ---

    /// Rejects an aggregate that reads a relation its own recursive stratum
    /// derives. Its input would change every round, retracting its earlier
    /// answers, so the loop has no fixpoint to settle on; only a lattice
    /// column may merge values across recursion. An aggregate over relations
    /// from earlier strata is fixed for the whole loop, so a recursive rule
    /// may hold one.
    fn reject_aggregates_in_recursion(&self) -> Result<(), PlanError> {
        let program_rules = self.program.rules();
        for stratum in self.strata.iter().filter(|stratum| stratum.is_recursive) {
            let heads: HashSet<u64> = stratum
                .rule_ids
                .iter()
                .map(|&rule_id| program_rules[rule_id].head().head_fingerprint())
                .collect();
            for &rule_id in &stratum.rule_ids {
                let rule = &program_rules[rule_id];
                for predicate in rule.rhs() {
                    let Predicate::Aggregate(aggregate) = predicate else {
                        continue;
                    };
                    if let Some(atom) = aggregate
                        .body_atoms()
                        .into_iter()
                        .find(|atom| heads.contains(&atom.fingerprint()))
                    {
                        return Err(PlanError::AggregateInRecursion {
                            relation: atom.raw_name().to_string(),
                            read_span: atom.span(),
                            rule_span: rule.span(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    // --- Stratum metadata ---

    /// Derives recursive, leave, and available relations for every stratum.
    fn build_stratum_metadata(&mut self) {
        let program = &self.program;
        let program_rules = program.rules();

        // Metadata vectors reach emitted code, so ordered input sets keep
        // output stable across processes.
        let idb_fp_set: HashSet<u64> = program
            .idbs()
            .into_iter()
            .map(|r| r.fingerprint())
            .collect();
        let mut later_union: HashSet<u64> = HashSet::new();
        let mut later_body_atoms = Vec::with_capacity(self.strata.len());
        for stratum in self.strata.iter().rev() {
            later_body_atoms.push(later_union.clone());
            later_union.extend(
                stratum
                    .rule_ids
                    .iter()
                    .flat_map(|&rule_id| body_atom_fps(&program_rules[rule_id])),
            );
        }
        later_body_atoms.reverse();

        let edb_fps = program.edb_fingerprints();
        let mut accumulated = HashSet::new();
        for (stratum, later_body_atoms) in self.strata.iter_mut().zip(later_body_atoms) {
            let heads: BTreeSet<u64> = stratum
                .rule_ids
                .iter()
                .map(|&rule_id| program_rules[rule_id].head().head_fingerprint())
                .collect();
            let body_atoms: BTreeSet<u64> = stratum
                .rule_ids
                .iter()
                .flat_map(|&rule_id| body_atom_fps(&program_rules[rule_id]))
                .collect();

            if stratum.is_recursive {
                stratum.recursive_relations = heads.intersection(&body_atoms).copied().collect();
            }

            stratum.leave_relations = heads
                .iter()
                .filter(|fp| later_body_atoms.contains(fp) || idb_fp_set.contains(fp))
                .copied()
                .collect();
            stratum.available_relations = accumulated.clone();
            stratum.available_relations.extend(&edb_fps);
            accumulated.extend(&stratum.leave_relations);
        }
    }

    /// Assigns each stratum's heads their mutability, in evaluation order so
    /// every body relation already has a value.
    ///
    /// An EDB has its declared mutability, static by default. A head takes
    /// the most mutable of its rules in the stratum, as [`body_mutability`]
    /// computes them, and of the value it already has from an `.input` or an
    /// earlier stratum.
    ///
    /// # Errors
    ///
    /// Returns an internal [`PlanError`] when a rule reads a relation that
    /// has no mutability yet.
    fn assign_mutabilities(&mut self) -> Result<(), PlanError> {
        let program_rules = self.program.rules();
        // A lattice relation merges the rows its rules derive, so it takes
        // the value of an aggregate over them.
        let lattices: HashSet<u64> = self
            .program
            .relations()
            .iter()
            .filter(|relation| relation.lattice().is_some())
            .map(Relation::fingerprint)
            .collect();
        let mut latest: HashMap<u64, Mutability> = self
            .program
            .edbs()
            .into_iter()
            .map(|rel| (rel.fingerprint(), rel.input_mutability()))
            .collect();

        for stratum in &mut self.strata {
            // A head starts from the value it already has, from an `.input`
            // or an earlier stratum, and otherwise from static, the least.
            let mut heads: BTreeMap<u64, Mutability> = stratum
                .rule_ids
                .iter()
                .map(|&rule_id| {
                    let fp = program_rules[rule_id].head().head_fingerprint();
                    (fp, latest.get(&fp).copied().unwrap_or_default())
                })
                .collect();

            // A recursive stratum reads its own heads, and a rule may read a
            // head that a later rule raises, so iterate to a fixpoint.
            // Values only rise and there are finitely many, so this
            // terminates; a non-recursive stratum settles in one pass.
            loop {
                let mut changed = false;
                for &rule_id in &stratum.rule_ids {
                    let rule = &program_rules[rule_id];
                    let found = body_mutability(rule.rhs(), &|fp| {
                        heads.get(&fp).or_else(|| latest.get(&fp)).copied()
                    })
                    .map_err(|fp| {
                        PlanError::internal(format!(
                            "rule {rule_id} reads relation 0x{fp:016x}, which has no \
                             mutability: prune makes every underived relation an EDB, \
                             and an earlier stratum produces every other one\n  {rule}"
                        ))
                    })?;
                    let head_fp = rule.head().head_fingerprint();
                    let found = if lattices.contains(&head_fp) {
                        aggregate_mutability(found)
                    } else {
                        found
                    };
                    let head = heads.entry(head_fp).or_default();
                    if found > *head {
                        *head = found;
                        changed = true;
                    }
                }
                if !changed {
                    break;
                }
            }

            // Every head of an SCC reads, directly or not, every other, and
            // a mutable body relation makes a rule mutable whether it is
            // negated or not. So the fixpoint leaves the SCC one value.
            debug_assert!(
                !stratum.is_recursive || heads.values().all_equal(),
                "a recursive stratum's heads should share one mutability, found {heads:?}"
            );

            latest.extend(heads.iter().map(|(&fp, &value)| (fp, value)));

            // Every body relation a rule reads is a head here or already has
            // a value in `latest`, as `body_mutability` confirmed above, so
            // the lookup finds each one.
            let reads: Vec<(u64, Mutability)> = stratum
                .rule_ids
                .iter()
                .flat_map(|&rule_id| body_atom_fps(&program_rules[rule_id]))
                .filter_map(|fp| latest.get(&fp).map(|&value| (fp, value)))
                .collect();
            heads.extend(reads);
            stratum.mutabilities = heads;
        }
        Ok(())
    }
}

// --- Display ---

impl fmt::Display for Stratifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "\nStratum:")?;
        writeln!(f, "{SUBSECTION_BAR}")?;

        let fp2name: HashMap<u64, String> = self
            .program
            .relations()
            .iter()
            .map(|r| (r.fingerprint(), r.name().to_string()))
            .collect();
        let name_of = |fp: &u64| -> String {
            fp2name
                .get(fp)
                .cloned()
                .unwrap_or_else(|| format!("0x{:016x}", fp))
        };
        let fmt_fps = |fps: &[u64]| -> String {
            let mut names: Vec<String> = fps.iter().map(name_of).collect();
            names.sort();
            names.dedup();
            names.join(", ")
        };
        let rules = self.program.rules();

        for (idx, stratum) in self.strata.iter().enumerate() {
            let label = if stratum.is_recursive {
                "recursive"
            } else {
                "non-recursive"
            };
            let ids = stratum
                .rule_ids
                .iter()
                .sorted()
                .map(|r| r.to_string())
                .join(", ");
            writeln!(f, "#{} [{}] [{}]", idx + 1, label, ids)?;

            if stratum.is_recursive && !stratum.recursive_relations.is_empty() {
                writeln!(
                    f,
                    "  recursive: [{}]",
                    fmt_fps(&stratum.recursive_relations)
                )?;
            }
            writeln!(f, "  leave: [{}]", fmt_fps(&stratum.leave_relations))?;
            let mutabilities = stratum
                .mutabilities
                .iter()
                .map(|(fp, mutability)| format!("{}: {mutability}", name_of(fp)))
                .sorted()
                .join(", ");
            writeln!(f, "  mutability: [{mutabilities}]")?;

            for &rid in &stratum.rule_ids {
                if let Some(rule) = rules.get(rid) {
                    writeln!(f, "{rule}")?;
                } else {
                    writeln!(f, "<invalid rule #{rid}>")?;
                }
            }
            writeln!(f)?;
        }
        Ok(())
    }
}

/// Returns the mutability of a relation derived from others: `positive`
/// holds the mutabilities of the relations it reads, and `negated` those
/// of the relations it negates.
///
/// The derived relation is as mutable as the most mutable relation it
/// reads. Negating a relation that is not static makes it mutable, since a
/// growing filter retracts what was already derived.
pub(crate) fn derived_mutability(
    positive: impl IntoIterator<Item = Mutability>,
    negated: impl IntoIterator<Item = Mutability>,
) -> Mutability {
    let derived = positive
        .into_iter()
        .fold(Mutability::Static, Mutability::max);
    negated
        .into_iter()
        .fold(derived, |derived, filter| match filter {
            Mutability::Static => derived,
            Mutability::Append | Mutability::Mutable => Mutability::Mutable,
        })
}

/// Returns the mutability of an aggregate over a group whose rows have
/// mutability `input`. A group that only grows changes its answer, and the
/// old answer has to go, so an aggregate over append rows is mutable; a
/// static group's answer is final, and a mutable group's is already signed.
#[must_use]
pub fn aggregate_mutability(input: Mutability) -> Mutability {
    match input {
        Mutability::Static => Mutability::Static,
        Mutability::Append | Mutability::Mutable => Mutability::Mutable,
    }
}

/// Returns the mutability of the rows `predicates` derive:
/// [`derived_mutability`] over what they read, given `of`, the mutability of
/// each body relation. An aggregate reads like a positive atom of
/// [`aggregate_mutability`] over its own body.
///
/// # Errors
///
/// Returns the fingerprint of the first body relation `of` has no value
/// for.
fn body_mutability(
    predicates: &[Predicate],
    of: &impl Fn(u64) -> Option<Mutability>,
) -> Result<Mutability, u64> {
    let mut positive = Vec::new();
    let mut negated = Vec::new();
    for predicate in predicates {
        match predicate {
            Predicate::PositiveAtom(atom) => {
                let fp = atom.fingerprint();
                positive.push(of(fp).ok_or(fp)?);
            }
            Predicate::NegativeAtom(atom) => {
                let fp = atom.fingerprint();
                negated.push(of(fp).ok_or(fp)?);
            }
            Predicate::Compare(_) => {}
            Predicate::Aggregate(aggregate) => {
                positive.push(aggregate_mutability(body_mutability(aggregate.body(), of)?));
            }
        }
    }
    Ok(derived_mutability(positive, negated))
}

fn body_atom_fps(rule: &FlowLogRule) -> impl Iterator<Item = u64> + '_ {
    rule.body_atoms().into_iter().map(|atom| atom.fingerprint())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use flowlog_parser::test_harness::program;
    use rstest::rstest;
    use tracing_test::traced_test;

    use super::*;

    fn stratify(source: &str) -> Stratifier {
        Stratifier::from_program(&program(source)).expect("stratifies")
    }

    /// `name`'s mutability in the stratum that evaluates rule `rule_id`.
    fn mutability_at(s: &Stratifier, rule_id: usize, name: &str) -> Option<Mutability> {
        let fp = s
            .program
            .relations()
            .iter()
            .find(|rel| rel.name() == name)
            .expect("declared")
            .fingerprint();
        s.strata()
            .iter()
            .find(|stratum| stratum.rule_ids().contains(&rule_id))
            .expect("stratified")
            .mutability(fp)
    }

    /// Relations of every mutability, for the rules the mutability tests
    /// derive `Out` and the lattice `L` from.
    const MUTABILITY_DECLS: &str = "\
        .decl S(x: int32) static\n.input S\n\
        .decl T(x: int32)\n.input T\n\
        .decl A(x: int32) append\n.input A\n\
        .decl B(x: int32) append\n.input B\n\
        .decl M(x: int32) mutable\n.input M\n\
        .decl N(x: int32) mutable\n.input N\n\
        .decl L(x: int32, m: min<int32>)\n.output L\n\
        .decl Out(x: int32)\n.output Out\n";

    /// A rule is as mutable as its most mutable positive atom, and mutable
    /// when it negates anything that is not static. The four filter cases
    /// are the antijoin matrix: only a static source over a static filter
    /// stays static.
    #[rstest]
    #[case::static_input("Out(x) :- S(x).", Mutability::Static)]
    #[case::append_input("Out(x) :- A(x).", Mutability::Append)]
    #[case::mutable_input("Out(x) :- M(x).", Mutability::Mutable)]
    #[case::mixed_join("Out(x) :- S(x), M(x).", Mutability::Mutable)]
    #[case::static_append_join("Out(x) :- S(x), A(x).", Mutability::Append)]
    #[case::append_mutable_join("Out(x) :- A(x), M(x).", Mutability::Mutable)]
    #[case::static_filter("Out(x) :- S(x), !T(x).", Mutability::Static)]
    #[case::append_filter("Out(x) :- S(x), !A(x).", Mutability::Mutable)]
    #[case::mutable_filter("Out(x) :- S(x), !M(x).", Mutability::Mutable)]
    #[case::static_filter_over_append_source("Out(x) :- A(x), !T(x).", Mutability::Append)]
    #[case::append_filter_over_append_source("Out(x) :- A(x), !B(x).", Mutability::Mutable)]
    #[case::static_filter_over_mutable_source("Out(x) :- M(x), !T(x).", Mutability::Mutable)]
    #[case::mutable_filter_over_mutable_source("Out(x) :- M(x), !N(x).", Mutability::Mutable)]
    #[case::aggregate_over_static("Out(c) :- c = count : S(_).", Mutability::Static)]
    #[case::aggregate_over_append("Out(c) :- c = count : A(_).", Mutability::Mutable)]
    #[case::extreme_over_append("Out(m) :- m = min x : A(x).", Mutability::Mutable)]
    #[case::aggregate_over_mutable("Out(c) :- c = count : M(_).", Mutability::Mutable)]
    #[case::aggregate_beside_a_static_atom(
        "Out(x) :- S(x), c = count : A(_), c > x.",
        Mutability::Mutable
    )]
    fn body_mutability_follows_the_body(#[case] rule: &str, #[case] expected: Mutability) {
        let s = stratify(&format!("{MUTABILITY_DECLS}{rule}\n"));
        assert_eq!(mutability_at(&s, 0, "out"), Some(expected));
    }

    /// A lattice relation merges the rows its rules derive, so it answers
    /// at the value of an aggregate over them: static over static rows,
    /// mutable over any other.
    // Cases: rule, mutability.
    #[rstest]
    #[case::over_static("L(x, x) :- S(x).", Mutability::Static)]
    #[case::over_append("L(x, x) :- A(x).", Mutability::Mutable)]
    #[case::over_mutable("L(x, x) :- M(x).", Mutability::Mutable)]
    fn a_lattice_relation_answers_at_the_aggregates_value(
        #[case] rule: &str,
        #[case] expected: Mutability,
    ) {
        let s = stratify(&format!("{MUTABILITY_DECLS}{rule}\n"));
        assert_eq!(mutability_at(&s, 0, "l"), Some(expected));
    }

    /// A negated IDB counts with the mutability its own stratum derived: a
    /// filter computed from a mutable input makes the antijoin mutable even
    /// though every relation this rule names directly is static or derived.
    #[test]
    fn negating_a_mutable_idb_makes_the_rule_mutable() {
        let src = "\
            .decl S(x: int32)\n.input S\n\
            .decl M(x: int32) mutable\n.input M\n\
            .decl F(x: int32)\n\
            .decl Out(x: int32)\n.output Out\n\
            F(x) :- M(x).\n\
            Out(x) :- S(x), !F(x).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 0, "f"), Some(Mutability::Mutable));
        assert_eq!(mutability_at(&s, 1, "out"), Some(Mutability::Mutable));
    }

    /// A relation split across strata has a value in each: the partial
    /// result from a static input stays static, and the recursive stratum
    /// that completes it over a mutable input is mutable, for every
    /// relation in its SCC.
    #[test]
    fn split_relation_gets_a_mutability_per_stratum() {
        let src = "\
            .decl S(x: int32)\n.input S\n\
            .decl E(x: int32) mutable\n.input E\n\
            .decl A(x: int32)\n\
            .decl B(x: int32)\n.output B\n\
            A(x) :- B(x), E(x).\n\
            B(x) :- A(x).\n\
            B(x) :- S(x).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 2, "b"), Some(Mutability::Static));
        assert_eq!(mutability_at(&s, 0, "a"), Some(Mutability::Mutable));
        assert_eq!(mutability_at(&s, 0, "b"), Some(Mutability::Mutable));
    }

    /// A body relation without a value is reported by its fingerprint.
    /// Parsing and stratum order keep this unreachable through
    /// `from_program`, so the helper is driven directly.
    #[test]
    fn body_mutability_reports_a_body_relation_without_a_value() {
        let program = program(
            ".decl S(x: int32)\n.input S\n.decl Out(x: int32)\n.output Out\nOut(x) :- S(x).\n",
        );
        let s_fp = program
            .relations()
            .iter()
            .find(|rel| rel.name() == "s")
            .expect("declared")
            .fingerprint();
        assert_eq!(
            body_mutability(program.rules()[0].rhs(), &|_| None),
            Err(s_fp)
        );
    }

    /// A stratum answers for every relation it reads or produces, with
    /// the final value of what it reads, and for nothing else.
    #[test]
    fn stratum_mutability_covers_what_it_reads_and_produces() {
        let src = "\
            .decl S(x: int32)\n.input S\n\
            .decl M(x: int32) mutable\n.input M\n\
            .decl F(x: int32)\n\
            .decl Out(x: int32)\n.output Out\n\
            F(x) :- M(x).\n\
            Out(x) :- S(x), !F(x).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 1, "s"), Some(Mutability::Static));
        assert_eq!(mutability_at(&s, 1, "f"), Some(Mutability::Mutable));
        assert_eq!(mutability_at(&s, 1, "out"), Some(Mutability::Mutable));
        assert_eq!(mutability_at(&s, 0, "s"), None);
        assert_eq!(mutability_at(&s, 0, "out"), None);
    }

    /// A rule can read a head that a later rule in the same recursive
    /// stratum makes mutable, so one pass in rule order is not enough.
    #[test]
    fn recursive_stratum_iterates_until_every_head_settles() {
        let src = "\
            .decl S(x: int32)\n.input S\n\
            .decl E(x: int32) mutable\n.input E\n\
            .decl A(x: int32)\n\
            .decl B(x: int32)\n\
            .decl C(x: int32)\n.output C\n\
            C(x) :- B(x).\n\
            B(x) :- A(x).\n\
            A(x) :- C(x), E(x).\n\
            C(x) :- S(x).\n";
        let s = stratify(src);
        for name in ["a", "b", "c"] {
            assert_eq!(
                mutability_at(&s, 0, name),
                Some(Mutability::Mutable),
                "{name}"
            );
        }
    }

    /// An `.input` that a recursive rule also derives carries its declared
    /// mutability into the recursive stratum, whose static rules alone
    /// would leave it static.
    #[test]
    fn recursive_input_with_rules_carries_its_declared_mutability() {
        let src = "\
            .decl T(x: int32, y: int32) mutable\n.input T\n\
            .decl E(x: int32, y: int32)\n.input E\n\
            .output T\n\
            T(x, z) :- T(x, y), E(y, z).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 0, "t"), Some(Mutability::Mutable));
    }

    /// A declared `static` covers only the relation's own input. Rules that
    /// read a mutable relation still make the relation mutable.
    #[test]
    fn static_input_with_mutable_rules_is_mutable() {
        let src = "\
            .decl H(x: int32) static\n.input H\n\
            .decl M(x: int32) mutable\n.input M\n\
            .output H\n\
            H(x) :- M(x).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 0, "h"), Some(Mutability::Mutable));
    }

    /// A relation with both an `.input` and rules keeps its declared
    /// mutability even when its rules alone would be static.
    #[test]
    fn input_with_rules_carries_its_declared_mutability() {
        let src = "\
            .decl H(x: int32) mutable\n.input H\n\
            .decl S(x: int32)\n.input S\n\
            .output H\n\
            H(x) :- S(x).\n";
        let s = stratify(src);
        assert_eq!(mutability_at(&s, 0, "h"), Some(Mutability::Mutable));
    }

    /// Each `.init` splices its instance's rules in at the position the
    /// `.init` held, so instance `a` negating `b.Keep`, produced by a *later*
    /// instance, reads as a forward reference. Stratifying the whole rule
    /// list as one SCC problem makes instance order irrelevant, matching
    /// Souffle's global stratification.
    #[test]
    fn cross_instance_forward_reference_stratifies() {
        let src = "\
            .decl In(x: int32)\n\
            .input In(IO=\"file\", filename=\"In.csv\", delimiter=\",\")\n\
            .comp A {\n\
              .decl Out(x: int32)\n\
              Out(x) :- In(x), !b.Keep(x).\n\
            }\n\
            .comp B {\n\
              .decl Keep(x: int32)\n\
              Keep(x) :- In(x).\n\
            }\n\
            .init a = A\n\
            .init b = B\n\
            .output a.Out\n";
        stratify(src);
    }

    /// Negation on a back-edge inside a recursive SCC must warn.
    #[test]
    #[traced_test]
    fn warns_negation_through_recursion() {
        let src = "\
            .decl Edge(a: int32, b: int32)\n\
            .decl A(a: int32, b: int32)\n\
            .decl B(a: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            A(x, y) :- Edge(x, y), !B(x, y).\n\
            B(x, y) :- A(x, y).\n\
            .output A\n\
            .output B\n";
        stratify(src);
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// A rule negating its own head is negation through recursion and
    /// must warn.
    #[test]
    #[traced_test]
    fn warns_self_negation() {
        let src = "\
            .decl Edge(a: int32, b: int32)\n\
            .decl A(a: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            A(x, y) :- Edge(x, y), !A(x, y).\n\
            .output A\n";
        stratify(src);
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// Shared declarations for the aggregate-in-recursion tests: a graph,
    /// a recursive `Dist`, and a `Total` read after it.
    const AGGREGATE_DECLS: &str = "\
        .decl Edge(x: int32, y: int32, w: int32)\n\
        .decl Dist(x: int32, d: int32)\n\
        .decl Total(n: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .output Dist\n\
        .output Total\n\
        Dist(x, 0) :- Edge(x, _, _).\n";

    /// An aggregate over its own recursion has no fixpoint, whatever the
    /// operator or the depth of nesting; the error names the relation it
    /// reads.
    // Cases: recursive rule.
    #[rstest]
    #[case("Dist(y, s) :- Dist(x, _), Edge(x, y, _), s = sum d : Dist(y, d).")]
    #[case("Dist(y, m) :- Dist(x, _), Edge(x, y, _), m = min d : Dist(y, d).")]
    #[case(
        "Dist(y, s) :- Dist(x, _), Edge(x, y, _), \
         s = sum c : { Edge(y, z, _), c = count : Dist(z, _) }."
    )]
    fn an_aggregate_over_its_own_recursion_is_rejected(#[case] rule: &str) {
        let program = program(&format!("{AGGREGATE_DECLS}{rule}\n"));
        let err = Stratifier::from_program(&program).expect_err("must be rejected");
        assert!(
            matches!(&err, PlanError::AggregateInRecursion { relation, .. } if relation == "Dist"),
            "{err:?}"
        );
    }

    /// An aggregate over relations of earlier strata is fixed for the whole
    /// loop, so a recursive rule may hold one, nested or not.
    // Cases: recursive rule.
    #[rstest]
    #[case("Dist(y, d + n) :- Dist(x, d), Edge(x, y, _), n = count : Edge(y, _, _).")]
    #[case(
        "Dist(y, d + n) :- Dist(x, d), Edge(x, y, _), \
         n = sum c : { Edge(y, z, _), c = count : Edge(z, _, _) }."
    )]
    fn a_recursive_rule_may_aggregate_over_earlier_strata(#[case] rule: &str) {
        stratify(&format!("{AGGREGATE_DECLS}{rule}\n"));
    }

    /// An aggregate depends on what its body reads, so one over a recursive
    /// relation runs in a stratum after the recursion has finished.
    #[test]
    fn an_aggregate_runs_after_the_recursion_it_reads() {
        let s = stratify(&format!(
            "{AGGREGATE_DECLS}\
             Dist(y, d + w) :- Dist(x, d), Edge(x, y, w).\n\
             Total(n) :- n = count : Dist(_, _).\n"
        ));
        let stratum_of = |rule_id| {
            s.strata()
                .iter()
                .position(|stratum| stratum.rule_ids().contains(&rule_id))
                .expect("every rule has a stratum")
        };
        assert!(stratum_of(2) > stratum_of(1));
        assert!(!s.strata()[stratum_of(2)].is_recursive());
    }

    /// Rules that feed a recursive SCC, the SCC itself, and rules that read
    /// its results land in separate strata, ordered by dependency.
    #[test]
    fn recursive_scc_is_isolated_from_its_neighbors() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            .output Reach\n\
            A(x) :- Edge(x, y).\n\
            Reach(x, y) :- Edge(x, y).\n\
            Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            Out(x) :- A(x).\n";
        let s = stratify(src);
        assert!(s.strata().len() >= 3);
        assert_eq!(
            s.strata()
                .iter()
                .filter(|stratum| stratum.is_recursive())
                .count(),
            1
        );
    }

    /// Inline fact-only relations are EDBs and must be available to the very
    /// first stratum just like file-backed `.input` relations.
    #[test]
    fn inline_fact_relations_are_available_before_first_stratum() {
        let src = "\
            .decl Param(x: int32)\n\
            .decl Out(x: int32)\n\
            Param(1).\n\
            Out(x) :- Param(x).\n\
            .output Out\n";
        let program = program(src);
        let param_fp = program
            .relations()
            .iter()
            .find(|r| r.name() == "param")
            .expect("param relation missing")
            .fingerprint();

        let s = Stratifier::from_program(&program).expect("stratifies");
        let first = s.strata().first().expect("first stratum missing");

        assert!(
            first.available_relations().contains(&param_fp),
            "inline fact relation should be available before the first stratum"
        );
    }

    /// Every head that also appears as a body atom in the same stratum is a
    /// feedback relation. In this k-core-like cycle all three heads qualify.
    #[test]
    fn recursive_relations_capture_every_feedback_head() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl active_edge(x: int32, y: int32)\n\
            .decl degree(x: int32, d: int32)\n\
            .decl removed(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output removed\n\
            active_edge(x, y) :- edge(x, y), !removed(x), !removed(y).\n\
            degree(x, y) :- active_edge(x, y).\n\
            removed(x) :- degree(x, d), d < 2.\n";
        let s = stratify(src);

        assert_eq!(s.strata().len(), 1);
        let stratum = s.strata().first().expect("recursive stratum missing");
        assert!(stratum.is_recursive());
        assert_eq!(
            stratum.recursive_relations().len(),
            3,
            "active_edge, degree, and removed all feed back"
        );
    }

    fn fp_of(program: &Program, name: &str) -> u64 {
        program
            .relations()
            .iter()
            .find(|r| r.name() == name)
            .unwrap_or_else(|| panic!("relation `{name}` missing"))
            .fingerprint()
    }

    /// Leave set for stratum N must contain a head relation consumed by any
    /// *later* stratum. If `later_body_atoms_per_stratum` accumulation breaks,
    /// intermediate relations get dropped and codegen silently loses data.
    #[test]
    fn leave_set_includes_relation_consumed_by_later_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Mid(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            Mid(x, y) :- Edge(x, y).\n\
            Out(x) :- Mid(x, y).\n";
        let program = program(src);
        let s = Stratifier::from_program(&program).expect("stratifies");

        let mid_fp = fp_of(&program, "mid");
        let first = s.strata().first().expect("first stratum missing");
        assert!(
            first.leave_relations().contains(&mid_fp),
            "mid should be retained for stratum 1 to consume"
        );
    }

    /// Leave set for the last stratum must contain any `.output` relation it
    /// heads, even with no later consumer. Guards the `idb_fp_set` branch of
    /// the leave-set computation; a bug there would drop outputs from the
    /// persisted set.
    #[test]
    fn leave_set_includes_idb_even_with_no_later_consumer() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Final(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Final\n\
            Final(x, y) :- Edge(x, y).\n";
        let program = program(src);
        let s = Stratifier::from_program(&program).expect("stratifies");

        let final_fp = fp_of(&program, "final");
        let last = s.strata().last().expect("last stratum missing");
        assert!(
            last.leave_relations().contains(&final_fp),
            "output relation must stay in leave set of its stratum"
        );
    }

    /// The last stratum's available set includes leaves from every predecessor.
    #[test]
    fn available_set_accumulates_leaves_across_strata() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            A(x, y) :- Edge(x, y).\n\
            B(x, y) :- A(x, y).\n\
            Out(x) :- A(x, y), B(x, y).\n";
        let program = program(src);
        let s = Stratifier::from_program(&program).expect("stratifies");

        assert!(s.strata().len() >= 3, "expected at least 3 strata");
        let a_fp = fp_of(&program, "a");
        let b_fp = fp_of(&program, "b");
        let last = s.strata().last().expect("last stratum missing");
        let available = last.available_relations();
        assert!(
            available.contains(&a_fp),
            "A's leave from stratum 0 missing"
        );
        assert!(
            available.contains(&b_fp),
            "B's leave from stratum 1 missing"
        );
    }

    /// Negation across *non-recursive* strata must not trigger the recursive-
    /// stratum negation warning. A regression that broadens the trigger would
    /// silently spam warnings on every cross-stratum `!B(...)` the user writes.
    #[test]
    #[traced_test]
    fn no_warn_on_non_recursive_negation() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl B(x: int32)\n\
            .decl A(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            B(x) :- Edge(x, y).\n\
            A(x) :- Edge(x, y), !B(x).\n";
        stratify(src);
        assert!(
            !logs_contain("Negation in recursive stratum"),
            "non-recursive negation should not fire the recursive-stratum warning"
        );
    }

    // --- Determinism (issue #231, byte-stable emission) ---

    /// The recursive and leave vectors order feedback variables and retained
    /// tuples in emitted code, so they are sorted by fingerprint at
    /// construction rather than following set-iteration order.
    #[test]
    fn stratum_metadata_vectors_are_sorted_by_fingerprint() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl active_edge(x: int32, y: int32)\n\
            .decl degree(x: int32, d: int32)\n\
            .decl removed(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output removed\n\
            .output active_edge\n\
            .output degree\n\
            active_edge(x, y) :- edge(x, y), !removed(x), !removed(y).\n\
            degree(x, y) :- active_edge(x, y).\n\
            removed(x) :- degree(x, d), d < 2.\n";
        let s = stratify(src);
        let stratum = s.strata().first().expect("recursive stratum missing");
        assert!(stratum.recursive_relations().is_sorted());
        assert!(stratum.leave_relations().is_sorted());
    }

    /// The base rule forms its own stratum and the self-referential rule a
    /// recursive one after it.
    #[test]
    fn recursion_separates_the_base_rule_from_the_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x, y) :- Edge(x, y).\n\
            Reach(x, z) :- Edge(x, y), Reach(y, z).\n";
        let s = stratify(src);
        assert_eq!(s.strata().len(), 2);
        assert!(!s.strata()[0].is_recursive());
        assert!(s.strata()[1].is_recursive());
        assert_eq!(s.strata()[1].recursive_relations().len(), 1);
    }
}
