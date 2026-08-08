//! Builds ordered strata and derives their relation metadata.
//!
//! Rules are ordered through dependency components; a component with a
//! cycle becomes a recursive stratum.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::AggregationOperator;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use itertools::Itertools;
use tracing::debug;
use tracing::info;
use tracing::warn;

use crate::stratifier::dependency_graph::DependencyGraph;
use crate::stratifier::error::StratifyError;
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
}

impl Stratum {
    fn new(rule_ids: Vec<usize>, is_recursive: bool) -> Self {
        Self {
            rule_ids,
            is_recursive,
            recursive_relations: Vec::new(),
            leave_relations: Vec::new(),
            available_relations: HashSet::new(),
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
    /// Returns a [`StratifyError`] when the program is structurally invalid:
    /// a forward reference to a later stratum, or an empty recursive stratum.
    pub(crate) fn from_program(program: &Program) -> Result<Self, StratifyError> {
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

        instance.build_stratum_metadata();
        instance.validate_forward_references()?;
        instance.validate_recursive_strata()?;
        instance.warn_aggregation();

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

    /// Rejects references to IDBs unavailable until a later stratum.
    ///
    /// EDBs, same-stratum heads, and relations with no defining rule are valid.
    fn validate_forward_references(&self) -> Result<(), StratifyError> {
        let edb_fps = self.program.edb_fingerprints();
        let program_rules = self.program.rules();
        // An orphan relation has no defining rule and remains empty. Only a
        // relation defined in a later stratum is a forward reference.
        let defined_fps: HashSet<u64> = program_rules
            .iter()
            .map(|rule| rule.head().head_fingerprint())
            .collect();

        for stratum in &self.strata {
            let heads: HashSet<u64> = stratum
                .rule_ids
                .iter()
                .map(|&rule_id| program_rules[rule_id].head().head_fingerprint())
                .collect();

            for &rule_id in &stratum.rule_ids {
                let rule = &program_rules[rule_id];
                for predicate in rule.rhs() {
                    let (fp, atom_span) = match predicate {
                        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => {
                            (atom.fingerprint(), atom.span())
                        }
                        Predicate::Compare(_) => continue,
                    };
                    if edb_fps.contains(&fp)
                        || stratum.available_relations.contains(&fp)
                        || heads.contains(&fp)
                        || !defined_fps.contains(&fp)
                    {
                        continue;
                    }
                    let rel_name = display_name(&self.program, fp, "<unknown>");
                    // Fall back to the rule's span if the atom has no recorded
                    // position (synthesized atoms, dummies in tests).
                    let span = if atom_span.is_dummy() {
                        rule.span()
                    } else {
                        atom_span
                    };
                    return Err(StratifyError::ForwardReference {
                        rule: rule_id,
                        span,
                        rel: rel_name,
                    });
                }
            }
        }
        Ok(())
    }

    /// Validates that each recursive stratum has a feedback relation.
    ///
    /// The emitted iterative scope requires at least one feedback variable.
    fn validate_recursive_strata(&self) -> Result<(), StratifyError> {
        let program_rules = self.program.rules();
        for (idx, stratum) in self.strata.iter().enumerate() {
            if stratum.is_recursive && stratum.recursive_relations.is_empty() {
                let rules = stratum
                    .rule_ids
                    .iter()
                    .map(|&rule_id| (rule_id, program_rules[rule_id].span()))
                    .collect();
                return Err(StratifyError::RecursiveStratumEmpty {
                    stratum: idx + 1,
                    rules,
                });
            }
        }
        Ok(())
    }

    /// Emits warnings for non-monotone aggregation in recursive strata.
    ///
    /// `min` and `max` are monotone and safe in a fixpoint loop. `sum`,
    /// `count`, and `avg` accumulate across iterations and will never
    /// stabilise, so the fixpoint may never be reached.
    fn warn_aggregation(&self) {
        let program_rules = self.program.rules();
        for (idx, stratum) in self.strata.iter().enumerate() {
            if !stratum.is_recursive {
                continue;
            }
            for &rule_id in &stratum.rule_ids {
                let rule = &program_rules[rule_id];
                for arg in rule.head().head_arguments() {
                    if let HeadArg::Aggregation(agg) = arg {
                        match agg.operator() {
                            AggregationOperator::Min | AggregationOperator::Max => {}
                            AggregationOperator::Sum
                            | AggregationOperator::Count
                            | AggregationOperator::Avg => {
                                warn!(
                                    "`{}` in recursive stratum #{} (rule {}): \
                                     not monotone; the fixpoint may never converge.\n  \
                                     Rule {}: {}",
                                    agg.operator(),
                                    idx + 1,
                                    rule_id,
                                    rule_id,
                                    rule
                                );
                            }
                        }
                    }
                }
            }
        }
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
        let fmt_fps = |fps: &[u64]| -> String {
            let mut names: Vec<String> = fps
                .iter()
                .map(|fp| {
                    fp2name
                        .get(fp)
                        .cloned()
                        .unwrap_or_else(|| format!("0x{:016x}", fp))
                })
                .collect();
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

fn body_atom_fps(rule: &FlowLogRule) -> impl Iterator<Item = u64> + '_ {
    rule.rhs().iter().filter_map(|predicate| match predicate {
        Predicate::PositiveAtom(atom) | Predicate::NegativeAtom(atom) => Some(atom.fingerprint()),
        Predicate::Compare(_) => None,
    })
}

/// Returns the source spelling of a relation, including synthesized relations.
fn display_name(program: &Program, fp: u64, canonical: &str) -> String {
    program
        .relation_by_fingerprint(fp)
        .map(|relation| relation.raw_name().to_string())
        .unwrap_or_else(|| canonical.to_string())
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tracing_test::traced_test;

    use super::*;

    fn parse_program(source: &str) -> Program {
        use flowlog_common::Config;
        use flowlog_common::ExecutionMode;
        use flowlog_common::SourceMap;
        use tempfile::NamedTempFile;
        let mut tmp = NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        let mut sm = SourceMap::new();
        let mut config = Config {
            mode: ExecutionMode::Batch,
            ..Default::default()
        };
        flowlog_parser::parse(&tmp.path().to_string_lossy(), &[], &mut sm, &mut config)
            .expect("parse failed")
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
        Stratifier::from_program(&parse_program(src))
            .expect("cross-instance forward reference must stratify");
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
        Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
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
        Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// A non-monotone aggregation (`sum`) heading a recursive rule must
    /// warn: it accumulates across rounds and may never stabilise.
    #[test]
    #[traced_test]
    fn warns_sum_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32, cost: int32)\n\
            .decl Running(x: int32, total: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Running(x, sum(cost)) :- Edge(x, y, cost).\n\
            Running(x, sum(cost)) :- Running(x, prev), Edge(x, y, cost).\n\
            .output Running\n";
        Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
        assert!(logs_contain("`sum` in recursive stratum"));
    }

    /// A monotone aggregation (`min`) heading a recursive rule is safe:
    /// no fixpoint warning.
    #[test]
    #[traced_test]
    fn no_warn_min_in_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32, cost: int32)\n\
            .decl Best(x: int32, b: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            Best(x, min(cost)) :- Edge(x, y, cost).\n\
            Best(x, min(cost)) :- Best(x, b), Edge(x, y, cost).\n\
            .output Best\n";
        Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
        assert!(!logs_contain("fixpoint may never converge"));
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
        let s = Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
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
        let program = parse_program(src);
        let param_fp = program
            .relations()
            .iter()
            .find(|r| r.name() == "param")
            .expect("param relation missing")
            .fingerprint();

        let s = Stratifier::from_program(&program).expect("stratify should succeed");
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
            degree(x, count(y)) :- active_edge(x, y).\n\
            removed(x) :- degree(x, d), d < 2.\n";
        let s = Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");

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
        let program = parse_program(src);
        let s = Stratifier::from_program(&program).expect("stratify should succeed");

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
        let program = parse_program(src);
        let s = Stratifier::from_program(&program).expect("stratify should succeed");

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
        let program = parse_program(src);
        let s = Stratifier::from_program(&program).expect("stratify should succeed");

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
        Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
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
            degree(x, count(y)) :- active_edge(x, y).\n\
            removed(x) :- degree(x, d), d < 2.\n";
        let s = Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
        let stratum = s.strata().first().expect("recursive stratum missing");
        assert!(stratum.recursive_relations().is_sorted());
        assert!(stratum.leave_relations().is_sorted());
    }

    // --- User errors ---

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
        let s = Stratifier::from_program(&parse_program(src)).expect("stratify should succeed");
        assert_eq!(s.strata().len(), 2);
        assert!(!s.strata()[0].is_recursive());
        assert!(s.strata()[1].is_recursive());
        assert_eq!(s.strata()[1].recursive_relations().len(), 1);
    }
}
