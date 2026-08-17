//! Builds ordered strata and derives their relation metadata.
//!
//! Plain rules are ordered through dependency components; explicit loop
//! blocks remain indivisible recursive strata.

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;

use flowlog_common::SUBSECTION_BAR;
use flowlog_error::Span;
use flowlog_parser::AggregationOperator;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::IterativeDirective;
use flowlog_parser::LoopCondition;
use flowlog_parser::Predicate;
use flowlog_parser::Program;
use flowlog_parser::Segment;
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
    loop_condition: Option<LoopCondition>,

    /// Source directives used to split recursive relations during construction.
    iterative_relations: Vec<IterativeDirective>,

    accumulate_recursive_relations: Vec<u64>,
    iterative_recursive_relations: Vec<u64>,
    leave_relations: Vec<u64>,
    available_relations: HashSet<u64>,
}

impl Stratum {
    fn new(
        rule_ids: Vec<usize>,
        is_recursive: bool,
        loop_condition: Option<LoopCondition>,
        iterative_relations: Vec<IterativeDirective>,
    ) -> Self {
        Self {
            rule_ids,
            is_recursive,
            loop_condition,
            iterative_relations,
            accumulate_recursive_relations: Vec::new(),
            iterative_recursive_relations: Vec::new(),
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

    /// Returns the loop condition, or `None` for a plain SCC-derived stratum.
    #[must_use]
    pub(crate) fn loop_condition(&self) -> Option<&LoopCondition> {
        self.loop_condition.as_ref()
    }

    /// Returns recursive relations that retain all prior rounds.
    ///
    /// The fingerprints are sorted. The slice is empty for a non-recursive
    /// stratum.
    #[must_use]
    pub(crate) fn accumulate_recursive_relations(&self) -> &[u64] {
        &self.accumulate_recursive_relations
    }

    /// Returns recursive relations that replace the prior round.
    ///
    /// The fingerprints are sorted. The slice is empty outside loops and for
    /// loops without an `.iterative` directive.
    #[must_use]
    pub(crate) fn iterative_recursive_relations(&self) -> &[u64] {
        &self.iterative_recursive_relations
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
    /// Consecutive plain segments share dependency analysis. Each explicit
    /// loop remains one recursive stratum and an evaluation barrier.
    ///
    /// # Errors
    ///
    /// Returns a [`StratifyError`] when the program is structurally invalid:
    /// recursion outside a `loop`/`fixpoint` block in extended mode, a forward
    /// reference across a loop barrier, an empty recursive stratum, a
    /// malformed `.iterative` directive, or an unreachable loop condition.
    pub(crate) fn from_program(program: &Program, extended: bool) -> Result<Self, StratifyError> {
        let mut strata = Vec::new();
        let mut id_offset = 0usize;

        let segments = program.segments();
        let mut i = 0;
        while i < segments.len() {
            match &segments[i] {
                Segment::Plain(_) => {
                    // Coalesce the maximal run of consecutive `Plain` segments
                    // and stratify it as one unit. Component instances splice
                    // into their own segment per `.init`, so a relation may be
                    // defined in a later instance than it is referenced. One
                    // SCC problem per run makes instance order irrelevant,
                    // matching Souffle's global stratification, while
                    // `Loop`/`Fixpoint` barriers still bound each run.
                    let mut run: Vec<&[FlowLogRule]> = Vec::new();
                    let mut total = 0usize;
                    while let Some(Segment::Plain(rules)) = segments.get(i) {
                        run.push(rules);
                        total += rules.len();
                        i += 1;
                    }
                    let segment_strata = if let [rules] = run[..] {
                        Self::stratify_segment(rules, id_offset, extended)?
                    } else {
                        let combined: Vec<FlowLogRule> =
                            run.iter().flat_map(|r| r.iter().cloned()).collect();
                        Self::stratify_segment(&combined, id_offset, extended)?
                    };
                    strata.extend(segment_strata);
                    id_offset += total;
                }
                Segment::Loop(block) | Segment::Fixpoint(block) => {
                    // A loop/fixpoint block is always exactly one recursive
                    // stratum. No SCC analysis is performed inside: all rules
                    // iterate together under the block's loop condition.
                    let rules = block.rules();
                    let rule_count = rules.len();

                    // Every negative edge inside a loop block is negation
                    // through recursion: the whole block is one recursive
                    // stratum, so no filter is needed.
                    let dep_graph = DependencyGraph::from_rules(rules);
                    Self::warn_negation_edges(&dep_graph, rules, id_offset, |_, _| true);

                    strata.push(Stratum::new(
                        (id_offset..id_offset + rule_count).collect(),
                        true,
                        block.condition().cloned(),
                        block.iterative_relations().to_vec(),
                    ));
                    id_offset += rule_count;
                    i += 1;
                }
            }
        }

        // SCC traversal order is incidental; global rule IDs preserve source
        // order for downstream plans and diagnostics.
        for stratum in &mut strata {
            stratum.rule_ids.sort_unstable();
        }

        let mut instance = Self {
            program: program.clone(),
            strata,
        };

        instance.build_stratum_metadata()?;
        instance.validate_forward_references()?;
        instance.validate_recursive_strata()?;
        instance.validate_loop_conditions()?;
        instance.warn_aggregation();

        debug!("\n{}", instance);
        info!(
            "Successfully stratified program: produced {} strata ({} recursive)",
            instance.strata.len(),
            instance.strata.iter().filter(|s| s.is_recursive).count()
        );

        Ok(instance)
    }

    /// Returns ordered strata for one run of plain rules.
    ///
    /// Rule IDs are 0-based local indices within the slice; on return they
    /// are shifted to global IDs by adding `id_offset`. In Extended Datalog
    /// mode any recursive SCC in the slice is a hard error.
    fn stratify_segment(
        rules: &[FlowLogRule],
        id_offset: usize,
        extended: bool,
    ) -> Result<Vec<Stratum>, StratifyError> {
        if rules.is_empty() {
            return Ok(Vec::new());
        }

        let dep_graph = DependencyGraph::from_rules(rules);
        let components = scc::compute_sccs(&dep_graph);

        if extended {
            for component in &components {
                if component.is_recursive() {
                    let offending: Vec<(usize, Span)> = component
                        .rule_ids()
                        .iter()
                        .map(|&local| (local + id_offset, rules[local].span()))
                        .collect();
                    return Err(StratifyError::RecursionOutsideLoop {
                        rules: offending,
                        hint: "wrap these rules in `fixpoint { ... }` or another loop form",
                    });
                }
            }
        }

        Self::warn_negation_edges(&dep_graph, rules, id_offset, |source, target| {
            scc::is_recursive_edge(&components, source, target)
        });

        let strata = scc::merge_strata(components, &dep_graph)
            .into_iter()
            .map(|component| {
                Stratum::new(
                    component
                        .rule_ids()
                        .iter()
                        .copied()
                        .map(|local| local + id_offset)
                        .collect(),
                    component.is_recursive(),
                    None,
                    Vec::new(),
                )
            })
            .collect();

        Ok(strata)
    }

    // --- Negation warnings ---

    /// Warns for negative dependency edges selected by `include`.
    fn warn_negation_edges(
        dep_graph: &DependencyGraph,
        rules: &[FlowLogRule],
        id_offset: usize,
        include: impl Fn(usize, usize) -> bool,
    ) {
        for &(src, dst) in dep_graph.negative_edges() {
            if !include(src, dst) {
                continue;
            }
            let source_rule = &rules[src];
            if src == dst {
                warn!(
                    "Negation in recursive stratum (rule {} negates itself): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}",
                    src + id_offset,
                    src + id_offset,
                    source_rule
                );
            } else {
                let target_rule = &rules[dst];
                warn!(
                    "Negation in recursive stratum (rule {} negates rule {}): \
                     negation is not monotone; the fixpoint may never converge.\n  \
                     Rule {}: {}\n  Rule {}: {}",
                    src + id_offset,
                    dst + id_offset,
                    src + id_offset,
                    source_rule,
                    dst + id_offset,
                    target_rule
                );
            }
        }
    }

    // --- Stratum metadata ---

    /// Derives recursive, leave, and available relations for every stratum.
    fn build_stratum_metadata(&mut self) -> Result<(), StratifyError> {
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
                    .flat_map(|&rule_id| body_atom_fps(program_rules[rule_id])),
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
                .flat_map(|&rule_id| body_atom_fps(program_rules[rule_id]))
                .collect();

            if stratum.is_recursive {
                let recursive_fps: Vec<u64> = heads.intersection(&body_atoms).copied().collect();

                for directive in &stratum.iterative_relations {
                    let fp = directive.fp();
                    if !heads.contains(&fp) {
                        return Err(StratifyError::IterativeNotInLoopHead {
                            rel: display_name(program, fp, directive.name()),
                            decl_span: directive.span(),
                        });
                    }
                    if recursive_fps.binary_search(&fp).is_err() {
                        return Err(StratifyError::IterativeNotRecursive {
                            rel: display_name(program, fp, directive.name()),
                            decl_span: directive.span(),
                        });
                    }
                }

                let iterative_fps: HashSet<u64> = stratum
                    .iterative_relations
                    .iter()
                    .map(IterativeDirective::fp)
                    .collect();
                let (iterative, accumulate) = recursive_fps
                    .into_iter()
                    .partition(|fp| iterative_fps.contains(fp));
                stratum.iterative_recursive_relations = iterative;
                stratum.accumulate_recursive_relations = accumulate;
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

        Ok(())
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
                let rule = program_rules[rule_id];
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
    /// The compiler's iterative scope requires at least one feedback variable.
    fn validate_recursive_strata(&self) -> Result<(), StratifyError> {
        let program_rules = self.program.rules();
        for (idx, stratum) in self.strata.iter().enumerate() {
            if stratum.is_recursive
                && stratum.accumulate_recursive_relations.is_empty()
                && stratum.iterative_recursive_relations.is_empty()
            {
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

    /// Validates that each loop condition is derived from recursive work.
    ///
    /// A condition that is never derived or is independent of recursion cannot
    /// change across iterations.
    fn validate_loop_conditions(&self) -> Result<(), StratifyError> {
        let program_rules = self.program.rules();
        for stratum in &self.strata {
            let Some(cond) = &stratum.loop_condition else {
                continue;
            };
            let Some(until_group) = cond.until_part() else {
                continue;
            };

            let stratum_rules: Vec<_> = stratum
                .rule_ids
                .iter()
                .map(|&rule_id| program_rules[rule_id].clone())
                .collect();
            let dep_graph = DependencyGraph::from_rules(&stratum_rules);

            let local_head_fp: Vec<u64> = stratum_rules
                .iter()
                .map(|r| r.head().head_fingerprint())
                .collect();
            let heads: HashSet<u64> = local_head_fp.iter().copied().collect();

            let body_fps: HashSet<u64> = stratum_rules.iter().flat_map(body_atom_fps).collect();
            let recursive_fps: HashSet<u64> = heads.intersection(&body_fps).copied().collect();

            let recursive_rule_ids: HashSet<usize> = local_head_fp
                .iter()
                .enumerate()
                .filter(|(_, fp)| recursive_fps.contains(fp))
                .map(|(i, _)| i)
                .collect();

            for rel in until_group.relations() {
                let (rel_name, fp, span) = (rel.name(), rel.fp(), rel.span());

                if !heads.contains(&fp) {
                    return Err(StratifyError::LoopConditionNotDerived {
                        rel: display_name(&self.program, fp, rel_name),
                        span,
                    });
                }

                let seed: Vec<usize> = local_head_fp
                    .iter()
                    .enumerate()
                    .filter(|(_, h)| **h == fp)
                    .map(|(i, _)| i)
                    .collect();
                if !Self::reaches_recursive(&dep_graph, &seed, &recursive_rule_ids) {
                    return Err(StratifyError::LoopConditionNotRecursive {
                        rel: display_name(&self.program, fp, rel_name),
                        span,
                    });
                }
            }
        }
        Ok(())
    }

    /// Returns `true` if any rule in `seeds` transitively depends on a rule in
    /// `targets` via the given dependency graph.
    fn reaches_recursive(
        dep_graph: &DependencyGraph,
        seeds: &[usize],
        targets: &HashSet<usize>,
    ) -> bool {
        let mut visited = HashSet::new();
        let mut stack: Vec<usize> = seeds.to_vec();
        while let Some(cur) = stack.pop() {
            if !visited.insert(cur) {
                continue;
            }
            for &dep in &dep_graph.dependencies()[cur] {
                if targets.contains(&dep) {
                    return true;
                }
                stack.push(dep);
            }
        }
        false
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
                let rule = program_rules[rule_id];
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
            let label = if let Some(cond) = &stratum.loop_condition {
                format!("loop: {}", cond)
            } else if stratum.is_recursive {
                "recursive".to_string()
            } else {
                "non-recursive".to_string()
            };
            let ids = stratum
                .rule_ids
                .iter()
                .sorted()
                .map(|r| r.to_string())
                .join(", ");
            writeln!(f, "#{} [{}] [{}]", idx + 1, label, ids)?;

            if stratum.is_recursive {
                if !stratum.accumulate_recursive_relations.is_empty() {
                    writeln!(
                        f,
                        "  accumulate: [{}]",
                        fmt_fps(&stratum.accumulate_recursive_relations)
                    )?;
                }
                if !stratum.iterative_recursive_relations.is_empty() {
                    writeln!(
                        f,
                        "  iterative:  [{}]",
                        fmt_fps(&stratum.iterative_recursive_relations)
                    )?;
                }
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
        use flowlog_error::SourceMap;
        use tempfile::NamedTempFile;
        let mut tmp = NamedTempFile::new().expect("failed to create temp file");
        tmp.write_all(source.as_bytes())
            .expect("failed to write temp file");
        let mut sm = SourceMap::new();
        let mut config = Config {
            mode: ExecutionMode::ExtendBatch,
            ..Default::default()
        };
        flowlog_parser::parse(&tmp.path().to_string_lossy(), &[], &mut sm, &mut config)
            .expect("parse failed")
    }

    /// Each `.init` splices its component instance into its own `Plain`
    /// segment, so instance `a` negating `b.Keep`, produced by a *later*
    /// instance, is a forward reference across segments. Coalescing the
    /// Plain run before SCC stratification makes instance order irrelevant,
    /// matching Souffle's global stratification.
    #[test]
    fn coalesced_plain_run_stratifies_cross_instance_forward_reference() {
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
        Stratifier::from_program(&parse_program(src), false)
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
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
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
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
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
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
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
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
        assert!(!logs_contain("fixpoint may never converge"));
    }

    /// A `loop` block becomes exactly one recursive stratum tagged with its
    /// condition.
    #[test]
    fn loop_block_is_single_recursive_stratum() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert_eq!(s.strata().len(), 1);
        let stratum = s.strata().first().expect("loop stratum missing");
        assert!(stratum.is_recursive());
        assert!(stratum.loop_condition().is_none());
    }

    /// Plain rules before and after a loop block are stratified independently
    /// from the loop stratum, yielding at least three strata total.
    #[test]
    fn segments_stratified_independently() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Out\n\
            .output Reach\n\
            A(x) :- Edge(x, y).\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n\
            Out(x) :- A(x).\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert!(s.strata().len() >= 3);
        assert_eq!(
            s.strata()
                .iter()
                .filter(|stratum| stratum.is_recursive())
                .count(),
            1
        );
    }

    /// Negation inside a `loop` block is always negation-through-recursion, so
    /// a warning fires, because the whole block is one recursive stratum.
    #[test]
    #[traced_test]
    fn warns_negation_in_loop_block() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            .output B\n\
            fixpoint {\n\
                A(x, y) :- Edge(x, y), !B(x, y).\n\
                B(x, y) :- A(x, y).\n\
            }\n";
        Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert!(logs_contain("Negation in recursive stratum"));
    }

    /// Extended Datalog mode: recursive rules inside a `loop` block are valid.
    #[test]
    fn extended_mode_loop_recursion_ok() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            fixpoint {\n\
                Reach(x, y) :- Edge(x, y).\n\
                Reach(x, z) :- Edge(x, y), Reach(y, z).\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        assert_eq!(s.strata().len(), 1);
        assert!(
            s.strata()
                .first()
                .expect("loop stratum missing")
                .is_recursive()
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

        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");
        let first = s.strata().first().expect("first stratum missing");

        assert!(
            first.available_relations().contains(&param_fp),
            "inline fact relation should be available before the first stratum"
        );
    }

    /// k-core-like loop: `active_edge` and `degree` are iterative (declared),
    /// `removed` is accumulative.  After stratification the two sets must be
    /// split correctly.
    #[test]
    fn loop_iterative_split_correctly() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl active_edge(x: int32, y: int32)\n\
            .decl degree(x: int32, d: int32)\n\
            .decl removed(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output removed\n\
            fixpoint {\n\
                .iterative active_edge\n\
                .iterative degree\n\
                active_edge(x, y) :- edge(x, y), !removed(x), !removed(y).\n\
                degree(x, count(y)) :- active_edge(x, y).\n\
                removed(x) :- degree(x, d), d < 2.\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");

        assert_eq!(s.strata().len(), 1);
        let stratum = s.strata().first().expect("fixpoint stratum missing");
        assert!(stratum.is_recursive());
        let acc = stratum.accumulate_recursive_relations();
        let itr = stratum.iterative_recursive_relations();

        assert_eq!(itr.len(), 2, "active_edge and degree should be iterative");
        // removed feeds back (it appears in active_edge's body), so it is
        // recursive; not declared iterative, so accumulative.
        assert_eq!(acc.len(), 1, "removed should be accumulative");
        let itr_set: HashSet<u64> = itr.iter().copied().collect();
        let acc_set: HashSet<u64> = acc.iter().copied().collect();
        assert!(
            itr_set.is_disjoint(&acc_set),
            "iterative and accumulative sets must be disjoint"
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
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

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
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

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
        let s = Stratifier::from_program(&program, false).expect("stratify should succeed");

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
        Stratifier::from_program(&parse_program(src), false).expect("stratify should succeed");
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
            fixpoint {\n\
                .iterative active_edge\n\
                .iterative degree\n\
                active_edge(x, y) :- edge(x, y), !removed(x), !removed(y).\n\
                degree(x, count(y)) :- active_edge(x, y).\n\
                removed(x) :- degree(x, d), d < 2.\n\
            }\n";
        let s =
            Stratifier::from_program(&parse_program(src), true).expect("stratify should succeed");
        let stratum = s.strata().first().expect("fixpoint stratum missing");
        assert!(stratum.iterative_recursive_relations().is_sorted());
        assert!(stratum.accumulate_recursive_relations().is_sorted());
        assert!(stratum.leave_relations().is_sorted());
    }

    // --- User errors ---

    #[test]
    fn recursion_outside_loop_is_rejected_in_extended_mode() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl Reach(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output Reach\n\
            Reach(x, y) :- Edge(x, y).\n\
            Reach(x, z) :- Edge(x, y), Reach(y, z).\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("plain-rule recursion must be rejected in extended mode");
        // The offending SCC is the single self-referential rule; the hint
        // must name the loop form that fixes it.
        assert!(
            matches!(
                &err,
                StratifyError::RecursionOutsideLoop { rules, hint }
                    if rules.len() == 1 && hint.contains("fixpoint")
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn forward_reference_across_loop_barrier_is_rejected() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            A(x, y) :- B(x, y).\n\
            fixpoint {\n\
                B(x, y) :- Edge(x, y).\n\
                B(x, z) :- Edge(x, y), B(y, z).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("reference to a relation derived only later must be rejected");
        assert!(
            matches!(&err, StratifyError::ForwardReference { rel, .. } if rel == "B"),
            "got {err:?}"
        );
    }

    #[test]
    fn recursive_stratum_without_recursive_relation_is_rejected() {
        let src = "\
            .decl Edge(x: int32, y: int32)\n\
            .decl A(x: int32, y: int32)\n\
            .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
            .output A\n\
            fixpoint {\n\
                A(x, y) :- Edge(x, y).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("a loop block with no feedback relation must be rejected");
        assert!(
            matches!(
                &err,
                StratifyError::RecursiveStratumEmpty { stratum: 1, rules } if rules.len() == 1
            ),
            "got {err:?}"
        );
    }

    #[test]
    fn iterative_relation_not_derived_in_loop_is_rejected() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl reach(x: int32, y: int32)\n\
            .decl ghost(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output reach\n\
            fixpoint {\n\
                .iterative ghost\n\
                reach(x, y) :- edge(x, y).\n\
                reach(x, z) :- edge(x, y), reach(y, z).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("`.iterative` on a relation with no rule in the loop must be rejected");
        assert!(
            matches!(&err, StratifyError::IterativeNotInLoopHead { rel, .. } if rel == "ghost"),
            "got {err:?}"
        );
    }

    #[test]
    fn iterative_relation_that_is_not_recursive_is_rejected() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl reach(x: int32, y: int32)\n\
            .decl sink(x: int32)\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output reach\n\
            .output sink\n\
            fixpoint {\n\
                .iterative sink\n\
                reach(x, y) :- edge(x, y).\n\
                reach(x, z) :- edge(x, y), reach(y, z).\n\
                sink(x) :- reach(x, y).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("`.iterative` on a non-feedback relation must be rejected");
        assert!(
            matches!(&err, StratifyError::IterativeNotRecursive { rel, .. } if rel == "sink"),
            "got {err:?}"
        );
    }

    #[test]
    fn until_condition_not_derived_in_loop_is_rejected() {
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl reach(x: int32, y: int32)\n\
            .decl done()\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output reach\n\
            .output done\n\
            loop until { done } {\n\
                reach(x, y) :- edge(x, y).\n\
                reach(x, z) :- edge(x, y), reach(y, z).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("an until relation never derived in the loop must be rejected");
        assert!(
            matches!(&err, StratifyError::LoopConditionNotDerived { rel, .. } if rel == "done"),
            "got {err:?}"
        );
    }

    #[test]
    fn until_condition_independent_of_recursion_is_rejected() {
        // `done` is derived inside the loop but only from the EDB, so it can
        // never change across iterations.
        let src = "\
            .decl edge(x: int32, y: int32)\n\
            .decl reach(x: int32, y: int32)\n\
            .decl done()\n\
            .input edge(IO=\"file\", filename=\"edge.csv\", delimiter=\",\")\n\
            .output reach\n\
            .output done\n\
            loop until { done } {\n\
                reach(x, y) :- edge(x, y).\n\
                reach(x, z) :- edge(x, y), reach(y, z).\n\
                done() :- edge(x, y).\n\
            }\n";
        let err = Stratifier::from_program(&parse_program(src), true)
            .expect_err("an until relation independent of the recursion must be rejected");
        assert!(
            matches!(&err, StratifyError::LoopConditionNotRecursive { rel, .. } if rel == "done"),
            "got {err:?}"
        );
    }
}
