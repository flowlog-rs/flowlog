//! Stratum planner that plans a stratum (a group of rules).

use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::mem;

use flowlog_common::Config;
use flowlog_common::SECTION_BAR;
use flowlog_common::SUBSECTION_BAR;
use flowlog_parser::AggregationOperator;
use flowlog_parser::FlowLogRule;
use flowlog_parser::HeadArg;
use flowlog_parser::Program;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use tracing::debug;
use tracing::trace;

use crate::catalog::Catalog;
use crate::optimizer::Optimizer;
use crate::planner::PlanError;
use crate::planner::RulePlanner;
use crate::planner::Transformation;
use crate::stratifier::Stratum;

/// Planned transformations and execution metadata for one stratum.
///
/// Equivalent transformations are shared across its rules. Recursive plans
/// separate work that runs once from work repeated at each iteration.
#[derive(Debug, Default)]
pub struct StratumPlanner {
    /// One planner per rule; these own the raw transformation infos.
    rule_planners: Vec<RulePlanner>,

    /// Whether the stratum is recursive.
    is_recursive: bool,

    /// Deduplicated transformations awaiting recursive partitioning.
    transformations: Vec<Transformation>,

    /// Transformations that depend only on EDB inputs; computed once.
    non_recursive_transformations: Vec<Transformation>,

    /// Transformations that touch IDB inputs; re-run during recursion.
    recursive_transformations: Vec<Transformation>,

    /// Fingerprints of collections that enter recursion.
    recursion_enter_collections: Vec<u64>,

    /// Fingerprints of the collections that feed back into the recursion.
    recursion_feedback_collections: Vec<u64>,

    /// Fingerprints of collections that exit recursion.
    recursion_leave_collections: Vec<u64>,

    /// Map each IDB fingerprint to the per-rule head fingerprints that feed it.
    /// Enables the compiler to locate the materialized results per rule.
    idb_to_heads_map: HashMap<u64, Vec<u64>>,

    /// Reverse map: per-rule head fingerprint to IDB fingerprint.
    /// Used to type-check rule outputs against their target IDB.
    head_to_idb_map: HashMap<u64, u64>,

    /// Aggregation metadata keyed by IDB fingerprint.
    /// Only populated for rules whose heads contain an aggregation argument.
    /// Values are `(AggregationOperator, output_position, output_arity)`.
    idb_to_aggregation_map: HashMap<u64, (AggregationOperator, usize, usize)>,

    /// Atom fingerprints unioned across every rule's rhs. Computed once at
    /// stratum construction so codegen can tell which transformation inputs
    /// are named atoms without re-walking the rule planners.
    atom_fps: HashSet<u64>,
}

impl StratumPlanner {
    /// Build a stratum planner from a stratum.
    pub(crate) fn from_stratum(
        config: &Config,
        program: &Program,
        stratified: &Stratum,
        optimizer: &mut Optimizer,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Self, PlanError> {
        let rules = program.rules();
        let stratum: Vec<FlowLogRule> = stratified
            .rule_ids()
            .iter()
            .map(|&rule_id| rules[rule_id].clone())
            .collect();
        let idb_to_aggregation_map = Self::build_idb_to_aggregation_map(&stratum)?;
        let is_recursive = stratified.is_recursive();
        let mut catalogs = Vec::with_capacity(stratum.len());
        let mut rule_planners = Vec::with_capacity(stratum.len());

        trace!("New Stratum");

        // Phase 1 applies local filters and comparisons before join ordering.
        for (i, rule) in stratum.iter().enumerate() {
            trace!("rule[{i}] init:");
            let mut catalog = Catalog::from_rule(rule)?;

            let mut planner = RulePlanner::new(rule.clone());
            planner.prepare(&mut catalog)?;

            debug!(
                "rule[{i}] prepare: {} transformations",
                planner.transformation_infos().len()
            );
            trace!("rule[{i}] after prepare:\n{catalog}");

            catalogs.push(catalog);
            rule_planners.push(planner);
        }

        // Phase 2 pushes filters through joins when SIP is enabled.
        if config.sip_enabled() {
            for (i, planner) in rule_planners.iter_mut().enumerate() {
                trace!("rule[{i}] SIP");
                planner.apply_sip(&mut catalogs[i])?;
            }
        }

        // Phase 3 uses optimizer guidance to limit intermediate join results.
        while !catalogs.iter().all(|c| c.is_planned()) {
            let join_decisions = optimizer.plan_stratum(&catalogs)?;

            for ((planner, catalog), join_decision) in rule_planners
                .iter_mut()
                .zip(catalogs.iter_mut())
                .zip(join_decisions)
            {
                if let Some(join_tuple_index) = join_decision {
                    planner.core(catalog, join_tuple_index)?;
                }
            }
        }

        // Phase 4: Fusion
        // to combine transformations and optimize execution
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter()) {
            planner.fuse(catalog.original_atom_fingerprints())?;
        }

        // Phase 5: Post-processing
        // align final output to the rule head (vars and arithmetic)
        // to apply final adjustments after fusion, e.g. convert to row type
        for (planner, catalog) in rule_planners.iter_mut().zip(catalogs.iter_mut()) {
            planner.post(catalog)?;
        }

        // Phase 6: Materialize per-rule transformations, rewriting lineage
        // (rhs_id-laden) fingerprints to content-canonical ones so identical
        // operations dedup across rules.
        for planner in rule_planners.iter_mut() {
            planner.materialize();
        }

        // Debug info for per-rule plan trees
        rule_planners.iter().for_each(|rp| {
            debug!("{}", rp);
        });

        // Profiling: record rule logic profiles if enabled
        with_plan_graph(plan_graph, |plan_graph| {
            for rule_planner in rule_planners.iter() {
                plan_graph.insert_rule(
                    rule_planner.rule().to_string(),
                    rule_planner
                        .transformations()
                        .iter()
                        .map(|tx| {
                            let inputs = if tx.is_unary() {
                                (tx.unary_input().fingerprint(), None)
                            } else {
                                let (left, right) = tx.binary_input();
                                (left.fingerprint(), Some(right.fingerprint()))
                            };
                            (inputs, tx.output().fingerprint())
                        })
                        .collect(),
                );
            }
        });

        // Phase 7: Cross-rule sharing: dedup the per-rule transformations
        // by content fingerprint
        let atom_fps: HashSet<u64> = rule_planners
            .iter()
            .flat_map(RulePlanner::rhs_atom_fps)
            .collect();
        let mut stratum_planner = Self {
            rule_planners,
            is_recursive,
            recursion_feedback_collections: stratified.recursive_relations().to_vec(),
            recursion_leave_collections: stratified.leave_relations().to_vec(),
            idb_to_aggregation_map,
            atom_fps,
            ..Self::default()
        };
        stratum_planner.deduplicate_transformations();

        // Phase 8: Recursive split and metadata mappings
        // this phase to factoring optimizations
        stratum_planner.build_idb_to_heads_map(&catalogs);
        stratum_planner.identify_recursive_transformations(is_recursive);
        stratum_planner.build_recursion_enter_collections(stratified.available_relations());

        // Debug info for non-recursive vs recursive transformations.
        debug!("\n{}", stratum_planner);

        Ok(stratum_planner)
    }
}

// =========================================================================
// Getters
// =========================================================================
impl StratumPlanner {
    /// Get non-recursive transformations that depend only on EDBs.
    /// These transformations can be computed once outside recursion.
    #[inline]
    pub fn non_recursive_transformations(&self) -> &[Transformation] {
        &self.non_recursive_transformations
    }

    /// Retain only the non-recursive transformations matching `f`. Used by
    /// the cross-stratum prune pass to drop transformations whose output
    /// fingerprint was already produced by an earlier stratum's prelude.
    pub(crate) fn retain_non_recursive_transformations<F>(&mut self, f: F)
    where
        F: FnMut(&Transformation) -> bool,
    {
        self.non_recursive_transformations.retain(f);
    }

    /// Get dynamic transformations that depend on IDB collections.
    /// These transformations must be re-evaluated during recursion.
    #[inline]
    pub fn recursive_transformations(&self) -> &[Transformation] {
        &self.recursive_transformations
    }

    /// Whether `tx` belongs to this stratum's recursive partition, i.e. its
    /// tokens are emitted inside the `iterate` scope (`Product<_, _>` time).
    /// Derived from the same partition the emitters iterate, so codegen cannot
    /// desync the emission scope from the call site.
    pub fn is_recursive_transformation(&self, tx: &Transformation) -> bool {
        self.recursive_transformations.contains(tx)
    }

    /// Get fingerprints of collections that enter recursion.
    #[inline]
    pub fn recursion_enter_collections(&self) -> &[u64] {
        &self.recursion_enter_collections
    }

    /// Get fingerprints of the collections that feed back into the recursion.
    #[inline]
    pub fn recursion_feedback_collections(&self) -> &[u64] {
        &self.recursion_feedback_collections
    }

    /// Get fingerprints of collections that leave recursion.
    #[inline]
    pub fn recursion_leave_collections(&self) -> &[u64] {
        &self.recursion_leave_collections
    }

    /// Output relation fingerprints produced by this stratum.
    #[inline]
    pub fn output_relations(&self) -> HashSet<u64> {
        self.idb_to_heads_map.keys().cloned().collect()
    }

    /// Get the mapping from each IDB fingerprint to per-rule head fingerprints.
    #[inline]
    pub fn idb_to_heads_map(&self) -> &HashMap<u64, Vec<u64>> {
        &self.idb_to_heads_map
    }

    /// Returns the IDB fingerprint for each per-rule head fingerprint.
    #[inline]
    pub fn head_to_idb_map(&self) -> &HashMap<u64, u64> {
        &self.head_to_idb_map
    }

    /// Get the mapping from IDB fingerprint to corresponding aggregation.
    /// Returns tuples of `(AggregationOperator, position in output relation,
    /// output arity)`.
    #[inline]
    pub fn idb_to_aggregation_map(&self) -> &HashMap<u64, (AggregationOperator, usize, usize)> {
        &self.idb_to_aggregation_map
    }

    /// Map of atom fingerprint to `"name(arg1, ..., argN)"` label for every
    /// positive/negative atom on any rule's rhs in this stratum. Used by
    /// codegen to annotate operator names with the EDB atom they consume so
    /// the profiler/visualizer can show `[Row -> KV] K:(V0) arc(x, y)` without
    /// any downstream knowledge of atoms.
    #[inline]
    pub fn atom_fps(&self) -> &HashSet<u64> {
        &self.atom_fps
    }

    /// Check if this stratum is recursive.
    #[inline]
    pub fn is_recursive(&self) -> bool {
        self.is_recursive
    }

    /// Test-only: per-rule transformations before cross-rule dedup.
    #[cfg(test)]
    pub(crate) fn rule_planners(&self) -> &[RulePlanner] {
        &self.rule_planners
    }
}

impl fmt::Display for StratumPlanner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "{}", SECTION_BAR)?;

        let stratum_name = if self.is_recursive {
            "Recursive Stratum"
        } else {
            "Non-Recursive Stratum"
        };
        writeln!(f, "[{stratum_name}] {} rules", self.rule_planners.len())?;
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        writeln!(f, "Rules:")?;
        for (idx, rule_planner) in self.rule_planners.iter().enumerate() {
            writeln!(f, "  ({:>2}) {:?}", idx, rule_planner.rule())?;
        }
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        writeln!(
            f,
            "Non-recursive transformations ({}):",
            self.non_recursive_transformations.len()
        )?;
        for (idx, tx) in self.non_recursive_transformations.iter().enumerate() {
            write!(f, "  [N{:>3}] {}", idx, tx)?;
        }
        writeln!(f, "\n{}", SUBSECTION_BAR)?;

        if self.is_recursive {
            writeln!(
                f,
                "Recursive transformations ({}):",
                self.recursive_transformations.len()
            )?;
            for (idx, tx) in self.recursive_transformations.iter().enumerate() {
                write!(f, "  [R{:>3}] {}", idx, tx)?;
            }
        } else {
            writeln!(f, "(Non-recursive stratum: no recursive transformations)")?;
        }

        if !self.idb_to_aggregation_map.is_empty() {
            writeln!(f, "\n{}", SUBSECTION_BAR)?;
            writeln!(f, "IDB to Aggregation Map:")?;
            for (fp, (op, pos, arity)) in &self.idb_to_aggregation_map {
                writeln!(
                    f,
                    "  fp=0x{:016x},\n  op={:?},\n  pos={},\n  arity={}",
                    fp, op, pos, arity
                )?;
            }
        }

        writeln!(f, "{}", SECTION_BAR)
    }
}

// =========================================================================
// Sharing Optimization
// =========================================================================
impl StratumPlanner {
    /// Dedup the per-rule materialized transformations by content
    /// fingerprint (first occurrence wins; order stays topological).
    fn deduplicate_transformations(&mut self) {
        let mut seen = HashSet::new();
        self.transformations = self
            .rule_planners
            .iter()
            .flat_map(|planner| planner.transformations())
            .filter(|tx| seen.insert(tx.output().fingerprint()))
            .cloned()
            .collect();
    }
}

// =========================================================================
// Recursive/Non-Recursive Separation
// =========================================================================
impl StratumPlanner {
    /// Splits transformations into one-time and per-iteration work.
    ///
    /// In recursive strata, a transformation is recursive when it consumes a
    /// stratum output or another recursive transformation.
    fn identify_recursive_transformations(&mut self, is_recursive: bool) {
        if !is_recursive {
            self.non_recursive_transformations = mem::take(&mut self.transformations);
            debug!(
                "Non-recursive stratum: all {} transformations are non-recursive",
                self.non_recursive_transformations.len()
            );
            return;
        }

        let mut dynamic_fingerprints: HashSet<u64> =
            self.idb_to_heads_map.keys().copied().collect();

        let mut dynamic_indices = HashSet::new();

        for (i, transformation) in self.transformations.iter().enumerate() {
            let consumes_dynamic = if transformation.is_unary() {
                let input_fp = transformation.unary_input().fingerprint();
                dynamic_fingerprints.contains(&input_fp)
            } else {
                let (left, right) = transformation.binary_input();
                dynamic_fingerprints.contains(&left.fingerprint())
                    || dynamic_fingerprints.contains(&right.fingerprint())
            };

            if consumes_dynamic {
                dynamic_indices.insert(i);
                dynamic_fingerprints.insert(transformation.output().fingerprint());
            }
        }

        for (i, transformation) in mem::take(&mut self.transformations).into_iter().enumerate() {
            if dynamic_indices.contains(&i) {
                self.recursive_transformations.push(transformation);
            } else {
                self.non_recursive_transformations.push(transformation);
            }
        }

        debug!(
            "Recursive stratum: separated {} non-recursive, {} recursive transformations (total: {})",
            self.non_recursive_transformations.len(),
            self.recursive_transformations.len(),
            self.non_recursive_transformations.len() + self.recursive_transformations.len()
        );
    }
}

// =========================================================================
// Metadata Mappings
// =========================================================================
impl StratumPlanner {
    /// Build the fingerprint of collections that enter recursion.
    fn build_recursion_enter_collections(&mut self, available_relations: &HashSet<u64>) {
        let mut recursion_input_fps: HashSet<u64> = HashSet::new();
        let mut recursion_output_fps: HashSet<u64> = HashSet::new();
        let mut available_fps = available_relations.clone();

        for tx in &self.non_recursive_transformations {
            available_fps.insert(tx.output().fingerprint());
        }

        for tx in &self.recursive_transformations {
            if tx.is_unary() {
                recursion_input_fps.insert(tx.unary_input().fingerprint());
            } else {
                let (left, right) = tx.binary_input();
                recursion_input_fps.insert(left.fingerprint());
                recursion_input_fps.insert(right.fingerprint());
            }
            recursion_output_fps.insert(tx.output().fingerprint());
        }

        // A value enters recursion only when no repeated transformation
        // produces it.
        self.recursion_enter_collections = recursion_input_fps
            .difference(&recursion_output_fps)
            .filter(|fp| available_fps.contains(fp))
            .copied()
            .collect();
        self.recursion_enter_collections.sort_unstable();
    }

    /// Maps each IDB fingerprint to the materialized rule heads that produce
    /// it.
    ///
    /// Shared transformations may leave a rule without a distinct materialized
    /// head.
    fn build_idb_to_heads_map(&mut self, catalogs: &[Catalog]) {
        for (rule_idx, catalog) in catalogs.iter().enumerate() {
            let head_idb_fp = catalog.head_idb_fingerprint();
            let Some(final_tx) = self.rule_planners[rule_idx].transformations().last() else {
                continue;
            };
            let head_fp = final_tx.output().fingerprint();
            // Rules with identical pipelines share one head fp; record it once.
            let heads = self.idb_to_heads_map.entry(head_idb_fp).or_default();
            if !heads.contains(&head_fp) {
                heads.push(head_fp);
            }
            self.head_to_idb_map.insert(head_fp, head_idb_fp);
        }
    }

    /// Returns aggregation metadata after checking compatibility within one
    /// stratum.
    ///
    /// # Errors
    ///
    /// Returns [`PlanError::InconsistentAggregation`] when rules deriving the
    /// same relation use different aggregation operators or head positions.
    fn build_idb_to_aggregation_map(
        rules: &[FlowLogRule],
    ) -> Result<HashMap<u64, (AggregationOperator, usize, usize)>, PlanError> {
        let mut aggregations = HashMap::new();
        let mut first_spans = HashMap::new();

        for rule in rules {
            let head = rule.head();
            let head_args = head.head_arguments();
            let Some((pos, op)) = head_args.iter().enumerate().find_map(|(i, arg)| match arg {
                HeadArg::Aggregation(agg) => Some((i, *agg.operator())),
                HeadArg::Var(_) | HeadArg::Arith(_) => None,
            }) else {
                continue;
            };

            let arity = head_args.len();
            let head_idb_fp = head.head_fingerprint();

            match aggregations.get(&head_idb_fp) {
                Some(&(existing_op, existing_pos, _))
                    if (existing_op, existing_pos) != (op, pos) =>
                {
                    return Err(PlanError::InconsistentAggregation {
                        head_span: head.span(),
                        prior_head_span: first_spans[&head_idb_fp],
                        rel: head.raw_name().to_string(),
                        existing_op,
                        existing_pos,
                        found_op: op,
                        found_pos: pos,
                    });
                }
                None => {
                    aggregations.insert(head_idb_fp, (op, pos, arity));
                    first_spans.insert(head_idb_fp, head.span());
                }
                Some(_) => {}
            }
        }
        Ok(aggregations)
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::SourceMap;
    use tempfile::NamedTempFile;

    use super::*;

    fn parse_rules(src: &str) -> (Vec<FlowLogRule>, SourceMap) {
        let mut file = NamedTempFile::new().expect("tempfile");
        file.write_all(src.as_bytes()).expect("write");
        let mut sources = SourceMap::new();
        let mut config = Config::default();
        let program = flowlog_parser::parse(
            &file.path().to_string_lossy(),
            &[],
            &mut sources,
            &mut config,
        )
        .expect("parse");
        let rules = program.rules().to_vec();
        (rules, sources)
    }

    #[test]
    fn conflicting_aggregation_operators_in_one_stratum_are_rejected() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n\
            Totals(max(amount)) :- Orders(id, amount).\n";
        let (rules, sources) = parse_rules(src);

        let err = StratumPlanner::build_idb_to_aggregation_map(&rules)
            .expect_err("one stratum must use one aggregation shape");
        let PlanError::InconsistentAggregation {
            head_span,
            prior_head_span,
            rel,
            existing_op,
            existing_pos,
            found_op,
            found_pos,
        } = err
        else {
            panic!("expected inconsistent aggregation, got {err}");
        };

        assert_eq!(rel, "Totals");
        assert_eq!(existing_op, AggregationOperator::Sum);
        assert_eq!(existing_pos, 0);
        assert_eq!(found_op, AggregationOperator::Max);
        assert_eq!(found_pos, 0);
        assert_eq!(sources.snippet(prior_head_span), "Totals(sum(amount))");
        assert_eq!(sources.snippet(head_span), "Totals(max(amount))");
    }

    #[test]
    fn conflicting_aggregation_positions_in_one_stratum_are_rejected() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(left: int32, right: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(id, sum(amount)) :- Orders(id, amount).\n\
            Totals(sum(amount), id) :- Orders(id, amount).\n";
        let (rules, _) = parse_rules(src);

        let err = StratumPlanner::build_idb_to_aggregation_map(&rules)
            .expect_err("one stratum must use one aggregation position");
        assert!(matches!(
            err,
            PlanError::InconsistentAggregation {
                existing_op: AggregationOperator::Sum,
                existing_pos: 1,
                found_op: AggregationOperator::Sum,
                found_pos: 0,
                ..
            }
        ));
    }

    #[test]
    fn different_strata_may_use_different_aggregation_shapes() {
        let src = "\
            .decl Orders(id: int32, amount: int32)\n\
            .decl Totals(total: int32)\n\
            .input Orders(IO=\"file\", filename=\"Orders.csv\", delimiter=\",\")\n\
            .output Totals\n\
            Totals(sum(amount)) :- Orders(id, amount).\n\
            Totals(max(amount)) :- Orders(id, amount).\n";
        let (rules, _) = parse_rules(src);

        StratumPlanner::build_idb_to_aggregation_map(&rules[..1])
            .expect("the first stratum should be valid");
        StratumPlanner::build_idb_to_aggregation_map(&rules[1..])
            .expect("the second stratum should be valid");
    }
}
