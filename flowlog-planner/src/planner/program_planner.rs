//! Whole-program planner: owns the per-stratum plans after cross-stratum
//! dedup of redundant non-recursive transformations.

use std::collections::HashMap;
use std::collections::HashSet;

use flowlog_common::BoxError;
use flowlog_parser::Program;
use flowlog_profiler::PlanGraph;

use crate::optimizer::Optimizer;
use crate::planner::StratumPlanner;
use crate::stratifier::Stratifier;

/// Whole-program planning.
#[derive(Debug)]
pub struct ProgramPlanner {
    strata: Vec<StratumPlanner>,
}

impl ProgramPlanner {
    /// Run the full planner pipeline against `program`: stratify, build a
    /// [`StratumPlanner`] per stratum, prune cross-stratum duplicates.
    pub fn from_program(
        program: &Program,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Self, BoxError> {
        let stratifier = Stratifier::from_program(program);
        let mut optimizer = Optimizer::new();
        let mut strata: Vec<StratumPlanner> = stratifier
            .strata()
            .iter()
            .map(|stratum| {
                StratumPlanner::from_stratum(program, stratum, &mut optimizer, plan_graph)
                    .map_err(BoxError::from)
            })
            .collect::<Result<_, _>>()?;
        prune_cross_stratum_duplicates(&mut strata);
        Ok(Self { strata })
    }

    pub fn strata(&self) -> &[StratumPlanner] {
        &self.strata
    }
}

/// Drop non-recursive transformations whose output fingerprint was already
/// emitted by an earlier stratum's prelude.
///
/// Soundness: the earlier binding only stays correct as long as none of the
/// IDBs its value transitively depends on have been updated between the two
/// strata. Stratification puts all consumers after all definers, so this
/// holds; the transitive check below keeps the later emission on the
/// remaining cases where an IDB is rewritten between two consumers of the
/// same content-addressed transformation.
fn prune_cross_stratum_duplicates(strata: &mut [StratumPlanner]) {
    let mut idb_writes: HashMap<u64, Vec<usize>> = HashMap::new();
    for (idx, stratum) in strata.iter().enumerate() {
        for fp in stratum.idb_to_heads_map().keys() {
            idb_writes.entry(*fp).or_default().push(idx);
        }
    }

    // Transitive set of IDB-head fps each fp's runtime value depends on.
    // IDB heads depend on themselves; intermediates inherit from their inputs.
    let mut idb_deps: HashMap<u64, HashSet<u64>> = idb_writes
        .keys()
        .map(|&fp| (fp, HashSet::from([fp])))
        .collect();
    let mut emitted_at: HashMap<u64, usize> = HashMap::new();

    for (idx, stratum) in strata.iter_mut().enumerate() {
        stratum.retain_non_recursive_transformations(|t| {
            let fp = t.output().fingerprint();
            let t_deps: HashSet<u64> = t
                .input_fingerprints()
                .into_iter()
                .filter_map(|f| idb_deps.get(&f))
                .flatten()
                .copied()
                .collect();
            idb_deps.entry(fp).or_default().extend(&t_deps);

            let keep = match emitted_at.get(&fp) {
                None => true,
                Some(&prev) => t_deps
                    .iter()
                    .any(|idb| idb_writes[idb].iter().any(|&k| k > prev && k <= idx)),
            };
            if keep {
                emitted_at.insert(fp, idx);
            }
            keep
        });
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use flowlog_common::Config;
    use flowlog_common::SourceMap;
    use flowlog_common::compute_fp;
    use tempfile::NamedTempFile;

    use super::*;

    /// Round-trip a tiny program through parse, typecheck, and program-plan,
    /// mirroring the temp-file pattern used by `stratifier::core::tests`.
    fn analyze(src: &str) -> ProgramPlanner {
        let mut tmp = NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let mut config = Config::default();
        let program =
            flowlog_parser::parse(&tmp.path().to_string_lossy(), &[], &mut sm, &mut config)
                .expect("parse");
        ProgramPlanner::from_program(&program, &mut None).expect("plan")
    }

    /// Canonical form of the head collection derived for relation `name`,
    /// rendered; the relation must be produced by exactly one rule.
    fn head_form(pp: &ProgramPlanner, name: &str) -> String {
        let idb = compute_fp(name);
        let stratum = pp
            .strata()
            .iter()
            .find(|stratum| stratum.idb_to_heads_map().contains_key(&idb))
            .expect("relation is produced by some stratum");
        let [head] = stratum.idb_to_heads_map()[&idb].as_slice() else {
            panic!("relation {name} has more than one rule");
        };
        stratum
            .non_recursive_transformations()
            .iter()
            .chain(stratum.recursive_transformations())
            .find(|tx| tx.output().fingerprint() == *head)
            .expect("head is produced by a transformation")
            .output()
            .canonical()
            .to_string()
    }

    /// Dyck has three strata:
    ///   0: `Zero`, `One` rule heads from `Arc`.
    ///   1: `Dyck` base joins (`Zero join Zero`, `One join One`),
    ///      non-recursive.
    ///   2: `Dyck` recursive joins (`Zero join Dyck join Zero`, ...),
    ///      recursive.
    ///
    /// Stratum 2's prelude would otherwise re-key `Zero`/`One` the same way
    /// stratum 1 does. The prune drops those duplicate emissions; what
    /// survives across strata forms a partition over output fingerprints.
    const DYCK_SRC: &str = "\
        .decl Arc(x: int32, y: int32, l: int32)\n\
        .input Arc(IO=\"file\", filename=\"Arc.csv\", delimiter=\",\")\n\
        .decl Zero(x: int32, y: int32)\n\
        .printsize Zero\n\
        .decl One(x: int32, y: int32)\n\
        .printsize One\n\
        .decl Dyck(x: int32, y: int32)\n\
        .printsize Dyck\n\
        Zero(x, y) :- Arc(x, y, 0).\n\
        One(x, y) :- Arc(x, y, 1).\n\
        Dyck(x, y) :- Zero(x, z), Zero(z, y).\n\
        Dyck(x, y) :- One(x, z), One(z, y).\n\
        Dyck(x, y) :- Zero(x, z), Dyck(z, w), Zero(w, y).\n\
        Dyck(x, y) :- One(x, z), Dyck(z, w), One(w, y).\n\
        Dyck(x, y) :- Dyck(x, z), Dyck(z, y).\n";

    #[test]
    fn dyck_prune_collapses_cross_stratum_duplicates() {
        let pp = analyze(DYCK_SRC);
        assert_eq!(pp.strata().len(), 3, "dyck should stratify into 3 strata");

        // Structural invariant: each surviving output fingerprint belongs to
        // exactly one stratum: no duplicate emissions across strata.
        let mut owner: HashMap<u64, usize> = HashMap::new();
        for (idx, stratum) in pp.strata().iter().enumerate() {
            for t in stratum.non_recursive_transformations() {
                let fp = t.output().fingerprint();
                if let Some(prev) = owner.insert(fp, idx) {
                    panic!("fp 0x{fp:016x} survives in both stratum {prev} and stratum {idx}");
                }
            }
        }

        // Headline count: 12 unpruned becomes 8 after prune (four re-keys
        // collapse). Locks the savings number in for regression.
        assert_eq!(
            owner.len(),
            8,
            "expected 8 non-recursive transformations after prune"
        );
    }

    /// Both rules key `B` on its first column, but at different rhs
    /// positions (1 vs 0). Lineage fps embed that position; content-canonical
    /// materialization must share one arrangement.
    const RHS_ID_SHARING_SRC: &str = "\
        .decl A(x: int32, y: int32)\n\
        .decl B(x: int32, y: int32)\n\
        .decl C(x: int32, y: int32)\n\
        .decl Out1(x: int32, y: int32)\n\
        .decl Out2(x: int32, y: int32)\n\
        .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
        .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
        .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
        .output Out1\n\
        .output Out2\n\
        Out1(x, y) :- A(x, z), B(z, y).\n\
        Out2(x, y) :- B(z, y), C(x, z).\n";

    #[test]
    fn rhs_id_does_not_split_identical_arrangements() {
        let pp = analyze(RHS_ID_SHARING_SRC);
        let b_fp = compute_fp("b");

        let b_arrangements: Vec<_> = pp
            .strata()
            .iter()
            .flat_map(|s| s.non_recursive_transformations())
            .filter(|t| t.is_unary() && t.unary_input().fingerprint() == b_fp)
            .collect();
        assert_eq!(
            b_arrangements.len(),
            1,
            "both rules key B on its first column; the arrangement must be shared"
        );

        // Sharing must be wired in: both joins consume the shared output.
        let shared_fp = b_arrangements[0].output().fingerprint();
        let consumers = pp
            .strata()
            .iter()
            .flat_map(|s| s.non_recursive_transformations())
            .filter(|t| !t.is_unary())
            .filter(|t| {
                let (left, right) = t.binary_input();
                left.fingerprint() == shared_fp || right.fingerprint() == shared_fp
            })
            .count();
        assert_eq!(
            consumers, 2,
            "both joins must consume the shared B arrangement"
        );
    }

    /// Tripwire against rhs_id-blind hashing: P and Q pair the same
    /// occurrences to swapped output slots. The positional flow preserves
    /// the pairing, so the heads must stay distinct (merging makes Q = P).
    #[test]
    fn swapped_output_columns_stay_distinct() {
        let pp = analyze(
            "\
            .decl R(k: int32, v: int32)\n\
            .decl S(k: int32, v: int32)\n\
            .decl P(a: int32, b: int32)\n\
            .decl Q(a: int32, b: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(a, b) :- R(k, a), S(k, b).\n\
            Q(a, b) :- R(k, b), S(k, a).\n",
        );

        let heads: Vec<u64> = pp
            .strata()
            .iter()
            .flat_map(|s| s.idb_to_heads_map().values().flatten().copied())
            .collect();
        assert_eq!(heads.len(), 2, "P and Q must each keep their own head");
        assert_ne!(
            heads[0], heads[1],
            "swapped output columns must not collapse into one head"
        );
    }

    /// Equal output fingerprint must imply equal content (operation, input
    /// fps, flow) across all per-rule transformations; otherwise dedup
    /// would substitute a different transformation.
    #[test]
    fn equal_fingerprint_implies_equal_content() {
        use crate::planner::TransformationFlow;

        for src in [DYCK_SRC, RHS_ID_SHARING_SRC] {
            let pp = analyze(src);
            let mut seen: HashMap<u64, (&str, Vec<u64>, TransformationFlow)> = HashMap::new();
            for stratum in pp.strata() {
                for planner in stratum.rule_planners() {
                    for tx in planner.transformations() {
                        let fp = tx.output().fingerprint();
                        let content = (
                            tx.operation_name(),
                            tx.input_fingerprints(),
                            tx.flow().clone(),
                        );
                        match seen.get(&fp) {
                            None => {
                                seen.insert(fp, content);
                            }
                            Some(prev) => assert_eq!(
                                *prev, content,
                                "fingerprint 0x{fp:016x} maps to two different contents"
                            ),
                        }
                    }
                }
            }
        }
    }

    /// The user-facing motivation for canonical forms: two rules over the
    /// same join keep different columns, so their fingerprints diverge at
    /// the first projection, while their forms differ only in the values.
    #[test]
    fn projections_of_one_join_share_a_form_up_to_their_outputs() {
        let pp = analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32, w: int32)\n\
            .decl T1(x: int32, z: int32)\n\
            .decl T2(x: int32, w: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output T1\n\
            .output T2\n\
            T1(x, z) :- R(x, y), S(y, z, w).\n\
            T2(x, w) :- R(x, y), S(y, z, w).\n",
        );

        assert_eq!(
            head_form(&pp, "t1"),
            "atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.1)"
        );
        assert_eq!(
            head_form(&pp, "t2"),
            "atoms(r, s) not() eq(0.1 = 1.0) where() key() value(0.0, 1.2)"
        );
    }

    /// A semijoin is the join with the filter side's columns projected
    /// away, and its form says so.
    #[test]
    fn semijoin_form_is_the_join_form_without_the_filter_columns() {
        let pp = analyze(
            "\
            .decl R(k: int32, a: int32)\n\
            .decl S(k: int32, b: int32)\n\
            .decl Wide(a: int32, b: int32)\n\
            .decl Semi(a: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Wide\n\
            .output Semi\n\
            Wide(a, b) :- R(k, a), S(k, b).\n\
            Semi(a) :- R(k, a), S(k, _).\n",
        );

        assert_eq!(
            head_form(&pp, "wide"),
            "atoms(r, s) not() eq(0.0 = 1.0) where() key() value(0.1, 1.1)"
        );
        assert_eq!(
            head_form(&pp, "semi"),
            "atoms(r, s) not() eq(0.0 = 1.0) where() key() value(0.1)"
        );
    }

    #[test]
    fn negated_atom_is_bound_to_the_positive_column_it_filters() {
        let pp = analyze(
            "\
            .decl B(x: int32, y: int32)\n\
            .decl C(x: int32, z: int32)\n\
            .decl N(x: int32)\n\
            .decl Out(x: int32, y: int32, z: int32)\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .input N(IO=\"file\", filename=\"N.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y, z) :- B(x, y), C(x, z), !N(x).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(b, c) not(n) eq(0.0 = 1.0 = !0.0) where() key() value(0.0, 0.1, 1.1)"
        );
    }

    #[test]
    fn constant_arguments_and_comparisons_are_filters_of_the_head_form() {
        let pp = analyze(
            "\
            .decl A(x: int32, y: int32)\n\
            .decl Out(x: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x) :- A(x, 5), x > 3.\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a) not() eq() where(0.0 > 3, 0.1 = 5) key() value(0.0)"
        );
    }

    /// Body order decides which read the plan joins on the left; the
    /// form does not depend on it.
    #[test]
    fn body_order_does_not_change_the_head_form() {
        let pp = analyze(
            "\
            .decl E(x: int32, y: int32)\n\
            .decl P(x: int32, z: int32)\n\
            .decl Q(x: int32, z: int32)\n\
            .input E(IO=\"file\", filename=\"E.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(x, z) :- E(x, y), E(y, z).\n\
            Q(x, z) :- E(y, z), E(x, y).\n",
        );

        assert_eq!(
            head_form(&pp, "p"),
            "atoms(e, e) not() eq(0.0 = 1.1) where() key() value(1.0, 0.1)"
        );
        assert_eq!(head_form(&pp, "q"), head_form(&pp, "p"));
    }

    #[test]
    fn spanning_equality_between_computed_values_is_a_filter() {
        let pp = analyze(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Out(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y) :- A(x), B(y), x + 1 = y + 2.\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, b) not() eq() where(0.0 + 1 = 1.0 + 2) key() value(0.0, 1.0)"
        );
    }

    /// Core folds `A` into `B` and pushdown copies it onto `C`; the head
    /// form reads `A` once.
    #[test]
    fn pushdown_copies_of_a_filter_collapse_into_one_read() {
        let pp = analyze(
            "\
            .decl A(x: int32)\n\
            .decl B(x: int32, y: int32)\n\
            .decl C(x: int32, z: int32, v: int32)\n\
            .decl D(z: int32, w: int32)\n\
            .decl Out(x: int32, y: int32, z: int32, w: int32, v: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .input C(IO=\"file\", filename=\"C.csv\", delimiter=\",\")\n\
            .input D(IO=\"file\", filename=\"D.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(x, y, z, w, v) :- C(x, z, v), D(z, w), B(x, y), A(x).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, b, c, d) not() eq(0.0 = 1.0 = 2.0; 2.1 = 3.0) where() key() \
             value(0.0, 1.1, 2.1, 3.1, 2.2)"
        );
    }

    /// Two reads of one relation that each keep a column the other drops
    /// ask for two rows; the head form keeps both reads.
    #[test]
    fn self_join_on_different_columns_keeps_both_reads() {
        let pp = analyze(
            "\
            .decl A(x: int32, y: int32, z: int32)\n\
            .decl Out(y: int32, w: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .output Out\n\
            Out(y, w) :- A(x, y, _), A(x, _, w).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(a, a) not() eq(0.0 = 1.0) where() key() value(0.1, 1.2)"
        );
    }

    /// The tuple projection `c.0` keys the join from the `Mk` side; the
    /// head spells `v` by the plain column it equals.
    #[test]
    fn tuple_projection_key_is_spelled_by_the_column_it_equals() {
        let pp = analyze(
            "\
            .type P = (a: int32, b: int32)\n\
            .decl Base(a: int32, b: int32)\n\
            .decl Mk(c: P)\n\
            .decl Val(v: int32, t: int32)\n\
            .decl Out(v: int32, t: int32)\n\
            .input Base(IO=\"file\", filename=\"Base.csv\", delimiter=\",\")\n\
            .input Val(IO=\"file\", filename=\"Val.csv\", delimiter=\",\")\n\
            .output Out\n\
            Mk(c) :- Base(a, b), c = (a, b).\n\
            Out(v, t) :- Mk(c), Val(v, t), c = (v, w).\n",
        );

        assert_eq!(
            head_form(&pp, "out"),
            "atoms(mk, val) not() eq() where(1.0 = (0.0).0) key() value(1.0, 1.1)"
        );
    }
}
