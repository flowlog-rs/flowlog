//! Whole-program planner: plans the strata in order, each sharing work
//! with the ones before it.

use flowlog_common::BoxError;
use flowlog_parser::Program;
use flowlog_profiler::PlanGraph;

use crate::optimizer::Optimizer;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;
use crate::stratifier::Stratifier;

/// Whole-program planning.
#[derive(Debug)]
pub struct ProgramPlanner {
    strata: Vec<StratumPlanner>,
}

impl ProgramPlanner {
    /// Runs the full planner pipeline against `program`: stratify, then
    /// build a [`StratumPlanner`] per stratum in order, each sharing the
    /// collections the preludes before it compute.
    pub fn from_program(
        program: &Program,
        plan_graph: &mut Option<PlanGraph>,
    ) -> Result<Self, BoxError> {
        let stratifier = Stratifier::from_program(program);
        let mut optimizer = Optimizer::new();
        let mut preludes: Vec<Transformation> = Vec::new();
        let mut strata = Vec::with_capacity(stratifier.strata().len());
        for stratum in stratifier.strata() {
            let planned = StratumPlanner::from_stratum(
                program,
                stratum,
                &mut optimizer,
                plan_graph,
                &preludes,
            )
            .map_err(BoxError::from)?;
            preludes.extend(planned.non_recursive_transformations().iter().cloned());
            strata.push(planned);
        }
        Ok(Self { strata })
    }

    pub fn strata(&self) -> &[StratumPlanner] {
        &self.strata
    }
}

#[cfg(test)]
impl ProgramPlanner {
    /// Plans `src` end to end: parse from a temporary file, typecheck,
    /// plan. Parsing is the smallest entry that yields planned strata, so
    /// tests of the stratum-level passes drive them from source too.
    pub(crate) fn analyze(src: &str) -> Self {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = flowlog_common::SourceMap::new();
        let mut config = flowlog_common::Config::default();
        let program =
            flowlog_parser::parse(&tmp.path().to_string_lossy(), &[], &mut sm, &mut config)
                .expect("parse");
        Self::from_program(&program, &mut None).expect("plan")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use flowlog_common::compute_fp;

    use super::*;

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

    /// The recursive Dyck stratum arranges `zero` and `one` exactly as the
    /// base stratum before it did; it reads those arrangements instead.
    #[test]
    fn dyck_reads_the_base_stratum_arrangements_instead_of_rebuilding_them() {
        let pp = ProgramPlanner::analyze(DYCK_SRC);
        assert_eq!(pp.strata().len(), 3, "dyck should stratify into 3 strata");

        // No prelude collection is computed by two strata.
        let mut owner: HashMap<u64, usize> = HashMap::new();
        for (idx, stratum) in pp.strata().iter().enumerate() {
            for t in stratum.non_recursive_transformations() {
                let fp = t.output().fingerprint();
                if let Some(prev) = owner.insert(fp, idx) {
                    panic!("fp 0x{fp:016x} survives in both stratum {prev} and stratum {idx}");
                }
            }
        }

        // Twelve prelude collections as planned, eight once the recursive
        // stratum reads the four arrangements the base stratum built.
        assert_eq!(owner.len(), 8, "expected 8 prelude transformations");
    }

    /// `Q`'s join of `R` and `S` holds the rows `P` computed one stratum
    /// earlier, keyed differently, so the later stratum arranges `P`'s join
    /// instead of joining again.
    #[test]
    fn later_stratum_arranges_an_earlier_join_instead_of_recomputing_it() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32)\n\
            .decl P(x: int32, y: int32, z: int32)\n\
            .decl Q(x: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(x, y, z) :- R(x, y), S(y, z).\n\
            Q(x) :- R(x, y), S(y, z), P(x, _, z).\n",
        );
        assert_eq!(
            pp.strata().len(),
            2,
            "Q reads P, so it plans one stratum later"
        );
        let [first, second] = pp.strata() else {
            unreachable!()
        };
        let joins = |stratum: &StratumPlanner| {
            stratum
                .non_recursive_transformations()
                .iter()
                .filter(|tx| !tx.is_unary())
                .map(|tx| tx.output().fingerprint())
                .collect::<Vec<u64>>()
        };
        let [p_join] = joins(first)[..] else {
            panic!("P's stratum computes one join")
        };
        assert_eq!(
            joins(second).len(),
            1,
            "Q's stratum joins once, against P, and reads P's join for R and S"
        );
        assert!(
            second
                .non_recursive_transformations()
                .iter()
                .any(|tx| tx.is_unary() && tx.unary_input().fingerprint() == p_join),
            "Q's stratum arranges P's join"
        );
    }

    /// `P` here drops `y`, which `Q`'s join of `R` and `S` needs to keep
    /// for its own join with `P`, so the earlier join cannot serve it and
    /// the later stratum joins `R` and `S` itself.
    #[test]
    fn later_stratum_recomputes_a_join_whose_columns_an_earlier_one_dropped() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32)\n\
            .decl P(x: int32, z: int32)\n\
            .decl Q(x: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(x, z) :- R(x, y), S(y, z).\n\
            Q(x) :- R(x, y), S(y, z), P(x, z).\n",
        );
        let [_, second] = pp.strata() else {
            panic!("Q reads P, so it plans one stratum later")
        };
        let joins = second
            .non_recursive_transformations()
            .iter()
            .filter(|tx| !tx.is_unary())
            .count();
        assert_eq!(joins, 2, "Q's stratum joins R with S and the result with P");
    }

    /// The recursive Dyck stratum arranges `dyck` inside its fixpoint, where
    /// the collection holds only what has been derived so far. `Tail`, one
    /// stratum later, needs the same arrangement of the complete relation
    /// and builds it itself.
    #[test]
    fn later_stratum_rebuilds_an_arrangement_a_recursive_stratum_made() {
        let src = format!(
            "{DYCK_SRC}.decl Tail(x: int32, y: int32)\n.output Tail\nTail(x, y) :- Dyck(x, z), Dyck(z, y).\n"
        );
        let pp = ProgramPlanner::analyze(&src);
        let tail = pp.strata().last().expect("Tail plans last");
        assert!(!tail.is_recursive());
        let arrangements = tail
            .non_recursive_transformations()
            .iter()
            .filter(|tx| tx.is_unary())
            .count();
        assert_eq!(
            arrangements, 2,
            "Tail arranges dyck on each join side itself"
        );
    }

    /// Both rules key `B` on its first column, but at different rhs
    /// positions (1 vs 0). Their fingerprints embed that position and
    /// differ; the canonical form does not, so the arrangements are one.
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
        let pp = ProgramPlanner::analyze(RHS_ID_SHARING_SRC);
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
        let pp = ProgramPlanner::analyze(
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

    /// One relation read three times, a filter that folds elsewhere, and a
    /// second rule with distinct projections: pushdown lands copies and
    /// fuse rewires inputs, the two phases that leave a reader hashing an
    /// input it no longer reads until fuse's own pass refreshes it.
    const PUSHDOWN_SRC: &str = "\
        .decl R(a: int32, b: int32, p: int32)\n\
        .decl S(c: int32, z: int32)\n\
        .decl F(c: int32)\n\
        .decl T(a: int32, b: int32, c: int32, z: int32)\n\
        .decl U(a: int32, b: int32, c: int32, z: int32)\n\
        .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
        .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
        .input F(IO=\"file\", filename=\"F.csv\", delimiter=\",\")\n\
        .output T\n\
        .output U\n\
        T(a, b, c, z) :- R(a, b, p), R(b, c, p), R(c, a, p), S(c, z), F(c).\n\
        U(a, b, c, z) :- R(a, b, p), R(b, c, q), R(c, a, r), S(c, z), F(c).\n";

    /// After planning, every fingerprint hashes the inputs its info reads
    /// now, so recomputing it changes nothing. Materialization takes the
    /// fingerprints as they stand, so a stale one would name a plan node
    /// by inputs it no longer has.
    #[test]
    fn planned_fingerprints_hash_the_inputs_actually_read() {
        for src in [DYCK_SRC, RHS_ID_SHARING_SRC, PUSHDOWN_SRC] {
            let pp = ProgramPlanner::analyze(src);
            for stratum in pp.strata() {
                for planner in stratum.rule_planners() {
                    for info in planner.transformation_infos() {
                        let mut recomputed = info.clone();
                        recomputed.refresh_output_fp();
                        assert_eq!(
                            recomputed.output_info_fp(),
                            info.output_info_fp(),
                            "stale fingerprint in {info}"
                        );
                    }
                }
            }
        }
    }

    /// Equal output fingerprint must imply equal content (operation, input
    /// fingerprints, flow) across all per-rule transformations: the plan
    /// graph is wired by fingerprint, so a collision would splice one
    /// collection's readers onto another.
    #[test]
    fn equal_fingerprint_implies_equal_content() {
        use crate::planner::TransformationFlow;

        for src in [DYCK_SRC, RHS_ID_SHARING_SRC, PUSHDOWN_SRC] {
            let pp = ProgramPlanner::analyze(src);
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
}
