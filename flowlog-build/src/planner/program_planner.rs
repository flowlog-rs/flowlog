//! Whole-program planner: owns the per-stratum plans after cross-stratum
//! dedup of redundant non-recursive transformations.

use std::collections::{HashMap, HashSet};

use crate::optimizer::Optimizer;
use crate::parser::Program;
use crate::planner::StratumPlanner;
use flowlog_profiler::Profiler;
use crate::stratifier::Stratifier;
use flowlog_common::{BoxError, Config};

/// Whole-program planning.
#[derive(Debug)]
pub struct ProgramPlanner {
    strata: Vec<StratumPlanner>,
}

impl ProgramPlanner {
    /// Run the full planner pipeline against `program`: stratify, build a
    /// [`StratumPlanner`] per stratum, prune cross-stratum duplicates.
    pub fn from_program(
        config: &Config,
        program: &Program,
        profiler: &mut Option<Profiler>,
    ) -> Result<Self, BoxError> {
        let stratifier = Stratifier::from_program(program, config.is_extended())?;
        let mut optimizer = Optimizer::new();
        let mut strata: Vec<StratumPlanner> = stratifier
            .stratum()
            .iter()
            .enumerate()
            .map(|(idx, rule_refs)| {
                let rules: Vec<_> = rule_refs.iter().copied().cloned().collect();
                StratumPlanner::from_rules(
                    config,
                    &rules,
                    &mut optimizer,
                    profiler,
                    &stratifier,
                    idx,
                )
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
/// strata. Under standard Datalog stratification, all consumers come after
/// all definers, so this is automatic; in extended mode the user controls
/// the segment order and may update an IDB *between* two consumers with the
/// same content-addressed transformation. The transitive check below keeps
/// the later emission whenever that's the case.
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
    use super::*;
    use std::io::Write;

    use flowlog_common::SourceMap;

    /// Round-trip a tiny program through parse → typecheck → program-plan,
    /// mirroring the temp-file pattern used by `stratifier::core::tests`.
    fn analyze(src: &str) -> ProgramPlanner {
        let mut tmp = tempfile::NamedTempFile::new().expect("tempfile");
        tmp.write_all(src.as_bytes()).expect("write");
        let mut sm = SourceMap::new();
        let mut program =
            Program::parse(&tmp.path().to_string_lossy(), false, &mut sm).expect("parse");
        crate::typechecker::check_program(&mut program, &Config::default()).expect("typecheck");
        ProgramPlanner::from_program(&Config::default(), &program, &mut None).expect("plan")
    }

    /// Dyck has three strata:
    ///   0: `Zero`, `One` rule heads from `Arc`.
    ///   1: `Dyck` base joins (`Zero⋈Zero`, `One⋈One`) — non-recursive.
    ///   2: `Dyck` recursive joins (`Zero⋈Dyck⋈Zero`, …) — recursive.
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
        // exactly one stratum — no duplicate emissions across strata.
        let mut owner: HashMap<u64, usize> = HashMap::new();
        for (idx, stratum) in pp.strata().iter().enumerate() {
            for t in stratum.non_recursive_transformations() {
                let fp = t.output().fingerprint();
                if let Some(prev) = owner.insert(fp, idx) {
                    panic!("fp 0x{fp:016x} survives in both stratum {prev} and stratum {idx}");
                }
            }
        }

        // Headline count: 12 unpruned → 8 after prune (four Zero/One re-keys
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
        let b_fp = flowlog_common::compute_fp("b");

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
    /// fps, flow) across all per-rule transformations — otherwise dedup
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
}
