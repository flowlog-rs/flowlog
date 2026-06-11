//! Whole-program planner: owns the per-stratum plans after cross-stratum
//! dedup of redundant non-recursive transformations.

use std::collections::{HashMap, HashSet};

use crate::common::{BoxError, Config};
use crate::optimizer::Optimizer;
use crate::parser::Program;
use crate::planner::StratumPlanner;
use crate::profiler::Profiler;
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

    use crate::common::SourceMap;

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

    /// Both rules re-key `Edge` on its first column, but `Edge` sits at a
    /// different body position (2nd atom in `Reach`, 1st in `Step`), so their
    /// lineage fingerprints differ while their positional flows are identical.
    /// After the canonical rehash no two surviving transformations describe the
    /// same operator over the same inputs — the duplicate `Edge` re-key is shared.
    const SLOT_POSITION_SRC: &str = "\
        .decl Seed(x: int32)\n\
        .input Seed(IO=\"file\", filename=\"Seed.csv\", delimiter=\",\")\n\
        .decl Live(x: int32)\n\
        .input Live(IO=\"file\", filename=\"Live.csv\", delimiter=\",\")\n\
        .decl Edge(x: int32, y: int32)\n\
        .input Edge(IO=\"file\", filename=\"Edge.csv\", delimiter=\",\")\n\
        .decl Reach(x: int32, y: int32)\n\
        .printsize Reach\n\
        .decl Step(x: int32, y: int32)\n\
        .printsize Step\n\
        Reach(x, y) :- Seed(x), Edge(x, y).\n\
        Step(x, y) :- Edge(x, y), Live(x).\n";

    #[test]
    fn canonical_rehash_shares_slot_position_duplicates() {
        let pp = analyze(SLOT_POSITION_SRC);
        let mut seen = std::collections::HashSet::new();
        for stratum in pp.strata() {
            for t in stratum
                .non_recursive_transformations()
                .iter()
                .chain(stratum.recursive_transformations())
            {
                assert!(
                    seen.insert((t.input_fingerprints(), t.flow().clone(), t.operation_name())),
                    "content-identical transformations were not shared:\n{t}"
                );
            }
        }
    }
}
