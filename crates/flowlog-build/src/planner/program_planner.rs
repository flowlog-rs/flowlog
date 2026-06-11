//! Whole-program planner: owns the per-stratum plans after cross-stratum
//! dedup of redundant non-recursive transformations.

use std::collections::{HashMap, HashSet};

use crate::common::{BoxError, Config};
use crate::optimizer::Optimizer;
use crate::parser::Program;
use crate::planner::{StratumPlanner, Transformation};
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
        mark_shared_arrangements(&mut strata);
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

/// Mark each arrangement-producing transformation as either canonical or an
/// alias of an earlier, content-identical one *in the same dataflow scope*.
///
/// This is the cross-rule arrangement-sharing decision, resolved entirely at
/// plan time — a sibling of [`prune_cross_stratum_duplicates`], but keyed on
/// the rule-independent [`arrangement_key`](crate::planner::Transformation::arrangement_key)
/// rather than the lineage fingerprint, so it catches the same arrangement
/// reached via differently-shaped rules (which carry distinct fingerprints).
/// Codegen then merely renders the mark: canonical → build + `arrange_by_*`;
/// alias → a cheap `clone()` of the canonical collection.
///
/// Scopes mirror codegen exactly, so the canonical is always emitted before
/// its aliases:
/// - **non-recursive** transformations share one program-wide outer scope, so
///   they are deduplicated across strata in emission order (stratum, then
///   in-stratum) — but a *cross-stratum* reuse is additionally gated by the
///   same transitive intervening-IDB-update check as
///   [`prune_cross_stratum_duplicates`]: in extended mode a `fixpoint`/`loop`
///   can rewrite an IDB between two strata, leaving an earlier arrangement
///   stale, so the later one must not alias to it. (In `datalog-batch` no IDB
///   is rewritten across strata, so this never blocks a share.);
/// - each stratum's **recursive** transformations share that stratum's
///   `iterative` block, so they are deduplicated within the stratum.
///
/// Cross-scope reuse (an outer arrangement used inside recursion) is the
/// separate `enter` mechanism and is intentionally not aliased here.
fn mark_shared_arrangements(strata: &mut [StratumPlanner]) {
    let mut aliases: Vec<HashMap<u64, u64>> = vec![HashMap::new(); strata.len()];

    // Program-wide non-recursive (outer) scope: one canonical per arrangement
    // key across all strata, in emission order (stratum, then in-stratum).
    //
    // A cross-stratum reuse is only sound while the canonical's value is still
    // current at the alias's stratum. In extended mode a `fixpoint`/`loop` can
    // rewrite an IDB between two strata, so we apply the same transitive
    // intervening-update check as `prune_cross_stratum_duplicates`: reuse a
    // canonical (emitted at `prev`) at stratum `idx` only if no IDB the
    // arrangement transitively depends on was (re)written at a stratum `k` with
    // `prev < k <= idx`. Otherwise the canonical is stale here and this
    // transformation becomes the new canonical for the key going forward.
    let mut idb_writes: HashMap<u64, Vec<usize>> = HashMap::new();
    for (idx, stratum) in strata.iter().enumerate() {
        for fp in stratum.idb_to_heads_map().keys() {
            idb_writes.entry(*fp).or_default().push(idx);
        }
    }
    let mut idb_deps: HashMap<u64, HashSet<u64>> = idb_writes
        .keys()
        .map(|&fp| (fp, HashSet::from([fp])))
        .collect();
    // arrangement key → (canonical output fp, stratum it was emitted at)
    let mut outer_seen: HashMap<u64, (u64, usize)> = HashMap::new();
    for (idx, stratum) in strata.iter().enumerate() {
        for t in stratum.non_recursive_transformations() {
            // Transitive IDB-head deps of this output (mirrors prune); computed
            // for every transformation so arrangement consumers resolve theirs.
            let fp = t.output().fingerprint();
            let t_deps: HashSet<u64> = t
                .input_fingerprints()
                .into_iter()
                .filter_map(|f| idb_deps.get(&f))
                .flatten()
                .copied()
                .collect();
            idb_deps.entry(fp).or_default().extend(&t_deps);

            if !t.produces_arrangement() {
                continue;
            }
            let key = stratum.arrangement_key(fp);
            match outer_seen.get(&key) {
                Some(&(canonical_fp, prev)) => {
                    let canonical_stale = t_deps
                        .iter()
                        .any(|idb| idb_writes[idb].iter().any(|&k| k > prev && k <= idx));
                    if canonical_stale {
                        outer_seen.insert(key, (fp, idx));
                    } else {
                        aliases[idx].insert(fp, canonical_fp);
                    }
                }
                None => {
                    outer_seen.insert(key, (fp, idx));
                }
            }
        }
    }

    // Each stratum's recursive (iterative) scope is independent.
    for (idx, stratum) in strata.iter().enumerate() {
        let mut rec_seen: HashMap<u64, u64> = HashMap::new();
        dedup_arrangements(
            &mut rec_seen,
            &mut aliases[idx],
            stratum,
            stratum.recursive_transformations(),
        );
    }

    for (stratum, alias) in strata.iter_mut().zip(aliases) {
        stratum.set_arrangement_alias(alias);
    }
}

/// Within a single dataflow scope, mark each arrangement-producing
/// transformation as either the canonical for its rule-independent arrangement
/// key (first seen) or an alias of that canonical. `seen` maps an arrangement
/// key to its canonical output fingerprint; `aliases` collects the
/// `duplicate output fp → canonical output fp` marks codegen reads back.
fn dedup_arrangements(
    seen: &mut HashMap<u64, u64>,
    aliases: &mut HashMap<u64, u64>,
    stratum: &StratumPlanner,
    transformations: &[Transformation],
) {
    use std::collections::hash_map::Entry;
    for t in transformations {
        if !t.produces_arrangement() {
            continue;
        }
        let fp = t.output().fingerprint();
        match seen.entry(stratum.arrangement_key(fp)) {
            Entry::Occupied(canonical) => {
                aliases.insert(fp, *canonical.get());
            }
            Entry::Vacant(slot) => {
                slot.insert(fp);
            }
        }
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
}
