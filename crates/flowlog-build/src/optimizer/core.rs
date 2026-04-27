//! Cyclic-core detection via structural GYO peel.
//!
//! Runs a GYO peel over the catalog's positive atoms. If a cyclic core
//! remains, returns a pair `(i, j)` of atom indices inside that core
//! that share at least one effective variable. Joining this pair first
//! breaks the cycle; the planner takes it from there as a normal join.
//! Returns `None` for acyclic rules.

use crate::catalog::Catalog;
use std::collections::{HashMap, HashSet};

/// If the catalog has a cyclic core, return a pair of atom indices in
/// that core to join first. Returns `None` for acyclic rules.
pub(crate) fn cyclic_core_join_pair(catalog: &Catalog) -> Option<(usize, usize)> {
    let n = catalog.positive_atom_number();
    if n < 2 {
        return None;
    }

    let atom_vars: Vec<&HashSet<String>> = (0..n)
        .map(|i| catalog.positive_atom_argument_vars_str_set(i))
        .collect();

    // Effective scope for α-acyclicity: vars appearing in ≥ 2 atoms.
    // Vars unique to one atom (whether or not they appear in the head) are
    // private and cannot block GYO peeling — head projection is independent
    // of body acyclicity.
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for vs in &atom_vars {
        for v in *vs {
            *counts.entry(v.as_str()).or_insert(0) += 1;
        }
    }
    let effective_scope: HashSet<String> = counts
        .into_iter()
        .filter(|&(_, c)| c >= 2)
        .map(|(v, _)| v.to_string())
        .collect();

    let effective: Vec<HashSet<String>> = atom_vars
        .iter()
        .map(|vs| vs.intersection(&effective_scope).cloned().collect())
        .collect();

    // GYO peel: drop atoms whose effective vars are contained in another live atom.
    let mut alive = vec![true; n];
    loop {
        let mut changed = false;
        for i in 0..n {
            if !alive[i] {
                continue;
            }
            for j in 0..n {
                if i == j || !alive[j] {
                    continue;
                }
                let subset = effective[i].is_subset(&effective[j]);
                if subset && (effective[i] != effective[j] || i > j) {
                    alive[i] = false;
                    changed = true;
                    break;
                }
            }
        }
        if !changed {
            break;
        }
    }

    let survivors: Vec<usize> = (0..n).filter(|&i| alive[i]).collect();
    if survivors.len() < 2 {
        return None;
    }

    // First pair within the core that shares an effective variable.
    for &i in &survivors {
        for &j in &survivors {
            if i >= j {
                continue;
            }
            if effective[i].intersection(&effective[j]).next().is_some() {
                return Some((i, j));
            }
        }
    }

    None
}
