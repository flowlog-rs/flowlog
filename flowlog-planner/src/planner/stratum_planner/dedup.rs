//! Sharing by canonical form within one stratum and with the preludes of
//! the strata before it.
//!
//! Every rule plans on its own, so the stratum's transformations repeat
//! work, their own and the earlier strata's. `merge_equal` makes
//! collections with the same form and shape one collection. `cover_bodies`
//! then takes each set of collections that hold the same rows, largest
//! bodies first, and picks which of them to compute so that every one the
//! rest of the plan needs is computed or a map over a computed one, with
//! the fewest joins. `drop_unread` and `sort_producers_first` tidy the
//! result.
//!
//! The preludes of earlier strata take part as collections computed
//! already: they may serve this stratum's collections but are never
//! served, and they are not in the stratum's own list, so nothing here
//! drops or reorders them. They hold the same rows for this stratum as for
//! their own: the stratifier places a rule after every rule writing a
//! relation it reads, so a prelude reads only relations that are complete
//! and stay so.

use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use tracing::trace;

use crate::planner::ArithmeticArgument;
use crate::planner::CanonicalForm;
use crate::planner::Collection;
use crate::planner::Constraints;
use crate::planner::PlanError;
use crate::planner::StratumPlanner;
use crate::planner::Transformation;
use crate::planner::TransformationFlow;

impl StratumPlanner {
    /// Folds the rules' transformations into one pipeline where equal
    /// work runs once: equal collections are merged, and of the
    /// collections holding the same rows only as many are computed as the
    /// rest of the plan needs, the others becoming maps over them. The
    /// collections in `preludes` count as computed already. The heads in
    /// `idb_to_heads_map` follow: a relation whose head was merged unions
    /// the surviving collection instead.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the result has a cycle, which cannot
    /// happen: a map is only ever placed over a computed collection of
    /// the same body.
    pub(super) fn dedup_transformations(
        &mut self,
        preludes: &[Transformation],
    ) -> Result<(), PlanError> {
        self.transformations = self
            .rule_planners
            .iter()
            .flat_map(|planner| planner.transformations())
            .cloned()
            .collect();
        self.merge_equal(preludes);
        self.cover_bodies(preludes);
        self.drop_unread();
        self.sort_producers_first()
    }

    /// The transformation at `index` when the preludes are numbered first
    /// and this stratum's own transformations after them.
    fn member<'a>(&'a self, preludes: &'a [Transformation], index: usize) -> &'a Transformation {
        match index.checked_sub(preludes.len()) {
            Some(own) => &self.transformations[own],
            None => &preludes[index],
        }
    }

    // --- Equal collections ---

    /// Makes every collection with the same form and shape as an earlier
    /// one that earlier collection: its producer goes, its readers read
    /// the earlier one through their unchanged flows, and a relation it
    /// fed unions the earlier one instead. Shape counts because a join
    /// reads only arranged input and a map only one shape. The preludes
    /// come first, so they only ever stand for others.
    fn merge_equal(&mut self, preludes: &[Transformation]) {
        let mut first: HashMap<(&CanonicalForm, bool), &Arc<Collection>> = HashMap::new();
        for tx in preludes {
            first
                .entry((tx.output().canonical(), tx.need_arrange()))
                .or_insert_with(|| tx.output());
        }
        let mut merges: Vec<(usize, Arc<Collection>)> = Vec::new();
        for (index, tx) in self.transformations.iter().enumerate() {
            match first.entry((tx.output().canonical(), tx.need_arrange())) {
                Entry::Occupied(earlier) => merges.push((index, Arc::clone(earlier.get()))),
                Entry::Vacant(slot) => {
                    slot.insert(tx.output());
                }
            }
        }
        for (later, earlier) in &merges {
            self.alias(*later, earlier);
        }
        let merged: HashSet<usize> = merges.iter().map(|&(later, _)| later).collect();
        let mut index = 0;
        self.transformations.retain(|_| {
            let keep = !merged.contains(&index);
            index += 1;
            keep
        });
    }

    /// Makes `earlier` stand for `later`'s output: every reader of it
    /// reads `earlier` through its unchanged flow, and a relation it fed
    /// unions `earlier` once instead. The two have the same columns in the
    /// same positions, so the flows keep meaning what they did.
    fn alias(&mut self, later: usize, earlier: &Arc<Collection>) {
        let from = self.transformations[later].output().fingerprint();
        trace!(
            "[dedup] {} is {}",
            self.transformations[later].output(),
            earlier
        );
        for tx in &mut self.transformations {
            tx.swap_input(from, earlier);
        }
        for heads in self.idb_to_heads_map.values_mut() {
            if heads.contains(&from) {
                heads.retain(|fp| *fp != from);
                if !heads.contains(&earlier.fingerprint()) {
                    heads.push(earlier.fingerprint());
                }
            }
        }
    }

    // --- Covered collections ---

    /// Decides, for each set of collections holding the same rows, which
    /// are computed and which become maps over a computed one. Sets are
    /// taken largest body first, so what a set must provide to the rest
    /// of the plan is settled before the set is decided: a collection is
    /// needed when a relation unions it or a transformation outside its
    /// set reads it, and a collection nothing needs is left for
    /// `drop_unread`, its inputs losing a reader at once. A prelude is
    /// computed already and takes part only as a server.
    fn cover_bodies(&mut self, preludes: &[Transformation]) {
        let heads: HashSet<u64> = self.idb_to_heads_map.values().flatten().copied().collect();
        // Readers by input fingerprint, as indices into this stratum's own
        // transformations; a prelude reads nothing of this stratum.
        let mut readers: HashMap<u64, Vec<usize>> = HashMap::new();
        for (reader, tx) in self.transformations.iter().enumerate() {
            for input in tx.input_fingerprints() {
                readers.entry(input).or_default().push(reader);
            }
        }
        let own = |member: usize| member.checked_sub(preludes.len());
        for group in self.body_groups(preludes) {
            let members: HashSet<usize> = group.iter().filter_map(|&member| own(member)).collect();
            let needed: Vec<bool> = group
                .iter()
                .map(|&member| {
                    own(member).is_some_and(|index| {
                        let output = self.transformations[index].output().fingerprint();
                        heads.contains(&output)
                            || readers.get(&output).is_some_and(|who| {
                                who.iter().any(|reader| !members.contains(reader))
                            })
                    })
                })
                .collect();
            let mut cover = self.cover(preludes, &group, &needed);
            for (position, &member) in group.iter().enumerate() {
                let Some(index) = own(member) else {
                    continue;
                };
                if cover.computed.contains(&member) {
                    continue;
                }
                for input in self.transformations[index].input_fingerprints() {
                    if let Some(who) = readers.get_mut(&input) {
                        who.retain(|reader| *reader != index);
                    }
                }
                if let Some((server, key, value)) = cover.served.remove(&position) {
                    let server = self.member(preludes, server);
                    trace!(
                        "[dedup] {} now a map over {}",
                        self.transformations[index].output(),
                        server.output()
                    );
                    let map = Self::map_over(server, &self.transformations[index], key, value);
                    readers
                        .entry(server.output().fingerprint())
                        .or_default()
                        .push(index);
                    self.transformations[index] = map;
                }
            }
        }
    }

    /// The transformations grouped by the body of their output, numbered
    /// as [`Self::member`] does and each group in that order, keeping the
    /// groups that hold at least one of this stratum's own; the groups by
    /// body rank from the most constrained down. Bodies depend only on
    /// forms, which sharing never changes.
    fn body_groups(&self, preludes: &[Transformation]) -> Vec<Vec<usize>> {
        let count = preludes.len() + self.transformations.len();
        let mut by_hash: HashMap<u64, Vec<Vec<usize>>> = HashMap::new();
        for index in 0..count {
            let form = self.member(preludes, index).output().canonical();
            let bucket = by_hash.entry(form.body_hash()).or_default();
            match bucket
                .iter_mut()
                .find(|group| form.same_body(self.member(preludes, group[0]).output().canonical()))
            {
                Some(group) => group.push(index),
                None => bucket.push(vec![index]),
            }
        }
        let mut groups: Vec<Vec<usize>> = by_hash
            .into_values()
            .flatten()
            .filter(|group| group.last().is_some_and(|&member| member >= preludes.len()))
            .collect();
        groups.sort_by_key(|group| {
            let (relations, filters) = self
                .member(preludes, group[0])
                .output()
                .canonical()
                .body_rank();
            (
                std::cmp::Reverse(relations),
                std::cmp::Reverse(filters),
                group[0],
            )
        });
        groups
    }

    /// The cheapest way to provide the needed members of one body group:
    /// which members to compute and, for each needed member left out, the
    /// computed member serving it with the map's key and value. A computed
    /// member keeps the members it reads computed, and a prelude is
    /// computed in every choice: it is paid for already, so it weighs the
    /// same in each and never decides between them. Cost is joins
    /// computed, then maps (computed maps and served members alike), then
    /// members served rather than kept, then the width of the servers
    /// read, then the earliest members; exact up to [`EXACT_COVER_LIMIT`]
    /// members of this stratum in the group, greedy beyond.
    fn cover(&self, preludes: &[Transformation], group: &[usize], needed: &[bool]) -> Cover {
        let n = group.len();
        let tx = |member: usize| self.member(preludes, group[member]);
        let fixed: Vec<bool> = group.iter().map(|&index| index < preludes.len()).collect();
        let covers: Vec<Vec<bool>> = (0..n)
            .map(|served| {
                (0..n)
                    .map(|server| {
                        served != server
                            && !fixed[served]
                            && tx(served)
                                .output()
                                .canonical()
                                .flow_over(tx(server).output().canonical())
                                .is_some()
                    })
                    .collect()
            })
            .collect();
        let inputs: Vec<Vec<usize>> = (0..n)
            .map(|member| {
                let read = tx(member).input_fingerprints();
                (0..n)
                    .filter(|&input| read.contains(&tx(input).output().fingerprint()))
                    .collect()
            })
            .collect();
        let width = |member: usize| {
            let (keys, values) = tx(member).output().arity();
            keys + values
        };
        let server_for = |member: usize, computed: &[bool]| {
            (0..n)
                .filter(|&server| computed[server] && covers[member][server])
                .min_by_key(|&server| (width(server), server))
        };
        let valid = |computed: &[bool]| {
            (0..n).all(|member| {
                if computed[member] {
                    inputs[member].iter().all(|&input| computed[input])
                } else {
                    !fixed[member] && (!needed[member] || server_for(member, computed).is_some())
                }
            })
        };
        let cost = |computed: &[bool]| {
            let chosen: Vec<usize> = (0..n).filter(|&member| computed[member]).collect();
            let joins = chosen
                .iter()
                .filter(|&&member| !tx(member).is_unary())
                .count();
            let served: Vec<usize> = (0..n)
                .filter(|&member| !computed[member] && needed[member])
                .collect();
            let read: usize = served
                .iter()
                .filter_map(|&member| server_for(member, computed))
                .map(width)
                .sum();
            (
                joins,
                chosen.len() - joins + served.len(),
                served.len(),
                read,
                chosen,
            )
        };

        // Only the members of this stratum are choices; the search is over
        // their subsets, with the fixed members computed in every one.
        let free: Vec<usize> = (0..n).filter(|&member| !fixed[member]).collect();
        let computed = if free.len() <= EXACT_COVER_LIMIT {
            (0..1u32 << free.len())
                .map(|mask| {
                    let mut computed = fixed.clone();
                    for (bit, &member) in free.iter().enumerate() {
                        computed[member] = mask & (1 << bit) != 0;
                    }
                    computed
                })
                .filter(|computed| valid(computed))
                .min_by_key(|computed| cost(computed))
                .expect("Planner error: computing every member is valid")
        } else {
            greedy_cover(&covers, &inputs, needed, &fixed)
        };
        let served = (0..n)
            .filter(|&member| !computed[member] && needed[member])
            .map(|member| {
                let server = server_for(member, &computed)
                    .expect("Planner error: a valid cover serves every needed member");
                let (key, value) = tx(member)
                    .output()
                    .canonical()
                    .flow_over(tx(server).output().canonical())
                    .expect("Planner error: a server covers what it serves");
                (member, (group[server], key, value))
            })
            .collect();
        Cover {
            computed: (0..n)
                .filter(|&member| computed[member])
                .map(|member| group[member])
                .collect(),
            served,
        }
    }

    /// The map that produces `served`'s output collection from `server`'s,
    /// with `key` and `value` read from `server`'s positions. Its variant
    /// follows whether each side is rows or arranged pairs, so the output
    /// keeps the shape its readers expect.
    fn map_over(
        server: &Transformation,
        served: &Transformation,
        key: Vec<ArithmeticArgument>,
        value: Vec<ArithmeticArgument>,
    ) -> Transformation {
        let input = Arc::clone(server.output());
        let output = Arc::clone(served.output());
        let flow = TransformationFlow::KVToKV {
            key: Arc::new(key),
            value: Arc::new(value),
            constraints: Constraints::new(Vec::new(), Vec::new()),
            compares: Vec::new(),
        };
        match (server.need_arrange(), served.need_arrange()) {
            (false, false) => Transformation::RowToRow {
                input,
                output,
                flow,
            },
            (false, true) => Transformation::RowToKv {
                input,
                output,
                flow,
            },
            (true, false) => Transformation::KvToRow {
                input,
                output,
                flow,
            },
            (true, true) => Transformation::KvToKv {
                input,
                output,
                flow,
            },
        }
    }

    // --- Cleanup ---

    /// Removes every transformation whose output no transformation reads
    /// and that no relation unions, repeating until none is left: a
    /// replaced join's private inputs fall in the second round.
    fn drop_unread(&mut self) {
        let heads: HashSet<u64> = self.idb_to_heads_map.values().flatten().copied().collect();
        loop {
            let read: HashSet<u64> = self
                .transformations
                .iter()
                .flat_map(Transformation::input_fingerprints)
                .collect();
            let before = self.transformations.len();
            self.transformations.retain(|tx| {
                let output = tx.output().fingerprint();
                heads.contains(&output) || read.contains(&output)
            });
            if self.transformations.len() == before {
                return;
            }
        }
    }

    /// Reorders the transformations so that every input is produced before
    /// it is read, keeping the current relative order otherwise. An input
    /// no transformation here produces is available from the start.
    ///
    /// # Errors
    ///
    /// Returns an internal error if the transformations form a cycle.
    fn sort_producers_first(&mut self) -> Result<(), PlanError> {
        let produced_here: HashSet<u64> = self
            .transformations
            .iter()
            .map(|tx| tx.output().fingerprint())
            .collect();
        let count = self.transformations.len();
        let mut produced = HashSet::new();
        let mut placed = vec![false; count];
        let mut order = Vec::with_capacity(count);
        while order.len() < count {
            let next = (0..count)
                .find(|&index| {
                    !placed[index]
                        && self.transformations[index]
                            .input_fingerprints()
                            .iter()
                            .all(|fp| !produced_here.contains(fp) || produced.contains(fp))
                })
                .ok_or_else(|| {
                    PlanError::internal("dedup_transformations: transformations form a cycle")
                })?;
            placed[next] = true;
            produced.insert(self.transformations[next].output().fingerprint());
            order.push(next);
        }
        let sorted = order
            .iter()
            .map(|&index| self.transformations[index].clone())
            .collect();
        self.transformations = sorted;
        Ok(())
    }
}

/// Up to this many members of this stratum in a body group, the group is
/// covered by trying every subset of them; beyond it [`greedy_cover`]
/// runs. Preludes do not count, since they are computed in every
/// subset. Picked so the exact search stays under a few thousand subsets
/// per group; DOOP's largest group has ten members.
const EXACT_COVER_LIMIT: usize = 12;

/// One body group's decision: the transformations to keep computing, and
/// for each member served instead, keyed by its position in the group,
/// the index of the transformation serving it and the map's key and
/// value.
struct Cover {
    computed: HashSet<usize>,
    served: HashMap<usize, (usize, Vec<ArithmeticArgument>, Vec<ArithmeticArgument>)>,
}

/// A valid choice of computed members for a group too large to search:
/// every fixed member, then every needed member nothing covers, then
/// repeatedly the member covering the most needed members still without a
/// computed server, each closed over the members it reads.
/// `covers[served][server]` says a map over `server` can produce `served`;
/// `inputs[member]` lists the members `member` reads.
fn greedy_cover(
    covers: &[Vec<bool>],
    inputs: &[Vec<usize>],
    needed: &[bool],
    fixed: &[bool],
) -> Vec<bool> {
    let n = needed.len();
    let mut computed = vec![false; n];
    let close = |computed: &mut Vec<bool>, member: usize| {
        let mut pending = vec![member];
        while let Some(member) = pending.pop() {
            if !computed[member] {
                computed[member] = true;
                pending.extend(inputs[member].iter().copied());
            }
        }
    };
    for member in (0..n).filter(|&member| fixed[member]) {
        close(&mut computed, member);
    }
    for member in (0..n).filter(|&member| needed[member]) {
        if !covers[member].iter().any(|&covered| covered) {
            close(&mut computed, member);
        }
    }
    loop {
        let uncovered: Vec<usize> = (0..n)
            .filter(|&member| {
                needed[member]
                    && !computed[member]
                    && !(0..n).any(|server| computed[server] && covers[member][server])
            })
            .collect();
        if uncovered.is_empty() {
            return computed;
        }
        let best = (0..n)
            .filter(|&server| !computed[server])
            .max_by_key(|&server| {
                let covered = uncovered.iter().filter(|&&m| covers[m][server]).count();
                (covered, std::cmp::Reverse(server))
            })
            .expect("Planner error: an uncovered member covers itself");
        close(&mut computed, best);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use flowlog_common::compute_fp;

    use crate::planner::ArithmeticArgument;
    use crate::planner::FactorArgument;
    use crate::planner::ProgramPlanner;
    use crate::planner::StratumPlanner;
    use crate::planner::TransformationArgument;

    const WIDE_NARROW: &str = "\
        .decl R(k: int32, a: int32)\n\
        .decl S(k: int32, b: int32, c: int32)\n\
        .decl Wide(a: int32, b: int32, c: int32)\n\
        .decl Narrow(a: int32, b: int32)\n\
        .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
        .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
        .output Wide\n\
        .output Narrow\n\
        Wide(a, b, c) :- R(k, a), S(k, b, c).\n\
        Narrow(a, b) :- R(k, a), S(k, b, _).\n";

    /// Every input of a stratum's transformation is produced earlier in
    /// the list or read directly from a relation.
    fn producers_precede_readers(stratum: &StratumPlanner) -> bool {
        let produced_here: HashSet<u64> = stratum
            .non_recursive_transformations()
            .iter()
            .map(|tx| tx.output().fingerprint())
            .collect();
        let mut produced = HashSet::new();
        stratum.non_recursive_transformations().iter().all(|tx| {
            let ready = tx
                .input_fingerprints()
                .iter()
                .all(|fp| !produced_here.contains(fp) || produced.contains(fp));
            produced.insert(tx.output().fingerprint());
            ready
        })
    }

    /// Narrow's join reads the same rows as Wide's and needs two of its
    /// three columns, so it becomes a map over Wide's head. The join and
    /// the arrangement of `S` that only it read are gone, and the map
    /// reads Wide's first two value columns.
    #[test]
    fn a_join_covered_by_a_wider_join_becomes_a_map_over_it() {
        let pp = ProgramPlanner::analyze(WIDE_NARROW);
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let wide_head = stratum.idb_to_heads_map()[&compute_fp("wide")][0];
        let narrow_head = stratum.idb_to_heads_map()[&compute_fp("narrow")][0];

        let binary: Vec<_> = txs.iter().filter(|tx| !tx.is_unary()).collect();
        assert_eq!(binary.len(), 1);
        assert_eq!(binary[0].output().fingerprint(), wide_head);

        let narrow = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == narrow_head)
            .expect("Narrow's head is still produced");
        assert!(narrow.is_unary());
        assert_eq!(narrow.unary_input().fingerprint(), wide_head);
        let column = |index| ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, index))),
            rest: vec![],
        };
        assert_eq!(**narrow.flow().value(), vec![column(0), column(1)]);

        let s_arrangements = txs
            .iter()
            .filter(|tx| tx.is_unary() && tx.unary_input().fingerprint() == compute_fp("s"))
            .count();
        assert_eq!(s_arrangements, 1);
        assert!(producers_precede_readers(stratum));
    }

    /// The narrow rule comes first here, so its server sits later in the
    /// list. The narrow join is still the one replaced, its private
    /// arrangement of `S` still goes, and the sort moves the wide join
    /// ahead of the map that reads it.
    #[test]
    fn a_later_wider_join_serves_an_earlier_narrow_one() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32, a: int32)\n\
            .decl S(k: int32, b: int32, c: int32)\n\
            .decl Narrow(a: int32, b: int32)\n\
            .decl Wide(a: int32, b: int32, c: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Narrow\n\
            .output Wide\n\
            Narrow(a, b) :- R(k, a), S(k, b, _).\n\
            Wide(a, b, c) :- R(k, a), S(k, b, c).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let wide_head = stratum.idb_to_heads_map()[&compute_fp("wide")][0];
        let narrow_head = stratum.idb_to_heads_map()[&compute_fp("narrow")][0];

        let binary: Vec<_> = txs.iter().filter(|tx| !tx.is_unary()).collect();
        assert_eq!(binary.len(), 1);
        assert_eq!(binary[0].output().fingerprint(), wide_head);
        let position = |fp| txs.iter().position(|tx| tx.output().fingerprint() == fp);
        assert!(position(wide_head) < position(narrow_head));
        let s_arrangements = txs
            .iter()
            .filter(|tx| tx.is_unary() && tx.unary_input().fingerprint() == compute_fp("s"))
            .count();
        assert_eq!(s_arrangements, 1);
        assert!(producers_precede_readers(stratum));
    }

    /// `P` and `Q` join the same two relations with the body written in
    /// either order. Their joins have different fingerprints, since the
    /// plan puts a different relation on the left, but equal forms, so
    /// `Q`'s join is dropped, its head is `P`'s head, and no map is added.
    #[test]
    fn a_commuted_join_is_the_same_collection() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl E(x: int32, y: int32)\n\
            .decl F(x: int32, y: int32)\n\
            .decl P(x: int32, z: int32)\n\
            .decl Q(x: int32, z: int32)\n\
            .input E(IO=\"file\", filename=\"E.csv\", delimiter=\",\")\n\
            .input F(IO=\"file\", filename=\"F.csv\", delimiter=\",\")\n\
            .output P\n\
            .output Q\n\
            P(x, z) :- E(x, y), F(y, z).\n\
            Q(x, z) :- F(y, z), E(x, y).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();

        assert_eq!(
            stratum.idb_to_heads_map()[&compute_fp("q")],
            stratum.idb_to_heads_map()[&compute_fp("p")]
        );
        assert_eq!(txs.iter().filter(|tx| !tx.is_unary()).count(), 1);
        // Two arrangements and one join: nothing of Q's own plan remains.
        assert_eq!(txs.len(), 3);
        assert!(producers_precede_readers(stratum));
    }

    /// Two rules with identical pipelines end in one head that both
    /// relations union, as fingerprint dedup used to arrange.
    #[test]
    fn identical_rules_share_one_head() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32)\n\
            .decl Out(x: int32, z: int32)\n\
            .decl Again(x: int32, z: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Out\n\
            .output Again\n\
            Out(x, z) :- R(x, y), S(y, z).\n\
            Again(x, z) :- R(x, y), S(y, z).\n",
        );
        let stratum = &pp.strata()[0];

        assert_eq!(
            stratum.idb_to_heads_map()[&compute_fp("again")],
            stratum.idb_to_heads_map()[&compute_fp("out")]
        );
        assert_eq!(stratum.non_recursive_transformations().len(), 3);
    }

    /// Both rules output the same computed string over the same join, and
    /// Label also carries a column the string is built from. Full has no
    /// plain column that spells the string, but Label outputs that very
    /// expression, so Full reads it from Label's first value position and
    /// the second join is gone.
    #[test]
    fn a_computed_output_is_read_from_the_server_that_outputs_it() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32, a: string)\n\
            .decl S(k: int32, b: string)\n\
            .decl Label(l: string, a: string)\n\
            .decl Full(l: string)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Label\n\
            .output Full\n\
            Label(cat(a, b), a) :- R(k, a), S(k, b).\n\
            Full(cat(a, b)) :- R(k, a), S(k, b).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let label_head = stratum.idb_to_heads_map()[&compute_fp("label")][0];
        let full_head = stratum.idb_to_heads_map()[&compute_fp("full")][0];

        assert_eq!(txs.iter().filter(|tx| !tx.is_unary()).count(), 1);
        let full = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == full_head)
            .expect("Full's head is still produced");
        assert!(full.is_unary());
        assert_eq!(full.unary_input().fingerprint(), label_head);
        let label_string = ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, 0))),
            rest: vec![],
        };
        assert_eq!(**full.flow().value(), vec![label_string]);
        assert!(producers_precede_readers(stratum));
    }

    /// Copy's head and the product's arrangement of `A` compute the same
    /// rows, but one is rows and the other is arranged under an empty key.
    /// Neither may stand for the other: the join must still read an
    /// arranged input, so both stay.
    #[test]
    fn a_row_head_does_not_stand_for_an_arrangement_with_an_empty_key() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl A(x: int32)\n\
            .decl B(y: int32)\n\
            .decl Copy(x: int32)\n\
            .decl Product(x: int32, y: int32)\n\
            .input A(IO=\"file\", filename=\"A.csv\", delimiter=\",\")\n\
            .input B(IO=\"file\", filename=\"B.csv\", delimiter=\",\")\n\
            .output Copy\n\
            .output Product\n\
            Copy(x) :- A(x).\n\
            Product(x, y) :- A(x), B(y).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let copy_head = stratum.idb_to_heads_map()[&compute_fp("copy")][0];

        let [product] = txs.iter().filter(|tx| !tx.is_unary()).collect::<Vec<_>>()[..] else {
            panic!("the product is the only join");
        };
        let (left, right) = product.binary_input();
        for input in [left, right] {
            let producer = txs
                .iter()
                .find(|tx| tx.output().fingerprint() == input.fingerprint())
                .expect("a join input is produced in the stratum");
            assert!(producer.need_arrange());
            assert_ne!(input.fingerprint(), copy_head);
        }
        let copy = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == copy_head)
            .expect("Copy's head is still produced");
        assert!(!copy.need_arrange());
        assert_eq!(txs.len(), 4);
    }

    /// Both's join of `R` and `S` is arranged under `k` alone, with no
    /// value, to meet `T`. Same's head holds the same rows, so it becomes
    /// a map over that key-only arrangement and reads `k` from the key.
    #[test]
    fn a_row_head_is_served_by_a_key_only_arrangement() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32)\n\
            .decl S(k: int32)\n\
            .decl T(k: int32, x: int32)\n\
            .decl Both(k: int32, x: int32)\n\
            .decl Same(k: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .input T(IO=\"file\", filename=\"T.csv\", delimiter=\",\")\n\
            .output Both\n\
            .output Same\n\
            Both(k, x) :- R(k), S(k), T(k, x).\n\
            Same(k) :- R(k), S(k).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let same_head = stratum.idb_to_heads_map()[&compute_fp("same")][0];

        assert_eq!(txs.iter().filter(|tx| !tx.is_unary()).count(), 2);
        let same = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == same_head)
            .expect("Same's head is still produced");
        assert!(same.is_unary());
        let server = same.unary_input();
        assert!(server.is_k_only());
        assert_eq!(server.arity(), (1, 0));
        let producer = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == server.fingerprint())
            .expect("the server is produced in the stratum");
        assert!(!producer.is_unary());
        let key = ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((true, 0))),
            rest: vec![],
        };
        assert_eq!(**same.flow().value(), vec![key]);
        assert!(producers_precede_readers(stratum));
    }

    /// Three rules keep different columns of one join and none covers the
    /// other two, but Wide covers both, so one join is computed and the
    /// two others are maps over it: the cover minimizes joins across the
    /// whole group, not pair by pair.
    #[test]
    fn one_join_serves_every_projection_of_its_rows() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32, a: int32)\n\
            .decl S(k: int32, b: int32, c: int32)\n\
            .decl Left(a: int32, b: int32)\n\
            .decl Right(b: int32, c: int32)\n\
            .decl Wide(a: int32, b: int32, c: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Left\n\
            .output Right\n\
            .output Wide\n\
            Left(a, b) :- R(k, a), S(k, b, _).\n\
            Right(b, c) :- R(k, _), S(k, b, c).\n\
            Wide(a, b, c) :- R(k, a), S(k, b, c).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let wide_head = stratum.idb_to_heads_map()[&compute_fp("wide")][0];
        assert_eq!(txs.iter().filter(|tx| !tx.is_unary()).count(), 1);
        for name in ["left", "right"] {
            let head = stratum.idb_to_heads_map()[&compute_fp(name)][0];
            let tx = txs
                .iter()
                .find(|tx| tx.output().fingerprint() == head)
                .expect("the head is still produced");
            assert!(tx.is_unary());
            assert_eq!(tx.unary_input().fingerprint(), wide_head);
        }
        assert!(producers_precede_readers(stratum));
    }

    /// Both's join of `R` and `S` is read by Both's own join with `T` and
    /// by nothing else, and Same's head holds the same rows wider. Serving
    /// Same from it would keep two joins; computing Same's head and
    /// serving Both's intermediate from it keeps one, so the cover picks
    /// the head as the computed member although it comes later.
    #[test]
    fn a_head_serves_an_earlier_intermediate_when_that_saves_a_join() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(k: int32, a: int32)\n\
            .decl S(k: int32, b: int32)\n\
            .decl T(k: int32, x: int32)\n\
            .decl Both(k: int32, x: int32)\n\
            .decl Same(k: int32, a: int32, b: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .input T(IO=\"file\", filename=\"T.csv\", delimiter=\",\")\n\
            .output Both\n\
            .output Same\n\
            Both(k, x) :- R(k, _), S(k, _), T(k, x).\n\
            Same(k, a, b) :- R(k, a), S(k, b).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let same_head = stratum.idb_to_heads_map()[&compute_fp("same")][0];
        let joins: Vec<_> = txs.iter().filter(|tx| !tx.is_unary()).collect();
        assert_eq!(joins.len(), 2);
        assert!(
            joins
                .iter()
                .any(|tx| tx.output().fingerprint() == same_head)
        );
        let both_join = joins
            .iter()
            .find(|tx| tx.output().fingerprint() != same_head)
            .expect("Both's join with T stays");
        let (left, right) = both_join.binary_input();
        let served = txs.iter().find(|tx| {
            [left.fingerprint(), right.fingerprint()].contains(&tx.output().fingerprint())
                && tx.is_unary()
                && tx.unary_input().fingerprint() == same_head
        });
        assert!(
            served.is_some(),
            "Both's R-S intermediate is a map over Same's head"
        );
        assert!(producers_precede_readers(stratum));
    }

    /// Both heads project the relation `S` directly, and the narrower one
    /// is covered by the wider. Serving it would trade one map over `S`
    /// for one map over the other head and free nothing, so both stay
    /// maps over `S` and no transformation reads another.
    #[test]
    fn a_map_over_a_relation_is_not_served() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl S(k: int32, b: int32, c: int32)\n\
            .decl Wide(k: int32, b: int32, c: int32)\n\
            .decl Narrow(k: int32, b: int32)\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Wide\n\
            .output Narrow\n\
            Wide(k, b, c) :- S(k, b, c).\n\
            Narrow(k, b) :- S(k, b, _).\n",
        );
        let txs = pp.strata()[0].non_recursive_transformations();

        assert_eq!(txs.len(), 2);
        assert!(
            txs.iter()
                .all(|tx| tx.is_unary() && tx.unary_input().fingerprint() == compute_fp("s"))
        );
    }

    /// `T1(x, z)` and `T2(x, w)` read the same rows but each needs a
    /// column the other dropped, so neither can serve the other and both
    /// joins stay.
    #[test]
    fn joins_neither_of_which_covers_the_other_both_stay() {
        let pp = ProgramPlanner::analyze(
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

        let binary = pp.strata()[0]
            .non_recursive_transformations()
            .iter()
            .filter(|tx| !tx.is_unary())
            .count();
        assert_eq!(binary, 2);
    }

    /// Two heads with the same columns in another order cover each other;
    /// the later rule's join becomes a map over the earlier rule's head,
    /// swapping the two columns.
    #[test]
    fn equal_outputs_in_another_order_are_served_by_the_earlier_rule() {
        let pp = ProgramPlanner::analyze(
            "\
            .decl R(x: int32, y: int32)\n\
            .decl S(y: int32, z: int32)\n\
            .decl Out(x: int32, z: int32)\n\
            .decl Swapped(z: int32, x: int32)\n\
            .input R(IO=\"file\", filename=\"R.csv\", delimiter=\",\")\n\
            .input S(IO=\"file\", filename=\"S.csv\", delimiter=\",\")\n\
            .output Out\n\
            .output Swapped\n\
            Out(x, z) :- R(x, y), S(y, z).\n\
            Swapped(z, x) :- R(x, y), S(y, z).\n",
        );
        let stratum = &pp.strata()[0];
        let txs = stratum.non_recursive_transformations();
        let out_head = stratum.idb_to_heads_map()[&compute_fp("out")][0];
        let swapped_head = stratum.idb_to_heads_map()[&compute_fp("swapped")][0];

        let binary: Vec<_> = txs.iter().filter(|tx| !tx.is_unary()).collect();
        assert_eq!(binary.len(), 1);
        assert_eq!(binary[0].output().fingerprint(), out_head);
        let swapped = txs
            .iter()
            .find(|tx| tx.output().fingerprint() == swapped_head)
            .expect("Swapped's head is still produced");
        assert_eq!(swapped.unary_input().fingerprint(), out_head);
        let column = |index| ArithmeticArgument {
            init: FactorArgument::Var(TransformationArgument::KV((false, index))),
            rest: vec![],
        };
        assert_eq!(**swapped.flow().value(), vec![column(1), column(0)]);
        assert!(producers_precede_readers(stratum));
    }
}
