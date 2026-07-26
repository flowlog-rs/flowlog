//! The edge layer: one worker's [`Channel`]s turned into [`Edge`]s and
//! per-operator tuple flow ([`Cardinality`]).
//!
//! Timely counts records, not tuples. Two fixups make the counts mean
//! tuples:
//!
//! - **Fanout.** Every channel on one output port ships the same tuples:
//!   output is the max per port, summed over ports.
//! - **Batch edges.** Arranged data ships batch handles, so the volume
//!   comes from the producer: an arrange holds exactly its input, a
//!   `Map`/`FlatMap` passes it through, anything else is opaque. A trace
//!   entering a subscope crosses as two edges, paired by (scope op, port).
//!
//! One unresolvable input edge wipes the whole input: a partial sum
//! would read as complete.
//!
//! Everything here is one worker's view; the edges are returned so the
//! layers above can measure plan-node boundaries.
//!
//! - [`resolve`]: one worker's channel rows -> flow map + resolved edges

use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;

use crate::Addr;
use crate::metrics::cardinality::Cardinality;
use crate::metrics::channel::Channel;

/// One operator-to-operator link and the tuples it moved; exactly one
/// per logged [`Channel`]. `None` endpoints face an unpaired scope
/// boundary; `None` volumes are unobservable.
#[derive(Debug, Clone)]
pub(crate) struct Edge {
    pub(crate) src: Option<Addr>,
    pub(crate) src_port: u32,
    pub(crate) tgt: Option<Addr>,
    /// Tuples sent / received on this worker; equal on batch edges.
    pub(crate) sent: Option<i64>,
    pub(crate) recvd: Option<i64>,
}

/// A batch channel's topology, counts dropped (they are handles, not
/// tuples); [`batch_volumes`] recovers the volume from the producer. A
/// `None` src is an unpaired boundary: volume unknowable.
#[derive(Debug)]
struct BatchEdge {
    src: Option<Addr>,
    /// The producer's port (the outer one after a boundary follow), so
    /// the fanout fold counts both halves once.
    src_port: u32,
    tgt: Option<Addr>,
}

/// Resolve raw channels into per-operator flow and per-edge volumes: one
/// flow entry per `op_names` address; anything unproved stays `None`,
/// never zero.
pub(crate) fn resolve(
    channels: &[Channel],
    op_names: &BTreeMap<Addr, String>,
) -> (BTreeMap<Addr, Cardinality>, Vec<Edge>) {
    // Stage 1: batch channels' topology (collection endpoints resolve on
    // the fly).
    let batch = batch_edges(channels);

    // Stage 2: tally the tuple-shipping channels per operator.
    let (mut tup_in, mut tup_out) = collection_volumes(channels);

    // Stage 3: recover what each batch producer's handles carry.
    let batch_vol = batch_volumes(&batch, op_names, &tup_in);

    // Stage 4: commit recovered volumes to consumers' inputs.
    apply_batch_inputs(&batch, &batch_vol, &mut tup_in);

    // A producer's output is assigned once: a shared trace is produced
    // once, read many times.
    for (addr, vol) in &batch_vol {
        tup_out.insert(addr.clone(), *vol);
    }

    // One flow entry per logged address; uncomputed volumes stay None.
    let flow = op_names
        .keys()
        .map(|addr| {
            let flow = Cardinality {
                tup_in: tup_in.get(addr).copied(),
                tup_out: tup_out.get(addr).copied(),
            };
            (addr.clone(), flow)
        })
        .collect();

    // Collection edges keep logged counts; batch edges carry the
    // recovered producer volume on both sides.
    let mut edges: Vec<Edge> = channels
        .iter()
        .filter(|c| !c.ships_batches)
        .map(|c| Edge {
            src: endpoint(&c.scope_addr, c.src),
            src_port: c.src_port,
            tgt: endpoint(&c.scope_addr, c.tgt),
            sent: Some(c.sent),
            recvd: Some(c.recvd),
        })
        .collect();
    edges.extend(batch.into_iter().map(|e| {
        let vol = e.src.as_ref().and_then(|s| batch_vol.get(s)).copied();
        Edge {
            src: e.src,
            src_port: e.src_port,
            tgt: e.tgt,
            sent: vol,
            recvd: vol,
        }
    }));
    (flow, edges)
}

/// The operator address of a channel endpoint; `None` for index `0`, the
/// scope boundary.
fn endpoint(scope: &Addr, idx: u32) -> Option<Addr> {
    (idx != 0).then(|| {
        let mut a = scope.0.clone();
        a.push(idx);
        Addr(a)
    })
}

/// Resolve batch channels into topology-only [`BatchEdge`]s, boundary
/// sources followed to their outer producer via the ingress pairing;
/// unpaired sources stay unknown.
fn batch_edges(channels: &[Channel]) -> Vec<BatchEdge> {
    // Entered traces: the outer half, keyed by (scope op, port), matched
    // by inner halves sourced at the boundary.
    let mut ingress: HashMap<(Addr, u32), (Addr, u32)> = HashMap::new();
    for c in channels.iter().filter(|c| c.ships_batches) {
        if let (Some(s), Some(t)) = (
            endpoint(&c.scope_addr, c.src),
            endpoint(&c.scope_addr, c.tgt),
        ) {
            ingress.insert((t, c.tgt_port), (s, c.src_port));
        }
    }

    let mut batch = Vec::new();
    for c in channels.iter().filter(|c| c.ships_batches) {
        let (src, src_port) = match endpoint(&c.scope_addr, c.src) {
            Some(s) => (Some(s), c.src_port),
            // A single hop: FlowLog compiles single-level recursion
            // only. If nested recursion lands, this must follow the
            // pairing as a chain; until then a deeper nest stays an
            // unpaired boundary, unknown.
            None => match ingress.get(&(c.scope_addr.clone(), c.src_port)) {
                Some((s, port)) => (Some(s.clone()), *port),
                None => (None, c.src_port),
            },
        };
        batch.push(BatchEdge {
            src,
            src_port,
            tgt: endpoint(&c.scope_addr, c.tgt),
        });
    }
    batch
}

/// Tally collection channels into per-operator `(input, output)`:
/// receives sum per target; sends max per port (fanout ships
/// duplicates), then ports sum.
fn collection_volumes(channels: &[Channel]) -> (HashMap<Addr, i64>, HashMap<Addr, i64>) {
    let mut tup_in: HashMap<Addr, i64> = HashMap::new();
    let mut out_by_port: HashMap<(Addr, u32), i64> = HashMap::new();
    for c in channels.iter().filter(|c| !c.ships_batches) {
        if let Some(s) = endpoint(&c.scope_addr, c.src) {
            let slot = out_by_port.entry((s, c.src_port)).or_insert(0);
            *slot = (*slot).max(c.sent);
        }
        if let Some(t) = endpoint(&c.scope_addr, c.tgt) {
            *tup_in.entry(t).or_insert(0) += c.recvd;
        }
    }
    let mut tup_out: HashMap<Addr, i64> = HashMap::new();
    for ((addr, _port), sent) in out_by_port {
        *tup_out.entry(addr).or_insert(0) += sent;
    }
    (tup_in, tup_out)
}

/// A batch-edge source's volume behavior, which decides what the edge recovers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BatchRole {
    /// A pure arrange: holds exactly the tuples it ingested.
    Seed,
    /// A wrapper that ships exactly what it receives.
    PassThrough,
    /// Builds its own output; volume unobservable.
    Opaque,
}

/// Classify a batch-edge source by name: differential's names plus
/// FlowLog's `Arrange: <X>` labels, which must keep the prefix.
fn batch_role(name: &str) -> BatchRole {
    if name.starts_with("Arrange") {
        BatchRole::Seed
    } else if name == "Map" || name == "FlatMap" {
        BatchRole::PassThrough
    } else {
        BatchRole::Opaque
    }
}

/// Recover batch producers' volumes: seed at arranges from their
/// collection input, then propagate through pass-through targets whose
/// sources are all known (one unknown poisons). Each sweep resolves a
/// target or stops, so termination is structural.
fn batch_volumes(
    batch: &[BatchEdge],
    op_names: &BTreeMap<Addr, String>,
    collection_in: &HashMap<Addr, i64>,
) -> HashMap<Addr, i64> {
    let role = |addr: &Addr| {
        op_names
            .get(addr)
            .map_or(BatchRole::Opaque, |n| batch_role(n))
    };

    let mut vol: HashMap<Addr, i64> = HashMap::new();
    for e in batch {
        if let Some(s) = &e.src
            && role(s) == BatchRole::Seed
        {
            vol.insert(s.clone(), collection_in.get(s).copied().unwrap_or(0));
        }
    }

    let targets: BTreeSet<&Addr> = batch
        .iter()
        .filter_map(|e| e.tgt.as_ref())
        .filter(|t| role(t) == BatchRole::PassThrough)
        .collect();
    let mut changed = true;
    while changed {
        changed = false;
        for t in &targets {
            if vol.contains_key(*t) {
                continue;
            }
            let total: Option<i64> = batch
                .iter()
                .filter(|e| e.tgt.as_ref() == Some(t))
                .map(|e| e.src.as_ref().and_then(|s| vol.get(s)).copied())
                .sum();
            if let Some(v) = total {
                vol.insert((*t).clone(), v);
                changed = true;
            }
        }
    }
    vol
}

/// Commit batch volumes to consumers' inputs: all in-edges resolved
/// adds their sum; one unresolved in-edge wipes the input entirely.
fn apply_batch_inputs(
    batch: &[BatchEdge],
    vol: &HashMap<Addr, i64>,
    tup_in: &mut HashMap<Addr, i64>,
) {
    // Recomputed from final volumes: they are write-once, so this equals
    // accumulating during propagation.
    let mut by_target: HashMap<&Addr, Option<i64>> = HashMap::new();
    for e in batch {
        let Some(t) = &e.tgt else { continue };
        let v = e.src.as_ref().and_then(|s| vol.get(s)).copied();
        let acc = by_target.entry(t).or_insert(Some(0));
        *acc = acc.zip(v).map(|(a, b)| a + b);
    }
    for (t, total) in by_target {
        match total {
            Some(v) => *tup_in.entry(t.clone()).or_insert(0) += v,
            None => {
                tup_in.remove(t);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metrics::channel::test_support::addr;
    use crate::metrics::channel::test_support::chan;
    use crate::metrics::channel::test_support::names;

    fn card(tup_in: Option<i64>, tup_out: Option<i64>) -> Cardinality {
        Cardinality { tup_in, tup_out }
    }

    /// One FlatMap output port feeding two consumers ships the same tuples
    /// twice; its tup_out must not double.
    #[test]
    fn fanout_counts_an_output_port_once() {
        let ns = names(&[(&[0, 1], "FlatMap"), (&[0, 2], "Map"), (&[0, 3], "Map")]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (1, 0), (2, 0), false, 500, 500),
                chan(&[0], (1, 0), (3, 0), false, 500, 500),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 1])].tup_out, Some(500));
    }

    #[test]
    fn join_input_sums_across_ports() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 2], "ArrangeBySelf"),
            (&[0, 3], "Join"),
            (&[0, 4], "FlatMap"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (4, 0), (1, 0), false, 100, 100),
                chan(&[0], (4, 1), (2, 0), false, 30, 30),
                chan(&[0], (1, 0), (3, 0), true, 7, 7),
                chan(&[0], (2, 0), (3, 1), true, 7, 7),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 3])].tup_in, Some(130));
    }

    /// A pure arrange holds exactly the tuples it ingested: its flow is
    /// input == output.
    #[test]
    fn arrange_flow_is_ingested_volume_in_and_out() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 3], "Join"),
            (&[0, 4], "FlatMap"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (4, 0), (1, 0), false, 100, 100),
                chan(&[0], (1, 0), (3, 0), true, 7, 7),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 1])], card(Some(100), Some(100)));
    }

    /// An outer arrange entering scope [0, 9] is rewrapped by a FlatMap
    /// inside before reaching the Join; the volume must survive both hops.
    #[test]
    fn entered_trace_bridges_the_scope_boundary() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 9], "Iterative"),
            (&[0, 9, 1], "FlatMap"),
            (&[0, 9, 2], "Join"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (2, 0), (1, 0), false, 1000, 1000),
                chan(&[0], (1, 0), (9, 3), true, 5, 5),
                chan(&[0, 9], (0, 3), (1, 0), true, 5, 5),
                chan(&[0, 9], (1, 0), (2, 0), true, 5, 5),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 9, 1])], card(Some(1000), Some(1000)));
        assert_eq!(flow[&addr(&[0, 9, 2])].tup_in, Some(1000));
    }

    #[test]
    fn opaque_batch_source_leaves_consumer_input_unknown() {
        let ns = names(&[(&[0, 1], "Reduce"), (&[0, 2], "AsCollection")]);
        let (flow, _) = resolve(&[chan(&[0], (1, 0), (2, 0), true, 3, 3)], &ns);
        assert_eq!(flow[&addr(&[0, 2])].tup_in, None);
        assert_eq!(flow[&addr(&[0, 1])].tup_out, None);
    }

    /// One resolvable arranged input plus one opaque input: a partial sum
    /// would read as complete, so the join reports unknown.
    #[test]
    fn partial_input_reports_no_input_at_all() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 2], "Reduce"),
            (&[0, 3], "Join"),
            (&[0, 4], "FlatMap"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (4, 0), (1, 0), false, 100, 100),
                chan(&[0], (1, 0), (3, 0), true, 7, 7),
                chan(&[0], (2, 0), (3, 1), true, 7, 7),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 3])].tup_in, None);
    }

    /// A boundary-sourced batch edge with no outer half to pair with (a
    /// deeper nest than the one-hop follow covers) stays unknown.
    #[test]
    fn unpaired_boundary_source_stays_unknown() {
        let ns = names(&[(&[0, 9], "Iterative"), (&[0, 9, 1], "FlatMap")]);
        let (flow, edges) = resolve(&[chan(&[0, 9], (0, 3), (1, 0), true, 5, 5)], &ns);
        assert_eq!(flow[&addr(&[0, 9, 1])].tup_in, None);
        assert!(edges[0].src.is_none());
    }

    #[test]
    fn output_sums_across_distinct_ports() {
        let ns = names(&[
            (&[0, 1], "FlatMap"),
            (&[0, 2], "Map"),
            (&[0, 3], "Map"),
            (&[0, 4], "Map"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (1, 0), (2, 0), false, 500, 500),
                chan(&[0], (1, 0), (3, 0), false, 500, 500),
                chan(&[0], (1, 1), (4, 0), false, 30, 30),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 1])].tup_out, Some(530));
    }

    /// A seed volume survives a two-wrapper batch chain, exercising the
    /// multi-round propagation the single-wrapper tests never reach.
    #[test]
    fn pass_through_chain_propagates_seed_volume_end_to_end() {
        let ns = names(&[
            (&[0, 1], "ArrangeByKey"),
            (&[0, 2], "Map"),
            (&[0, 3], "FlatMap"),
            (&[0, 4], "Join"),
            (&[0, 5], "Input"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (5, 0), (1, 0), false, 100, 100),
                chan(&[0], (1, 0), (2, 0), true, 7, 7),
                chan(&[0], (2, 0), (3, 0), true, 7, 7),
                chan(&[0], (3, 0), (4, 0), true, 7, 7),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 2])], card(Some(100), Some(100)));
        assert_eq!(flow[&addr(&[0, 3])], card(Some(100), Some(100)));
        assert_eq!(flow[&addr(&[0, 4])].tup_in, Some(100));
    }

    /// One row per name family of the batch-role table; iterating spellings
    /// through `resolve` keeps future rows covered.
    #[rstest::rstest]
    #[case("ArrangeByKey", Some(100))]
    #[case("ArrangeBySelf", Some(100))]
    #[case("Arrange: ThresholdTotal", Some(100))]
    #[case("Reduce", None)]
    #[case("Mapper", None)]
    fn batch_source_role_decides_consumer_input(
        #[case] source_name: &str,
        #[case] expected_in: Option<i64>,
    ) {
        let ns = names(&[
            (&[0, 1], source_name),
            (&[0, 2], "AsCollection"),
            (&[0, 4], "FlatMap"),
        ]);
        let (flow, _) = resolve(
            &[
                chan(&[0], (4, 0), (1, 0), false, 100, 100),
                chan(&[0], (1, 0), (2, 0), true, 7, 7),
            ],
            &ns,
        );
        assert_eq!(flow[&addr(&[0, 2])].tup_in, expected_in);
    }
}
