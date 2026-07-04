# FlowLog on sasy workloads — benchmark report

This branch (`main-next-for-sasy` = `main-next` + 5 codegen perf commits) was
benchmarked against **Soufflé** on **sasy's** Datalog security policies, to
answer one question: *can incremental FlowLog replace the warm Soufflé
evaluator sasy runs today, and where does it stand?*

The engine integration and the benchmark harness live in the sibling repo
(`sasy-labs/sasy`, branch `flowlog-inc-integration`,
`sasy-services/flowlog/`). This file is the summary of what was measured and
what it showed.

---

## TL;DR

- **Correctness:** on every workload, FlowLog-inc and Soufflé-warm produce
  **byte-identical decisions** (parity-gated on each query). On the real
  airline workload: **1162/1162 queries agree** (910 allow / 252 deny).
- **Speed on sasy's real workload:** Soufflé-warm ~**136× faster** (p50 21 µs
  vs 2865 µs/query) — because real agent conversations are **short**
  (median 19 messages), and FlowLog's incremental advantage only appears on
  *long* conversations.
- **Where FlowLog wins:** only when conversations grow long and full-context
  (synthetic fan-in): FlowLog crosses over around turn ~45 and reaches
  **~15×** at 200 turns. Real traces never get that long.
- **Bottleneck:** FlowLog's ~3 ms/query is a *fixed* cost (independent of the
  data), dominated by Timely's per-step progress coordination over the
  policy's ~919 operators — i.e. it scales with **policy size**, and the usual
  structural optimizations (arrangement sharing, fusion, pruning) are
  **already applied**.

---

## The 5 optimizations on this branch

All in `flowlog-build` codegen, incremental (`--mode datalog-inc`) mode:

| commit | what |
| --- | --- |
| `b72664b` | elide identity projections — drop no-op `flat_map(|r| once(r))` operators |
| `f58da4e` | use `threshold_total` for non-recursive dedup (lighter than general `threshold`) |
| `27337a9` | make incremental antijoin dedup scope-aware (correct under `Product` time) |
| `bfea673` | make SIP projection dedup scope-aware; unify the dedup-operator choice through one choke point |
| `9aec504` | `--assume-set-inputs` — skip the per-input dedup when the driver stages set-semantic inputs |

`--assume-set-inputs` was verified effective by a codegen A/B on the airline
policy: **195 → 180** dedup operators with the flag on.

The engine is driven **incrementally in library mode**, one **net-delta
commit** per query (retract the previous query's ephemerals + insert the new
ones in a single `begin/commit`), not two commits per round.

---

## What was benchmarked

A single harness links **both engines in-process** (Soufflé via its real
`evaluator_shim.cpp` over an FFI shim; FlowLog via the generated
`DatalogIncrementalEngine`), drives them with the **identical** input, and
**gates on decision parity** before trusting any timing. No subprocess, no
IPC, no file I/O — same clock, only the evaluation model differs. The Soufflé
side is the exact production model (resident graph, purge derived relations +
swap ephemerals + re-run) — verified against sasy's `sasy-policy` crate.

Workloads:

1. **MALADE** (sasy's request-response policy) — synthetic growing conversation.
2. **Airline (tau2)** — synthetic growing conversation (fan-in and linear).
3. **Airline, real traces** — replays sasy's **actual 400 tau2-airline agent
   conversations** (1162 authorization queries, real reservation JSON), one
   query per assistant tool-call. This is the faithful, representative test.

---

## Results

### Real airline workload (the representative one)

400 conversations, **median 19 / max 46 messages**, 1162 queries:

| | Soufflé-warm | FlowLog-inc |
| --- | --- | --- |
| decisions | ✅ parity 1162/1162 (910 allow / 252 deny) | ✅ identical |
| p50 / query | **21 µs** | 2865 µs (**~136× slower**) |

FlowLog is **correct**, just not competitive at these conversation lengths.
Its fixed per-commit cost dominates the tiny (5–19-node) real graphs.

### Airline synthetic (stress ceiling)

FlowLog speedup vs Soufflé (engine-only, 1 worker; >1 = FlowLog wins):

| shape | T=19 | T=46 | T=96 | T=200 |
| --- | --: | --: | --: | --: |
| linear (realistic) | 0.06× | 0.16× | 0.38× | 0.96× |
| fan-in (full context) | 0.21× | 1.12× | 5.3× | 15.1× |

FlowLog only pulls ahead on **long, full-context** conversations — a ceiling
real traces don't reach. MALADE shows the same shape (up to ~97× fan-in at
T=200, but warm wins at the short/linear operating point).

---

## Bottleneck — why FlowLog is ~3 ms/query

Perf-profiled on 3486 commits. The cost is **data-independent** (flat vs graph
size: 3.0 ms at 5 nodes, 3.3 ms at 40), so it is a fixed cost, not data work:

| bucket | ~% self | what |
| --- | --: | --- |
| Timely progress / scheduling | **~26 %** | `propagate_pointstamps`, operator-scheduling `BinaryHeap`, frontier/progress bookkeeping |
| sorting | ~6 % | DD batch/arrangement |
| allocation | ~5 % | malloc/free |

The airline policy (147 rules) compiles to **~919 operators** (227
arrangements, 167 `join_core`, 245 `flat_map`, 172 thresholds). Every commit
schedules + progress-tracks *all* of them regardless of the tiny delta.

Crucially, this is **after** FlowLog's structural optimizations:

- **arrangement sharing** — `register_arrangement` (fingerprint-keyed); 227 is
  the post-sharing distinct count.
- **operator fusion** — `join_core` (join + projection) and `flat_map`
  (filter + map), plus identity-projection elision (this branch).
- **pruning** — `prune_cross_stratum_duplicates` + EDB pruning.

So the ~919 operators are the *inherent size* of a large policy after
optimization, not removable redundancy. Additionally, sasy creates one engine
per session, so **per-session construction (~4.9 ms × 400 ≈ 2 s)** is a second
fixed cost, comparable to the commits, on short conversations.

## Where FlowLog could still improve (harder levers)

1. **Tighter arrangement sharing** (PR #148 *fix_insufficient_sharing*, not in
   this stack) — the fingerprint can be over-specific and miss shareable
   arrangements; ~10 % fewer on similar flattened programs.
2. **Amortize per-session construction** — driver keeps a resident engine per
   `(tenant, session)` and *retracts* a finished conversation instead of
   rebuilding. Removes ~all of the 2 s construction on this workload.
3. **Lighter progress for the many-tiny-commits regime** — the dominant cost
   is Timely's per-step coordination, which is O(operators) regardless of
   delta size; fewer scopes/regions or a lighter progress path is the real
   ceiling-lifter.

(Policy-side, dropping `.output DenialReason` — long human-readable denial
strings the allow/deny decision doesn't need — would let FlowLog prune that
work, but that is a sasy policy choice, not a compiler fix.)

## Notes

- The harness normalizes conversation content to ASCII on **both** engines:
  sasy's Rust functor port assumes ASCII input (it byte-slices `&str`),
  whereas Soufflé's C++ functors are byte-oriented. With that normalization,
  FlowLog completes the entire workload with full parity.
- The optimizations on this branch change **no decisions** — every workload
  stays parity-identical to Soufflé.

Full methodology, per-turn CSVs, and the profiling breakdown are in
`sasy-labs/sasy` → `sasy-services/flowlog/airline-lib-bench/`
(`README.md`, `PROFILING.md`, `results/`).
