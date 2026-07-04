# FlowLog on sasy workloads — benchmark report

This branch (`main-next-for-sasy` = `main-next` + 5 codegen perf commits) was
benchmarked against Soufflé on sasy's Datalog security policies, to answer one
question: can incremental FlowLog replace the warm Soufflé evaluator sasy runs
today, and where does it stand? The harness lives in the sibling repo
(`sasy-labs/sasy`, branch `flowlog-inc-integration`, `sasy-services/flowlog/`).

## TL;DR

- **Correctness:** byte-identical decisions on every workload; real airline
  traces **1162/1162 agree** (910 allow / 252 deny).
- **Speed:** Souffle-warm ~**130x faster** at sasy's operating point (p50 21 µs
  vs 2.8 ms/query) — real conversations are short (median 19 messages), and
  FlowLog's incremental advantage only shows on long ones.
- **Crossover:** FlowLog pulls ahead only past turn ~35 (full-context fan-in),
  reaching ~16x at 200 turns — a ceiling real traces never reach.
- **Bottleneck:** FlowLog's ~2.8 ms/query is a fixed cost, dominated by Timely
  per-step progress coordination over the policy's ~918 operators. It scales
  with policy size; the usual structural optimizations are already applied.

## The 5 codegen optimizations on this branch

All in `flowlog-build`, incremental mode:

| commit | what |
| --- | --- |
| `b72664b` | elide identity projections (drop no-op `flat_map(\|r\| once(r))`) |
| `f58da4e` | `threshold_total` for non-recursive dedup |
| `27337a9` | scope-aware incremental antijoin dedup (correct under `Product` time) |
| `bfea673` | scope-aware SIP projection dedup; one dedup-choice choke point |
| `9aec504` | `--assume-set-inputs` — skip per-input dedup for set-semantic inputs (195->180 dedup ops) |

## Setup

One binary links both engines in-process — Souffle via its production
`evaluator_shim.cpp` (resident graph, purge derived + swap ephemerals + re-run),
FlowLog via the generated `DatalogIncrementalEngine` — drives them with the
identical input, and gates on decision parity before trusting any timing. No
IPC, no file I/O, same clock. FlowLog runs incrementally: one net-delta commit
per query (retract prev ephemerals + insert current in a single `begin/commit`),
never two commits per round.

## Results — real airline traces (400 conversations, 1162 queries)

| | Souffle-warm | FlowLog-inc |
| --- | --: | --: |
| decisions | parity 1162/1162 (910 allow / 252 deny) | identical |
| p50 / query | **21 µs** | **2.8 ms** (~130x) |

Per-turn cost inside one growing conversation shows the crossover (engine-only):

| turn | nodes | Souffle | FlowLog | winner |
| --: | --: | --: | --: | --- |
| 5   | 18  | 105 µs  | 3.5 ms | Souffle 33x |
| 33  | 102 | 2.7 ms  | 4.4 ms | ~tie |
| 200 | 603 | 324 ms  | 20 ms  | FlowLog 16x |

Souffle re-derives the whole slice each query (O(slice) — cheap when small,
explodes when large); FlowLog is a fixed floor + O(delta) (flat). sasy
conversations end at turn ~3-9, in the Souffle-wins zone.

## Bottleneck — why ~2.8 ms/query

Flat vs graph size (3.0 ms at 5 nodes, 3.3 ms at 40), so a fixed cost. `perf`
self-time:

| bucket | ~% self | what |
| --- | --: | --- |
| Timely progress / scheduling | **~26 %** | `propagate_pointstamps`, op-scheduling `BinaryHeap`, frontier/progress bookkeeping |
| sorting | ~6 % | DD arrangement build |
| allocation | ~5 % | malloc/free |

The 147-rule policy compiles to ~918 operators (227 arrangements, 168 joins,
245 flat_maps, 172 thresholds); every commit schedules and progress-tracks all
of them regardless of the tiny delta — and this is **after** FlowLog's
arrangement sharing (PR #148, merged), operator fusion, and dead/duplicate
pruning. sasy also builds one engine per session, so per-session construction
(~5 ms x 400 ≈ 2 s) is a second fixed cost on short conversations.

## Remaining levers

1. **Idle-operator progress elision** (the real one) — DD `master-next`
   `FrontierInterest::IfCapability` (#687) drops idle regions from progress,
   once FlowLog's flat dataflow is carved into regions (it has none today).
2. **Amortize per-session construction** — a resident engine per
   `(tenant, session)` that retracts a finished conversation instead of
   rebuilding (prototyped in `sasy-labs/sasy`: total -22 %, time-to-first-
   decision -38 %).

Dropping `.output DenialReason` does **not** help — `Unauthorized :-
DenialReason(...)` makes it load-bearing for the deny decision.

## Notes

- The harness normalizes conversation content to ASCII on both engines (sasy's
  Rust functor port byte-slices `&str`; Souffle's C++ functors are
  byte-oriented). With that, FlowLog completes the workload with full parity.
- These optimizations change no decisions — every workload stays
  parity-identical to Souffle.

Full methodology, per-turn CSVs, and the profiling breakdown live in
`sasy-labs/sasy` → `sasy-services/flowlog/airline-lib-bench/`.
