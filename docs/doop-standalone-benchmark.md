# DOOP standalone: FlowLog vs Soufflé — correctness & scaling

A per-analysis-family study of FlowLog against Soufflé on the **DOOP standalone**
context-sensitivity variants (the `verify/standalone` programs), complementing the
whole-suite `default.dl` numbers in the [README](../README.md#vs-soufflé--doop).

- **Programs:** 19 standalone analyses (context-insensitive, k-object/type/call-site
  sensitive, ± heap, type-object hybrids, adaptive).
- **Input:** DaCapo `luindex` facts (713 MB), identical for both engines.
- **Threads:** FlowLog `-w 32` vs Soufflé `-j 32` (compiled and run with `-j 32`).
- **Oracle:** Soufflé. It is self-consistent — `-j1` and `-j32` are byte-identical —
  so any divergence or nondeterminism is objectively FlowLog's.
- Each program was run **2×** per engine; wall time, peak RSS, and the `VarPointsTo`
  count were recorded, plus byte-exact correctness (vs Soufflé) and determinism
  (FlowLog run-to-run).

## TL;DR

- **Correctness: all 19 families are byte-exact with Soufflé and deterministic at 32
  threads** — once the `ord` determinism fix ([#208](https://github.com/flowlog-rs/flowlog/pull/208))
  is applied (see below).
- **Performance: FlowLog wins 16/19** by 1.5–4.9×. **Soufflé wins the 3 type-object
  hybrids by 5–6×** — a scaling gap, not an algorithmic one (see the deep-dive).
- FlowLog uses ~1.3–2.7× more RAM.

## Correctness required a determinism fix: `ord` under parallelism

Before the fix, the 8 type-sensitive families were **nondeterministic at `-w32`**
(counts drifted run-to-run; e.g. `3-type-sensitive+2-heap` gave
1 483 341 / 1 479 030 / 1 480 075 vs Soufflé's stable 1 483 057), while `-w1` was
byte-exact.

**Root cause.** DOOP picks a canonical representative heap with `min(ord(heap))`.
FlowLog compiled `ord(s)` to the string's intern key, assigned in first-insertion
order. Under `-w>1`, workers load fact-file byte ranges concurrently, so the intern
order — and therefore `ord`, and therefore the `min`-representative — was
nondeterministic. Soufflé's `ord` is insertion-independent, so it never drifts.

**Fix ([#208](https://github.com/flowlog-rs/flowlog/pull/208)).** Make interning
deterministic instead of changing what `ord` returns: when string interning is on,
worker 0 loads all fact files (peers load none), so intern order — hence `ord` — is
fixed regardless of worker count; Differential Dataflow redistributes the tuples
during evaluation. `ord` stays the intern key, keeping it a cheap dense integer.
Result: `-w32` becomes byte-identical to `-w1` / Soufflé on every family.

> Approaches that instead change `ord`'s *value* (e.g. a content hash) diverge from
> Soufflé — they alter the chosen representative (measured +1197 tuples on one family)
> and a non-injective hash breaks the `ord(rep) = min_ord` reverse lookup. The only
> correct fix keeps `ord` and makes interning deterministic.

Cost of the fix: a one-time ~+1.75 s fact-load serialization (~10 % on the fastest
family, <4 % typically, 0 % at `-w1` and for programs that don't use `ord`). No RSS
regression.

## Performance (FlowLog `-w32` vs Soufflé `-j32`)

Wall-clock seconds (`r1`, the clean run); peak RSS in GB. All rows byte-exact
(`MATCH`) and deterministic.

| family | FL s | SF s | speedup | FL RSS | SF RSS |
|---|---:|---:|:---:|---:|---:|
| 3-type-sensitive+3-heap | 18.8 | 29.1 | **1.6× FL** | 3.9 | 1.5 |
| 3-type-sensitive+2-heap | 19.9 | 30.3 | **1.5× FL** | 3.9 | 1.5 |
| 3-object-sensitive+2-heap | 21.3 | 57.1 | **2.7× FL** | 5.4 | 2.7 |
| 3-object-sensitive+3-heap | 21.6 | 59.4 | **2.8× FL** | 5.6 | 2.7 |
| 4-object-sensitive+4-heap | 21.8 | 55.5 | **2.5× FL** | 6.0 | 2.6 |
| context-insensitive | 22.0 | 43.3 | **2.0× FL** | 4.5 | 2.8 |
| 1-type-sensitive | 24.7 | 76.9 | **3.1× FL** | 5.4 | 4.4 |
| 2-type-sensitive+heap | 24.8 | 49.1 | **2.0× FL** | 4.5 | 2.8 |
| 1-call-site-sensitive | 29.0 | 116.5 | **4.0× FL** | 7.2 | 7.5 |
| 1-call-site-sensitive+heap | 29.6 | 129.3 | **4.4× FL** | 7.3 | 7.8 |
| 1-object-sensitive | 35.2 | 172.7 | **4.9× FL** | 7.2 | 7.0 |
| 1-type-sensitive+heap | 38.8 | 98.2 | **2.5× FL** | 6.4 | 5.6 |
| adaptive-2-object-sensitive+heap | 51.7 | 117.2 | **2.3× FL** | 8.1 | 5.4 |
| 2-object-sensitive+heap | 51.9 | 119.2 | **2.3× FL** | 8.0 | 5.4 |
| 1-object-sensitive+heap | 59.8 | 211.2 | **3.5× FL** | 8.7 | 9.4 |
| 2-object-sensitive+2-heap | 125.8 | 186.3 | **1.5× FL** | 10.6 | 6.7 |
| **1-object-1-type-sensitive+heap** | **620.7** | **109.1** | **5.6× SF** | 7.0 | 5.2 |
| **2-type-object-sensitive+heap** | **625.2** | **109.1** | **5.9× SF** | 7.2 | 5.2 |
| **2-type-object-sensitive+2-heap** | **703.0** | **137.1** | **5.0× SF** | 10.1 | 5.6 |

> Timing note: `r1` is the clean measurement. The harness copies+sorts multi-GB outputs
> between the two timed runs, so `r2` is often 10–45 % higher for the *fast* families;
> the slow families are eval-bound and `r1 ≈ r2`. Correctness and determinism are exact
> in every case, both runs.

## Deep-dive: the 3 type-object hybrids don't parallelize

The three families where Soufflé wins all combine **type- and object-sensitivity** in
the calling context. Their `VarPointsTo` sizes (8.7–9.8 M) are *not* the largest
(`1-object-sensitive+heap` is 20 M and FlowLog wins there 3.5×), so it is not volume.

**It's a scaling gap, not a slower algorithm.** On `2-type-object-sensitive+heap`:

| | 1 thread | 32 threads | speedup |
|---|---:|---:|:---:|
| FlowLog | 885 s | 625 s | **1.42×** |
| Soufflé | 915 s | 109 s | **8.4×** |

Single-threaded the engines tie (885 s vs 915 s). The entire 32-thread gap is FlowLog
getting almost no parallel speedup.

**Bottleneck: one recursive join, fully serialized onto one worker.** The `-P`
per-operator profile shows 94 % of the run is the `mainAnalysis.VarPointsTo` fixpoint,
and inside it a single `Join` is pinned to one worker:

| Join operator | worker 5 | each of the other 31 workers |
|---|---:|---:|
| active time | **599 s** | ~7 ms |
| tuples in / out | 1 463 430 / 1 404 274 | **0 / 0** |

Worker 5 processes **all** 1.46 M tuples; the other 31 receive **none** — and 599 s
≈ the whole `-w32` runtime.

**Root cause: an empty-key cross-join from a projection equi-join.** The hot join is
the DOOP context constructor `configuration.ContextResponse :- configuration.ContextRequest(…, hctx, …, value, …), Value_DeclaringType(obj, …)`,
where `obj` is the object *inside* the heap context `hctx`. For type-object
sensitivity `hctx` is a **record**, so after FlowLog desugars the record pattern the
shared variable appears as a **projection `(hctx).0`** in `ContextRequest` but as a
**bare argument** in `Value_DeclaringType`. FlowLog's join-key selection matches keys
by whole-argument **name**, so a projection never matches a bare variable → it finds
**no shared key**, lowers the join to an **empty-key (unit) arrangement on both sides —
a cross-join — and demotes `(hctx).0 == value` to a post-join filter**
(`Join … F:(if (LV1).0 == RV0)`; the profile shows both `Arrange` operators with
`V:(all columns)` and no `K:`). In Differential Dataflow an empty key routes every
tuple to `hash(()) % workers` — one fixed worker — so worker 5 runs the entire join,
which is also O(|L|·|R|) instead of O(|L|+|R|). Soufflé indexes the join on the object
and work-steals across threads, so it is neither serial nor a cross-product.

**The clean fix: key the equi-join on the projection (hash join, not cross-join).**
`(hctx).0 == value` *is* an equi-join with a projection on one side. Have the planner
materialize the projection as a fresh column (`g = (hctx).0`) via a `Map`, key the left
arrangement on `g` and the right on `value`, and drop the redundant filter. That turns
the unit-key cross-join into a normal hash join keyed on the **object** — high
cardinality, so the hash spreads tuples across all 32 workers, and cost drops to
O(|L|+|R|). It is **byte-exact** (the key enforces exactly the equality the filter did)
and needs no new runtime operator — FlowLog already builds arrangement keys with a
`flat_map` before `arrange_by_key`, so a computed/`TupleProj` key is a planner-only
change: generalize join-key selection from "shared **bare** variable" to "equi-join
where each side is computable from one relation", triggered when the shared-variable
key would otherwise be empty. Expected to move these three families from ~1.4× toward
near-linear scaling (Soufflé gets 8.4×). Two runtime fallbacks — a skew-aware exchange
for genuine empty-key joins, and pushing the `ThresholdTotal` dedup ahead of the
exchange — cover the rare case with no projection to key on.

These are FlowLog-engine changes, orthogonal to the `ord` fix (which only touches fact
loading and does not affect these families' evaluation beyond the shared load cost).

## Reproducing

- Build the engine, compile each `verify/standalone` program with the FlowLog compiler,
  and run `-w 32`; compile+run each Soufflé equivalent with `-j 32` on the same
  `luindex` facts.
- Compare outputs byte-for-byte after canonicalizing bracket/whitespace; check FlowLog
  run-to-run determinism the same way.
- The `ord` fix ([#208](https://github.com/flowlog-rs/flowlog/pull/208)) is required for
  the type-sensitive families to be deterministic at `-w32`.
