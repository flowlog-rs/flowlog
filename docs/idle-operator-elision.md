# Idle-operator elision (timely `FrontierInterest::IfCapability`)

Status: **experimental, default-off, and blocked on upstream differential-dataflow.**
This document records the optimization, its measured effect, the correctness
argument, and the upstream change it depends on. FlowLog is **not** shipping this
soon; we are waiting for differential-dataflow to expose it (DD PR #723). The
FlowLog-side change in this PR is an inert, default-off codegen hook; the
differential-dataflow changes are described here but intentionally **not** carried
in this repo.

## The idea in one paragraph

FlowLog compiles a program into a graph of ~10^3 differential-dataflow operators.
On every incremental commit, timely wakes each operator whose input frontier
advanced to ask "any work?" — even when a tiny delta only flows through a handful
of rules. Most operators are idle yet still pay a fixed scheduling cost. Timely
already exposes a per-input flag, `FrontierInterest`, that lets an operator opt
out of those wakeups: with `IfCapability` an operator is scheduled on a frontier
advance only when it holds a capability (i.e. it actually has un-processed data).
Flipping the idle-prone operators to `IfCapability` makes each commit touch only
the rules that received new tuples.

## Which operators (and which do not apply)

Only operators that ask to be notified on frontier advances can be idle-woken —
those built on timely's `unary_frontier` / `binary_frontier`, which default to
`Always`. In differential-dataflow that is the **frontier family**:

| operator                         | built on          | benefits |
| -------------------------------- | ----------------- | -------- |
| `arrange` (all `arrange_by_*`)   | `unary_frontier`  | yes      |
| `join` / `join_core`             | `binary_frontier` | yes      |
| `reduce`                         | `unary_frontier`  | yes      |
| `threshold` / `distinct`         | `unary_frontier`  | yes      |
| `count`                          | `unary_frontier`  | yes      |
| `map` / `filter` / `flat_map` / `negate` / `consolidate` | `unary`/`binary` (data-driven) | no-op |

`map`-like operators are only activated by data, so there is nothing idle to
skip. `probe` / `capture` / `feedback` must stay `Always` — reacting to frontier
advances is their job.

## Why it is correct

`FrontierInterest` gates only whether an operator's `schedule()` runs; it does
**not** change progress bookkeeping. In timely's scheduler
(`progress/subgraph.rs`):

```rust
let activate = match notify[port] {
    Always       => true,
    IfCapability => operators[node].cap_counts > 0,   // run only if it holds a capability
    Never        => false,
};
if activate { /* schedule the operator */ }

// Keep this current independent of the interest.
self.children[node].shared_progress.frontiers[port].update(time, diff);
```

Two things could go wrong, and neither does:

1. **Skipping something it needed to do.** In differential-dataflow an operator
   holds a capability exactly while it has pending, un-processed tuples (un-sealed
   batches for `arrange`, pending matches for `join`, pending output corrections
   for `reduce`). `IfCapability` runs it precisely then. With no capability it has
   nothing to emit, so skipping it yields the identical result.
2. **Falling behind.** The last line above updates the operator's input frontier
   **whether or not it ran** — so its view of "which times are complete" stays
   correct at all times. When real data arrives, the data (not the clock) wakes
   the operator, and it reads a still-correct frontier.

Timely already relies on this: nested subgraphs default their inputs to
`IfCapability`. In Datalog terms: a rule that received no new tuples this round
derives none this round; skipping its operator changes nothing, and a later tuple
wakes it with a correct completed-time.

## Measured effect (tau2-airline, library mode, incremental)

Real trace: 400 conversations, 1162 queries, single worker, parity-gated against
Soufflé (`GATE OK` = FlowLog output identical to the Soufflé oracle on all 1162).
Per-commit latency:

| coverage                                   | commit p50 | vs baseline |
| ------------------------------------------ | ---------- | ----------- |
| baseline (all `Always`)                    | 2846 us    | —           |
| arrange only                               | 2480 us    | -15%        |
| + `threshold`/dedup arranges               | 2262 us    | -22%        |
| + `join`                                   | 1943 us    | -32%        |
| + `reduce`/`threshold`/`count` operators   | 1709 us    | **-40%**    |

`GATE OK` in every configuration; holds at 4 workers (2847 -> 1707).

Honesty on magnitude: the saving is a near-fixed per-idle-operator cost, so the
percentage is largest when deltas are tiny (the real sasy regime) and shrinks
when a commit carries a lot of data (a synthetic data-heavy run is single-digit
percent). It is a self-improvement in FlowLog's per-commit overhead, not a change
to the asymptotics and not an overtaking of warm Soufflé on tiny-graph workloads.

## Correctness vetting (generality)

Beyond airline parity, the whole frontier family on `IfCapability` was run against
FlowLog's own end-to-end lib fixture suite (`tests/fixtures`, 123 programs across
`datalog-batch` / `datalog-inc` / `extend-batch`), comparing outputs to expected:

- baseline (`Always`): 123/123 pass — the mechanism is behaviourally identical to
  stock at `Always`.
- `IfCapability` (all frontier operators): 123/123 pass — including many recursive
  programs (`recursive_tc_delta`, `recursive_neg_delta`, `recursive_min/max_delta`,
  `recursive_sip_delta`), aggregation, negation, delete, multi-transaction, and
  fixpoint loops.

This is the engine's own correctness bar across diverse programs, so the win is
not airline-specific, and recursion/feedback is empirically safe.

## The differential-dataflow changes (documented, not carried here)

These belong upstream in differential-dataflow, not in FlowLog. They are small and
mechanical — each frontier operator is built on timely's `*_frontier`, and the
change is to pass `FrontierInterest::IfCapability` on its input(s) instead of
accepting the `Always` default:

```rust
// timely's unary_frontier/binary_frontier hardcode `Always`. The whole change
// is one call on the operator builder, before `build`:
builder.set_notify_for(0, FrontierInterest::IfCapability);   // + input 1 for joins
```

Applied across `arrange_core`, `join`, `reduce`, `threshold`, `count`. `map`-like
operators are untouched (they never requested frontier notifications).

One caveat drives the API shape: an `IfCapability` arrangement stops advancing its
**trace** frontier while idle, which is unobservable to consumers in the **same**
dataflow (they take progress from the arranged stream frontier) but breaks a
consumer that `import`s the trace into a **different** dataflow (progress tracking
does not span dataflows). So the upstream API must be opt-in with that contract.

Frank McSherry's **DD PR #723 ("Introduce `_inter` and `_intra` arrangement
variants")** does exactly this: it splits `TraceAgent` into `TraceIntra`
(cheap to schedule, single-dataflow, `IfCapability`) and `TraceInter` (shareable
across dataflows, `Always`), and its branch already makes `join` and `reduce` use
`IfCapability` too. That PR is the correct home for this work. It is currently
dormant (last touched 2026-04-19) and unmerged; upstream's active focus is
elsewhere (formal verification and a columnar/chunk storage refactor).

## FlowLog integration

Because progress tracking is per-dataflow and FlowLog builds a **single** dataflow,
every FlowLog arrangement is a single-dataflow consumer — the safe case. So once
DD PR #723 lands with `IfCapability`/intra as the default, **FlowLog benefits with
no code change**: arranges get the intra default automatically, and FlowLog never
needs the shareable `_inter` variant.

The codegen hook in this PR (`flowlog-build`, `register_arrangement`, gated by the
default-off `FLOWLOG_IFCAP` env var) exists only so we can drive and measure the
optimization ahead of the upstream landing, using a patched local
differential-dataflow that exposes `arrange_by_*_if_capability`. It emits the
stock `arrange_by_*` in a normal build. If #723 lands with an automatic default,
this hook can be removed; if it lands with explicit per-arrangement control, the
hook already routes non-recursive arranges to the elided variant.

## Plan

1. Keep this PR as documentation + an inert, default-off hook. Do not ship.
2. Help revive / land differential-dataflow PR #723 upstream (the real fix), using
   the numbers above as motivation.
3. When #723 releases, drop the hook (or trivially retarget it) and enable the
   optimization through stock differential-dataflow — no timely and no bespoke
   scheduling code in FlowLog.

## Reproduction

With a differential-dataflow that exposes the elided operators patched in
(`[patch.crates-io]`), a single binary A/Bs the whole family via one runtime knob
(`DD_IFCAP_OPS=1`), and the codegen hook A/Bs the arrange emission via
`FLOWLOG_IFCAP=1` at build time. Airline numbers above are from the sasy library
bench; the 123-program parity is `bash tests/fixtures/run_lib.sh`.
