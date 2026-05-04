# `stratifier/` — SCC-based rule scheduling

Decides **the order** in which rule groups must run so every rule's dependencies
are fully computed before it fires. One public type — `Stratifier` — produced
once per program by `Stratifier::from_program(&Program, extended: bool)`.

```
parser ──▶ typechecker ──▶ stratifier ──▶ planner ──▶ codegen
                          ^^^^^^^^^^^
                          you are here
```

## Vocabulary

- **Stratum** — a set of rules that evaluate as a unit. Rules within the same
  stratum may depend on each other (they form an SCC); rules in stratum *i*
  may only depend on relations produced by strata *0…i-1*.
- **Recursive stratum** — contains a cycle: either a multi-rule SCC, or a
  single rule that references its own head. Evaluated to fixpoint.
- **Non-recursive stratum** — no cycle. Single pass.

## How it runs

```mermaid
flowchart TD
    PROG["Program<br/>(Vec&lt;Segment&gt;)"] --> SEG{"per Segment"}
    SEG -->|"Plain"| DG["build dependency graph<br/>(intra-segment edges only,<br/>negation edges tracked separately)"]
    SEG -->|"Loop / Fixpoint"| FORCE["force into ONE<br/>recursive stratum<br/>+ LoopCondition"]
    DG --> SCC["Tarjan SCC<br/>+ topo sort"]
    SCC --> NEGCHK{"extended mode AND<br/>recursive SCC<br/>in plain rules?"}
    NEGCHK -->|"yes"| ERR(["StratifyError::<br/>RecursionOutsideLoop"])
    NEGCHK -->|"no"| WARN["log a warning per<br/>negation-through-recursion edge<br/>(advisory, not fatal)"]
    WARN --> EM["emit strata in topo order"]
    FORCE --> EM
    EM --> OUT["Stratifier::stratum()<br/>= Vec&lt;Vec&lt;&amp;FlowLogRule&gt;&gt;"]

    classDef bad fill:#fdd,stroke:#a00
    class ERR bad
```

Cross-segment edges are ignored — the stratifier treats prior segments as
already-computed EDB by the time the current segment runs. Each `Plain`
segment stratifies independently; each `Loop`/`Fixpoint` segment becomes
exactly one recursive stratum, regardless of how many rules it contains.

## Two semantic modes

| `extended` | Plain-rule recursion | Loop-block recursion | Status |
|---|---|---|---|
| `false` (Datalog mode) | **Allowed** — handled implicitly via SCC detection (classic stratified-Datalog semantics). | Allowed; one stratum per block. | ✅ supported |
| `true`  (Extended mode) | **Hard error** (`StratifyError::RecursionOutsideLoop`). Recursion *must* be expressed via `loop`/`fixpoint` blocks. | The only place recursion is allowed. | 🚧 partial — `extend-batch` has unit fixtures; `extend-inc` has no fixtures yet and `--profile` panics under either. |

This is the lever the user pulls with `--mode extend-batch` / `--mode extend-inc`.

## Negation safety

A negation edge that closes a cycle is *classically* unstratifiable
(`!p :- q`, `q :- p`). The dependency graph tracks `negative_edges`
separately, and the stratifier currently **logs a `warn!` per such edge**
(see `warn_negation_edges` in [`core.rs`](core.rs)) so the user sees
`negation through recursion: q → ¬p → … → q` printed during compilation —
sorted in `BTreeSet` order for deterministic output. It is **advisory only
today**, not a hard error; programs with negation-through-recursion still
compile. Tightening this into a `StratifyError` variant is a clean future
extension.

The hard errors the stratifier *does* raise are listed in
[`error.rs`](error.rs) — most notably `RecursionOutsideLoop` (extended-mode
plain-rule recursion), `IterativeNotInLoopHead` / `IterativeNotRecursive`
(misused `.iterative` directive), `ForwardReference`, and the empty / malformed
loop variants.

## Layout

| File | Holds |
|---|---|
| [`mod.rs`](mod.rs) | Crate-level rustdoc + re-exports (`Stratifier`, `StratifyError`). |
| [`core.rs`](core.rs) | `Stratifier` itself: per-segment driver, recursive/non-recursive classification, loop-block handling, the public API. |
| [`dependency_graph.rs`](dependency_graph.rs) | `DependencyGraph` — local indices over a single segment's rules; tracks polarity-agnostic edges plus a separate set of negation edges. |
| [`error.rs`](error.rs) | `StratifyError` — extended-mode plain-rule recursion + negation-through-recursion variants, with span-anchored offending rules. |

## Reading order

1. [`mod.rs`](mod.rs) — concept overview.
2. [`dependency_graph.rs`](dependency_graph.rs) — what an edge looks like.
3. [`core.rs`](core.rs) — the per-segment driver and SCC handling.
