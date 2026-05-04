# `optimizer/` — relation cardinalities + join trees

A small, deliberately-stubby module. Today it does two things:

1. Stores **per-relation cardinalities** (so the planner can ask "how big is
   `Edge`?").
2. Builds a **`PlanTree`** per rule — currently a left-deep chain over the
   rule's core atoms, in source order.

```
parser ──▶ typechecker ──▶ stratifier ──▶ catalog ──▶ optimizer ──▶ planner ──▶ codegen
                                                       ^^^^^^^^^
                                                       you are here
```

The interfaces are intentionally lightweight so cost-based optimization can be
slotted in later without disturbing the planner.

## Today's plan tree (left-deep chain)

Given a rule whose core atoms are `[A, B, C, D]` (in source order), `PlanTree`
emits the join chain you'd write by hand:

```text
        ((A ⋈ B) ⋈ C) ⋈ D       i.e.

           D     ← root (rightmost)
           │
           C
           │
           B
           │
           A     ← leaf (leftmost)
```

This is **not** cost-based. The atom order is exactly the source order, post-
GYO core-atom selection done by the catalog. The TODO in `core.rs::plan_stratum`
is explicit: future work will pick a better order using `relation_cardinality`.

## API surface

```rust
let mut opt = Optimizer::new();

// Reader handler — load/update EDB cardinalities (currently u64).
opt.set_cardinality(rel_fp, 1_234_567);
opt.get_cardinality(rel_fp);                // -> Option<u64>

// Planner handler — for each rule in a stratum, return either:
//   - None              if it's already been planned, or
//   - Some((i, j))      the index pair of the first join tuple.
let first_joins = opt.plan_stratum(&catalogs);
```

## Layout

| File | Holds |
|---|---|
| [`mod.rs`](mod.rs) | Public re-export of `Optimizer`. |
| [`core.rs`](core.rs) | `Optimizer` — cardinality store + `plan_stratum`. |
| [`plan_tree.rs`](plan_tree.rs) | `PlanTree` — the left-deep chain, with a `Display` impl that pretty-prints the tree for `RUST_LOG=debug`. |

## Design notes for future work

- **Cardinalities are typed `u64`** as a placeholder. Real cost-based planning
  will need fractional cardinalities (selectivity estimates) and per-column
  histograms; expect this to grow into something more structured.
- **`get_first_join_tuple_index()` always returns `(0, 1)`** — pairwise join
  order is *not* yet derived from the tree's shape. When the tree starts to
  reorder atoms, this should return indices into the chosen left-deep order.
- **Stratum-level vs rule-level** — `plan_stratum` is a `&self` method
  intentionally: the optimizer is a read-only oracle for the planner. Any
  feedback loop ("I observed 10× more tuples than predicted, re-plan") will
  need a separate mutating path.
