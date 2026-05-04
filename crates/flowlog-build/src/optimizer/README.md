# `optimizer/` — cardinalities + plan trees

A small, deliberately stubby module the planner consults for join ordering. Today it does two things:

1. Stores per-relation cardinalities (so the planner can ask "how big is `Edge`?").
2. Builds a `PlanTree` per rule — currently a left-deep chain over the rule's core atoms, **in source order**.

## Today's plan tree

For core atoms `[A, B, C, D]` (post-GYO, in source order):

```text
((A ⋈ B) ⋈ C) ⋈ D            D     ← root (rightmost)
                              │
                              C
                              │
                              B
                              │
                              A     ← leaf (leftmost)
```

Not cost-based. The TODO in `core.rs::plan_stratum` is explicit: future work picks a better order using `relation_cardinality`.

## API

```rust
let mut opt = Optimizer::new();

// Reader handler — load/update EDB cardinalities (today: u64).
opt.set_cardinality(rel_fp, 1_234_567);
opt.get_cardinality(rel_fp);              // -> Option<u64>

// Planner handler — for each rule in a stratum, return either:
//   None             if already planned, or
//   Some((i, j))     index pair of the first join tuple.
let first_joins: Vec<Option<(usize, usize)>> = opt.plan_stratum(&catalogs);
```

## Layout

| File | Holds |
|---|---|
| [`mod.rs`](mod.rs) | Re-export of `Optimizer`. |
| [`core.rs`](core.rs) | `Optimizer` — cardinality store + `plan_stratum`. |
| [`plan_tree.rs`](plan_tree.rs) | `PlanTree` + a `Display` impl that pretty-prints under `RUST_LOG=debug`. |

## Notes for future cost-based work

- Cardinalities are typed `u64` as a placeholder — real planning will need fractional / histogram-based estimates.
- `get_first_join_tuple_index()` always returns `(0, 1)`; pairwise order isn't yet derived from the tree.
- `plan_stratum` is `&self` on purpose: the optimizer is a read-only oracle. Feedback loops ("re-plan after observing 10× more tuples") need a separate mutating path.
