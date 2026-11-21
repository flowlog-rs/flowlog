# Aggregation

Status: implemented, both mechanisms and choice domains, from the parser
to the runtime; the old head aggregation (`R(x, min(d)) :- ...`) is gone.

FlowLog has two kinds of aggregation, and each gets its own syntax and its
own operator:

| | Aggregate expression | Lattice relation |
|---|---|---|
| Belongs to | one rule, as a value inside its body | a relation |
| Syntax | `c = count : { Edge(x, _) }` | `.decl Dist(n: int32, d: min<int32>)` |
| Meaning | for each binding of the outer variables, aggregate the inner matches | union every rule, keep the best value per key |
| Operators | `count`, `sum`, `min`, `max`, `mean` | `min`, `max` |
| Recursion | no, stratified | yes |
| Planner | `Aggregate`, a binary step inside the rule plan | `Lattice`, at the stratum, after the relation's rules meet |

No aggregate appears in a rule head.

A *choice domain* merges a relation as a lattice column does, at the
stratum, after its rules meet, but keeps one row per key instead of a best
value. It is not an aggregation, but it runs as a lattice does.

The design is split by mechanism, from the general to the specific:

- [Aggregate expressions](aggregate.md): an aggregate inside one rule's
  body, stratified.
- [Lattice relations](lattice.md): a relation that merges its rules' rows
  to the best value per key, inside recursion too.
- [Choice domains](choice.md): a relation that keeps one row per key; its
  merge is a lattice's, over an order of its own.

## Why two

One head syntax used to carry both meanings, and every reading of it broke
one of them:

- Read per relation (union, then aggregate), rules of one relation had to
  agree on the aggregate, and a non-recursive grouping such as a per-rule
  count could not be written without an auxiliary relation.
- Read per rule (aggregate, then union, as on main since #427), the
  recursive programs it was written for change meaning. With a cycle back
  to the source, `sssp` holds two rows for it, and `cc.dl` no longer
  labels components. The `min(0)` in `sssp(x, min(0)) :- id(x).` is a
  minimum over one value: it exists only to declare that the relation
  merges by `min`.
- A per-rule `sum` or `count` inside recursion has no fixpoint in general.
  In `tests/fixtures/recursive_agg_mixed`,
  `Acc(0). Acc(sum(x)) :- Acc(n), n < 3, x = n + 1.` alternates between
  `{0, 1}` and `{0, 3}`; the expected `{0, 3}` is not a model, since the
  rule derives `1` from it.

The only aggregations with a clear recursive meaning are monotone merges,
which are properties of a relation. Everything else is a computation inside
one rule. The two mechanisms split along that line.

## Plan

1. **Parser.** Done: aggregate expressions, nested ones included
   (grammar, hoisting, every pipeline pass), `min<T>`/`max<T>` columns,
   and head aggregation removed from the grammar: `R(x, sum(y))` now reads
   `sum` as a function call.
2. **Planner and codegen.** The `Aggregate` and `Lattice` steps; remove the
   per-rule head reduce (#427). Migrate
   fixtures and examples: recursive ones (`recursive_*`,
   `mixed_agg_heads`, `example/graph_analysis/{sssp,cc}.dl`) to lattices,
   the rest to aggregate expressions; drop `Acc` from
   `recursive_agg_mixed`.
