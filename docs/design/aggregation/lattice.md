# Lattice relations

```text
.decl Dist(n: int32, d: min<int32>)
Dist(s, 0) :- Source(s).
Dist(y, d + w) :- Dist(x, d), Edge(x, y, w).
```

A lattice column makes the relation hold, per value of its other columns,
only the least (`min`) or greatest (`max`) value any rule derives. Every
rule's rows are unioned first, so rules of one relation never disagree. The
merge is idempotent and monotone, which is what lets it sit inside a loop.
`sum` and `count` are not lattices and are rejected; the runtime's
`flowlog_lattice` would not compile with one either, as it takes only an
`Idempotent` merge.

A non-recursive relation may be a lattice too; it is then the plain
"union, then aggregate" that a per-relation aggregate used to mean.

This is the old per-relation reduce, renamed for what it is. Its mutability
follows [Mutability](../mutability.md#aggregation); in-loop lattices over
signed input keep the restrictions described there.

## Why on the declaration

The merge is a property of the relation, not of one rule. Marking it in rule
heads (`Dist(y, min(d + w)) :- ...`, as Dyna's `min=` and BigDatalog's
`mmin` do) would require every rule of the relation to carry the same mark,
bringing back a consistency check and leaving a rule without the mark
ambiguous. Systems with recursive lattices declare them with the relation:

| System | Declaration |
|---|---|
| Flix | `lat Dist(x: Int32; Down[Int32])`: key columns, then the lattice column |
| Ascent | `lattice dist(Node, Dual<u32>);`: the last column is the lattice |
| egglog | `(function dist (i64) i64 :merge (min old new))` |
| Bloom^L | lattice-typed state, such as `lmax :best` |

Soufflé's subsumption (`Dist(x, d1) <= Dist(x, d2) :- d2 <= d1.`) states
which row dominates another in a rule of its own; it covers any partial
order, at the cost of more text for `min` and `max`.

FlowLog follows the same idea: the merge wraps the column's type,
`d: min<int32>`, so the declaration says which column merges and how, and
rules stay plain. The key is every other column, so no separator is
needed in rules, unlike Flix's `Dist(x; d)`. A later user-defined lattice
takes the same place, `d: MyLattice<int32>`. A relation has at most one
lattice column, of a numeric or string type. Strings merge in the order
`<` compares them: by their text, even under `--str-intern`.

## Planner: `Lattice`

Nothing of a rule: a rule deriving a lattice relation plans as any other,
and its head is a plain head, shared like any other. The merge is the
relation's. Codegen reads the lattice column and its `min` or `max` from
the declaration and reduces the union of the relation's heads by it,
grouped by every other column; inside a loop that reduce is the loop's
merge, and at its exit it folds the rounds' answers once more. The
stratifier values a lattice relation as an aggregate over its rows, those
its rules derive and those its input loads (`aggregate_mutability`):
static over static rows, mutable otherwise.

The runtime's `flowlog_lattice` takes the paths of an aggregate's
evaluation ([Aggregate expressions](aggregate.md#planner-aggregate)) over
the relation's own rows. Those rows are never arranged by anything else,
so a mutable lattice arranges them itself and reduces, and a presence one
lifts each row to a weight, settled or replaced. No group is answered
without a row.

The per-rule head reduce of #427 is gone with it: a rule's last step is
its head, and no transformation reduces.

## Open questions

- More than one lattice column, and user-defined lattice types.
- Recursive `count` and `sum` with a monotone meaning (BigDatalog's
  `mcount`, `msum`), as further lattice kinds if needed.
- Merging an input-only lattice relation, whose loaded rows now pass as
  they are.
