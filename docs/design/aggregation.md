# Aggregation

Status: the parser implements both mechanisms and no longer has the old head
aggregation (`R(x, min(d)) :- ...`); the planner and codegen do not yet.

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
one rule. The two mechanisms below split along that line.

## Aggregate expressions

### Syntax

```text
count : { body }        count : Atom(...)
sum e : { body }        sum e : Atom(...)
min e, max e, mean e    likewise
```

The braces hold a body: atoms and comparisons, with arithmetic. A single
atom may drop the braces. The aggregate is an expression and may appear
wherever a value may, in a head argument, a comparison or arithmetic:

```text
Deg(x, c) :- Node(x), c = count : { Edge(x, _) }.
Deg(x, count : Edge(x, _)) :- Node(x).
Hub(x) :- Node(x), count : { Edge(x, _) } > 10.
```

The front end hoists every aggregate into a binding of a fresh variable,
`@agg_0 = agg : { ... }`, before planning, so the planner sees only the
binding form. An aggregate inside another's braces or target is bound
inside that one's body. This is a local rewrite of the rule; the aggregate
stays a step of the rule's own plan and creates no auxiliary relation.

The operators `count`, `sum`, `min`, `max` and `mean` are keywords, as in
Soufflé: no relation, variable or function may take their names. So
`sum (x + y) : ...` is an aggregate with a parenthesized target, never a
call, and the grammar parses it without backtracking.

### Semantics

A name means one variable throughout a rule, inside braces or out. A
variable inside the braces is **correlated** if it also occurs outside
them, and **local** otherwise. Every correlated variable must be bound by
a body around the aggregate, the rule's or an enclosing aggregate's;
Soufflé rejects the same program ("witness problem"). The target must read
a local variable: with every target variable fixed outside, each group has
one value and there is nothing to aggregate (`FixedAggregateTarget`).

For each binding of the correlated variables, the inner body's **matches**
are the distinct bindings of its named variables. `_` names nothing and
never tells two matches apart. Then:

- `count` is the number of matches;
- `sum e`, `mean e`, `min e`, `max e` evaluate `e` once per match, so a
  value that occurs in two matches counts twice in `sum`.

Over `T = {(1,10,a), (1,10,b), (1,20,a)}` with `x = 1`:

| Expression | Value | Matches |
|---|---|---|
| `count : T(x, _, _)` | 1 | `{()}` |
| `count : T(x, y, z)` | 3 | `(y, z)` |
| `sum y : T(x, y, _)` | 30 | `y in {10, 20}` |
| `sum y : T(x, y, z)` | 40 | `(10,a), (10,b), (20,a)` |

A distinct-value aggregate projects by writing `_`; a per-tuple aggregate
names the columns. Both are one expression, which is why the rule is "named
variables decide": the opposite rule, aggregating head-aligned rows as main
does today, cannot express a per-tuple `sum` at all.

With no matches, `count` and `sum` are `0`, and `min`, `max` and `mean` have
no value, so the rule derives nothing for that binding. `mean` answers a
float whatever its target's type, `float64` unless a use of it fixes the
width; the others answer in the target's.

Aggregate expressions are stratified: every relation inside the braces must
be complete before the rule runs, as for negation. The rule itself may be
recursive, so long as its aggregates read only relations of earlier strata:
those are fixed for the whole loop. An aggregate reading a relation of its
own recursion would change every round and retract its earlier answers, so
the planner rejects it (`AggregateInRecursion`); a value merged across
recursion is a lattice column's job.

### Differences from Soufflé

- **`_` in a single atom.** Soufflé counts the tuples of a lone atom, so
  `count : { T(x, _, _) }` is 3 there and 1 here. With two or more atoms
  Soufflé outlines the braces into a relation of the named variables
  (`__agg_subclause`), which drops `_`, and the two agree: over the data
  above, `count : { T(x, _, z), U(z) }` is 2 in both, and
  `sum y : { T(x, y, _), V(y) }` is 30 in both.
- **Scoping.** Soufflé reads a target variable as fresh even when the rule
  binds it outside, so `E(d, w), s = sum w : E(d, w)` sums every `w` of
  `d` there, and FlowLog rejects it (`FixedAggregateTarget`). Soufflé also
  keeps a local name in two sibling aggregates apart; FlowLog reads the
  two as one variable, which no body around them binds, and rejects the
  rule (`UngroundedAggregateVariable`) until one is renamed. A target
  reading a correlated variable beside a local one, `sum y + c`, is
  ungrounded in Soufflé and ordinary here.
- **Braces reading nothing.** Soufflé answers `count : { 1 < 2 }` as one
  match; FlowLog rejects braces that read no relation and no variable of
  the rule (`EmptyAggregateBraces`), having no row to match. Braces
  reading a variable alone, `count : { x > 3 }` under `Node(x)`, answer
  as in Soufflé.
- **Implementation.** Soufflé outlines the braces into a relation; FlowLog
  plans them inside the rule (below).

### First version

The braces hold positive atoms, comparisons, arithmetic and further
aggregates, nested to any depth:

```text
R(c) :- c = count : { Dept(d), sum w : { Emp(_, d, w) } > 200 }.
```

Here `d` is local to the `count` and correlated in the `sum`. Negation
inside the braces comes later; Soufflé accepts it.

## Lattice relations

```text
.decl Dist(n: int32, d: min<int32>)
Dist(s, 0) :- Source(s).
Dist(y, d + w) :- Dist(x, d), Edge(x, y, w).
```

A lattice column makes the relation hold, per value of its other columns,
only the least (`min`) or greatest (`max`) value any rule derives. Every
rule's rows are unioned first, so rules of one relation never disagree. The
merge is idempotent and monotone, which is what lets it sit inside a loop.
`sum` and `count` are not lattices and are rejected.

A non-recursive relation may be a lattice too; it is then the plain
"union, then aggregate" that a per-relation aggregate used to mean.

This is the old per-relation reduce, renamed for what it is. Its mutability
follows `docs/design/mutability.md` (Aggregation); in-loop lattices over
signed input keep the restrictions described there.

### Why on the declaration

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

## Planner

### `Aggregate`

A binary step in the rule plan, beside join and antijoin, taken as soon as
an atom of the body covers the variables the braces read (the group key),
before any further join:

```text
keys:    the covering atom when it holds the group key alone, else the
         projection of its keys; none for an empty group key
matches: the plan of  @agg_n(key..., locals...) :- keys(key...), braces...
groups:  the keys as the braces arranged them when that is by the group
         key, else their projection onto it; none without keys
  -> Aggregate(op, target) over the matches by the key, with the groups
answers: one row per group, (key..., value); one row without groups
```

- **Placement.** The answers replace the covering atom when it holds the
  group key alone, else join it on the key; the body then folds on as
  after a join. An aggregate with an empty group key reads no keys and
  answers once, and its answer joins the body on the empty key, which
  keeps it only where the body has rows. Of the atoms covering the aggregate, one outside the
  stratum's loop is taken before one inside it, one holding the key
  alone before one with other columns, then the narrowest.
- **Inner plan.** The braces are planned by the same rule planner, as a
  body of their own starting from the keys. Its collections are ordinary
  ones, so sharing and fusion apply; a produced collection keeps one
  layout, so a map over a collection read elsewhere too stays a map.
- **Pushdown.** Each body is walked from its own root. A fold inside the
  braces holds for the matches alone, never for a group answered with no
  match, so it is copied nowhere outside them; a fold of the body reaches
  into the braces through the matches, whose keys are the groups'.
- **Evaluation.** The matches and the groups are keyed collections, so
  codegen arranges them as it does every keyed output, and the runtime's
  `flowlog_aggregate` reads the arrangements. A signed reduce runs on the
  matches' arrangement as it is and counts each distinct match once; a
  presence reduce reads the arrangement back as rows, dedups them, and
  accumulates contributions as weights. The aggregated column is read out
  of a match's value by its own closure, so two matches with equal
  targets both count. A `count` or `sum` answers `0` for a group with no
  match: under a signed weight the groups' arrangement reduces to one
  default per key, which the key's answer cancels; under presence each
  key lifts to a neutral weight beside the matches' contributions.
  `min`, `max` and `mean` answer nothing for an empty group, so they take
  no groups. A lattice relation goes through `flowlog_lattice` instead:
  its rows are never arranged by anything else, so it arranges them
  itself, and no group is answered without a row.
- **Canonical form.** Structural: the operator, the target, the matches'
  form and the groups' form, canonical over the order of the value
  columns and of the key pairs. Two aggregates are equal only for the
  same aggregate over the same matches and groups.
- **Mutability.** The aggregate's value takes `aggregate_mutability` of the
  matches, combined with the groups' as for a join.
- **Stratification.** Relations inside the braces are dependencies of the
  rule's head like any other, so they land in the same or an earlier
  stratum; the stratifier then rejects the same-stratum case.

### `Lattice`

Nothing of a rule: a rule deriving a lattice relation plans as any other,
and its head is a plain head, shared like any other. The merge is the
relation's. Codegen reads the lattice column and its `min` or `max` from
the declaration and reduces the union of the relation's heads by it,
grouped by every other column; inside a loop that reduce is the loop's
merge, and at its exit it folds the rounds' answers once more. The
stratifier values a lattice relation as an aggregate over the rows its
rules derive (`aggregate_mutability`): static over static rows, mutable
otherwise.

The per-rule head reduce of #427 is gone with it: a rule's last step is
its head, and no transformation reduces.

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

## Open questions

- More than one lattice column, and user-defined lattice types.
- Negation inside braces.
- Recursive `count` and `sum` with a monotone meaning (BigDatalog's
  `mcount`, `msum`), as further lattice kinds if needed.
