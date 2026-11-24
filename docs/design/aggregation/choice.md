# Choice domains

```text
.decl Tree(parent: symbol, node: symbol) choice-domain node
Tree("-", n) :- Root(n).
Tree(p, n) :- Tree(_, p), Edge(p, n).
```

A choice domain names a relation's key columns. The relation keeps one
row per key, the key's *choice*, and drops the others: here each node
keeps one parent, so `Tree` is a spanning tree. The declaration is
Soufflé's; DOOP's `--choice-bound` caps points-to sets with it:

```text
.decl VarPointsTo_Bounded(
    ?hctx:mainAnalysis.configuration.HContext, ?value:Value,
    ?ctx:mainAnalysis.configuration.Context, ?var:Var, ?n:number
) choice-domain (?ctx, ?var, ?n)
```

DOOP sets `?n` to `(ord(value) * ord(hctx)) % 37`, so a variable keeps
at most 37 points-to facts per context.

- **Syntax.** `choice-domain c` keys by one column; `choice-domain (a, b)`
  by several together, named in any order. One domain per relation.
- **Which row.** The least candidate; in a loop, the least of the first
  round with one. "Least" compares the other columns as a tuple, in
  column order; strings by text, even under `--str-intern`. Each epoch
  chooses as a batch run over its rows would, on any number of workers.

  | rows | the choice | revised |
  |---|---|---|
  | static, outside a loop | the least candidate | never |
  | static, in a loop | the least of the first round with one | never |
  | append, outside a loop | the least candidate so far | when a lesser one arrives |
  | mutable, outside a loop | the least current candidate | as candidates come and go |
  | append or mutable, in a loop | the least of the earliest round with one | when a candidate leaves, or one of an earlier round arrives |

- **Why the first round.** The least over every round is not a choice
  model ([Semantics](#semantics)); the least of the first round is. Epochs
  have no rounds of their own: an epoch's rows are all there at once, as
  in a batch run.
- **Recursion over changing input.** A changing loop keeps the first round
  as a static one does: each candidate carries the round it appears in,
  and the choice takes the least `(round, candidate)`. An epoch can still
  retract a candidate or add one of an earlier round. The candidates are
  deduped first, so a candidate derived again in a later round adds no
  second stamp for the reduce to read.
- **Beside a lattice.** A choice and a lattice relation may not feed back
  into one recursion over append or mutable input
  (`ChoiceBesideLatticeInChangingRecursion`); a static loop may hold both.
  See [Semantics](#semantics) for why.
- **Inputs.** A relation with rules chooses among the rows its input
  loads too, beside those its rules derive. An input-only relation is not
  merged: its loaded rows pass as they are, as a lattice relation's do.
- **With a lattice column.** Rejected (`LatticeWithChoiceDomain`): both
  merge the same rows, and neither order is the evident one.
- **Runtime.** `choice(rows, name, split, merge)`: `split` cuts a
  row into its key and its other columns, `merge` rebuilds it. It is
  `operators::lattice` under a reduction that keeps the least candidate, so
  it answers as a lattice does, static over static rows and mutable
  otherwise, and takes the lattice's path for its weight;
  [Mutability](../mutability.md#choice) lists the paths.

## Semantics

A choice relation means what Saccà and Zaniolo give it ([PODS 1990][sz90]):
the program is rewritten with negation, and its meanings are the stable
models of the rewrite.

**The rewrite.** `T`'s rules derive into a fresh `Cand` instead, and `T`
keeps a candidate unless its key holds another row:

```text
Cand(k, v)  :- body.                        one per rule of T
T(k, v)     :- Cand(k, v), !Other(k, v).
Other(k, v) :- Cand(k, v), T(k, v2), v != v2.
```

Here `k` stands for the domain's columns and `v` for the others. The
negation runs through `T`'s own recursion, so the rewrite is not
stratified, and a program may have several stable models. The spanning
tree above becomes:

```text
Cand("-", n) :- Root(n).
Cand(p, n)   :- Tree(_, p), Edge(p, n).
Tree(p, n)   :- Cand(p, n), !Other(p, n).
Other(p, n)  :- Cand(p, n), Tree(p2, n), p != p2.
```

**Stable models.** For a set of rows `M`, the reduct `P^M` drops every
rule instance with a negated atom in `M` and deletes the remaining
negated atoms. `M` is stable when it is the least model of `P^M` over the
same input: guessing `M` and deriving from the input under that guess
gives `M` back. Read on the original program, a stable model's `T` is a
*choice model*:

1. **Functional.** No two rows of `T` agree on the domain.
2. **Maximal.** Every candidate whose body holds is in `T` unless its key
   holds another row: a choice drops a candidate only for another of its
   key.
3. **Founded.** Every row has a derivation from the input through rows of
   the model alone; no set of rows only supports itself.

**Which model.** Soufflé returns any one. FlowLog returns one fixed by
the input, close to the *choice-least* of Greco and Zaniolo
([TPLP 2001][gz01]): evaluation runs in rounds, round `i`'s candidates are
the rows `T`'s rules derive from rows of earlier rounds, and a key not yet
chosen takes the least candidate of the first round with one. Loaded rows
are candidates from the first round. Outside a loop there is one round,
so the choice is the least candidate. This is a choice model, by
induction on rounds:

- **Functional.** A key is chosen once.
- **Founded.** Round `i`'s candidates read only rows chosen before round
  `i`, each founded by induction.
- **Maximal.** At the fixpoint, a rule instance whose body holds in the
  model reads rows of rounds before some `i`, so its head is a candidate
  by round `i`; its key was chosen then or earlier.

**Example.** Take `Root(r)` and edges `r->c`, `c->a`, `a->b`, `b->a`. Node
`a` has candidate parents `c` and `b`.

- FlowLog chooses `c` for `a`, reached a round before `b`:
  `Tree = {(-,r), (r,c), (c,a), (a,b)}`, with
  `Cand = Tree ∪ {(b,a)}` and `Other = {(b,a)}`. The reduct drops only
  the instance deriving `Tree(b, a)`; from the input it derives `r`, `c`,
  `a`, then `b`, then `Cand(b, a)` and `Other(b, a)`: `M` again. Stable.
- The least candidate over every round chooses `b` for `a`:
  `Tree = {(-,r), (r,c), (b,a), (a,b)}`, with
  `Cand = Tree ∪ {(c,a)}` and `Other = {(c,a)}`. The reduct drops the
  instance deriving `Tree(c, a)`; from the input it derives `r` and `c`,
  then `Cand(c, a)`, and stops. It never derives `Tree(b, a)` or
  `Tree(a, b)`, which only support each other. Not stable.

A lattice column has no such failure: its value only grows worse along a
derivation, so a row derived through a key's own value never beats it.
Taking the round first restores that order for a choice: a candidate
derived through a key's choice appears in a later round than the choice.

**Incremental.** Each epoch's output is the stable model a batch run over
that epoch's input returns. Outside a loop this is the least current
candidate. In a loop, the round a candidate first appears in can change
from epoch to epoch, so the choice cannot keep the round of the epoch that
first derived it. It does not need to: differential stores a collection as
diffs at times `(e, i)`, and the rows at `(e, i)` are the sum of the diffs
at every time no later, which is what a batch run over epoch `e`'s input
holds after round `i`:

```text
rows(r, e, i) = Σ { diff(r, e', i') : e' ≤ e, i' ≤ i }
```

The choice dedups the candidates, so `rows` is `0` or `1`, then stamps
each diff with its own round, `i'`, before merging. The copy of `r`
stamped `j` gathers only diffs at round `j`, so at `(e, i)`, for `j ≤ i`,
its weight is

```text
Σ { diff(r, e', j) : e' ≤ e } = rows(r, e, j) - rows(r, e, j - 1)
```

the rows a batch run adds at round `j`. Within an epoch the candidates
only grow from round to round: a key is chosen once, so `T` only grows,
and so does `Cand`, which reads `T`, relations of earlier strata, fixed
within the epoch, and other relations of the recursion, which grow as
`T` does. The weight is then `1` at the round `r` first appears in and
`0` at every other. So the least `(round, candidate)` is the batch
choice, whatever epoch first derived each candidate. A stamp left from an
earlier epoch is retracted with the diff that carried it:

| diff | stamped |
|---|---|
| epoch 1: `+r` at `(1, 3)` | `+(3, r)` at `(1, 3)` |
| epoch 2: `+r` at `(2, 2)` | `+(2, r)` at `(2, 2)` |
| epoch 2: `-r` at `(2, 3)` | `-(3, r)` at `(2, 3)` |

At `(2, 3)` and later, `(3, r)` sums to zero and `(2, r)` to one.

**Beside a lattice.** The argument needs every relation of the recursion
to grow from round to round, and a lattice does not: over append or
mutable input it retracts each value it improves on, and the candidates
derived from that value go with it. This holds even when every rule is
monotone. Take a shortest-path tree, where each node picks a distance and
its neighbours' distances grow from the pick:

```text
.decl Dist(n: symbol, d: min<int32>)
.decl Pick(n: symbol, d: int32) choice-domain n
Dist(s, 0)     :- Src(s).
Pick(n, d)     :- Dist(n, d).
Dist(y, d + w) :- Pick(x, d), Edge(x, y, w).
```

With `Src(s)` and edges `s->a` of weight 10, `s->b` of weight 1 and
`b->a` of weight 1:

| round | derives |
|---|---|
| 0 | `Dist(s, 0)` |
| 1 | `Pick(s, 0)` |
| 2 | `Dist(a, 10)`, `Dist(b, 1)` |
| 3 | `Pick(a, 10)`, `Pick(b, 1)` |
| 4 | `Dist(a, 2)`, through `b` |

A static loop's lattice cannot retract, so `Pick(a, 10)`, made at round 3,
stands, and the loop ends with `Dist(a, 2)` beside `Pick(a, 10)`. A mutable
loop's lattice retracts `Dist(a, 10)` at round 4, the candidate `(a, 10)`
goes with it, and `a` picks again: `Pick(a, 2)`. Both converge, to
different answers. The choice fixes a key at its first round while the
lattice still improves the value it chose from, so the planner rejects a
choice and a lattice that feed back into one recursion over changing
input (`ChoiceBesideLatticeInChangingRecursion`).

A negation of a relation of the recursion breaks the argument too, as it
retracts what it derived once the negated row arrives. It does so without
a choice as well, so FlowLog treats it as it does any negation inside a
recursion: it warns that the fixpoint may not converge, and leaves the
program to its author.

**Counterexample without the round.** A loop over append or mutable rows
runs at a time that is only partially ordered, so it cannot settle a key
at its first round as a static loop does. Merging the candidates without
their rounds takes each key's least current candidate over every round,
and that is not a stable model. Take the spanning tree above, with
`Root` and `Edge` appended over epochs, and count rounds from
`Tree(-, r)` at round 0.

*Epoch 1* loads `Root(r)` and the edges `r->c`, `c->a` and `a->b`:

```text
r -> c -> a -> b
```

| round | derives | because |
|---|---|---|
| 0 | `Tree(-, r)` | `r` is the root |
| 1 | `Tree(r, c)` | `r` is in the tree, with edge `r->c` |
| 2 | `Tree(c, a)` | `c` is in the tree, with edge `c->a` |
| 3 | `Tree(a, b)` | `a` is in the tree, with edge `a->b` |

Every node has one candidate, so there is nothing to choose: the tree is
the chain `r -> c -> a -> b`.

*Epoch 2* appends the edge `b->a`:

```text
r -> c -> a <-> b
```

`b` is in the tree from round 3, so round 4 derives a second candidate
for `a`, `Tree(b, a)`:

| candidate parent of `a` | round | path |
|---|---|---|
| `c` | 2 | `r -> c -> a` |
| `b` | 4 | `r -> c -> a -> b -> a`, back around the cycle |

- *The least candidate* is `b`, since `b < c`, so `a` switches to `b` and
  the tree becomes `Tree(-, r), Tree(r, c), Tree(b, a), Tree(a, b)`:
  `a` is `b`'s parent and `b` is `a`'s, cut off from `r`. It is no longer
  a tree. `b` became a candidate only through `Tree(a, b)`, which holds
  only because `a` is in the tree, so `b` used `a`'s own choice to
  displace it. This is the unstable model of the example above, which a
  batch run over epoch 2's edges never returns.
- *The least `(round, candidate)`* is `(2, c)`, before `(4, b)`, so `a`
  keeps `c` and epoch 2 changes nothing. A batch run over epoch 2's edges
  agrees: `c` chooses `a` at round 2, and `b`'s offer comes too late.

The round still lets a choice change when a batch run would. *Epoch 3*
appends `r->b`, and now `b` is reached at round 1:

| round | derives |
|---|---|
| 0 | `Tree(-, r)` |
| 1 | `Tree(r, c)`, `Tree(r, b)` |
| 2 | candidates `c` and `b` for `a`, both at round 2: `a` takes the lesser, `b` |
| 3 | candidate `a` for `b`, which has `r` from round 1 |

The epoch retracts `Tree(c, a)` and `Tree(a, b)` and adds `Tree(r, b)` and
`Tree(b, a)`, as a batch run over its edges returns.

`tests/fixtures/append_choice_spanning_tree` runs these three epochs
over append edges, writing the root as `Tree(r, r)`;
`choice_spanning_tree_delta` covers the same tree over mutable edges.
Both fail without the round.

[sz90]: https://vldb.org/dblp/db/conf/pods/SaccaZ90.html
[gz01]: https://arxiv.org/abs/cs/0312041

## Differences from Soufflé

- **Which row.** Soufflé keeps the first row its evaluation derives, in
  the order of its facts, rules and indexes; FlowLog keeps the least.
  Over rules both return a choice model. In a loop both keep the earliest
  round's, so the spanning tree above is the same in both.
- **Inputs.** Soufflé does not apply the domain to loaded rows: it keeps
  them all, and a derived row only takes a key none of them has. Its
  result then breaks the functional dependency wherever the input holds
  two rows of one key, so it is a choice model of the rules alone.
- **Several domains.** Soufflé reads `choice-domain a, b` as two keys,
  each kept separately; FlowLog takes one (`MultipleChoiceDomains`), and
  points to `choice-domain (a, b)` for a key of both.
- **Negation in a recursion.** Soufflé rejects any negation inside a
  recursion, as it stratifies every program; FlowLog only warns, with or
  without a choice.
- **Changing input.** Soufflé has none. FlowLog's append or mutable
  choice is the least current candidate, as a batch rerun would choose.

## Planner: `Choice`

As with `Lattice`, a rule deriving a choice relation plans as any other.
Codegen reads the domain from the declaration and chooses from the union
of the relation's heads, with no dedup first, as duplicate rows do not
change a choice; inside a loop the choice is the loop's merge. A static
loop's exit needs no fold: a key is chosen once. The stratifier values a
choice relation as a lattice one.

## Open questions

- Several choice domains in one relation.
- Merging an input-only choice relation, whose loaded rows now pass as
  they are (**Inputs** above).
- A choice beside a lattice in a recursion over append or mutable input,
  now rejected (**Beside a lattice** under [Semantics](#semantics)).
