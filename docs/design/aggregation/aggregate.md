# Aggregate expressions

## Syntax

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

## Semantics

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

## Differences from Soufflé

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

## First version

The braces hold positive atoms, comparisons, arithmetic and further
aggregates, nested to any depth:

```text
R(c) :- c = count : { Dept(d), sum w : { Emp(_, d, w) } > 200 }.
```

Here `d` is local to the `count` and correlated in the `sum`. Negation
inside the braces comes later; Soufflé accepts it.

## Planner: `Aggregate`

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
  codegen arranges them as it does every keyed output, and passes the
  arrangements to the runtime entry point of the aggregate's path:

  | matches | path | entry point |
  |---|---|---|
  | static | fold, settle, lower | `aggregate` |
  | append | fold, then replace: one weight per key | `aggregate` |
  | mutable, integer `count`, `sum` or `mean`, outside a loop | fold in and out, then replace | `aggregate_abelian` |
  | mutable, otherwise | reduce | `aggregate` |

  - *Fold* reads the matches' batches in place into one weight per key
    and time. A distinct match adds its contribution when its count
    rises above zero and subtracts it when its count returns to zero. A
    presence match never leaves, so it contributes once, at its first
    time.
  - *Fold in and out* is the fold over mutable matches. It needs a
    reduction that can subtract a match exactly, one whose every weight
    has a negative (`Abelian`, in differential dataflow's terms). An
    integer total can: its addition wraps on overflow, so it is exact
    again once it is back in range. A float total cannot, because float
    addition rounds (`1e20 + 1 - 1e20` is `0`), and `min` and `max`
    cannot, because they do not keep the values they passed over; those
    reduce. `aggregate_abelian` takes only an `Abelian`
    reduction, so passing it one of those does not compile. A loop
    reduces too: the fold needs each match's changes in time order, and
    a loop's times are only partially ordered.
    The weight holds the group's match count next to its accumulation
    (`Counted`). With the count, a group whose matches total `0` still
    answers, a same-time swap of one match for another is not dropped,
    and a group stops answering once its last match goes.
  - *Settle* turns the weights into static answers, never retracted.
  - *Replace* arranges the weights by key and answers each key's
    accumulated weight, retracting the previous answer. Compaction merges
    a key's historical weights, so an update reads the remaining weights
    rather than all of the group's matches. Uncompacted times can retain
    more than one weight per key.
  - *Reduce* reads all of a changed group's matches in the arrangement,
    each distinct match once, supporting insertions and deletions.

  Floating-point `sum` and `mean` use the same append path as integer
  aggregates. Their additions round, so changing batch boundaries,
  compaction, worker scheduling, or execution strategy may change the
  result. Bitwise reproducibility across runs or agreement with a batch
  recomputation is not guaranteed. This trades a fixed summation order
  for incremental work proportional to the new contributions rather than
  the group's history. It does not use compensated summation.

  The aggregated column is read out of a match's value by its own
  closure, so two matches with equal targets both count. A `count` or
  `sum` answers `0` for a group with no match: under a reduce the groups'
  arrangement reduces to one default per key, which the key's answer
  cancels; under a fold each group starts at a zero weight that its
  matches' contributions add to. Under a mutable fold that starting
  weight carries the key's count, so the group keeps its answer while
  the key exists and loses it when the key is removed.
  `min`, `max` and `mean` answer nothing for an empty group, so they take
  no groups. A lattice relation goes through `operators::lattice` instead
  ([Lattice relations](lattice.md#planner-lattice)).
- **Canonical form.** Structural: the operator, the target, the matches'
  form and the groups' form, canonical over the order of the value
  columns and of the key pairs. Two aggregates are equal only for the
  same aggregate over the same matches and groups.
- **Mutability.** The aggregate's value takes `aggregate_mutability` of the
  matches, combined with the groups' as for a join.
- **Stratification.** Relations inside the braces are dependencies of the
  rule's head like any other, so they land in the same or an earlier
  stratum; the stratifier then rejects the same-stratum case.

## Open questions

- Negation inside braces.
- Mutable `min`, `max`, float `sum`/`mean` and aggregates inside a loop
  still read a changed group again. `min` and `max` could keep each
  group's values ordered so that one can be removed; a loop would need a
  fold that works over partially ordered times.
