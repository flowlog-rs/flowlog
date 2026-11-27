# Union columns

Status: proposal. Nothing is implemented.

```text
.type Person <: string
.decl Mother(child: Person, mom: union<Person>)

Mother("ann", "mary").
Mother("ann", "maria").    // one mother per child, so mary = maria
```

A `union<T>` column holds one value per value of the other columns, as a
`min<T>` column does. When two rows disagree, `min` keeps the smaller value
and drops the other. `union` keeps one too, but first declares the two
values **equal**: they join one *class*, named by its *leader*, and from
then on every column of type `T`, in every relation, reads the leader.

> **Learn in one place, use everywhere.** A union column is where equalities
> are learned; every column of its type is where they are used.

## Why

The chase needs four things. FlowLog has three:

| The program needs to | FlowLog today |
|---|---|
| derive facts | rules |
| skip a rule when a witness exists | `!R(...)` inside recursion |
| make a value for a missing witness | a function of the witness: an `.extern fn`, or `ord` of a tuple |
| **merge two values into one** | **missing** |

Negation inside recursion is accepted today, with a warning; in an
insert-only loop a key learned at round `i` blocks rows from round `i` on
and retracts none before ([Negation](mutability.md#negation)). That is the
restricted chase's "skip if a witness exists".

Merging is what these workloads add:

- **The chase**, which data-integration tools such as CQL run to move data
  between schemas. An equality-generating dependency, `x = y :- body`, is
  a merge.
- **Equality saturation** (egglog). A function's output is a class; two
  rows with the same inputs merge their outputs.
- **Unification-based analyses**, such as Steensgaard points-to and type
  inference.
- **Entity resolution.** Two records that share an email are one customer.

## A column kind is a dedup policy

Every column kind FlowLog has already says when two rows are one.
`union<T>` adds one more:

| Kind | Two rows are one when | What is kept |
|---|---|---|
| plain | every column is equal | one copy |
| [`min<T>`, `max<T>`](aggregation/lattice.md) | the other columns are equal | the best value; the other is dropped |
| [`choice-domain k`](aggregation/choice.md) | the key `k` is equal | one of the rows |
| **`union<T>`** | the other columns are equal | the leader; **the two values become equal everywhere** |

The last column is the whole difference:

```text
                Mother                        Friend(bob, mary) elsewhere
min<Person>     (ann, mary) (ann, maria)  ->  (ann, maria)   stays (bob, mary)
union<Person>   (ann, mary) (ann, maria)  ->  (ann, maria)   reads (bob, maria)
```

## Syntax

```text
.type Person <: string                             // its values can merge
.decl Mother(child: Person, mom: union<Person>)    // merge on a collision
.decl Same(a: Person, b: Person) eqrel             // merge every pair it holds
```

- `union<T>` wraps a column's type, in the place of `min<T>`
  (`attribute_decl`). A relation has at most one union or lattice column.
- `T` must be a subtype, `.type T <: base`. A subtype has an identity of its
  own, so only columns of type `T` merge. A plain `string` column, or an
  alias (`.type T = base`), never does.
- `eqrel` marks a binary relation over one subtype, as in Souffle. Each row
  it derives merges its two values, and `Same(x, y)` holds when `x` and `y`
  share a leader. It is for merges that are not collisions: an egglog
  rewrite to a variable (`x * 1 = x`), or a chase rule `x = y :- body`.
- There is no `eq<T>` marker. A type merges exactly when some union column
  or `eqrel` relation uses it, so the compiler knows which columns read
  leaders.

## Semantics

Each type that merges has a hidden `Leader(x, l)` table: the class of `x`
is named `l`, its smallest member. A value with no entry leads itself.

1. **Read through leaders.** Every column of type `T` reads its leader: in
   joins, dedup, negation, collision checks and output.
2. **Merge on a collision.** Two rows of a union relation that agree on
   every other column, as read through leaders, merge their union values.
   Each row of an `eqrel` relation merges its two values.
3. **Congruence comes free.** Rule 1 covers key columns too. A merge can
   make two keys equal, which makes a new collision, which merges again.
   egglog calls this rebuilding; here it is the next round.
4. **Classes only grow.** No merge is undone. A row that holds under old
   leaders still holds under new ones, so the loop is monotone.

One loop runs it:

```text
   +--> run the rules, reading every T column as its leader
   |               |
   |    find merges: union collisions and eqrel rows
   |               |
   +--- yes -- did a leader change? -- no --> fixpoint
```

Output is written in leaders, one row per combination of classes. A type's
`Leader` table can be output too, to show which values merged.

## Example: grandmothers

```text
.type Person <: string
.decl Mother(child: Person, mom: union<Person>)
.decl Grandma(child: Person, gm: Person)
.output Grandma

Mother("ann",   "mary").
Mother("ann",   "maria").
Mother("mary",  "rose").
Mother("maria", "rosa").

Grandma(c, g) :- Mother(c, m), Mother(m, g).
```

| Round | Sees | Learns |
|---|---|---|
| 1 | ann has two mothers, mary and maria | mary = maria, led by maria |
| 2 | `Mother(mary, rose)` reads as `Mother(maria, rose)`: maria has two mothers | rose = rosa, led by rosa |
| 3 | no new collision | fixpoint |

```text
Mother    (ann, maria)  (maria, rosa)
Grandma   (ann, rosa)
Leader    mary -> maria   rose -> rosa
```

Round 2 is congruence: the second merge exists only because the first one
changed a key. `Grandma.gm` is a plain `Person` column, yet it reads leaders
too, so `(ann, rose)` and `(ann, rosa)` are one row.

The same program in Souffle states each of these steps by hand:

```text
.decl Same(a: Person, b: Person) eqrel
// seeds, so that Same(c, c) holds
Same(p, p) :- Mother(p, _).
Same(p, p) :- Mother(_, p).
// collision and congruence
Same(m1, m2) :- Mother(c1, m1), Mother(c2, m2), Same(c1, c2).
// every read of a Person joins Same
Grandma(c, g) :- Mother(c, m1), Same(m1, m2), Mother(m2, g).
```

It outputs both `(ann, rose)` and `(ann, rosa)`, and every rule that reads
a `Person` needs a `Same` join. egglog avoids that join by reading
canonical ids, as rule 1 does; on a points-to analysis it ran 4.96x faster
than the fastest sound Souffle encoding
([Better Together, PLDI 2023](https://arxiv.org/abs/2304.04332)).

## More examples

**Entity resolution.** A merge of one type causes a merge of another:

```text
.type Cust <: int32
.type City <: string
.decl Rec(id: Cust, email: string, city: City)
.decl ByEmail(email: string, id: union<Cust>)   // one customer per email
.decl Lives(id: Cust, city: union<City>)        // one city per customer
.decl Store(city: City, name: string)
.decl Shop(id: Cust, store: string)

Rec(1, "ann@x.com", "NYC").
Rec(2, "ann@x.com", "New York").
Store("New York", "Macy's").

ByEmail(e, id) :- Rec(id, e, _).
Lives(id, c) :- Rec(id, _, c).
Shop(id, s) :- Lives(id, c), Store(c, s).
```

| Round | Learns | Because |
|---|---|---|
| 1 | 1 = 2 | they share an email |
| 2 | "NYC" = "New York" | customer 1 now lives in both |

`Shop` holds `(1, "Macy's")`, and `Store` reads `("NYC", "Macy's")`. The
`email` column is a plain `string`, so it never merges.

**Steensgaard points-to.** A pointer's targets are one class:

```text
.type Loc <: string
.decl Pts(p: Loc, o: union<Loc>)

Pts(p, a) :- AddrOf(p, a).                         // p = &a
Pts(p, o) :- Assign(p, q), Pts(q, o).              // p = q
Pts(p, o) :- Load(p, q), Pts(q, r), Pts(r, o).     // p = *q
Pts(r, o) :- Store(p, q), Pts(p, r), Pts(q, o).    // *p = q
```

`p = &a; q = &b; p = q;` gives `Pts(p, a)` and `Pts(p, b)`, so `a = b`.

**egglog.** Each constructor is a relation whose last column is its class,
and egglog's `:merge` on a class is `union<T>`:

```text
.type Expr <: int64
.decl Num(n: int64, e: union<Expr>)
.decl Add(a: Expr, b: Expr, e: union<Expr>)
.decl Mul(a: Expr, b: Expr, e: union<Expr>)
.decl Same(a: Expr, b: Expr) eqrel

// (rewrite (Add a b) (Add b a))
Add(b, a, e) :- Add(a, b, e).
// (rewrite (Mul a (Num 1)) a)
Same(e, a) :- Mul(a, one, e), Num(1, one).
```

From `Add(x, y, e1)` the first rule derives `Add(y, x, e1)`; if
`Add(y, x, e2)` exists, the two collide and `e1 = e2`. The second rule's
right side is a variable, not a new term, so it merges explicitly.

**The chase.** A partial order, written as CQL holds it: a table of rows,
each with two foreign keys, `Lo` and `Hi`. A foreign key is a function,
one value per row, so it is a union column:

```text
.type Dom <: int32
.type Row <: int32
.decl Lo(r: Row, a: union<Dom>)
.decl Hi(r: Row, b: union<Dom>)
.decl Le(a: Dom, b: Dom)
.decl Same(a: Dom, b: Dom) eqrel

Le(a, b) :- Lo(r, a), Hi(r, b).

// transitivity: a new row for (a, c), unless one exists
Lo(r, a) :- Le(a, b), Le(b, c), !Le(a, c), r = row(a, c).
Hi(r, c) :- Le(a, b), Le(b, c), !Le(a, c), r = row(a, c).

// anti-symmetry
Same(a, b) :- Le(a, b), Le(b, a).
```

All four pieces of the chase appear: rules derive, `!Le(a, c)` skips a
pair that already has a row, `row(a, c)` names a new row by its ends (see
*Fresh values* below), and `Same` merges. Without `!Le(a, c)` an ordered
pair can get a second row (the oblivious chase); with it, none does (the
restricted chase).

## Compared with

| | Souffle `eqrel` | egglog | `union<T>` |
|---|---|---|---|
| Merge on a collision | a rule per relation, by hand | `:merge`, automatic | automatic |
| Congruence | a rule per relation, by hand | rebuild | automatic, next round |
| Reading a class | a `Same` join in every rule | canonical ids | leaders, everywhere |
| Output | every member | canonical ids | leaders, and `Leader` if asked |
| Skip if a witness exists | stratified negation only | no generic negation ([egglog#695](https://github.com/egraphs-good/egglog/issues/695)) | `!R` inside recursion |

## Plan

**Stage 1: a compiler rewrite, no new operator.** It turns each merging
type into plain FlowLog, built from parts that exist today. For
`Mother(child: Person, mom: union<Person>)` it adds:

```text
// a lattice: each value keeps the smallest leader it has seen
.decl Leader(x: Person, l: min<Person>)

// seeds: one rule per column of type Person, in every relation
Leader(c, c) :- Mother(c, _).
Leader(m, m) :- Mother(_, m).

// a collision of the union column, read through leaders
Merge(m1, m2) :- Mother(c1, m1), Leader(c1, c),
                 Mother(c2, m2), Leader(c2, c), m1 != m2.

// point each leader at the other; the lattice keeps the smaller
Leader(l1, l2) :- Merge(a, b), Leader(a, l1), Leader(b, l2).
Leader(l2, l1) :- Merge(a, b), Leader(a, l1), Leader(b, l2).

// members follow their leader
Leader(x, m) :- Leader(x, l), Leader(l, m).
```

An `eqrel` relation adds `Merge(a, b) :- Same(a, b).`, and a read of
`Same(x, y)` becomes `Leader(x, l), Leader(y, l)`. Every other atom reads
its `Person` columns through a `Leader` join. A merge settles in about two
rounds: the losing leader points at the winner, then its members follow.

Inside a loop, a lattice's earlier answers can stay until the exit folds
them ([Lattice relations](aggregation/lattice.md#planner-lattice)), so a
member's old leaders can stay until then too. They are still true, since
classes only grow, and a collision found through an old leader is a real
one, since that leader is in the same class. The result is right; the cost
is extra rows, which is the main risk on merge-heavy work.

**Stage 2: a union-find operator.** One worker per round merges the round's
pairs and broadcasts the leaders that changed, and rows are rewritten when
their leader changes, as egglog rebuilds. No seeds, no stale rows. A
union-find cannot undo a merge, so this stage takes static and append input
only.

## egglog benchmarks

What [egglog-benchmarks](https://github.com/yihozhang/egglog-benchmarks)
(at `3398305`) would need beyond this design:

| Suite | Also needs | Covered |
|---|---|---|
| datalog (12 programs) | nothing new | yes |
| pointer-analysis | nothing | yes |
| matmul-assoc (5), horner (5) | terms written as facts | yes |
| math, math-microbenchmark, actions | a bounded run (`run N`) | after `run N` |
| storel (10) | rulesets, an OR merge, `frozen-min` | partly |
| luminal (8) | `delete`, `subsume` | no |
| hardboiled (42) | functions as values, `Vec`, schedules, `:cost` | no |
| eggcc (32) | `delete`, `subsume`, `Set`, schedules | no |

The suites that fit only add facts. The rest remove facts, or need
containers, functions as values, or schedules, which are outside this
design.

## Non-goals

- **Extraction**, picking the cheapest term of a class. A `min<T>` cost
  relation already computes it per class:
  `Cost(e, ca + cb + 1) :- Add(a, b, e), Cost(a, ca), Cost(b, cb).`
- **Removing facts**: egglog's `delete` and `subsume`.
- **Containers** (`Vec`, `Set`, `Map`) and functions as values.
- **Rules added at run time.** FlowLog compiles ahead of time.

## Open questions

- **Leader choice.** The smallest value is deterministic. Keeping the
  larger class's leader moves fewer rows.
- **Loop shape.** Run the `Leader` closure to its fixpoint inside each round
  (a nested `iterate`), or share one loop with the rules (stage 1).
- **Seeds.** One `Leader` row per value is simple. Reading "its leader, or
  itself" without them needs an outer join, which FlowLog lacks.
- **Fresh values.** `.extern fn` returns a primitive, and `ord` numbers
  overlap loaded ids. A built-in could name a value of `T` by a tuple, in
  `T`'s own space.
- **Bounded runs and rulesets.** egglog's `run N` and schedules, for
  programs that never saturate. A loop has at most 65,535 rounds today.
- **Accidental merges.** One wrong row merges two classes and everything
  built on them. Outputting `Leader` helps audit; its `.output` spelling is
  open.
- **Mutable input.** A retraction can split a class. Stage 1 inherits the
  lattice rules for signed input
  ([Mutability](mutability.md#aggregation)); stage 2 rejects it.
- **More than one union column** in a relation, or a union column beside a
  lattice column.
