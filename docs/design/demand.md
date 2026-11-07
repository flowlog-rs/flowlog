# Demand pushdown

Status: on by default. Stage 7 of the parser pipeline
(`flowlog-parser/src/pipeline/demand.rs`) rewrites the rules before they
are stratified. `--no-demand` on the compiler, or `Builder::demand(false)`
from `build.rs`, turns it off. The outputs are the same either way, and so
are the inputs the program loads and accepts.

## What it does

A body atom naming a constant reads only part of a relation:

```text
Reach(s, d) :- Edge(s, d).
Reach(s, d) :- Reach(s, m), Edge(m, d).
FromOne(d)  :- Reach(1, d).
.output FromOne
```

`FromOne` reads `Reach` only where `s = 1`, so the pass filters the base
rule to that value:

```text
Reach(s, d) :- Edge(s, d), s = 1.
Reach(s, d) :- Reach(s, m), Edge(m, d).
```

and leaves the recursive rule alone. It copies `s` from `Reach(s, m)`,
whose rows already have `s = 1`. The loop then derives node 1's row of the
closure instead of the whole closure.

The filter is an ordinary comparison, and the planner places it like any
other. Here the generated dataflow differs from the unfiltered one in one
operator: `Edge` is filtered to `s = 1` before its rows `enter` the loop's
scope. The loop itself (its `Variable`, the join with `Edge`, and `leave`)
is generated as before, and holds only node 1's rows.

## The analysis

A **demand** gives each relation column the values the program can read
there: `Any`, or a finite set of constants. A relation's demanded rows, its
*box*, are those whose every column lies in its column's demand.

- **Seeds.** Every column of an `.output` or `.printsize` relation is
  `Any`. So is every column of a relation in a recursive component with a
  lattice relation, and a head column computed by an expression. Every
  other column starts empty.
- **Needed values.** The values a rule can *need* of a variable are those
  in the demand of every head column that holds it and, if the rule pins
  it (`x = c`), among its pins. A variable in neither the head nor a pin is
  needed at every value.
- **Liveness.** A rule is *live* unless some head constant lies outside its
  column's demand, some head column has an empty demand, or some variable
  can be needed at no value. A rule that is not live derives no row the
  program reads.
- **Reads.** A live rule demands of each column of each body atom
  (positive, negated, or inside an aggregate's braces) the constant written
  there, every value it can need of the variable there, or every value for
  a placeholder.

`solve` iterates from the seeds until every live rule's reads are inside
the demand. Columns only grow, and a column past `MAX_VALUES` (4)
constants widens to `Any`, so this stops. The demand it stops at is
**closed**: a rule instance that derives a row in its relation's box reads
only rows in their boxes.

## The rewrite

`plan_rule` acts on each rule under the closed demand:

- **Drop** a rule that is not live.
- **Keep** a live rule whose head columns with a finite demand already
  stay inside it. Each such column holds one of:
  - a constant, admitted by liveness;
  - a pinned variable, equal to a pin that liveness admits;
  - a variable *carried* from a column of a positive body atom (not one
    inside an aggregate) whose demand is no wider. That relation must have
    no input and no inline facts: then every rule deriving it keeps the
    column inside its demand, so every row of it does too.
- **Filter** every other head variable with a finite demand: one copy of
  the rule per value it can need, each with `x = v` appended. Several
  filtered variables multiply. Past `MAX_COPIES` (16) copies, the widest
  filter is given up, its columns widen to `Any`, and the analysis reruns.

An expression never fills a finite-demand head column, since such columns
are seeded `Any`.

A kept rule may read a relation whose every rule the plan drops: within
its demand the relation is empty. Left with no rule, it would become an
empty input, one the program did not declare. Its columns are demanded at
every value instead, which makes all its rules live, and the analysis
reruns.

Finally `prune` removes the rules and relations that only dropped rules
read. It keeps every input, read or not, so the program loads and accepts
the same inputs, and runs incrementally if it did. Code generation gives
an input that nothing reads its handle alone: its rows are accepted, but
not deduplicated or stored.

## Why the outputs do not change

Three facts:

1. **The rewrite keeps every instance that derives a row in the box.** It
   drops only rules that derive none, and each filter lists every value the
   rule can need.
2. **Closure.** Such an instance reads only rows in the box. This covers
   negated atoms and aggregate groups too, so a lower stratum's rows in the
   box are all a higher stratum's rule can see. By induction over the
   least fixpoint, in both directions and stratum by stratum, the program
   as written and the rewritten one have the same rows in the box.
3. **An output's box holds every row**, so the outputs are the same.

The rewritten program also computes inside the demand. Each rule it writes
keeps its head in the box as long as the rows it reads are in their boxes
or are given facts (input and inline). By induction, every row it derives
is in the box or a given fact. Since the rows in the box are the
original's, it derives no row the original does not.

These are machine-checked in Lean, in `Demand/Program.lean` of the
companion repository `flowlog-demand-paper`. The model describes FlowLog
rules (terms, pins, negated atoms, aggregates) under a per-column demand.
It assumes the conditions `solve` and `plan_rule` establish, not their
code.

| Theorem | Statement |
| --- | --- |
| `syntactic_closed` | the conditions `solve` establishes make the demand closed |
| `pass_rewrites` | drops and copies keep every instance that derives a row in the box |
| `pass_keeps_outputs` | the outputs do not change |
| `pass_clean` | a rewritten rule reading rows in their boxes or given facts derives a row in the box |
| `pass_inside_demand` | the rewritten program derives only rows in the box and given facts |
| `pass_invents_nothing` | it derives no row the original does not |
| `Counterexample.closure_needed` | sound filters at a demand that is not closed lose a row |

Closure is what makes this work. Filtering only by what the outputs read is
wrong:

```text
P(x) :- E(x).
P(y) :- P(x), S(x, y).
```

With `P` read only at `2`, filtering both rules' head variables to `2`
loses `P(2)`, which the program derives from `E(1)` and `S(1, 2)`: the
recursive rule reads `P` at `1`. The closed demand includes that read, so
`P` is demanded at every value and the pass filters nothing. This is
`Counterexample.closure_needed` in the same file.

## What it leaves alone

- **Programs that are not stratified.** A recursion through a negation or
  an aggregate has no strata, and the proof assumes strata. FlowLog runs
  a recursion through negation as one loop, with a warning, and the rows
  it settles on depend on every rule in it. It rejects one through an
  aggregate. The pass leaves such a program as written, so it gets the
  same warning, rows, or error.
- **Demand that changes across recursion.** In
  `Tc(x, y) :- Edge(x, z), Tc(z, y).`, read at `Tc(1, y)`, the recursive
  rule reads `Tc` at every `z`. The demand on `Tc`'s first column is
  `Any`, and nothing is filtered. Restricting it needs the demanded keys
  computed alongside the loop (Magic Sets), which this pass does not do.
  Read at `Tc(x, 1)` instead, the same rule carries `y`, and the pass
  filters.
- **Lattice relations.** Their rules merge rows rather than add them, so
  their recursive components are kept as written.
- **Columns whose equality is not exact**, such as floats (`1.0` and
  `1.00`) and tuples, are demanded at every value.
- **The caps.** `MAX_VALUES` and `MAX_COPIES` bound the copies a rule is
  split into. Both were picked by hand.

## Testing

- **Unit tests** in `demand.rs` cover each case of the analysis and the
  rewrite, programs that are not stratified, inputs whose readers are
  dropped, and relations still read whose rules all fall outside their
  demand.
- **`random_programs_keep_their_outputs`** generates random stratified
  programs with recursion, negation, `count`, arithmetic heads and
  constants. It evaluates each with and without the pass and checks that:
  - the inputs and inline facts are unchanged;
  - every output is unchanged;
  - no relation gains a row;
  - each relation keeps every row in its box and, outside it, only its
    inline facts;
  - the demand is closed on the original program's model.

  It runs 400 programs by default; set `DEMAND_CASES` for more.
- **Fixtures** `demand_*` and `append_demand_reach` run the pass end to
  end, eight of them over several workers, and `demand_reach_delta` and
  `append_demand_reach` incrementally. `demand_ancestor` and
  `demand_ancestor_descendants` are the two queries of Frank McSherry's
  "The Demand Transform": the pass filters the first, and leaves the
  second, which needs Magic Sets, as written. `mixed_demand_unread_input`
  drops the only rule reading a `mutable` input, which must stay an input
  of a program that runs per epoch.
- **A differential test** generated random programs of the same kinds,
  packed 25 to a FlowLog program, and compiled them. It ran each compiled
  program three ways and compared every output with Soufflé's:
  - with the pass, on 1 worker;
  - with the pass, on 4 workers;
  - with `--no-demand`, on 1 worker.

  Of 1000 programs, 996 were checked, and in 477 of them the pass found
  a finite demand on some column. Their 2255 outputs (1030 nonempty) all
  agreed. The 4 others hit existing code generation errors that occur
  with `--no-demand` too.
- **Adversarial programs.** A separate review wrote 44 programs aimed at
  the pass and ran them as 50 cases: strings, interning, every integer
  width, floats, pins, the caps, negation, aggregates, lattices, UDFs,
  components, inline facts, missing input files, `append` and `mutable`
  inputs, transactions, library mode, and 4 workers. Each ran with and
  without the pass, comparing outputs, errors, the shell, and the library
  API. It found four bugs, now fixed and covered by the tests above:
  - dropping the only rules reading an input removed the input. A program
    could stop running incrementally, the shell could reject the input, a
    missing input file went unreported, and the library API lost an
    `insert_` method;
  - a relation still read, whose every rule was dropped, became an input,
    adding an `insert_` method;
  - a recursion through negation lost the rule that closed it, and
    settled on other rows;
  - a recursion through an aggregate, which FlowLog rejects, compiled once
    the pass dropped the rule that closed it.

## Performance

Each program below ran with the pass and with `--no-demand` on random
graphs of `n` nodes and `2n` edges. The graphs are relabeled so that
nodes 1 and 2 lie in the giant strongly connected component: each reaches,
and is reached from, about 80% of the nodes. The table gives the median of
3 runs at `n = 8000`: wall time and peak RSS of the whole executable, on an
AMD EPYC 9V84. Both runs produced the same outputs every time.

| Query | Demand | 1 worker: off → on | 8 workers: off → on |
| --- | --- | --- | --- |
| `Tc(1, y)`, left-linear `Tc` | `Tc.0 ∈ {1}` | 22.8 s, 1251 MiB → 0.01 s, 18 MiB | 3.47 s, 1641 MiB → 0.01 s, 34 MiB |
| `Tc(x, 1)`, right-linear `Tc` | `Tc.1 ∈ {1}` | 16.4 s, 1337 MiB → 0.01 s, 18 MiB | 2.68 s, 1374 MiB → 0.01 s, 34 MiB |
| `Tc(1, y)` and `Tc(2, y)`, left-linear | `Tc.0 ∈ {1, 2}` | 22.8 s, 1481 MiB → 0.02 s, 24 MiB | 3.47 s, 1672 MiB → 0.02 s, 44 MiB |
| `Node(y), !Tc(1, y)`, left-linear | `Tc.0 ∈ {1}` | 22.8 s, 1438 MiB → 0.02 s, 16 MiB | 3.44 s, 1657 MiB → 0.01 s, 46 MiB |
| `Tc(1, y)`, right-linear `Tc` | none | 16.3 s, 1216 MiB, both | 2.68 s → 2.66 s, 1430 → 1365 MiB |
| same generation as node 1 of a random tree | none | 2.42 s, 248 MiB, both | 0.50 s, 374 → 370 MiB |

With the pass, the loop derives the query node's slice of the closure,
not all of it. Its time is process start-up at every size measured, so the
gain grows with the graph: 136×, 517× and 1615× on one worker at 2000,
4000 and 8000 nodes for the first query. Where the pass finds no finite
demand, the program and its cost are unchanged.
