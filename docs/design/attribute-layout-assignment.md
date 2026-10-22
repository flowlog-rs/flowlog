# Problem: Assigning Physical Attribute Orders Across a Dataflow

## Problem statement

Attribute order is a physical design decision. It affects how updates are
generated, sorted, consolidated, exchanged between workers, merged into
arrangements, and traversed by later operators. Aligning one operator's
layout with its upstream or downstream can substantially reduce work, but
the best order for one edge can be expensive for another.

The rule-head experiment in [PR #418][field-order-pr] chooses a physical
row order from an upstream join, normalizes rows in that order, and restores
the logical head order. The broader problem is to consider
attribute order for **every arrangement and the complete path through the
dataflow**, including inputs, intermediate results, rule heads, recursive
feedback, and outputs.

> Given a planned Datalog dataflow and an expected workload, assign
> physical attribute orders to its records, arrangement keys and values,
> and join outputs to minimize end-to-end execution time, while preserving
> semantics and respecting a memory budget.

The argument order written by the user defines logical attribute bindings;
it does not have to define physical storage order. Input ingestion and
output serialization may also use chosen layouts when their interfaces
allow it. A rule head may retain an optimized physical order through
recursion and downstream consumers instead of immediately restoring the
user-written order. Explicit conversions are needed where a fixed external
format is required.

## What can be assigned?

The logical plan determines which attributes a lookup or join needs. The
physical planner can choose their order:

| Object | Layout decisions |
|---|---|
| Input relation | Stored attribute order after input decoding |
| Arrangement | Order within its key tuple and within its value tuple |
| Join inputs | A common ordered join key, plus each side's value order |
| Join output | Projection and physical order of the retained attributes |
| Rule head | Physical row order used by its union, dedup, and feedback |
| Output relation | Stored and emitted order, subject to interface requirements |

For an arrangement with required key attributes `K` and value attributes
`V`, its layout is an ordered pair of permutations:

```text
Arrangement layout = (order(K), order(V))
```

Reordering a key tuple does not change the equality predicate, provided
both sides of a join use the same logical attributes in the same physical
key order. Key membership and key order are separate decisions: an
attribute required for a lookup cannot simply be moved into the value.

For example:

```prolog
J(a,b,c,d,e,f) :- L(a,b,c,d), R(a,b,e,f).
```

One compatible assignment is:

```text
left arrangement:  K=(b,a), V=(d,c)
right arrangement: K=(b,a), V=(f,e)
join traversal model: (b,a,d,c,f,e)
```

This can favor a consumer needing `K=(b,a,d,c), V=(f,e)`, while a consumer
needing `K=(a,b,e,f), V=(c,d)` favors a different layout. The choice of
`(a,b)` versus `(b,a)` and the orders inside each value tuple must therefore
consider both the source arrangements and the consumers.

### The join-order model needs a scope

`(join key, left value, right value)` is a useful model of local join
enumeration, especially for a straightforward value cross product. The
orders inside all three tuples are meaningful choices. Changing an input
arrangement's value order changes match traversal; changing the output
tuple's field positions alone changes the layout in which those matches
are buffered and sorted. These are related but distinct decisions.

Actual incremental joins process multiple batches and timestamped
histories. Larger histories may replay updates from either side according
to time, rather than always using a fixed left-outer/right-inner loop.
Projection can also discard the leading join key or value fields, breaking
the ordering inherited from traversal. Output buffers still sort and
consolidate their chunks. Layout alignment should therefore be modeled as
a potential cost reduction, with explicit order properties where they
are guaranteed.

Arrangement maintenance sorts and consolidates updates and merges sorted
batches into a trace. An arranged join traverses two such traces with
merge-style cursors. Relevant costs include output-buffer sorting, batch
merging, history access, and worker exchange, not just tuple construction.

## Examples motivating the problem

### 1. DOOP/Jython: the head layout can match the producing join

Consider the points-to propagation rule:

```prolog
VarPointsTo(heap,to) :- Assign(from,to), VarPointsTo(heap,from).
```

With `Assign` on the left, the join inputs have:

```text
left:  K=(from), V=(to)
right: K=(from), V=(heap)
```

After dropping `from`, `(to,heap)` follows the left-value/right-value
model, while the user-written head is `(heap,to)`.

```text
experimental physical path:
join -> (to,heap) -> ordinary dedup -> restore (heap,to)

broader layout opportunity:
join -> (to,heap) -> ordinary dedup -> retain that layout for consumers
```

In the full-suite comparison described below, DOOP/Jython execution time
decreased from approximately **315 seconds to 214 seconds** with the field-order
implementation. This is a whole-program result involving multiple heads;
it is not an isolated timing of this rule or of sorting alone.

The local benefit has a downstream cost. Some consumers need an arrangement
whose complete key is `(heap,var)`. Restoring logical field positions with
a map does not restore the previous sorted order or worker partition.
Earlier DOOP profiles showed that a cheaper VarPointsTo head could coexist
with a more expensive downstream full-key arrangement.

The assignment should account for both kinds of consumers. It may be useful
to keep a variable-first primary layout, use a heap-first view where needed,
or choose another shared layout. Each conversion or additional view has a
cost. Changing the input orientation can also change which retained value
comes first, so a layout preference is conditional on the chosen join plan.

### 2. Scallop: the largest contributor may be a seed, not a join

The Polonius query used by Scallop contains:

```prolog
subset(o1,o2,p) :- subset_base(o1,o2,p).

subset(o1,o3,p) :-
    subset(o1,o2,p),
    subset_base(o2,o3,p),
    o1 != o3.

subset(o1,o2,p2) :-
    subset(o1,o2,p1),
    cfg_edge(p1,p2),
    origin_live_on_entry(o1,p2),
    origin_live_on_entry(o2,p2).
```

The transitive rule joins on `(o2,p)` and retains `p`, the left value `o1`,
and the right value `o3`. It suggests a point-first head layout. The last
semijoin in the CFG propagation rule instead suggests `(o2,p2,o1)`.

But this workload has about **27.4 million seed records**, accounting for
roughly **97% of the input updates to the recursive subset head**. Choosing
a layout from a smaller join also forces the large seed through that
layout. The subsequent consumers need several different keys:

```text
(o2,p), (p), (o1), (o1,p)
```

The previous exploration found that `(o2,p,o1)` improved the `(o2,p)`
arrangement relative to the point-first layout, but made the point-only
arrangement more expensive. It also retained costs at the seed and
recursive head. Restoring the original layout for the entire subset path
performed better in that comparison than restoring only the seed layout.

This example requires weighting all producers and consumers by their work:
seed volume, join updates, recursive iterations, and downstream index
construction. Picking the first eligible join cannot express that trade-off.

### 3. Z3: a locally attractive order can hurt recursive trace access

Abbreviate `Stack_def_use_live_var_at_block_end` as `StackEnd`:

```prolog
StackEnd(prev,used,reg,pos) :-
    StackEnd(block,used,reg,pos),
    !Stack_def_use_ref_in_block(block,reg,pos),
    !Reg_def_use_defined_in_block(block,reg),
    Block_next(prev,_,block).
```

The final join has:

```text
K=(block), LV=(reg,pos,used), RV=(prev)
```

The experimental field-order policy selects `(reg,pos,used,prev)` instead
of the original `(prev,used,reg,pos)`. However, the join key `block` is projected
away: value order within one block does not imply sorted projected output
across different blocks. Incremental history replay adds another source
of variation in emission order.

The head's `prev` becomes the next iteration's `block`. Recursive and later
consumers need block-leading keys such as `(block,reg,pos)`. Previous profiles
showed higher costs in the head arrangement, membership threshold, and
downstream Block indexes after the variable-first layout was introduced.

The assignment must include the cost of the persistent head trace and its
feedback consumers. Improving a local traversal pattern is insufficient
if the chosen layout makes these larger or repeated operations more costly.

### 4. Multiple upstreams: a shared head needs a shared decision

```prolog
Out(c,a) :- R(a,b), S(b,c).
Out(c,a) :- T(c,d), U(d,a).
Out(c,a) :- Seed(c,a).
```

Under the corresponding left/right input orientations, the first join
favors `(a,c)` and the second favors `(c,a)`. The seed has its own input
layout. If these contributions share one head trace, they must agree on
one physical schema before normalization.

Selecting the first join's preferred order can benefit that branch while
reordering the other join and the seed. A downstream consumer can introduce
a third preference. Choosing a shared layout should consider the volume and
order properties of every contributor, rather than treating one upstream
as the representative of the entire relation.

Likewise, a shared intermediate arrangement may serve several joins with
different preferences. The assignment must decide when a single layout is
adequate and when another materialized view is worth its memory and update
cost.

## Objective and constraints

The objective is **end-to-end execution time**, subject to an acceptable
peak-memory budget. Local sorting time or summed worker CPU is evidence
for a cost model, but does not directly equal parallel wall-clock time.

A useful cost model should account for:

- Input volume, duplicate witnesses, and the order of incoming update runs.
- Join output-buffer sorting and consolidation under each proposed layout.
- Arrangement batch construction, merging, and historical trace access.
- Conversions and exchanges between incompatible producer/consumer layouts.
- Repeated work along recursive feedback, including delta and seed volumes.
- Worker load balance, shared arrangements, and additional materialized views.

The assignment must preserve these constraints:

1. Logical attribute bindings, types, projections, predicates, and outputs
   remain semantically equivalent under the physical representation.
2. Both join inputs use compatible key identities and key order.
3. Contributors to a shared union or trace agree on a physical schema, or
   have explicit, correctly costed conversions.
4. Dedup membership still uses the intended projected record. A discarded
   witness or join key cannot be added to its identity merely to preserve
   ordering. Signed multiplicities, aggregates, and timestamps retain their
   meaning.
5. A persistent trace has a consistent interpretation across its lifetime;
   recursive feedback and any alternate views respect that interpretation.
6. Input/output mappings honor any required external schema. Where the
   interface is flexible, its physical order participates in the assignment.

Sorting compatibility and partitioning compatibility must be tracked
separately. Ordinary head dedup hashes a complete physical row; an ordinary
keyed arrangement exchanges by its key. Matching their attribute order
does not automatically put records on the same worker. Permuting a tuple
can itself change its hash and worker placement.

## What a good assignment should achieve

The goal is to choose mutually compatible layouts over the operator graph,
with conversions and additional indexes introduced where their total cost
is justified. It should favor expensive or frequently executed paths,
account for conflicting producers and consumers, and keep beneficial
layouts through rule heads and recursion when possible. The user-written
column order and `(key,left-value,right-value)` traversal model provide
reference layouts, not a universal optimum.

## Experimental context

The motivating full-suite comparison used main at [44ad2c5d][baseline]
and the rule-head field-order experiment at [d6ae8553][experiment]. It
covered 55 workloads with five fresh runs per version, 32 workers on one
NUMA node, and median execution time. The reported execution interval
included dataflow assembly, input loading, and execution; it excluded
compilation. The Jython numbers above refer to that comparison. The
Scallop and Z3 explanations use earlier profiles and layout experiments.

These observations motivate the assignment problem; they are not
measurements of the current main branch. This description does not add
new benchmark runs or claim that one layout improves every workload.

Related FlowLog documentation:

- [Canonical form and the physical plan](canonical-form.md).
- [Mutability, collection semantics, and deduplication](mutability.md).

[field-order-pr]: https://github.com/flowlog-rs/flowlog/pull/418
[baseline]: https://github.com/flowlog-rs/flowlog/commit/44ad2c5d
[experiment]: https://github.com/altramarine/flowlog/commit/d6ae8553f67861b43b15a697fb0016bb42be26e0
