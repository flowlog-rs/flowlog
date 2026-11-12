# Physical Attribute Layout Assignment

## Problem

Choose physical attribute orders across a planned dataflow so that producers
and consumers require less reordering. The choices include input storage,
arrangement keys and values, join orientation, rule heads, and recursive
feedback. Logical argument order stays unchanged; fixed external formats
are handled by input/output mappings.

The design optimizes layouts for an existing join tree and sharing
structure. It minimizes an additive layout cost, with every sorting
boundary weighted equally. An optimal assignment means optimal under
this cost model; actual runtime also depends on cardinality, skew,
timestamps, and worker partitioning. Workload examples are collected in
[Appendix A](#appendix-a-case-studies).

## Transition cost: C[sigma_1 -> sigma_2]

A layout is a comparison order of attribute origins. Origins track fields
through renaming and projection, so unrelated fields with the same name
remain distinct.

Define $C[\sigma_1\to\sigma_2]$ as the estimated reordering work from a
producer's order to a consumer's comparator. It is zero for an unchanged
comparator and excludes fixed scanning and hashing work. The objective is

$$
\min_\ell\sum_{e\in E_{\mathrm{sort}}}
C[\sigma_e(\ell)\to\tau_e(\ell)],
$$

where $\ell$ assigns all layouts and join orientations. Charge each actual
sorting boundary once. Pure maps compose until the next such boundary.

### Raw binary-record inversions

Our initial $C$ is the raw inversion count $I$: assume every source field
is binary and all $2^m$ records appear once. Sort them by $\sigma_1$ and
count record pairs reversed by $\sigma_2$. Equal projected records do not
count as inversions. This counts record inversions, not attribute swaps:

| Source | Target | I |
|---|---|---:|
| `(a,b,c)` | `(b,a,c)` | 4 |
| `(a,b,c)` | `(a,c,b)` | 2 |
| `(c,a,b)` | `(a,b)` | 6 |
| `(c,b,a)` | `(a,b)` | 8 |

The score can be computed from field orders without generating the binary
records. It is a data-independent sorting surrogate, not a prediction of
sorter comparisons or execution seconds.

### Projection

Keep dropped fields as a **virtual suffix in source order** when scoring:

```text
source:             (a,b,c,d,e)
retained comparator: (c,d,e)
score:              (a,b,c,d,e) -> (c,d,e,a,b)
```

This preserves the order and multiplicity of projected witnesses. The
suffix is used only for scoring; it never becomes part of the physical
row or dedup identity. Consecutive pure projections compose, for example
`(a,b,c) -> (b,c) -> (c)` is scored once as `(a,b,c) -> (c,a,b)`.
A real intervening materialization starts a new boundary using its actual
surviving fields.

For an arrangement consuming a join:

```text
R: K=(k), V=(a,b)
S: K=(k), V=(c,d)
join source: (k,a,b,c,d)
consumer: K=(b,d), V=(a,c)
score: (k,a,b,c,d) -> (b,d,a,c,k)
```

The conceptual `V'=(k)` is the dropped suffix. Required key fields stay
before value fields; orders within `K` and `V` remain choices.

## Graph model

Use a bipartite, undirected **factor graph**:

- A variable chooses a physical layout block or join orientation.
- A factor charges a transition cost or enforces compatibility among
  the variables that determine that transition.

A block of $b$ fields has $b!$ permutations. An arrangement has
$k!v!$ legal layouts: permute its key and value separately, with the key
first. A free full-row layout has $m!$ states. Write $d_x$ for variable
$x$'s domain size.

The model preserves these constraints:

1. Both join inputs use the same ordered key; share its permutation
   variable. A swappable join has two orientations, while asymmetric
   operators keep their required direction.
2. Producers of one head or union share its layout. Recursive feedback
   reuses the same representation variables, without unrolling iterations.
3. Different arrangements of a relation have separate variables. Only
   actual shared representations share a layout choice.
4. Projections, logical bindings, set membership, signed updates, and
   required external schemas keep their semantics. Unsupported choices
   are excluded from the domains or assigned infinite cost.

`K ++ LV ++ RV` models local join traversal. Incremental history replay
and output chunking do not guarantee that the complete stream has this
order. The cost model uses it as a reference order.

For a join, a factor can depend on the common key order, both value
orders, orientation, and target layout. A simplified view is:

```text
[A layout] --- (join cost) --- [C layout]
                    |
                [B layout]
```

The factor graph must be acyclic for tree DP. A dataflow DAG alone is
insufficient: `Q -> A -> H` and `Q -> B -> H` couple the shared layouts
of both `Q` and `H`, forming an undirected cycle. Likewise, an arrangement
sharing tree does not automatically make the layout factor graph a tree.

### Multiple producers must agree

```prolog
Out(c,a) :- R(a,b), S(b,c).
Out(c,a) :- T(c,d), U(d,a).
Out(c,a) :- Seed(c,a).
```

The first join favors `(a,c)` and the second favors `(c,a)`. Both and the
seed feed one head layout. Each producer contributes an alignment cost;
they cannot independently choose incompatible layouts for that head.

## DP on a forest

Root each factor-tree component anywhere. A message is a table of minimum
costs indexed by the state of the variable connecting a subtree to its
parent. Let $N$ denote neighbors. A variable sums its child messages:

$$
M_{x\to f}(s)=\sum_{g\in N(x)\setminus\{f\}}M_{g\to x}(s).
$$

A factor minimizes over its other variables:

$$
M_{f\to x}(s)=
\min_{\{s_y:y\in N(f)\setminus\{x\}\}}
\left[f(s,\{s_y\})+
\sum_{y\in N(f)\setminus\{x\}}M_{y\to f}(s_y)\right].
$$

At the root, sum incoming messages and choose the cheapest state. Store
minimizing choices to recover the full assignment. Sum the optima of
independent components. This is exact because subtrees are independent
once their connecting variable is fixed, and every factor is charged once.

### A join can be solved in either direction

For `A join B -> C`, writing input/output layout states as $a,b,c$ and
orientation as $o$, a message toward `A` is

$$
M_{J\to A}(a)=\min_{b,c,o}
\left[J(a,b,c,o)+M_{B\to J}(b)+M_{C\to J}(c)+M_{o\to J}(o)\right].
$$

`J` enforces compatible input keys. The message from `C` includes its
downstream costs. Thus the transition jointly chooses `B`, `C`, and
orientation rather than freezing a locally preferred output. A message
toward `C` instead minimizes over `A`, `B`, and orientation. Reversing the
DP direction does not reverse the physical transition cost.

### Reduce transition work

- Compose pure maps, substitute fixed states, and share common key
  variables before solving.
- Cache local factor tables across cut assignments. Reuse messages when
  their relevant cut states have not changed.
- For raw $I$, remove an unrestricted full-row intermediate layout with
  only two incident transitions on the same field set:
  $\min_\zeta[I(\sigma,\zeta)+I(\zeta,\tau)]=I(\sigma,\tau)$.
  The triangle inequality gives the bound, attained by $\zeta=\tau$.
- Use raw $I$'s source-block decomposition. For a fixed target and block
  order, effects of internal source-block permutations are additive:

$$
I(q\Vert p\Vert t,\tau)=
c(\tau)+g_K(q,\tau)+g_L(p,\tau)+g_R(t,\tau).
$$

Here $q,p,t$ are key, left-value, and right-value orders. Partitioning
record pairs by their first unequal source block explains the identity:
only that block's internal order determines their source comparison.
It also holds after projection. For fixed key order, orientation, and
target, independent left and right child choices can therefore be
minimized separately. Compute each block's contribution as its cost change
from a fixed reference layout; these differences can be negative. Keep
shared variables consistent across factors. Contracting attributes into
consecutive blocks requires a proof that it preserves an optimum.

## Cutset enumeration

Choose variable nodes $S$ whose removal leaves a forest. For every
assignment to $S$, substitute those states in all factors, solve the
remaining forest, and keep the cheapest result. Factors with no remaining
variables still contribute scalar costs. The outer assignment count is

$$
P(S)=\prod_{x\in S}d_x.
$$

Find a small-product cut rather than merely a small number of variables:
conditioning a two-state block is cheaper than a 720-state row layout.
The cut may contain key, value, row-layout, or orientation variables.
Fixing recursive IDB rows alone may leave arrangement cycles.

### Finding the cut

First obtain a feasible cut greedily: repeatedly delete a variable that
removes many independent cycles per $\log d_x$ paid, then remove redundant
choices. Improve or certify it with branch and bound:

1. Find a remaining cycle.
2. Branch on each variable in that cycle: every cut must select one.
3. Prune products that cannot beat the incumbent. Disjoint-variable cycles
   give a lower bound by multiplying their smallest domain sizes.
4. Record a cut when no cycle remains.

Keep factor identities fixed during search so the cycle certificates
remain valid. Memoize selected-variable sets. For $n$ candidate variables,
exhaustive search can visit $2^n$ subsets; a search limit can return a
valid cut without proving it minimum.

**Any valid cut gives the optimal modeled layout if all its assignments
are solved exactly.** Minimum cut product only reduces work. It need not
minimize total DP work, so also compare residual table sizes. For global
optimality across strata, retain shared boundary states or pass joint
boundary messages; solving strata with frozen boundaries is conditional
optimization.

## Optimization cost

For a join with $k$ key, $l$ left-value, and $r$ right-value fields, and
$D$ target layouts, direct transition enumeration costs

$$
O\left(\kappa(m)Dk!l!r!\right),\qquad m=k+l+r,
$$

where $\kappa(m)$ is one cost evaluation. Trying both orientations adds
local factor 2. With the source-block decomposition and independent child
messages, the value-domain product becomes a sum:

$$
O\left(\kappa(m)Dk!(l!+r!)\right).
$$

For a target arrangement, $D=k_{\mathrm{out}}!v_{\mathrm{out}}!$; for a
free projected row with $p$ fields, $D=p!$. The source width $m$ includes
dropped fields.
General factor transitions enumerate their actual scopes, including
shared variables when solving against dataflow.

### Counting the benchmark plans

The following analysis covers **16 queries, 82 strata, and 55 datasets**
from main at [5d345922][cost-baseline]. Layout work is counted once per
query. Both configurations use per-stratum contraction, full-row
elimination, source-block decomposition, and free imported layouts.
One fixes join directions; the other includes their orientation variables.

Using the earlier structural budget, for each independent component $H$:

$$
B_H=\sum_{f\in H}m_f\prod_{x\in X_f}d_x,\qquad
T_H=\sum_{\text{elimination bags }b}\prod_{x\in b}d_x,
$$

$$
W=\sum_H\left[B_H+P(S_H)T_H\right].
$$

$X_f$ is a factor's variable scope. Cache its table once in $B_H$; $T_H$
counts residual DP state assignments using the cheapest of three tested
elimination-order heuristics. This budget charges $m_f$ per table entry;
it is not elapsed time or a literal operation count. Degree-based
arithmetic and cut-search work are excluded; scalar lookups and outer
minimum scans are recorded separately in the CSV.

All cuts were proved minimum-product for their factor graphs. Both modes
have **62 strata already forming forests**. Table entries below were
checked by independently reconstructing the elimination bags; the fixed
counts also reproduce the earlier analysis. Construction and DP columns
refer to free orientation; the cut column is the largest independent
component product.

| Query | Datasets | Fixed total | Free construction | Free DP | Free total | Max free cut product |
|---|---:|---:|---:|---:|---:|---:|
| TC | 2 | 32 | 30 | 12 | 42 | 1 |
| SG | 2 | 53 | 54 | 24 | 78 | 1 |
| Reach | 4 | 11 | 11 | 4 | 15 | 1 |
| CC | 3 | 26 | 26 | 10 | 36 | 1 |
| SSSP | 2 | 33 | 27 | 6 | 33 | 1 |
| Bipartite | 4 | 29 | 36 | 12 | 48 | 1 |
| Dyck | 2 | 142 | 148 | 54 | 202 | 1 |
| CRDT | 1 | 9,666 | 7,510 | 5,724 | 13,234 | 4 |
| Galen | 1 | 2,388 | 1,280 | 3,458 | 4,738 | 16 |
| Andersen | 2 | 86 | 90 | 36 | 126 | 1 |
| Polonius | 4 | 3,602 | 2,222 | 4,378 | 6,600 | 16 |
| CSPA | 3 | 178 | 176 | 62 | 238 | 2 |
| CSDA | 3 | 32 | 30 | 12 | 42 | 1 |
| CVC5 | 1 | 45,222,512 | 38,774,858 | 74,081,328 | 112,856,186 | 2,048 |
| Z3 | 1 | 45,222,512 | 38,774,858 | 74,081,328 | 112,856,186 | 2,048 |
| DOOP | 20 | 236,724 | 196,458 | 48,782 | 245,240 | 4 |
| **Total** | **55** | **90,698,026** | **77,757,814** | **148,225,230** | **225,983,044** | **2,048** |

Allowing orientations gives total budget **225,983,044**, versus
**90,698,026** with fixed directions. Although a local join trial doubles,
a shared orientation can also enlarge the cut and repeated DP work in
this factorization. The resulting 2.49-times budget describes this
schedule, not a lower bound on every solver.

The [CSV](attribute-layout-cutset-costs.csv) contains both modes for all
82 strata. `outer_product` combines all cut domains within a stratum;
`max_component_outer` is the largest component product. Budgets enumerate
independent components separately. Table sizes count states, not bytes.

## Implementation proposal

Add a physical-layout pass in `flowlog-planner` after transformations and
sharing identities are available, before code generation. Keep the
existing join tree and runtime operator structure.

1. **Extract the model.** Assign stable origin IDs; compose field mappings;
   create layout domains, shared-key variables, orientation variables, and
   cost factors. Identify each physical representation by its actual scope
   and sharing identity, not only its relation name.
2. **Solve it.** Apply exact reductions, select cuts, enumerate assignments,
   and run forest messages with backpointers. Cache tables within explicit
   memory limits. Use exact cost arithmetic, including signed terms from
   source-block decomposition. On ties, prefer main's existing assignment.
3. **Apply a physical plan.** A proposed `PhysicalLayoutPlan` stores chosen
   permutations and orientations. `flowlog-codegen` uses it for tuple
   construction, accesses, head unions, and feedback. Update dependent
   references consistently, restore required external schemas, and preserve
   mutability, aggregate, and timestamp semantics.
4. **Handle boundaries explicitly.** Pin unsupported expression layouts.
   Aggregate grouping inputs can use their non-aggregate projection;
   payload and result fields need their own layout contract. Per-stratum
   solving must report whether boundary layouts were fixed or optimized
   jointly. Respect [canonical sharing's scope](canonical-form.md#scope-the-one-assumption-a-caller-can-break).
5. **Validate and expose diagnostics.** Compare costs with binary-record
   enumeration and small DP solutions with exhaustive assignments,
   including reverse join transitions, projections, union, and shared
   branches. Recompute the full objective after applying the plan. Run
   correctness suites and benchmark against the same main revision.
   Report cut products, proof status, table sizes, planning time, and any
   fallback that gives up exhaustive layout optimization.

## Appendix A: Case studies

These historical experiments illustrate layout trade-offs. Their runtime
observations are separate from the proposed DP and its state-count analysis.

### DOOP/Jython: head and downstream layouts

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

### Scallop: a large seed and conflicting consumers

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

### Z3: rule-head order versus recursive indexes

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

### Multiple upstreams and shared arrangements

The shared-head example in the [graph model](#multiple-producers-must-agree)
has two joins favoring different layouts and a seed with its own order.

Selecting the first join's preferred order can benefit that branch while
reordering the other join and the seed. A downstream consumer can introduce
a third preference. Choosing a shared layout should consider the volume and
order properties of every contributor, rather than treating one upstream
as the representative of the entire relation.

Likewise, a shared intermediate arrangement may serve several joins with
different preferences. The assignment must decide when a single layout is
adequate and when another materialized view is worth its memory and update
cost.

### CVC5 and Z3: optimizer cost

CVC5 and Z3 have identical structure here. For either query:

| Stratum | Fixed total | Free construction | Free DP | Free total | Free combined cut product |
|---|---:|---:|---:|---:|---:|
| 0 | 43,275,751 | 36,603,779 | 57,650,932 | 94,254,711 | 12 |
| 1 | 7,612 | 4,628 | 4,240 | 8,868 | 4 |
| 2 | 367,067 | 36,073 | 12,517,378 | 12,553,451 | 2,048 |
| 3 | 20,594 | 16,272 | 6,068 | 22,340 | 2 |
| 4 | 554 | 608 | 102 | 710 | 1 |
| 5 | 3,162 | 2,134 | 1,360 | 3,494 | 4 |
| 6 | 1,547,772 | 2,111,364 | 3,901,248 | 6,012,612 | 32 |

Stratum 0 contributes **83.5%** of the free-orientation budget. Its largest
factor has **2,073,600 states**, despite cut product 12. Small cuts reduce
outer repetition but do not remove large local transitions.

In stratum 2, the cut grows from six to eleven two-state variables:
$64\to2{,}048$ assignments. Its budget grows from 367,067 to 12,553,451.
Enumerating all five recursive IDB row layouts would instead require
$3!\times4!\times3!\times4!\times5!=2{,}488{,}320$ assignments and
still require checking remaining arrangement cycles.

### DOOP: a small cut in a large stratum

DOOP's final stratum has 147 variables and 137 factors, but cycle rank 4.
A three-variable cut has combined product 8, largest component product 4,
and budget 11,679. Its full query budget is 245,240.

### Galen: savings on the critical worker

A separate Raw I versus Normalized study, based on main
[5d345922][cost-baseline], examined:

```prolog
Q(x,e,o) :- Q(x,y,z), R(y,u,e), Q(z,u,o).
```

The first `Q join R` produces **19,835,387 updates** in every variant.
`Raw I` uses the binary-record inversion count; `Normalized` uses
$mI/I_{\max}$, where $m$ is the source width and $I_{\max}$ is the maximum
for that source width and retained-field count.

| Pipeline active time / s | main | Raw I | Normalized |
|---|---:|---:|---:|
| Join plus output arrangement | 4.523 | 2.446 | 4.354 |
| Internal Join | 1.852 | 0.801 | 1.891 |
| Output arrangement | 2.658 | 1.649 | 2.459 |

These are cumulative active times across 32 workers, not wall time;
component medians need not sum to the pipeline median. Raw I reduces the
busiest worker's Join activity from about 0.98s to 0.41s. Both optimized
versions make the later Q rule-head dedup more expensive, by about 0.97
and 1.08 active seconds respectively. Raw I's earlier join savings outweigh
that cost; Normalized does not achieve the same critical-worker benefit.

### Other profiling observations

The same Raw I versus Normalized study found the following. These rows
summarize its follow-up profiles and remeasurements, rather than new runs.

| Case | Observation | Implication for layout assignment |
|---|---|---|
| Z3 | Total raw I falls from 173,537 to 155,929, but one 37.77-million-update consumer's arrangement activity rises from 7.443s to 9.703s. | A lower unit-weight total can hurt a high-volume edge. |
| Andersen/medium | Large initial time regressions shrink to +0.68% / +0.84% on remeasurement; RSS still rises about 7%. Input maps become more expensive while downstream arrangements improve. | Include conversion costs and distinguish persistent effects from noisy timings. |
| CRDT | The `value_blank_star` head rises from 1.953s to 2.052s of activity with unchanged record counts; its restoration map takes about 0.0716s. | A layout conversion can offset local sorting savings without adding another dedup. |
| Dyck/kernel | Only two join orientations change; the target pipeline's activity is essentially unchanged. | A short wall-time difference alone does not establish a sorting benefit or regression. |
| CSPA | Remeasurements are approximately tied. PostgreSQL's local head increase is mainly threshold activity; arrangement time and record counts barely change. | I omits batching and activation costs. |
| CSDA/httpd | Raw I and Normalized have byte-identical binaries, yet timing differences between their labels change direction. | Label-to-label differences cannot establish a layout effect when the executable is identical. |

## Appendix B: Experimental context

The Jython timing above comes from a 55-workload comparison of main
[44ad2c5d][baseline] and the field-order experiment [d6ae8553][experiment]:
five measured runs, medians, and 32 workers on one NUMA node. It measures
whole-program execution, including assembly and loading, not compilation.
The restored Scallop and rule-head Z3 examples use earlier field-order
profiles. The Galen and other Raw-I observations come from the separate
study based on main [5d345922][cost-baseline], comparing Raw I with
$mI/I_{\max}$ normalization. Its original suite used five interleaved runs;
follow-up cases used seven or fifteen runs. Profile activity is cumulative
across 32 workers. Results from these studies are kept separate from the
structural optimizer budgets above.

Related documentation: [canonical form](canonical-form.md) and
[mutability and deduplication](mutability.md).

[field-order-pr]: https://github.com/flowlog-rs/flowlog/pull/418
[baseline]: https://github.com/flowlog-rs/flowlog/commit/44ad2c5d
[experiment]: https://github.com/altramarine/flowlog/commit/d6ae8553f67861b43b15a697fb0016bb42be26e0
[cost-baseline]: https://github.com/flowlog-rs/flowlog/commit/5d345922654a6ded5a296511a3d07d898a258adb
