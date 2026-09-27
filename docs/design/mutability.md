# Mutability: one engine for batch and incremental

Today a program is compiled for one mode. `--mode batch` gives every
collection `Diff = Present` at `Ts = ()`. `--mode inc` gives every collection
`Diff = i32` at `Ts = u32` (`flowlog-build/src/codegen/ty/`). This note
proposes a third option: each input relation declares how it may change, the
compiler infers a mutability for every derived collection, and each collection
uses the cheapest weight that is still correct for its mutability. Batch mode is the
case where every relation is static, and incremental mode the case where
every relation is mutable.

## Scope

The two endpoints are today's modes and stay unchanged. An all-static
program must generate the same code as `--mode batch`, and an all-mutable
program the same code as `--mode inc`. The existing suites already verify
both; a byte-for-byte comparison of the generated code guards against
regression.

Everything this note adds lies between the endpoints, and only that part
needs new verification:

- **Append itself.** First-occurrence dedup at the EDB inputs,
  rule heads and `Lex` loops; supersede; and the antijoin with presence arms
  and a signed decode.
- **Mutability boundaries.** The static-to-append retype, the static-mutable join
  through `Multiply`, and append against mutable through the same
  `Multiply`. The latter is exact only under the lemma in
  [Presence as `+1` inside the multiply](#presence-as-1-inside-the-multiply).
- **Static collections in a program that is not all static.** `diff::Static`
  at a `u32` clock and in `Lex` loops.

The efficiency target is set by what a user can do today. A mixed program is
correct only in `--mode inc`, so the middle must beat an all-mutable run. It
should also approach batch speed on its static part.

The hard part is deduplication and the other set-semantics obligations. This
note states what each mutability guarantees, where a dedup is required and
why, and what happens at every point where two mutabilities meet. Claims marked
**[E*n*]** were checked with the `flowlog-runtime` operators on this commit;
see [Evidence](#evidence).

## The three mutabilities

| mutability  | input contract                                   | weight             | clock          |
|-------------|--------------------------------------------------|--------------------|----------------|
| **static**  | complete at the first epoch, then the handle closes | `diff::Static` | minimum time only |
| **append**  | insertions only, at any epoch                    | presence (`Present`-like) | `u32` epochs |
| **mutable** | insertions and deletions                         | `i32`              | `u32` epochs   |

The mutabilities are ordered `static < append < mutable`: each admits every
update history of the one below it. The driver enforces each contract
(`flowlog-runtime/src/txn.rs`). Every mutability keeps inserts idempotent, as
today.

- **Static:** any operation after the first commit is an error.
- **Append:** a negative diff is an error, and `diff > 1` counts as a single
  insert.
- **Mutable:** keeps today's multiset input, so an insert, an insert, then a
  delete leaves the fact present. Dedup clamps positive counts to 1.

The static contract is load-bearing, not advisory. Negation and aggregation
over a static relation are correct only because that relation cannot change
after its first epoch (see [Negation](#negation) and [Aggregation](#aggregation)).

## What each mutability guarantees

The **snapshot** of a collection at epoch `e` is its accumulation at `e`. The
engine is correct if every snapshot equals the Datalog fixpoint over the input
snapshots at `e`. Each mutability keeps its own invariant:

- **Static.** Every update is at the minimum outer time. Inside a loop, every
  update is at `(t0, i)`, so these times form a total order by `i`.
- **Append: monotone presence.** A datum `d` is *announced* at a set of times
  `A(d)`. It is a member at `t` iff some `a` in `A(d)` satisfies `a <= t`, so
  membership never shrinks. After a dedup at a total clock, `|A(d)| <= 1`.
  After a dedup at the partial clock `Product<u32, u16>`, `A(d)` is an
  antichain of minimal times.
- **Mutable.** Accumulated counts are `>= 0` at every time. After a dedup
  they are `0` or `1`.

Monotone presence is what makes a presence weight sound at advancing epochs.
Presence has no inverse, so it can represent a collection only when every
membership, once true, stays true.

## Mutability inference

A collection's mutability is computed from its inputs, then fixed. The rules are
applied to the rule graph, and each SCC is solved to a fixpoint.

| construct                             | result mutability                             |
|---------------------------------------|-----------------------------------------------|
| join, union, map, filter, projection  | the most mutable input                        |
| `S, !F` (negation)                    | `mut(S)` if `F` is static, else **mutable**   |
| aggregate over `R`                    | `static` if `R` is static, else **mutable**   |
| recursive SCC                         | the most mutable among members and inputs     |
| `.fact`                               | static                                        |
| hybrid relation (`.input` and a head) | the more mutable of declared and derived      |

Positive constructs are monotone, so their mutability is the upper bound of their
inputs. This includes semijoins, whether written or introduced by the
planner. Negation is antitone in `F`, so it does not take the upper bound. A
filter that can grow removes rows that were already emitted, so the output
jumps straight to mutable; append is never a possible result. This holds even
when `S` is static:

| `S` \ `F`   | static  | append  | mutable |
|-------------|---------|---------|---------|
| **static**  | static  | mutable | mutable |
| **append**  | append  | mutable | mutable |
| **mutable** | mutable | mutable | mutable |

"`F` is static" includes a derived static relation. A relation derived only
from static inputs has every update at `t0`, and `leave` maps `(t0, i)` back
to `t0`. So a derived static relation satisfies the filter contract as well
as an EDB does. A mutable antijoin output still keeps presence arms when
neither input is mutable (see [Negation](#negation)). Only the decode is
signed.

Negation and aggregation are the only constructs that widen a mutability beyond
its inputs. Both make results that are not monotone in their inputs, so the
results need retractions. Mutable absorbs everything downstream: one antijoin
with an append filter makes every consumer mutable, and an SCC mutable if
the antijoin feeds that SCC. Inference therefore runs on planned collections,
not source rules, so that joins and antijoins introduced by the planner are
classified too. A collection's mutability is determined by its canonical form:
the form names the positive inputs `A` separately from the negated ones `N`.
The mutability is therefore the upper bound over `A` if every relation in `N` is
static, and mutable otherwise. So sharing by canonical form never merges two
mutabilities.

## Dedup: what it is for

With a signed weight, dedup is what makes a collection a set. With a presence
weight, membership is already correct without a dedup: presence is
idempotent at one time, and membership at later times is the OR over
announcements. So a presence dedup serves only three separate purposes, and
a planner can check each one on its own.

1. **Termination.** A loop's feedback must drop facts it has already
   announced. Otherwise every iteration announces them again and the loop
   never converges. **Required in every loop.**
2. **Exact multiplicity for consumers that count announcements.** These are:
   - `count`, `sum` and `avg`, which lift each announcement to one
     contribution;
   - every signed multiply, that is, a join with an `i32` side (see
     [Presence as `+1` inside the multiply](#presence-as-1-inside-the-multiply));
   - output change reporting.

   **Required before these consumers.** `min` and `max` are idempotent and
   need no dedup.
3. **Work.** Duplicate announcements do redundant work downstream. This is
   optional, and it trades trace memory for less join work.

Where each dedup is required:

| site                        | static                | append                                    | mutable                   |
|-----------------------------|-----------------------|-------------------------------------------|---------------------------|
| EDB input                   | consolidate (no trace) | first occurrence at `u32` **[E1]**       | `threshold_total`         |
| rule head (union of rules)  | consolidate           | first occurrence (purposes 2 and 3)       | `threshold_total`         |
| before `min` / `max`        | skip                  | skip **[E4]**                             | skip                      |
| before `count`/`sum`/`avg`  | required              | required **[E4]**                         | skip, `reduce` ignores multiplicity |
| loop feedback               | `threshold_semigroup` in a `Lex` loop | `threshold_semigroup` in a `Lex` loop **[E8]** | `threshold` |
| after `leave`               | none                  | none with a `Lex` loop **[E8]**; first occurrence at `u32` if a `Product` loop is used **[E2, E6, E7]** | none |
| presence side of a signed join | none (one announcement) | deduped at the outer clock, or a loop-local collection **[E7]** | not applicable |

Two rows need explanation.

### After `leave`

This subsection applies only to presence collections in `Product<u32, u16>`
loops. The design avoids that case with a
[lexicographic loop](#loop-time-for-presence-sccs). It is kept because it
explains why a partial clock breaks presence.

In a loop, `Product<u32, u16>` is a partial order. When a later epoch derives
a fact in fewer iterations, `first_occurrences` announces it again at an
incomparable time. It must: the fact is present at `(1, 1)`, and `(0, 4)`
does not cover that time. For example, reach `5` over `1->2->3->4->5` at
epoch 0 is announced at `(0, 4)`. Adding `1->5` at epoch 1 announces it again
at `(1, 1)` **[E2]**.

`leave` maps these times to epochs `0` and `1`. The membership is still
correct, but the fact is now announced twice. A downstream `count` counts it
twice, and a signed join reads it as `2`. The output reports an insert
that already happened. Signed recursion has no such problem: `threshold`
corrects the overlap inside the loop.

The fix is a first-occurrence dedup at the outer clock after `leave`. In the
randomized test, running without it announced 2984 facts a second time and
made 782 epoch snapshots of `count` wrong; with it both numbers were 0
**[E6]**.

This dedup costs a trace, measured at +9% time and +47% peak memory on
transitive closure. So insert it only when the leaving collection reaches a
purpose-2 consumer. A presence-only join followed by a head dedup already
absorbs the repeats. A join against an `i32` side does not; see the next
section.

### Presence as `+1` inside the multiply

A mixed join needs no lift operator and no second arrangement. The join
reads presence as `+1` inside its multiply: `impl Multiply<Append> for i32`
returns the signed side unchanged. The presence arrangement stays shared with
presence consumers. The price is a precondition, which this section states
and proves.

**Bilinearity.** Accumulate every update at times `<= t`. For a join, the
output's accumulation at `t` equals the left side's accumulation times the
right side's accumulation. This holds because `t1 v t2 <= t` exactly when
`t1 <= t` and `t2 <= t`. Read presence through `phi(Append) = 1`, and let
`n(d, t)` be the number of entries for datum `d` at or before `t`. The output
weight is then `c(t) * n(t)`, where `c(t) >= 0` is the signed side's
accumulated count.

This needs no exact `n`. Every signed consumer tests only positivity: the
`i32` threshold and an `i32` reduce, which reads distinct values. So it
would suffice that `n(t) >= 1` exactly when `d` is present.

**The catch: `phi` is not a homomorphism.** `Append + Append = Append`, but
`1 + 1 = 2`. The join reads `n` from a trace, and trace compaction may
consolidate two entries of the same datum into one. This happens once the
compaction frontier makes their times equal. Output emitted before the merge
used `n = 2`; output emitted after the merge uses `n = 1`. The two stop
cancelling.

Here is the case, reproduced deterministically in **[E7]**:

- `R(5)` is announced at epochs 0 and 1 (a `leave` without a dedup).
- `M(5)` is inserted at epoch 0, so the join emits `+1@0` and `+1@1`.
- By epoch 3, compaction has merged the two `R(5)` entries.
- Deleting `M(5)` then emits only `-1`, and `Q(5)` stays present forever.

**Lemma.** The multiply is exact if no consolidation ever merges two entries
of the same datum in the presence operand's trace. Either of these
conditions suffices:

1. **Total clock, deduped.** A first-occurrence dedup at a total clock
   leaves one entry per datum (`|A(d)| <= 1`), so there is nothing to merge.
   This covers EDB inputs, rule heads, and a leaving collection deduped at
   the outer clock.
2. **Inside the loop that produced the antichain, while the input is open.**
   The elements of an antichain `A(d)` have pairwise distinct inner
   coordinates, because a later outer time must have a smaller inner time.
   The scope's frontier contains `(e + 1, 0)` for the next open epoch `e + 1`.
   Advancing by a frontier that contains an element with inner coordinate
   `0` leaves every inner coordinate unchanged. So compaction can merge outer
   coordinates, never inner ones, and the antichain elements stay distinct.
   After the inputs close, no signed updates arrive, so later merges cannot
   unbalance anything.

Any other presence collection can carry repeats across outer epochs. That
includes a `leave` without a dedup, and an intermediate result inside a rule
body, such as `E join E` computed before `M` joins in. Such a collection must
be deduped before it meets a signed side, or the planner must order the join
so that the signed side is consumed first. In **[E7]**, each of these shapes
fails on 73–528 epoch snapshots without the dedup and on none with it. The
join inside the loop (condition 2) passes without a dedup.

The same lemma applies to the mixed antijoin in [Negation](#negation). Its
`+1` and `-1` arms must come from collections that meet condition 1 or 2. A
bare `S join F` over repeated announcements can emit fewer `-1`s than the
source has `+1`s.

## Where mutabilities meet

| boundary              | conversion                                                             |
|-----------------------|------------------------------------------------------------------------|
| static to append      | retype only: a complete set at `t0` is a valid monotone-presence history |
| static and mutable    | none: `impl Multiply<diff::Static> for diff::Mutable` joins the two arrangements directly **[E5]** |
| append to mutable     | none: presence reads as `+1` inside the multiply, on an operand that meets the [lemma](#presence-as-1-inside-the-multiply) **[E7]** |
| mutable to narrower   | never: inference guarantees no narrower collection consumes a mutable one |

`join_core` needs `Diff1: Multiply<Diff2>`. The orphan rule rejects
`impl Multiply<Present> for i32`, because both types are foreign. So each
presence mutability needs its own local weight type: `diff::Static` and
`diff::Append`, both in `flowlog_runtime::diff`. With these types,
one arrangement of an append or static relation can serve both presence and
signed consumers. Without them, every mixed join needs a lifted copy and a
second arrangement. The append type must not be used for a presence
collection that fails the lemma.

## Negation

`A(x, y) :- S(x, y), !F(x)` computes `+S - (S join F)`, then clamps
(`flowlog_antijoin`). Its presence output requires every filter key to arrive
at or before the source rows it blocks (`join.rs:110`).

| `F`      | `S`              | output                                                     |
|----------|------------------|------------------------------------------------------------|
| static   | static or append | presence: `F` is complete at `t0`, before any source time **[E3]** |
| append   | static or append | **mutable**: presence arms, `i32` decode **[E3]**          |
| mutable  | static or append | mutable: presence source as `+1`, and the filter arm through the multiply |
| any      | mutable          | mutable: today's `i32` path; a static filter joins through `Multiply` |

When the filter can grow, a new filter key blocks rows already emitted. For
example, adding `F(2)` at epoch 1 must retract `(2, b)` from epoch 0. The
presence decode cannot represent that retraction, so the snapshot keeps
`(2, b)`. **[E3]** shows the wrong snapshot.

The arms need no signed input. Mapping a first-occurrence stream to `+1` and
the join matches to `-1`, then running an `i32` dedup, gives an exact
retracting output: `(2, b)` gets `+1` at epoch 0 and `-1` at epoch 1. The
arrangements keep their presence weights. Only the concatenated difference is
signed. This needs a new `AntijoinOutput` pairing with presence inputs and
`i32` output. Today the input and output weights must be the same type.

## Aggregation

| input     | strategy                                                                         |
|-----------|----------------------------------------------------------------------------------|
| static    | today's batch semiring path: lift, `threshold_semigroup`, lower                  |
| append    | the same lift and threshold, then **supersede** to `i32`; output is mutable **[E4]** |
| mutable   | today's `reduce_abelian`                                                         |
| append, in a loop | `i32` for now: presence `ReduceStrategy` needs `TotalOrder`, and `Product<u32, u16>` is not one |

The presence reduce over advancing epochs emits each new answer but never
retracts the old one. So a count that grows `2 -> 3 -> 4` leaves all three
rows live **[E4]**. The raw answer stream is correct; only its reading as a
set is wrong.

*Supersede* turns that stream into a signed stream. It keeps the last answer
per key, and emits `(old, -1), (new, +1)` when a new answer arrives.

- **Contract:** a total clock, and at most one answer per key per time. The
  thresholded semiring stream satisfies both.
- **State:** one value per key.
- **Result:** in **[E4]** and **[E6]** it matched `reduce_abelian` on every
  snapshot.

The input still needs a first-occurrence dedup for `count`, `sum` and `avg`.
Without it, re-inserting `(1, 30)` counts it again, giving 5 instead of 4
**[E4]**.

Recursive aggregates over append input, such as SSSP with an in-loop `min`,
make the SCC mutable. A presence reduce over a partial clock is future work.
Timely implements `TotalOrder` for `Product` only when the outer time is
`Empty` (`timely/src/order.rs:158`). This is why batch loops at
`Product<(), u16>` can use `threshold_semigroup`, and incremental loops
cannot.

## Loop time for presence SCCs

Batch loops run at `Product<(), u16>`. Timely makes that type `TotalOrder`
because `()` is `Empty`. At `Ts = u32`, `iterative` gives
`Product<u32, u16>`, which is only a partial order. That rules out every
total-order operator: `threshold_semigroup`, the presence reduce, and
`flowlog_reduce_leave`. A new diff type cannot fix this, because the bound
is on the timestamp. `diff::Static` (PR #354) is still needed for the
signed multiply and dispatch, but it does not unlock these operators.

**Static and append SCCs therefore loop in a scope with a lexicographic
timestamp `Lex(epoch, iteration)`.** This is `scope.scoped::<Lex>`, and `Lex`
implements `Refines<u32>`, `Lattice` (max/min) and `TotalOrder`. Mutable SCCs
keep `Product<u32, u16>`.

**Soundness.** Under lexicographic order, iteration `i` of epoch `e`
accumulates every update of every earlier epoch. So each epoch's fixpoint
iteration resumes from the previous epoch's fixpoint. For a monotone
operator `f_e`, whose inputs only grow, this is correct:

- `lfp(f_(e-1))` is contained in `lfp(f_e)`, and it is a post-fixpoint of
  `f_e`.
- So the inflationary iteration that starts there converges to `lfp(f_e)`.

This is semi-naive evaluation continued across epochs. It is wrong under
deletions, where the classic failure is facts that support each other in a
cycle. That is why the order is reserved for presence SCCs, which inference
guarantees are monotone.

What it buys **[E8]**:

- **No antichains.** First-occurrence dedup in a total order announces each
  datum once. `leave` maps `(e, i)` to `e`, so a recursive output is also
  announced once. That removes the dedup after `leave`, and a `leave` output
  meets condition 1 of the multiply lemma directly. Repeat announcements over
  200 random seeds: 0.
- **The batch operators apply unchanged.** `threshold_semigroup` for dedup,
  and the presence reduce and `flowlog_reduce_leave` for in-loop aggregates.
  A static SCC with `min` compiles and runs as in batch mode.
- **Cost, on transitive closure at 8000 nodes:**
  - Static: 2.59 s and 307 MB, against 2.45 s and 489 MB with
    `first_occurrences`. Memory falls 37%; time stays within 6%.
  - Append updates: 3.65 s and 354 MB, against 6.39 s and 780 MB for
    `Product` plus the leave dedup, and 12.15 s and 1042 MB for mutable.

Static data still costs about 1.7× the time of `Ts = ()`, even with `Lex`.
Every update carries an 8-byte timestamp instead of 2 bytes. For batch speed
on the static part, run the static-only strata in a `Ts = ()` dataflow (a
*static prelude*). Then feed only the collections that non-static strata
consume into the incremental dataflow, as static inputs. All-static programs
keep `Ts = ()` outright; that is exactly today's batch mode.

## Outputs

- **Static relations** are reported once, after the first epoch.
- **Append relations** report only insertions, and a total-clock dedup makes
  each insertion appear exactly once.
- **Mutable relations** report signed changes, as today.

`.printsize` is the accumulated snapshot size under every mutability.

## Code impact

Decided so far:

- **Mode is a property of each relation.** No program-level execution mode
  is derived from the mutabilities. The goal is to delete that mode entirely:
  - every collection picks its weight from its own mutability;
  - the engine's shape is computed from the relations that need it.

  `--mode` and `Builder::mode` go when codegen stops reading the program
  mode.
- **Syntax.** An EDB's `.decl` ends in `static` or `mutable`, or names
  neither, which means static. Both words are reserved. `Relation` keeps
  the declaration. A derived relation that declares one is rejected: its
  mutability is inferred.
- **Inference.** The stratifier assigns mutability per stratum: each
  stratum maps the fingerprint of every relation it reads or produces to
  one `Mutability`. A relation it reads has its final value, because a rule
  reading a relation runs after every rule producing it. So planning a
  stratum needs only that stratum's map.
  - A relation whose rules span strata has a value in each. A partial
    result from static inputs stays static, even when the stratum that
    completes it is mutable. A later stratum folds in what earlier ones
    produced, so its value is never lower.
  - A recursive stratum is one SCC, and all its heads share one value.
  - A relation that is both an EDB and an IDB (an `.input` or inline facts,
    plus rules) starts from its declared mutability. The declaration covers
    only the input, meaning whether outside data can still be inserted or
    deleted after the first epoch. The relation also holds what its rules
    derive, so it takes the most mutable of the declaration and its rules.
    A `static` input whose rules read a mutable relation is therefore
    mutable.
  - Strata are processed in evaluation order, so every body relation
    already has a value: an EDB's declaration, or the most recent stratum
    that produced it.

  Nothing consumes the values yet; `--mode` still selects the weight and
  clock.
- **Weights.** `flowlog_runtime::diff` holds one type per mutability, always
  named with the module prefix:
  - `diff::Static` and `diff::Append` are presence;
  - `diff::Mutable = i32`.

  `txn::Diff` is an alias of `diff::Mutable`. The reduce strategies live in
  `reduce/presence.rs` and `reduce/mutable.rs`.

Still assuming one weight per program. Steps 2 and 3 must change these:

- **Generated code.** Codegen emits one global `type Diff` and
  `SEMIRING_ONE`, which feed:
  - every `Loader`, `InputSession` and `inline_facts` in
    `codegen/relation.rs`;
  - `new_collection` in `codegen/edb_handles.rs`;
  - batch preload;
  - the compiler's `dispatch.rs` (`load_put` and `load_file` take `Diff`).
- **Driver.** `Loader::load_flag` requires `D: Neg`, so a presence weight
  cannot pass through it; a static relation must not reach it. The library
  `IncrementalEngine` stages `(rows, i32)` for every EDB. A static EDB needs
  an insert-only, load-once API instead.
- **Operator choice keyed on the program mode.** These sites must key on the
  collection's mutability instead:
  - codegen: `flow/recursive.rs` (`flowlog_reduce_leave` and its profiler
    nodes) and `flow/non_recursive.rs` (the aggregate's profiler node);
  - profiler: `steps::dedup_recursive`, `steps::anti_join`,
    `steps::inspect_content` and `PlanGraph.mode`.
- **Engine shape, computed from the relations.** These branches follow the
  program mode today:
  - `Ts`;
  - the REPL or batch main, and the scaffold dependencies;
  - probes and the library engine choice.

  They stay program-wide in effect, but each is computed from the mutabilities
  instead: any input that can change after the first epoch needs `Ts = u32`
  and a transaction driver, and otherwise `Ts = ()` and a single run. That
  is the last reader of the program mode, and removing it removes the mode.
- **Output.** The emitter, the `Writer` trait, and the host, file, stdout and
  SQLite writers carry a signed *reported* change, `i32`, whatever the
  collection's weight. Batch already lifts presence to `1_i32` at the
  inspector. With per-collection weights, every mutability converts to this
  report type at the inspector, and it deserves its own name then. The
  public `IncrementalResults` exposes it.

Remaining work, by step:

- **Step 2 (codegen).** Per-collection weights replace the global `Diff`, as
  a pure refactor.
- **Step 3 (planner).** Planned collections take their mutability from
  `Stratum::mutability`.
- **Step 3 (compiler and library).** Remove `--mode` and `Builder::mode`, and
  derive the engine shape from the relations.
- **Step 3 (codegen).**
  - Conversions at mutability boundaries, and `Lex` scopes for static SCCs.
  - `Ts = ()` when every input is static.
  - Profiler prediction per mutability.
- **Step 3 (runtime).**
  - `Multiply` between `diff::Mutable` and `diff::Static`, in both
    directions.
  - The antijoin with presence input and `diff::Mutable` output.
  - The static dedup and reduce dispatch in `Lex` loops.
  - The driver closing static handles.
- **Step 4 (planner).** The leave-dedup decision by consumer.
- **Step 4 (runtime).** `diff::Append` dispatch, `Multiply` impls, and
  `supersede`.

## Evidence

Experiments live outside the repository. They link this checkout's
`flowlog-runtime` and drive the real `flowlog_dedup`, `flowlog_antijoin`,
`flowlog_join` and `flowlog_reduce`.

- **E1** Append input `{1, 2, 2} | {1} | {2, 3}`: announcements `1@0, 2@0,
  3@2`, one each.
- **E2** Reach over `1->2->3->4->5`, then `+1->5`:
  - In-loop dedup emits `5` at `(0, 4)` and `(1, 1)`.
  - After `leave` it is announced at epochs `0` and `1`, and a lift counts
    it twice.
  - A `u32` dedup after `leave` keeps only `0`.
  - The `i32` reference nets `+1@0`.
- **E3** `S = {(1, a), (2, b)} | {(1, c), (3, d)}`:
  - Static `F = {1}`: the presence antijoin gives `{(2, b), (3, d)}`, correct.
  - Append `F = {} | {2}`: the presence antijoin keeps `(2, b)`, wrong.
  - Presence arms with an `i32` decode give `{(1, a), (1, c), (3, d)}`,
    correct.
- **E4** `R(1, ·) = {20, 10} | {30} | {30, 5}`:
  - Presence count answers `2, 3, 4`, all three live as a set.
  - With supersede: `{(1, 4)}`, which equals `reduce_abelian`.
  - Without the input dedup the count reads `5`.
  - Presence `min` without a dedup answers `10, 5`, correct.
- **E5** Mutable `M` joined with a closed static `S` through
  `Multiply<Static> for i32`: correct snapshots across insert, delete and
  re-insert.
- **E6** 200 random seeds, 10 nodes, 8 epochs of 3 appended edges each.
  - Checked: TC, then `count` per source.
  - Both recipes matched a from-scratch oracle at every epoch, 200/200: the
    presence recipe (in-loop `first_occurrences`, leave dedup, supersede)
    and the `i32` one.
  - Without the leave dedup: 2984 repeat announcements and 782 wrong `count`
    snapshots.
- **E8** Lexicographic loop timestamp:
  - Append TC over 200 seeds: 200/200 snapshots match the oracle, with 0
    repeat announcements after `leave` and no leave dedup.
  - The presence `min` reduce compiles and runs inside the loop.
- **E7** Presence read as `+1` inside the join multiply, with no lift.
  - Setup: 200 seeds each at 10 nodes / 8 epochs and 40 nodes / 20 epochs.
    Append edges; mutable `M` and `F` with random insert and delete toggles.
  - Queries:
    - `M(x), TC(x, z)`;
    - `TC(x, z), !F(x)`;
    - `M(x), !T(x)` with `T(x) :- TC(x, _)`;
    - the same join inside TC's loop;
    - `M(x), E(x, y), E(y, z)`, joining `E` with `E` first.
  - With every presence operand deduped at the outer clock: 200/200 at both
    sizes.
  - Without the dedup after `leave`, or on the `E join E` intermediate, these
    queries fail:
    - the `M, TC` join and the `TC, !F` antijoin: 176–528 epoch snapshots;
    - `E join E`: 73–145.
  - The loop-local join passes without a dedup. `M, !T` passes too, but only
    by luck: its miscount only pushes the weight further negative.
  - Minimal repro: `R(5)` announced at epochs 0 and 1. The join emits
    `+1@0, +1@1`, then only `-1@3` after compaction, so `Q(5)` stays. With
    the leave dedup it emits `+1@0, -1@3`.

Performance. Transitive closure on a random graph, 8 workers on a 40-core
host, median of 3 runs. For the append rows, 90% of edges load at epoch 0 and
the rest arrive over 10 epochs.

| config (8000 nodes, 9600 edges) | time           | peak RSS |
|---------------------------------|----------------|----------|
| batch, `Present` at `()`        | 1.49 s         | 234 MB   |
| static-at-`u32`, `Present`      | 2.45 s         | 489 MB   |
| mutable-at-`u32`, all at epoch 0 | 3.56 s        | 743 MB   |
| append, 10 epochs (update time) | 6.39 s         | 781 MB   |
| append, no leave dedup          | 5.85 s         | 533 MB   |
| static, `Lex` loop               | 2.59 s         | 307 MB   |
| append, `Lex` loop, 10 epochs (update time) | 3.65 s | 354 MB |
| mutable, 10 epochs (update time) | 12.15 s       | 1042 MB  |

At 4000 nodes and 6000 edges the ratios hold:

- **Batch against static-at-`u32` against mutable-at-`u32`:** 1.11 s / 1.37 s
  / 2.27 s.
- **Append updates against mutable updates:** 6.05 s against 13.13 s, and
  1.08 GB against 1.22 GB.

The case for append is its roughly 2× faster updates at lower
memory.

## Plan

Each step is its own PR and keeps both endpoints byte-identical.

0. **Weight types.** Rebase PR #354 onto `main`. Name the weights
   `diff::Static`, `diff::Append` (defined, unused) and `diff::Mutable`, and
   rename the `i32` and `present` spellings that mean a weight.
1. **Syntax and per-stratum inference, with no codegen change.**
   - An EDB's `.decl` may say `static` / `mutable`, and a derived relation
     that does is rejected.
   - The stratifier assigns each IDB head a mutability per stratum, using
     the antijoin matrix, the aggregate rule, and one shared value per SCC.
   - Nothing consumes either, so behavior is unchanged and `--mode` stays.
2. **Per-collection weight types in codegen, as a pure refactor.**
   - Replace the global `type Diff` and `SEMIRING_ONE` with per-collection
     types chosen by mutability.
   - With uniform mutabilities, the generated code must be byte-identical to
     today's in both modes. Guard this with a fixture test that diffs the
     generated code.
3. **Static plus mutable.**
   - Planner: give planned collections the mutability of the relations
     they read, from the stratum being planned (`Stratum::mutability`).
   - Codegen reads mutabilities instead of the program mode. Drop `--mode` and
     `Builder::mode`, and compute the engine shape from the relations.
   - Runtime: `Multiply` between `diff::Mutable` and `diff::Static` in both
     directions, and `diff::Static` dispatch at `u32` (consolidate).
   - Codegen:
     - `Lex` scopes for static SCCs;
     - static inputs entering mutable joins through the multiply;
     - an antijoin implementation per matrix cell. The cell depends on the
       source's and the filter's mutabilities, not only the output's:

       | source \ filter | static | mutable |
       |---|---|---|
       | **static** | today's batch antijoin, both arms `diff::Static` | new: static source arms as `+1`, a `diff::Mutable` negative arm, `diff::Mutable` output |
       | **mutable** | today's `diff::Mutable` antijoin, with the static filter joined through `Multiply` | today's `diff::Mutable` antijoin |

       With two values, the output's mutability is the maximum of the two,
       but the static-over-mutable cell still needs its own operator;
     - a weight conversion where a relation's parts differ. A static input
       unioned with mutable rule output, or a static partial result from
       an earlier stratum entering a mutable one, is lifted to
       `diff::Mutable` before the union;
     - profiler predictions per mutability.
   - Driver: close static handles after the initial load.
   - No new dedup is needed. Every static collection lives at `t0`, so each
     datum has at most one entry.
   - Tests:
     - mixed fixtures, and an oracle that compares every epoch with a batch
       recompute over the accumulated inputs;
     - an ablation check: removing any required dedup must fail.
4. **Append.**
   - `diff::Append` dispatch and its `Multiply` impls.
   - `Lex` scopes for append SCCs, with `threshold_semigroup` everywhere.
   - The antijoin with presence arms and an `i32` decode.
   - Aggregates: the presence reduce, then supersede to mutable. In loops,
     `flowlog_reduce_leave`, then supersede.
   - The remaining multiply-lemma obligation: a presence intermediate inside
     a rule body that meets a signed side needs a dedup or a join order that
     consumes the signed side first.
   - Driver: reject negative diffs on append relations.
5. **Performance.** Build the static prelude (`Ts = ()` for the static-only
   strata) and measure it against `Lex`.

## Open questions

- Should append accept a delete of a fact it never saw, as a no-op, or reject
  every negative diff? The proposal above rejects every negative diff.
- Should the leave dedup be chosen per consumer, or always inserted and then
  optimized away? This trades planner complexity against 47% more memory on
  recursive append programs.
- Supersede exchanges answers by key, a second exchange after the reduce.
  Could it instead share the reduce's key partition?
- Aggregates over append input, outside loops, could stay presence when every
  consumer is itself only an output, avoiding the signed conversion. Is that
  worth a special case?
