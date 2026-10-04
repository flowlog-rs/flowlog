# Mutability: one engine for batch and incremental

Before this work, a program was compiled for one mode. `--mode batch` gave every
collection `Diff = Present` at `Ts = ()`. `--mode inc` gave every collection
`Diff = i32` at `Ts = u32` (`flowlog-codegen/src/ty/`). This note
proposes a third option: each input relation declares how it may change, the
compiler infers a mutability for every derived collection, and each collection
uses the cheapest weight that is still correct for its mutability. Batch mode is the
case where every relation is static, and incremental mode the case where
every relation is mutable.

## Scope

The two endpoints are the old modes. An all-static program keeps the
dataflow `--mode batch` generated. A program whose inputs are all mutable
keeps the dataflow `--mode inc` generated for every collection that reads a
mutable input. A collection that reads none is static, as inference says,
and now costs presence instead of a signed count. The existing suites
verify both endpoints.

Everything this note adds lies between the endpoints, and only that part
needs new verification:

- **Append itself.** The set arrangement; first-occurrence dedup at the
  EDB inputs, rule heads and `LexLoop` loops; the antijoin with presence
  arms and a signed decode; and the signed aggregate.
- **Mutability boundaries.** The static-to-append retype, the static-mutable join
  through `Multiply`, and append against mutable through the same
  `Multiply`. The latter is exact because an append arrangement is a set by
  construction, which discharges the lemma in
  [Presence as `+1` inside the multiply](#presence-as-1-inside-the-multiply).
- **Static collections in a program that is not all static.** `diff::Static`
  at a `u32` clock and in `LexLoop` loops.

The efficiency target is set by what a user can do without mixing. A mixed
program is correct only when every input is declared mutable, so the middle
must beat an all-mutable run. It should also approach batch speed on its
static part.

The hard part is deduplication and the other set-semantics obligations. This
note states what each mutability guarantees, where a dedup is required and
why, and what happens at every point where two mutabilities meet. Claims marked
**[E*n*]** were checked with the `flowlog-runtime` operators on this commit;
see [Evidence](#evidence).

## The three mutabilities

| mutability  | input contract                                   | weight             | clock          |
|-------------|--------------------------------------------------|--------------------|----------------|
| **static**  | complete at the first epoch, then the handle closes | `diff::Static` | minimum time only |
| **append**  | insertions only, at any epoch                    | `diff::Append`     | `u32` epochs   |
| **mutable** | insertions and deletions                         | `diff::Mutable`    | `u32` epochs   |

The mutabilities are ordered `static < append < mutable`: each admits every
update history of the one below it. The driver enforces each contract
(`flowlog-runtime/src/txn.rs`). Every mutability keeps inserts idempotent, as
today.

- **Static:** any operation after the first commit is an error.
- **Append:** the shell refuses `delete` (`RuntimeError::AppendRelation`);
  a repeated insert changes nothing (first occurrence).
- **Mutable:** the input is a set. A row is present after an epoch whose
  insertions of it outnumber its deletions, absent after one where
  deletions outnumber insertions, and unchanged by one where they balance;
  one deletion removes a row however often it was inserted
  (`flowlog_input_dedup`, whose arrangement holds the membership).

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

## Sharing collections by canonical form

The planner shares collections by canonical form: `merge_equal` merges
collections with equal forms, and `cover_bodies` serves one collection from
another with the same body. Across strata, a stratum reuses the
*preludes* of the strata before it. A collection's mutability is not part of
either key. This section proves it does not need to be: collections with
equal forms, or with the same body, always have the same mutability, in any
two strata. Unlike the rows (see "Scope" in `canonical-form.md`), this holds
without any assumption that the two collections see the same database.

### Notation

- `M = {static < mutable}`, a total order; `max` over the empty set is
  `static`.
- A rule `r` has a head relation `h(r)`, positive body relations `P(r)`,
  and negated body relations `N(r)`. It *reads* `P(r)` and `N(r)`.
- `decl(X)` is the mutability an EDB `X` declares, `static` by default.
- `beta(p, n)` is `derived_mutability` (`flowlog-planner/src/stratifier/
  core.rs`) over the multisets `p` of positive and `n` of negated values:

  ```text
  beta(p, n) = mutable        if some value in n is mutable
             = max(p)         otherwise
  ```

  It depends only on the values in `p` and `n`.
- The strata are `S_1 .. S_s` in evaluation order, and `sigma(r)` is the
  index of the stratum holding rule `r`.
- `last(X)` is the largest `sigma(r)` over the rules `r` with `h(r) = X`,
  or `0` when no rule produces `X`.

### The assignment

`assign_mutabilities` computes, for `k = 1 .. s`, a map `L_k` from
relations to `M`, starting from `L_0 = decl` on the EDBs:

1. For each head `X` of `S_k`, `H_k(X)` is the least fixpoint of

   ```text
   H_k(X) = max( L_{k-1}(X) if defined, else static,
                 max over r in S_k with h(r) = X of
                   beta(v(P(r)), v(N(r))) )
   ```

   where `v(Y) = H_k(Y)` for a head `Y` of `S_k`, else `L_{k-1}(Y)`.
2. `L_k` is `L_{k-1}` with `H_k` written over it.
3. The stratum's map is `mu_k(X) = L_k(X)` for every relation `X` that
   `S_k` reads or produces. For a head this is `H_k(X)`.

`final(X) = L_s(X)`.

### Assumptions

These are facts about the code, and the proof uses each one.

- **(A1) Every producer is a dependency.** Rule `r` depends on every rule
  `r'` with `h(r')` in `P(r)` or `N(r)`: `DependencyGraph::from_rules`
  adds an edge to each rule of `head_to_rule_map[X]` for each body atom
  `X`.
- **(A2) Dependencies come first.** If `r` depends on `r'`, then
  `sigma(r') <= sigma(r)`, and `sigma(r') = sigma(r)` only when that
  stratum is recursive.
  - `merge_strata` emits a component only after every component it
    depends on (`has_pending_dependency`).
  - A component that depends on itself is recursive by definition
    (`compute_sccs`: several rules, or one rule depending on itself).
  - A non-recursive stratum joins components without pending dependencies,
    so its rules depend only on earlier strata.
- **(A3) A form names only what its rule reads.** For a collection
  materialized from rule `r`, `atoms(F)` is contained in `P(r)` and
  `negated(F)` in `N(r)`.
  - `Transformation::input` builds `CanonicalForm::relation(name)` only for
    an input the rule does not produce itself, which is a relation read
    directly.
  - `CanonicalForm::derive` builds a form's lists only from its inputs'
    lists:
    - a join concatenates both sides' `atoms` and `negated`;
    - an antijoin requires its filter side to read exactly one relation
      without negation, and moves that relation into `negated`;
    - relabeling and dropping duplicate reads reorder entries or remove
      repeats.

    So no step adds a relation name.
  - `canonical-form.md` states and mechanically corroborates this: "the
    relations a form names are exactly the source relations the plan graph
    reaches".
- **(A4) Collections are valued step by step.** `Transformation::input`
  and `Transformation::from_info` give each collection `C` of a rule in
  `S_k` its mutability `m(C)`, and nothing else sets it:
  - a relation `X` read directly gets `mu_k(X)` (`Stratum::mutability`);
  - an input the rule produced earlier keeps its producer's value;
  - a unary step's output gets its input's value;
  - a join's output gets `beta([m(left), m(right)], [])`;
  - an antijoin's output gets `beta([m(source)], [m(filter)])`, where
    the filter is the left input.

### Lemma 1 (producers precede readers)

If `S_k` reads `X`, then `last(X) <= k`.

*Proof.* Let `r` in `S_k` read `X`, and let `r'` be any rule with
`h(r') = X`. By (A1), `r` depends on `r'`. By (A2),
`sigma(r') <= sigma(r) = k`. QED

### Lemma 2 (a value stops changing after its last producer)

For `last(X) <= j <= j'`, `L_j(X) = L_{j'}(X)`.

*Proof.* Step 2 changes `L` at `X` only in a stratum whose heads include
`X`, and no such stratum comes after `last(X)`. QED

### Lemma 3 (a stratum reads final values)

If `S_k` reads `X`, then `mu_k(X) = final(X)`.

*Proof.* By step 3, `mu_k(X) = L_k(X)`. By Lemma 1, `last(X) <= k`.
By Lemma 2, `L_k(X) = L_s(X)`, which is `final(X)`. QED

This covers the relation a recursive stratum completes: `S_k` then both
reads and produces `X`, `last(X) = k`, and `mu_k(X) = H_k(X) = final(X)`.

### Lemma 4 (mutability is a function of the form)

For a collection `C` planned in any stratum `S_k`,

```text
m(C) = Phi(F(C)),   where   Phi(F) = beta(final(atoms(F)), final(negated(F)))
```

`Phi` is a device of the proof: the code never computes it.

*Proof.* Take the rule `r` in `S_k` that materializes `C`, and induct over
the order in which `r`'s steps are materialized, case by case on (A4).

- **A relation `X` read directly.** Its form is `CanonicalForm::relation`,
  with `atoms = [X]` and nothing negated, so `Phi(F) = final(X)`. By (A4)
  `m(C) = mu_k(X)`. The rule reads `X`, so by Lemma 3 `mu_k(X) = final(X)`.
- **An input produced earlier.** It carries its producer's form and value,
  and the induction hypothesis holds for the producer.
- **A unary step.** Its form has the input's `atoms` and `negated`, up to
  order and repeats (A3). `Phi` depends only on the set of values in
  each list, so `Phi` is the input's, which is `m(C)` by (A4).
- **A join.** Its lists are the concatenation of the two inputs' lists
  (A3). If either side negates a relation that is not static, both `beta`
  over the union and the larger of the two sides' values are `mutable`.
  Otherwise each side's `Phi` is its positive maximum, and the maximum
  over the union is the larger of the two. So
  `Phi(F) = max(Phi(F_left), Phi(F_right))`, which is
  `beta([m(left), m(right)], [])` by the induction hypothesis.
- **An antijoin.** Its `atoms` are the source's, and its `negated` are the
  source's plus the filter side's one relation `f` (A3). The filter side
  reads only `f`, so `Phi(F_filter) = final(f)`. If `final(f)` is not
  static, `Phi(F)` is `mutable`; otherwise `Phi(F)` is the source's `Phi`.
  So `Phi(F) = beta([Phi(F_source)], [final(f)])`, which is
  `beta([m(source)], [m(filter)])` by the induction hypothesis.

A prelude reused in a later stratum was materialized, and so valued, in the
stratum that planned it, and the same argument applies there. QED

The join and antijoin cases use only how `beta` is defined, so they hold
for any totally ordered set of mutabilities that keeps that definition.

### Theorem (sharing preserves mutability)

For any collections `C1` and `C2`, in any strata:

- if `F(C1) = F(C2)`, then `m(C1) = m(C2)`;
- if `F(C1)` and `F(C2)` have the same body (`CanonicalForm::same_body`),
  then `m(C1) = m(C2)`.

*Proof.* By Lemma 4, `m(Ci) = Phi(F(Ci))`. `Phi` reads only a form's
`atoms` and `negated`, and `final` is one map for the whole program. Equal
forms, and forms with the same body, agree on both lists. QED

### Consequences

- **Sharing needs no mutability in its key.** `merge_equal` keys on the
  form and the need to arrange, and `cover_bodies` requires the same body.
  By the theorem, neither can replace a collection with one of a different
  mutability, and adding mutability to either key would change nothing.
- **It holds where the rows need (P4).** `canonical-form.md` limits sharing
  *rows* to one stratum and the preludes of earlier ones. Its Dyck example
  arranges `dyck` in the stratum that completes it (a feedback variable,
  still growing) and again in a later stratum, with equal forms and
  different rows. Their mutabilities still agree: both equal
  `final(dyck)`, by Lemma 3.
- **A split relation cannot differ between strata.** Let `R` have a static
  partial result in `S_1` and be completed as mutable in a later
  recursive stratum. No form in `S_1` names `R`, because `S_1` does not
  read `R`: a rule reading `R` would depend on itself (A1) and be
  recursive (A2). The rule's own output there has a form over its inputs.
  Every form that names `R` is planned in a stratum that reads `R`, where
  `R` has its final value.
  `split_relation_collections_agree_across_strata` checks this on a plan.

### What would break it

Each of these changes falsifies one assumption, and so the theorem:

- A dependency graph that skipped some producer of a body relation (A1).
- A stratum order that let a reader run before a producer outside its SCC
  (A2).
- A form naming a relation its rule does not read, for example by
  inlining another relation's definition into it (A3).
- A collection whose mutability comes from anything but the propagation
  in (A4), such as a per-collection override (A4).
- A step whose propagation disagrees with how `derive` builds its lists:
  a new transformation kind, or a change to `beta` that makes a join's
  value differ from `beta` over the union of its inputs' lists (Lemma 4).

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
| EDB input                   | consolidate (no trace) | first occurrence at `u32` **[E1]**       | membership latch (`flowlog_input_dedup`) |
| rule head (union of rules)  | consolidate           | first occurrence (purposes 2 and 3)       | `threshold_total`         |
| before `min` / `max`        | skip                  | skip **[E4]**                             | skip                      |
| before `count`/`sum`/`avg`  | required              | required **[E4]**                         | skip, `reduce` ignores multiplicity |
| loop feedback               | `threshold_semigroup` in a `LexLoop` | `threshold_semigroup` in a `LexLoop` **[E8]** | `threshold` |
| after `leave`               | none                  | none with a `LexLoop` **[E8]**; first occurrence at `u32` if a `Product` loop is used **[E2, E6, E7]** | none |
| presence side of a signed join | none (one announcement) | none: the arrangement is a set (`flowlog_arrange`) **[E7]** | not applicable |

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
body, such as `E join E` computed before `M` joins in. In **[E7]**, each of
these shapes fails on 73 to 528 epoch snapshots when the join reads an
arrangement holding the repeats, and on none when it holds each datum once.

Rather than dedup such collections or order joins around them, the
arrangement itself keeps `|A(d)| <= 1`: `flowlog_arrange` and
`flowlog_arrange_self` arrange an append collection as a set. Before each
sealed chain of updates becomes a batch, a pair the trace already holds, or
an earlier update of the chain announces, is dropped (`SetRewrite for
diff::Append`, on the `flowlog_arrange_set` core the signed input's
membership latch also uses). The trace and the batch stream agree, so the
arrangement is the operator's only state, and condition 1 holds for every
arranged append collection wherever it sits. The dedups in the table above
remain for purposes 1 to 3; none is needed for the multiply.

The same holds for the mixed antijoin in [Negation](#negation): its `+1` and
`-1` arms read set arrangements, so a bare `S join F` emits exactly one `-1`
per blocked pair.

## Where mutabilities meet

| boundary              | conversion                                                             |
|-----------------------|------------------------------------------------------------------------|
| static to append      | retype only: a complete set at `t0` is a valid monotone-presence history |
| static and mutable    | none: `impl Multiply<diff::Static> for diff::Mutable` joins the two arrangements directly **[E5]** |
| static into a mutable union | lift: each static row becomes a count of one; the union's dedup clamps repeats |
| append into a mutable union | lift, as a static part                                                |
| append to mutable     | none: presence reads as `+1` inside the multiply, exact because the append arrangement is a set (`flowlog_arrange`) **[E7]** |
| mutable to narrower   | never: inference guarantees no narrower collection consumes a mutable one |

`join_core` needs `Diff1: Multiply<Diff2>`. The orphan rule rejects
`impl Multiply<Present> for i32`, because both types are foreign. So each
presence mutability needs its own local weight type: `diff::Static` and
`diff::Append`, both in `flowlog_runtime::diff`. With these types and the
set arrangement, one arrangement of an append relation serves presence and
signed consumers alike.

The alternatives were measured on five join shapes at 1 to 16 workers and
up to 2M nodes. A lifted copy arranged at `diff::Mutable` costs 13 to 26%
memory where a presence reader shares the arrangement; a dedup before the
join costs 22 to 26% on intermediates; pinning logical compaction changes
nothing, because differential's join accumulates a key's history before it
multiplies. The set arrangement costs no memory and roughly neutral time.

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
signed. `AntijoinOutputWeight<Rs>` in `join.rs` is the output column of
this table: a static filter keeps the source's weight, and a filter that
grows gives `diff::Mutable`. Each arm encodes by its own weight
(`AntijoinWeight`): a presence arm maps to `+1` or `-1`, a signed arm is
deduped first. The matched arm is the plain `flowlog_join` of the two
arrangements, carrying the `Multiply` product of their weights.

A presence arm is exactly one update per pair because its arrangement is a
set: a static one lives at its scope's minimum time, an append one is
arranged by `flowlog_arrange`. So neither presence arm needs a dedup, and a
filter key arriving after compaction meets each blocked pair once.

## Aggregation

| input     | strategy                                                                         |
|-----------|----------------------------------------------------------------------------------|
| static    | today's batch semiring path: lift, `threshold_semigroup`, lower                  |
| append    | `reduce_abelian` over append rows (`flowlog_reduce_append`); output is mutable **[E4]** |
| mutable   | today's `reduce_abelian`                                                         |
| append, in a loop | never: an aggregate over append rows is mutable by inference, and so is its loop |

The presence reduce over advancing epochs emits each new answer but never
retracts the old one. So a count that grows `2 -> 3 -> 4` leaves all three
rows live **[E4]**. The raw answer stream is correct; only its reading as a
set is wrong. A group that only grows still changes its answer, so the
answers are signed and each retracts the one before: `flowlog_reduce_append`
is the mutable strategy's `reduce_abelian` over append input, a copy for
now until append gets its own accumulation. The alternative, the presence
reduce followed by a *supersede* that keeps the last answer per key and
emits `(old, -1), (new, +1)`, matched `reduce_abelian` on every snapshot of
**[E4]** and **[E6]** and was measured equal, so it was dropped.

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
timestamp `LexLoop(epoch, iteration)`.** This is `scope.scoped::<LexLoop>`,
and `LexLoop` (`flowlog_runtime::time`) implements `Refines<u32>`, `Lattice`
(max/min) and `TotalOrder`; like timely's integer times, it is its own
path summary. Mutable SCCs keep `Product<u32, u16>`. In an engine whose
epochs do not advance (`Ts = ()`), a static SCC keeps `Product<(), u16>`,
which is already total.

Nothing here assumes the epochs are processed in order: each time's result
is a function of the inputs at times at or before it, so a transaction may
write to any epoch its input has not advanced past. A write below that
frontier, to an epoch already final, needs a partially ordered outer time
(event time and system time). That would rework every operator relying on
a total outer order, the signed ones included, not only `LexLoop`.

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

Static data still costs about 1.7 times the time of `Ts = ()`, even with `LexLoop`.
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
  is derived from the mutabilities:
  - every collection has the weight of its own mutability;
  - the engine's shape is computed from the relations that need it.

  `--mode`, `Builder::mode` and `Config::mode` are gone. What remains
  program-wide is the engine's shape, and `Program::is_incremental` computes
  it from the inputs: any append or mutable input means an incremental
  engine.
- **Syntax.** An EDB's `.decl` ends in `static`, `append` or `mutable`, or
  names none, which means static. The three words are reserved. `Relation`
  keeps the declaration. A derived relation that declares one is rejected:
  its mutability is inferred.
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
- **Weights.** `flowlog_runtime::diff` holds one type per mutability, always
  named with the module prefix:
  - `diff::Static` and `diff::Append` are presence;
  - `diff::Mutable = i32`.

  `txn::Diff` is an alias of `diff::Mutable`. `diff::Unit::one()` is the
  weight of one inserted row in each. The reduce strategies live in
  `reduce/presence.rs` and `reduce/mutable.rs`; `reduce/append.rs` holds
  `flowlog_reduce_append`, the signed reduce over append rows.
- **Generated code.** Only the inputs name a weight: each EDB's
  `new_collection`, `InputSession` and `Loader` carry its declared one.
  Every derived collection's weight follows by type inference from its
  operator's inputs, which is the propagation of
  [the assignment](#the-assignment) done by the compiler: a join's weight is
  the `Multiply` output of its sides, and an antijoin's is the product of
  its filter's and its source's. Codegen records each collection's
  mutability (`Codegen::global_fp_to_mutability`) for the few places that must name it:
  - a union at a relation's weight lifts the parts below it: a rule over
    static relations only, an input binding, or a partial result from an
    earlier stratum, each lifted to the union's weight by `flowlog_lift`;
  - every keyed collection is arranged once, through `flowlog_arrange` or
    `flowlog_arrange_self`, which arrange an append collection as a set;
  - an aggregated relation binds at the weight of its answers
    (`aggregate_mutability`), which for append rows is mutable;
  - a recursive stratum takes the value its heads share, which picks the
    loop's time (below). Every collection the loop body produces has that
    value: each reads a feedback variable, and the planner factors the rest
    out as the stratum's prelude. So in a mutable loop only collections
    entered from outside can be static: a prelude result, which meets the
    signed side through `Multiply`, or an earlier stratum's partial result
    for a head, which the loop's union lifts;
  - a static aggregated relation leaves its loop through
    `flowlog_reduce_leave`;
  - the reports and the profiler's predictions follow each relation's
    weight.
- **Times.** `flowlog_runtime::time` names them at two levels. The outer
  time is the engine's: `time::Once = ()` or `time::Epoch = u32`. A loop's
  time refines it with an iteration, ordered by the loop's mutability:

  | engine | static or append loop | mutable loop |
  |---|---|---|
  | `Once` | `OnceLoop = Product<(), u16>` | none |
  | `Epoch` | `LexLoop` | `EpochLoop = Product<u32, u16>` |

  Loop times never meet: a loop's results leave to the outer time before
  another scope reads them, and `leave` maps `(e, i)` to `e` under both
  orders. A static non-recursive collection of an `Epoch` engine lives at
  `Epoch`, not `Once`, until a static prelude gives it its own dataflow.
- **Engine shape, computed from the relations.** `Program::is_incremental`
  decides these, program-wide by nature: any input that can change after
  the first epoch needs `Ts = time::Epoch` and a transaction driver, and
  otherwise `Ts = time::Once` and a single run.
  - `Ts`;
  - the REPL or batch main, and the scaffold dependencies;
  - probes and the library engine choice.
- **Driver.** A static input loads once. The REPL loads its files and
  inline facts at the preload epoch, then closes every static input before
  the first advance: an open one would hold every static operator at time
  0. A `put` or `file` on a static relation is refused with
  `RuntimeError::StaticRelation`. The library `IncrementalEngine` offers a
  static relation only `insert_*` (`set_*` when nullary), staged before the
  first commit, which loads and closes it; a later call panics. An append
  input stays open like a mutable one (the `Inputs` container's `*_dynamic`
  methods drive both). Its `insert` loads with presence, and its `delete`
  is refused with `RuntimeError::AppendRelation`. The library engine offers
  an append relation `insert_*` (`set_*`) at any commit and no `remove_*`
  (`unset_*`).
- **Output.** The emitter, the `Writer` trait, and the host, file, stdout and
  SQLite writers carry a signed *reported* change, `i32`, whatever the
  collection's weight. A static or append relation's presence reports as
  one insertion, at the inspector. The public `IncrementalResults` exposes
  it.

The remaining work is listed under [Plan](#plan), step 5.

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
- **E4** `R(1, _) = {20, 10} | {30} | {30, 5}`:
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
    - the `M, TC` join and the `TC, !F` antijoin: 176 to 528 epoch snapshots;
    - `E join E`: 73 to 145.
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
| static, `LexLoop`                | 2.59 s         | 307 MB   |
| append, `LexLoop`, 10 epochs (update time) | 3.65 s | 354 MB |
| mutable, 10 epochs (update time) | 12.15 s       | 1042 MB  |

At 4000 nodes and 6000 edges the ratios hold:

- **Batch against static-at-`u32` against mutable-at-`u32`:** 1.11 s / 1.37 s
  / 2.27 s.
- **Append updates against mutable updates:** 6.05 s against 13.13 s, and
  1.08 GB against 1.22 GB.

The case for append is its roughly 2× faster updates at lower
memory.

## Plan

Each step is its own PR. Steps 0 to 4 are done.

0. **Weight types** (#354). Name the weights `diff::Static`, `diff::Append`
   (defined, unused) and `diff::Mutable`, and rename the `i32` and `present`
   spellings that mean a weight.
1. **Syntax and per-stratum inference** (#385). An EDB's `.decl` may say
   `static` / `mutable`, and a derived relation that does is rejected. The
   stratifier maps every relation each stratum reads or produces to a
   mutability, using the antijoin matrix, the aggregate rule, and one shared
   value per SCC.
2. **Collection mutability** (#387). Every planned collection carries a
   mutability, derived step by step from its inputs.
3. **Static plus mutable.**
   - `--mode`, `Builder::mode` and `Config::mode` are removed;
     `Program::is_incremental` decides the engine's shape.
   - Runtime: `Multiply` between `diff::Mutable` and `diff::Static` in both
     directions, the static-over-mutable antijoin below, and `LexLoop`.
   - Codegen: per-collection weights from each collection's mutability,
     replacing the shared `Diff` and `SEMIRING_ONE`; `LexLoop` scopes for static
     SCCs, with the loop times named in `flowlog_runtime::time`; lifts where
     a relation's parts differ; reports and profiler predictions per
     mutability.
   - The antijoin implementation per matrix cell. The cell depends on the
     source's and the filter's mutabilities, not only the output's:

     | source \ filter | static | mutable |
     |---|---|---|
     | **static** | today's static antijoin, both arms `diff::Static` | static source arm as `+1`, a `diff::Mutable` negative arm, `diff::Mutable` output |
     | **mutable** | today's `diff::Mutable` antijoin, with the static filter joined through `Multiply` | today's `diff::Mutable` antijoin |

     One generic `flowlog_antijoin` covers all four: the output weight is the
     filter's weight times the source's, and each arm encodes by its own.
   - Driver: static inputs load once and close before the first advance;
     commands on them are refused.
   - No new dedup is needed. Every static collection lives at its scope's
     minimum time, so each datum has at most one arranged entry.
   - Tests: runtime cells for the mixed join and antijoins and the
     `LexLoop`, and mixed fixtures (`tests/fixtures/mixed_*`).
4. **Append.**
   - `diff::Append` dispatch: `Multiply` with each of the other weights,
     the presence operators written once over a sealed `Presence` marker
     that Static and Append share, and `LexLoop` scopes for append SCCs.
   - The set arrangement: `flowlog_arrange` and `flowlog_arrange_self`,
     with `SetRewrite for diff::Append` on the `flowlog_arrange_set` core,
     so `Multiply<Mutable> for Append` is exact and no lift or second
     arrangement exists.
   - The antijoin's output weight by its filter's (`AntijoinOutputWeight`):
     signed under a filter that grows.
   - Aggregates: `flowlog_reduce_append`, the signed reduce over append
     rows. No in-loop case: inference makes a loop with an aggregate over
     append rows mutable.
   - Driver: the shell refuses `delete` on an append relation, and the
     library engine offers it no `remove_*`.
   - Tests: runtime tests for the set arrangement, each antijoin cell, the
     dedup and the reduce, and the `append_*` fixtures through both
     lowering paths. An oracle that compares every epoch with a batch
     recompute over the accumulated inputs, and an ablation check that
     removing any required dedup fails, are still to be built.
5. **Performance and one engine.** Build the static prelude (`Ts = ()` for
   the static-only strata) and measure it against `LexLoop`. A static dedup
   at `u32` could consolidate instead of keeping a trace.
   - With the prelude, an all-static program is the case whose incremental
     part is empty, so the two engine shapes merge: one binary that loads,
     reports, and enters the transaction loop only when some input is
     mutable, and one library engine whose `commit` exists only then.
   - Open: the report format of the merged engine. A batch run writes a
     snapshot per relation, an incremental one a signed change per epoch.

## Open questions

- Append refuses every `delete`, of a fact it never saw included: the shell
  cannot tell that case from a real retraction without the relation's
  contents, and a silent no-op would hide a caller's bug.
- Should the leave dedup be chosen per consumer, or always inserted and then
  optimized away? This trades planner complexity against 47% more memory on
  recursive append programs.
- Aggregates over append input, outside loops, could stay presence when every
  consumer is itself only an output, avoiding the signed conversion. Is that
  worth a special case?
