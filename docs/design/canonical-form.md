# Soundness of the canonical form

`flowlog-planner/src/planner/canonical.rs` gives every planned collection a `CanonicalForm`. A sharing pass will substitute one collection for another when their forms agree, so the property that matters is:

> **No false sharing.** If two collections have equal forms, they hold the same rows.

This note proves that. It does *not* prove the converse; two spellings of one query may still get two forms, which costs a sharing opportunity and nothing else.

## Notation

A **database** `D` maps each relation name to a finite set of tuples.

A **form** is `F = (A, N, E, Phi, k, v)`:

- `A = [A_0 .. A_{m-1}]`, the positive relation names it reads;
- `N = [N_0 .. N_{p-1}]`, the negated relation names;
- columns `i.j` (argument `j` of `A_i`) and `!i.j` (argument `j` of `N_i`);
- `E`, a set of classes, each a set of at least two columns;
- `Phi`, a set of comparisons over expressions in columns;
- `k`, `v`, sequences of expressions: the key and value columns.

A **valuation** `a` picks `a(i)` in `D(A_i)` for each positive atom, and `a(i.j)` is that tuple's argument `j`. Expressions evaluate under `a` in the obvious way.

Two well-formedness invariants hold of every form this module builds:

- **(W1)** every class and every filter mentions columns of at most one negated atom;
- **(W2)** `k` and `v` mention no negated column.

By (W1) the constraints split into `E_pos, Phi_pos` (no negated column) and, for each negated atom `n`, `E_n, Phi_n` (mentioning `!n` and possibly positive columns). The **denotation** of `F` over `D` is

```text
[[F]](D) = { (eval_a(k), eval_a(v))
           | a in product of D(A_i),
             a satisfies E_pos and Phi_pos,
             for every n: no t in D(N_n) has (a, t) satisfy E_n, Phi_n }
```

This is a set of key/value pairs: the rows of a collection.

## Assumptions

- **(P1) Positional inputs.** A reader's input layout column `p` is its
  producer's output column `p`, key columns first. Codegen relies on the
  same invariant when it indexes `KV` and `Jn` arguments.
- **(P2) Rows are a set.** Codegen dedups each EDB as it is read and each
  IDB once its rules' heads are unioned, before any aggregate sees it, so
  a multiplicity inside a rule is a count of derivations that the next
  dedup flattens. Row-set equality is therefore the right notion for
  substituting one collection for another.
- **(P3) Expressions are pure.** A function call denotes a deterministic
  function of its arguments.
- **(P4) One database.** Both collections are evaluated against the same
  `D`. See "Scope" below; this is the only assumption a caller can break.

## Lemma 1 (leaves)

`relation(R, n)` denotes `{ ((), t) | t in D(R) }`, the rows of `R`.
Immediate from the definition: one atom, no constraints, `v` the `n`
arguments in order.

## Lemma 2 (derive is faithful)

If the input forms denote their collections' rows, so does the derived
form. By cases on the transformation.

**Unary.** The output is `{ (eval(out.key, r), eval(out.value, r)) | r in
rows(L), r satisfies the predicates }`. By (P1), `column_exprs` maps each
layout column of `L` to the expression `F_L` holds at that position, and
`atom_expr` composes an output expression with that map. The predicates
become classes (a plain equality between columns) or filters. So the
derived `k`, `v` evaluate exactly as the output's expressions do on `r`,
under the same constraints.

**Join.** The output is the pairs of an `L` row and an `R` row whose keys
agree position by position, so the added constraint is exactly
`zip(F_L.k, F_R.k)`. The right form's atom indices are shifted past the
left's before the atom lists are concatenated, so no column reference
collides, and a valuation of the concatenation is precisely a pair of
valuations. Comparisons resolve as in the unary case.

**Antijoin.** The output is `{ r in rows(R) | no l in rows(L) with
l.key = r.key }`. `derive` requires `F_L` to be one atom whose filters are
all `column = constant` and whose output is key only, and reports an
internal error otherwise. Under that shape, `F_L` denotes the tuples of
one relation satisfying its classes and constant filters, so the
condition "no `l` with matching key" is exactly "no `t` in `D(N_n)`
satisfying `E_n, Phi_n`" once the filter side is relabeled under the
negated polarity and its key columns are put in classes with the matched
positive keys. (W1) holds because those constraints mention only `!n` and
positive columns; (W2) holds because `k`, `v` come from `R`.

## Lemma 3 (each normalization step preserves the denotation)

**Step 1, classes.** An equality between two plain columns is replaced by
a class containing both; a class asserts exactly that its columns are
equal under `a`. Any other equality becomes a filter, possibly with its
sides swapped, and `=` is symmetric.

**Step 2, duplicate reads.** See Lemma 4.

**Step 3, labeling.** Each candidate is a bijective renaming of atom
indices with the valuation renamed to match, so all candidates have the
same denotation; taking the smallest picks one of them.

**Step 4, representatives.** A column is replaced by another in the same
class. Every `a` in the denotation makes them equal, so every expression
evaluates the same. A filter whose two sides became syntactically equal
is an equality every `a` satisfies, so dropping it changes nothing; note
that only `=` is dropped, since `x < x` is false rather than vacuous.
Only positive columns are substituted, which is what preserves (W1):
rewriting a negated column to a positive representative would move a
filter out of `Phi_n` into `Phi_pos` and turn a constraint on the negated
atom into a constraint on the row.

**Step 5, aliases.** A computed expression `e` is replaced by a plain
positive column `c` only when the filter `c = e` is present and retained,
so every `a` in the denotation has `a(c) = eval_a(e)`. The defining
filter itself is not rewritten; rewriting it through its own alias would
turn it into `c = c` and step 4 would drop it, destroying the premise. A
constant is never an alias, so `x = 5` leaves other occurrences of `5`
alone, and the target is never a negated column, which no output may
read.

**Step 6, filter order.** `Phi` is a conjunction, so reordering it and
removing a repeated conjunct changes nothing.

## Lemma 4 (dropping a duplicate read)

Let atoms `i` and `i'` read the same relation `R`. Write `ref(i)` for the
arguments of `i` that some class, filter or output expression mentions,
and say `i` is **covered by** `i'` when every `j` in `ref(i)` has `i.j`
and `i'.j` in a common class. Let `F'` be `F` with atom `i'` deleted and
every `i'.j` rewritten to `i.j`.

**Positive case.** If `i'` is covered by `i`, or `i` is covered by `i'`,
then `[[F]] = [[F']]`.

*Proof.* Every constraint of `F'` is the image of one of `F` under
`i'.j -> i.j`.

`[[F']]` contains `[[F]]`: given `a` in `[[F]]`, define `a'` by deleting
`i'` and setting `a'(i) := a(i')` when `i` is covered by `i'`, and
`a'(i) := a(i)` otherwise. In the first case a constraint of `F` on `i.j`
has `j` in `ref(i)`, so `i.j` and `i'.j` share a class and `a(i.j) =
a(i'.j) = a'(i.j)`; a constraint on `i'.j` maps to `i.j` with the same
value. In the second case the roles swap. Either way every image
constraint holds, and the outputs agree argument by argument.

`[[F]]` contains `[[F']]`: given `a'` in `[[F']]`, set `a(i) := a'(i)`
and `a(i') := a'(i)`. This is a valuation because `A_i = A_{i'} = R`, so
`a'(i)` is in `D(R)`. Each constraint of `F` is the preimage of one that
holds, and arguments outside `ref(i')` constrain nothing. The outputs
agree. QED

The condition is necessary as well as sufficient in practice: a filter
that mentions an argument of one read and not the other blocks it, which
is what keeps `A(x, y), A(x, z), y < z` from collapsing into the
unsatisfiable `y < y`.

**Negated case.** Merging two negated atoms replaces `not exists t: C`
and `not exists t: C'` by `not exists t: C and C'`. If `C'` implies `C`
then the conjunction of the two negations is just `not exists t: C`,
while the merged form says `not exists t: C'`, which is weaker. So the
merge is sound only when each of `C`, `C'` implies the other, that is
when the two reads cover each other in both directions. That is why
`!N(x, _)` and `!N(x, 5)` both stay.

## Theorem

For every collection `C` of a planned stratum, `rows(C) = [[F(C)]](D)`.

*Proof.* Induction over the pipeline. The base case is Lemma 1; the step
is Lemma 2 followed by Lemma 3 and Lemma 4, which leave the denotation
unchanged. QED

**Corollary (no false sharing).** If `F(C1) = F(C2)` and (P4) holds, then
`rows(C1) = rows(C2)`. Equal values have equal denotations, and by the
theorem each denotation is the collection's rows.

## Scope: the one assumption a caller can break

(P4) says both collections see the same database. A form names a
relation, not the moment it is read, and a relation does not hold the
same rows throughout a run: a recursive relation read inside its fixpoint
holds what has been derived so far, and a relation whose rules span two
strata holds more after the second than after the first. The Dyck program
shows this concretely. With

```text
Dyck(x, y) :- Zero(x, z), Zero(z, y).          // stratum 1
Dyck(x, y) :- Zero(x, z), Dyck(z, w), Zero(w, y).  // stratum 2
Tail(x, y) :- Dyck(x, z), Dyck(z, y).          // stratum 3
```

stratum 2 and stratum 3 both arrange `dyck` by its first column, and the
two collections carry the same form *and the same fingerprint*, yet
stratum 2's is the feedback variable inside the fixpoint and holds only
what the fixpoint has derived so far.

So forms are comparable within one stratum, where a stratum's own
relations are read only by its recursive part, and between a stratum and
the *preludes* of the strata before it. A prelude is safe to share because
the stratifier places a rule after every rule writing a relation it reads:
a prelude reads only relations that are complete, and nothing writes them
again, so its rows are the same for every later stratum. A recursive
collection, like stratum 2's arrangement above, never serves a later
stratum; only preludes do. Dedup therefore runs over the earlier preludes
and the stratum's own transformations together, with the preludes fixed.

## What this does not claim

Completeness. One query can still reach two forms, when a comparison
between two columns is written in either order, or when an equality folds
into a class along one plan and stays a filter along another. Each such
gap costs a sharing opportunity and can never cause a wrong match.

## Mechanical corroboration

Beyond the proof, the invariant "the relations a form names are exactly
the source relations the plan graph reaches" was checked over the 123
fixture programs and the 20 benchmark programs under `example/`,
including `doop.dl`: 146 programs, 1405 collections, no violation. The
plan graph is walked through input fingerprints, which the form
derivation never consults, so a step that dropped or invented a relation
would show up there.
