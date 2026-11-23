# `ord`

`ord(v)` returns an `int32` that names `v` among the values of its type. It
takes a symbol or a tuple of any shape, and needs `--str-intern`.

## Contract

- Within one run, two values of one type have the same ordinal exactly when
  they are equal. Ordinals start at 1.
- Values of different types may share an ordinal: a symbol and a tuple, or
  two tuple types. Souffle's records and symbols overlap the same way.
- An ordinal is deterministic only for a symbol loaded from input. With
  `ord` in the program, input is interned serially on worker 0, so every
  loaded symbol has the same ordinal at any worker count. A symbol a rule
  builds (`cat`, `substr`, `to_string`) and every tuple are numbered when a
  worker first asks for them, so with several workers their ordinals can
  change from run to run. Tuples cannot be loaded (a tuple-valued `.input`
  column is refused), so no tuple ordinal is fixed by input.

### A literal needs a width

A tuple's type includes the width of each field, so `(1,)` as `(int32)` and
as `(int64)` are numbered in two tables. Elsewhere a literal with no width
takes a default, `int32` or `float32`. In `ord`'s operand that default
could differ from the width the same tuple takes in a column, and `ord`
would number the two apart without a word. `ord` therefore refuses a
numeric literal that nothing gives a width:

```datalog
R(p, ord(p)) :- Seed(_), p = (1,).              // refused
R(p, ord(p)) :- Seed(_), p = (as(1, int64),).   // p: (a: int64)
```

A literal has a width when a cast states one or a typed term of its
expression fixes one, as `n + 1` does for a column `n`. A cast to a tuple
type sizes no field of the tuple under it; a field taken out of that tuple
has the width its type declares. A program whose tuples come from relations,
as DOOP's contexts do, holds no such literal.

## Implementation

Codegen lowers every `ord(v)` to `flowlog_runtime::intern::ord(v)`, which
numbers `v` through the `Ordinal` trait by its Rust type, so codegen need not
know which kind `v` is. Past `i32::MAX` a number wraps negative; it stays
distinct from every other number.

- **Symbol.** Its intern key, `Symbol::ordinal`, as before.
- **Tuple.** Its number in a table of its own Rust type, which numbers each
  distinct tuple densely from 1 in the order threads first ask. Under
  `--str-intern` a tuple's string leaves are already symbols, so the tuple
  value itself is the key; nothing is encoded.

### One table per tuple type

The alternative is one table for every tuple, keyed by type and value.
Separate tables keep each type's ordinals dense, which suits `% n`
buckets and keeps each type far from the `int32` limit, and two types never
contend for one map. Souffle also keeps one record table per arity.

A generic function cannot own a static per instantiation, so the tables
live in one map from `TypeId` to a leaked table. Each thread caches the
tables it has found, so workers do not all lock the same shard of that map
on every row.

### Not the symbol table

Tuples do not go into the string interner, even encoded as text. That table
is also the symbol table written to SQLite: under `--str-intern` output
appends every string interned since the load to `__SymbolTable`, so tuple
keys would show up there as symbols, and they would take numbers from the
symbols a rule builds.

### No way back

The table maps a tuple to its number and keeps no reverse map. FlowLog
already outputs tuple values (as text in CSV, as one column per leaf in
SQLite), so the engine exports no record table. A program that wants the
mapping outputs it as a relation, as DOOP's incremental-output addon does:

```datalog
.decl ctxToNum(?ctx:mainAnalysis.configuration.Context, ?strCtx:number)
.output ctxToNum
ctxToNum(?ctx, ?ctxNum) :- mainAnalysis.isContext(?ctx), ?ctxNum = ord(?ctx).
```

## Why tuples

DOOP's `--choice-bound` caps each variable's points-to set with a choice
domain whose bucket is `(ord(value) * ord(hctx)) % N`
([choice.md](aggregation/choice.md)). In the analyses where contexts are
records, such as 2objH (`HContext = [ value:Value ]`), `ord(hctx)` numbers a
record, which FlowLog writes as a tuple.

## Limits

- At most `u32::MAX` distinct tuples of one type; `ord` panics past that.
- `ord` of a tuple needs `--str-intern`, as `ord` of a symbol does, so one
  flag governs the builtin.
