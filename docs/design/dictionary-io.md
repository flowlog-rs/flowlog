# Dictionary I/O

Why a `--str-intern` program stores strings in SQLite as symbol ids, and the
constraints that make that sound. User-facing behavior is in
`docs/sqlite.md`; code is `flowlog-runtime/src/intern/dictionary.rs` and the
SQLite reader and writer under `flowlog-runtime/src/io/`.

## The problem

With `--str-intern`, every string the program handles is a `Symbol`, the
position of its text in the process-wide interner. Writing a relation to
SQLite resolved each symbol to text and bound it; a consumer that keeps a
symbol table of its own (Souffle, and CTADL's tooling around it) then
re-hashed every text into that table and rewrote the rows with its ids.
Reading went the other way: the consumer joined its ids back to text, and
the program interned the text again. The same string was hashed three times
and written twice, and the rewrites dominated the end-to-end time of an
analysis whose dataflow took a few seconds.

## The layout

Souffle's SQLite layout already separates strings from rows: `__SymbolTable`
numbers the strings, `_R` holds ids, and a view `R` joins them back. FlowLog
adopts that layout rather than defining one, so that the same database serves
Souffle, FlowLog, and any SQLite client that reads `R`. Column naming,
view shape, and the symbol table's schema follow `WriteStreamSQLite.h` in
Souffle; the differences are listed in `docs/sqlite.md`.

## Identity, not mapping

The design rests on one invariant: **a loaded string's symbol ordinal is its
id in the table.** Then a relation's ids are read as symbols without a
lookup, and written from symbols without one.

The invariant is cheap to establish and cheap to check. The interner numbers
strings in storing order from 1, so loading the table in id order into an
empty interner gives each row its id exactly when the ids run from 1 without
gaps. The load checks `ordinal == id` on every row and stops at the first
mismatch, which also catches a table loaded after another string was
interned (a rule literal, a `.fact`, another input): the dictionary therefore
loads in `main`, before the workers start, and the generated program puts it
there.

The candidates are the run's SQLite inputs and outputs alike: a run that
extends a database must number its new strings after the ones the database
holds, so the output's table is loaded like an input's.

A mapping table (`id -> Symbol`) would lift the constraints: any ids, any
number of databases with a symbol table. It would also put a lookup on every
id read and a reverse lookup on every id written, and it would let a run's
output ids differ from its input ids, breaking the consumer's existing `_R`
tables that reference the old ones. CTADL, the first consumer, reads and
writes one database per phase; identity covers it. The one-database rule is
the place a mapping would be added if a consumer needs it.

## The whole table, not the referenced strings

Output appends every string interned since the loaded count, including
intermediate strings no output references. Writing only the referenced ones
would leave gaps in the ids, and the next run could not load the table under
the identity invariant. The cost is table size; the benefit is that a chain
of runs over one database keeps every id stable.

## What stays as it was

Text I/O is unchanged: a `.facts` file has no consumer for a side dictionary.
Host (library) I/O is unchanged: the host holds typed values. A program
without `--str-intern` has no symbols and keeps the plain layout; it can read
a Souffle-layout database through its views. The dictionary is loaded on one
thread; a table of millions of strings takes seconds, and splitting the load
would require preserving storing order.
