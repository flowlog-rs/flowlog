# Dictionary I/O

Repeated strings can account for a substantial portion of the data stored
and transferred through FlowLog's SQLite I/O. Dictionary compression reduces
this overhead by storing each distinct string once in a persistent
dictionary and replacing its occurrences in the data with integer IDs. For
datasets with many repeated strings, this can reduce storage size and I/O
costs. This document describes the design and how it integrates with
FlowLog's string interning and SQLite I/O.

## The problem

Without dictionary compression, FlowLog converts string IDs back to full
strings when writing to SQLite and converts those strings to IDs again when
reading, adding conversion work and storing repeated copies of the same
text.

## The layout

Souffle's SQLite layout already separates strings from rows: `__SymbolTable`
numbers the strings, `_R` holds ids, and a view `R` joins them back. FlowLog
adopts that layout rather than defining one, so that the same database serves
Souffle, FlowLog, and any SQLite client that reads `R`. Column naming,
view shape, and the symbol table's schema follow `WriteStreamSQLite.h` in
Souffle; the differences are listed in `docs/sqlite.md`.

Using `__R` instead of `_R` would avoid collisions with user-defined relation
names, since FlowLog permits a leading `_` but not `__`; however, the initial
implementation uses `_R` for compatibility with Souffle.

## Sharing IDs between SQLite and FlowLog

A string has the same ID in SQLite and in FlowLog's in-memory string table,
so reading and writing an ID requires no translation between the two.

For example, if SQLite stores `5 -> "hello"`, FlowLog must also assign ID
`5` to `"hello"` when loading the dictionary.

This requires four rules:

- **Load in ID order:** IDs must start at 1 with no gaps, so loading strings
  into an empty string table assigns the same IDs as the database.
- **Load the dictionary first:** It loads before workers start, so string
  constants and other inputs cannot take IDs that belong to the dictionary.
- **Check output databases too:** An existing output dictionary must also
  be loaded, so new strings receive IDs after the existing ones.
- **Load at most one database's dictionary:** Different databases may use
  the same ID for different strings, so a run rejects multiple databases
  with existing dictionaries among its SQLite inputs and outputs.

The loader checks each assigned ID against the stored ID and stops with an
error if they differ.

An alternative is to keep separate IDs and translate them through a mapping
table, allowing gaps in database IDs and dictionaries from multiple
databases at the cost of extra lookups when reading and writing.

The current design shares IDs to avoid those lookups and supports CTADL's
use of one database per phase; a mapping could be added if multiple existing
dictionaries need to be supported.

## Persisting all interned strings

When writing output, FlowLog saves every string added to its in-memory
string table, including strings used only during computation that do not
appear in the results.

For example, if the results use IDs `1` and `3` but not `2`, omitting the
string at ID `2` would cause the next run to assign ID `2` to the string
stored at ID `3`, so the loader would reject the dictionary.

Keeping these unused strings takes extra space, but keeps IDs consecutive
and allows later runs to reuse the database with the same string IDs.

## Scope

Dictionary compression applies to SQLite I/O in programs compiled with
`--str-intern`:

- **Text I/O:** Files such as `.facts` still contain full strings, with no
  separate dictionary.
- **Library I/O:** Host applications still exchange typed values through
  the library API, which does not use the SQLite dictionary layout.
- **SQLite I/O without `--str-intern`:** Output stores full strings in
  ordinary tables, and input can read a dictionary-layout database through
  its views, which resolve IDs back to strings.

## Dictionary loading

The dictionary is currently loaded on one thread before computation starts,
so large dictionaries add startup time.

Parallel loading would need to preserve ID assignment order so that each
string receives the same ID as in the database.
