# SQLite I/O in compiled programs

Use `IO="sqlite"` on an input or output directive:

```datalog
.decl Edge(src: int32, dst: int32)
.input Edge(IO="sqlite", filename="graph.sqlite")

.decl Reach(src: int32, dst: int32)
Reach(x, y) :- Edge(x, y).
Reach(x, z) :- Reach(x, y), Edge(y, z).
.output Reach(IO="sqlite", filename="results.sqlite")
```

The table name preserves the relation's spelling. The default filename is
`<Relation>.sqlite`. Input paths resolve against `-F`; output paths resolve
against `-D`. `-D -` selects existing stdout output instead of writing a database.
Parent directories within a filename must already exist.

The compiler enables the runtime's optional `sqlite` feature, which bundles
SQLite. Generated library engines continue to accept typed host input and
return typed results; directives do not make them access databases.

## Input and column types

Input selects columns by their declared attribute names, regardless of physical
column order. Extra columns are ignored. Ordinary tables are read concurrently
through separate worker connections, each scanning a disjoint rowid range.
Consecutive rowids give workers row counts differing by at most one, without
a preliminary counting scan. Gaps remain valid but can make loads uneven.
Views, WITHOUT ROWID tables, and tables shadowing all three rowid aliases fall back to worker zero. When the program uses `ord`,
worker zero traverses the rowid index sequentially for deterministic string
interning without sorting column values. Fallback sources use their query
traversal order, so their `ord` values can change if that order changes. Keep
input databases unchanged during loading: each worker and relation has its
own read snapshot.

| FlowLog value | SQLite representation |
|---|---|
| Signed and unsigned integers | INTEGER; values must fit both the declared type and signed 64-bit storage |
| `bool` | INTEGER, exactly 0 or 1 |
| `f32`, `f64` | REAL; INTEGER input is also accepted with floating-point conversion |
| `string` | TEXT, preserving whitespace, quotes, line breaks, and embedded NULs |
| Output tuple column | Separate scalar columns, e.g. `pair.0`, `pair.1`, `pair.1.0` |
| Nullary relation or tuple with no scalar leaves | `__flowlog_present` INTEGER, always 1 for each stored row |

NULL, BLOB, non-finite floats, and out-of-range integers are rejected. Numeric
text is not parsed into numbers. SQLite's column affinity may convert values
before FlowLog reads them. Input errors report the relation and database path
and terminate execution; missing databases are never created. A nullary input
asserts presence for each source row, ignoring its columns.

The parser's existing restriction on tuple-valued `.input` columns still
applies. Nested tuple output is supported, but cannot be loaded directly into
a tuple-valued compiler input declaration. SQLite identifiers are quoted;
column names beginning with `__flowlog_` are reserved for metadata.

## Batch output

Batch output replaces each declared output table with its current result,
including creating empty tables. Unrelated tables are preserved. Tables use
the declared column names and the types above, with NOT NULL constraints.
`order_by` and `limit` select and order emitted rows; SQL consumers must still
use `ORDER BY` in their queries to request a particular order.

## Incremental output

Incremental execution appends change history to the same database file; it
does not create a new database per epoch. Each row has two additional columns:

- `__flowlog_timestamp`: the dataflow's logical timestamp, starting at 0.
- `__flowlog_insert`: INTEGER 1 for insertion, 0 for retraction.

These are set-membership changes, not internal derivation counts. Adding a
second derivation of an existing tuple produces no insertion. Removing its
last derivation produces a retraction. An unchanged tuple produces no row.
The writer rejects weights other than +1 or -1 instead of discarding their
magnitude. Empty epochs produce no history rows or completion markers.

SQLite history preserves every change and ignores `order_by` and `limit`.
Filtering deltas could discard retractions required to reconstruct the result.
For example, the current `Reach` relation can be queried with:

```sql
SELECT src, dst
FROM Reach
GROUP BY src, dst
HAVING SUM(CASE __flowlog_insert WHEN 1 THEN 1 ELSE -1 END) > 0;
```

The first output emission of a new execution replaces the targeted tables,
so histories from runs with restarted timestamps do not mix. Until that first
emission, previous tables remain unchanged. The process is not resumable from
an existing history.

All output relations targeting the same resolved database path commit in one
transaction per epoch. Relative-path and symlink aliases are grouped together;
hard-link aliases are unsupported. Different databases commit independently.
A failed write rolls back that database's epoch and terminates the run. The
writer does not retry consumed dataflow output. Avoid modifying output schemas
or writing to output tables while the program is running.

SQLite input is read once at startup. In incremental mode, `file <relation>
<path> +1` reads that relation's table again and inserts its rows; `-1` retracts
them. It does not compute a database diff or watch external changes. Scalar
`put` commands remain available and use the existing text syntax.
