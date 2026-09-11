# Ingest Pipeline

How a fact gets from where it lives into a worker's `InputSession`. One
pipeline, three sources, and a name for each role so the placement of every
piece is a decision, not an accident. Code: `flowlog-runtime/src/io/`.

## The four roles

| role       | question it answers                                  |
|------------|------------------------------------------------------|
| acquire    | where does the raw material come from?               |
| distribute | which worker applies this fact, exactly once?        |
| decode     | how does the source's record become the slot tuple?  |
| apply      | `InputSession::update(tuple, diff)`, always the same |

Distribution is load balancing, not correctness: a tuple applied on any
worker enters the same collection, and every join and dedup downstream
arranges with its own `exchange`. One thing observes where a fact was
decoded, string interning order under `ord`, and one rule handles it:
`Loader::new` fixes the worker's input partition before any reader opens
(see its doc).

## The sources

| source        | acquire                     | distribute (at `open`)     | decode        |
|---------------|-----------------------------|----------------------------|---------------|
| `.input` file | `Loader` opens the file     | byte range                 | `DecodeCell`  |
| `put`         | `PutReader`, every worker   | `ordinal % peers == index` | `DecodeCell`  |
| host rows     | `HostReader`, shared `&[U]` | index range                | `DecodeField` |
| `.fact`       | compiled into the program   | worker 0                   | constant      |

Every reader derives its share from worker coordinates without messaging
other workers. Host and put readers return `None` for an unassigned share;
a file reader with an empty byte range yields no rows. A put's owner is
selected by its position among the transaction's operations, which is the
same on every worker.

`Loader` validates text delimiters before preparing a source. It opens files
and reports file-open failures using the relation name and path. The file
reader receives the opened file and `keep_empty_lines`, derived from the
relation's arity; it does not receive the relation declaration.

## Where each piece lives

| file            | holds                                                          |
|-----------------|----------------------------------------------------------------|
| `relation.rs`   | `Relation` (`NAME`, `ARITY`, `Tuple`, `facts`) |
| `loader.rs`     | `Loader<R, T, D>`: worker settings, loading sources, and managing the session |
| `reader/put.rs` | The single-text-row reader |
| `reader.rs`     | `Reader<T>`: the shared row-reading contract, and the `ingest` loop |
| `reader/`       | Source-specific readers and their constructors |
| `decode.rs`     | `Decode<Src>`: the shared row-decoding contract |
| `decode/`       | Text and typed implementations, with `DecodeCell` and `DecodeField` |

Two facts the rustdoc cannot carry on any one item:

- Text-decode bounds sit on the entry points that read text (`load_file`,
  `load_put`), not on `Relation`. A host program's relation never proves it
  can parse a line. Every decoder is an impl that already exists in the
  runtime, selected by the pair of slot tuple and record type; no relation
  generates one, and a mispaired one does not compile. Only `Relation` and
  `Loader` are exposed from `io::input`; readers and decoding are internal.
- Readers yield finished tuples rather than records because a lending
  `Record<'_>` would need a generic associated type; decode running inside
  `next` is that constraint, not a preference.

## Errors

Two failures, split at `Reader::next`: a refused row (inner `Err`) is
reported once and the load continues; a cursor error (outer `Err`) stops
the load and is returned, because a partly read relation is
indistinguishable from a smaller one. The per-source policies (a missing
file is empty, a refused `put` fails the call) are on the `Loader` entry
points.

## Generated code

Both generators share the relation declarations and loader container:

```rust
pub struct RelEdge;
impl ::flowlog_runtime::io::input::Relation for RelEdge {
    const NAME: &'static str = "Edge";
    const ARITY: usize = 2;
    type Tuple = (i32, Spur);
    // fn facts() only when the program has .fact rows for Edge
}
// in the Inputs container:  pub in_edge: Loader<RelEdge, Ts, Diff>
```

Each worker constructs its loader once:

```rust
let mut loader = Loader::<RelEdge, Ts, Diff>::new(session, peers, index, uses_ord)?;
loader.load_file(path, b'\t', false, diff)?;
loader.load_put(text, ordinal, b'\t', diff)?;
loader.load_rows(&rows, diff)?;
```

`uses_ord` belongs to the program's execution settings, not the relation.
File delimiter and header handling belong to each load; there is no
file-options wrapper. Text puts take their delimiter directly too. Typed
host rows do not carry any text-format settings.

A compiled binary calls `load_file` at preload and `load_put` /
`load_flag` per transaction op, passing the op's index as `ordinal`.
A library-mode batch engine stages `insert_edge(rows)` into one flat
`Vec` and shares it with the workers in an `Arc` at `run()`. An incremental
engine retains each insert or remove call as `(rows, diff)` and shares
these batches at `commit()`. Workers call `load_rows` for each batch in
per-relation call order, with one weight for the whole batch. Retaining
the incoming vectors avoids copying rows into a weighted buffer, at the
cost of retaining a separate vector per staged call. The loader owns
partitioning and conversion in both execution modes.
