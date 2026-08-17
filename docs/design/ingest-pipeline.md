# Ingest Pipeline

How a fact gets from where it lives into a worker's `InputSession`. One
pipeline, four sources, and a name for each role so the placement of every
piece is a decision, not an accident.

## The four roles

Every fact passes through the same four roles, whatever its source:

| role | question it answers |
|------------|----------------------------------------------------|
| acquire | where does the raw material come from? |
| distribute | which worker applies this fact, exactly once? |
| decode | how does the source shape become the slot tuple? |
| apply | `Input::update(tuple, diff)` — always the same |

## The sources, and where each role runs

| source | acquire | distribute | decode |
|--------------|---------------------------|----------------------------------|----------------------|
| `.input` file | `TextReader::open(spec)` | share derived at `open` (byte range) | `accept` (`FromCell`) |
| `put` line | broadcast to every worker | share derived at `open` (ownership hash) | `accept` (`FromCell`) |
| host `Vec` | `insert_*` appends to one flat `Vec`, shared read-only (`Arc`) | share derived at `open` (index range) | `accept` (`FromCell`) |
| inline `.fact` | compiled into the program | worker 0 | constant-folded at compile time |

Every runtime source derives its share at `open`: a file by byte range, a
vec by index range, and a broadcast line by hashing its first column,
where the share is the whole line or nothing. Distribution is one
question, "does this worker have a share?", and `open` returning `Option`
is its one answer. The vec is shared read-only rather than split by
moves, because decode runs on the worker and only reads the source;
nothing is bucketed at insert.

Decode has no excuse to vary at all: it runs in `accept`, on the worker,
for every runtime source. Inline facts are the one exception, because a
constant needs no runtime decode.

## The contracts

```
InputSpec<'a, Src>   rel + source: &'a Src + peers + index; one spec type,
                     Src names what the reader opens
Reader<'src, T>      type Source; open(&InputSpec<Source>) -> Option<Self>;
                     next() -> the relation's slot tuples, ready to apply
  TextReader<T>      Source = Path   share = byte range    parses each line
  VecReader<U, T>    Source = [U]    share = index range   converts per position
  LineReader<T>      Source = str    share = the line, if column 0 hashes here
ParseRow / ParseCell how one text line becomes a slot tuple (text's own)
FromUser             how one host tuple becomes a slot tuple (vec's own)
Ingest / Loader      the handler's typed and erased faces; every entry point
                     pumps tuples straight into the relation's Session
```

`open` derives this worker's share from the same spec shape and answers
`None` when it has none, including the worker-0 collapse under `ord`.
Readers yield finished tuples: text owns its parse, host rows own their
per-position conversion, and nothing between a reader and a session
carries an untyped row.

## Consequences

- Per-relation code is identical in both modes: a `RelationSpec` static,
  one `relation!` call, one `Ingest` impl. Library mode stops owning a
  second handler shape.
- Library-mode decode moves off the host thread onto the workers, where
  the file path already runs it.
- Under `ord`, interning order must not vary with worker count. Every
  `open` applies the same collapse: return the whole share on worker 0
  and `None` elsewhere. Library mode's determinism today rests on the
  host thread converting serially at insert; moving decode onto workers
  requires this collapse or `ord` numbers change with worker count.
- The costs are paid where the file path already pays them: an uninterned
  string column is copied out of the row rather than moved, and integers
  ride through the cell union. Symmetry prices ingest like a file read.

## Out of scope

The output direction (drain, sort, convert, sink) is a separate, smaller
pipeline and already shared: `gen_drain_block` orders and truncates,
`IntoUser` converts slots back to host values, and the writers live with
their modes.
