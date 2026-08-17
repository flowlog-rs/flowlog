# The drain pipeline

How a derived fact becomes bytes in a file or a tuple in the host's `Vec`.

The read side's counterpart is [`ingest-pipeline.md`](ingest-pipeline.md). The
two are duals in their conversion layer and deliberately *not* duals in their
distribution layer; the reason is in [Fan-out and funnel-in](#fan-out-and-funnel-in).

## Six stages

A row crosses six stages between the operator that derives it and the byte or
tuple a consumer sees. Only stages 4 and 5 change; the rest are described so
the change can be read against them.

```
  1 inspect     .inspect() clones the datum into a thread-local Vec
  2 flush       one lock per worker per epoch: local Vec -> shared Vec<Vec<_>>
  3 barrier     every worker has flushed
  --------------------------------------------------------------- gather
  4 order       concatenate | k-way merge | top-k          (slot values only)
  5 convert     slot tuple -> bytes, or -> host tuple      <- THE CHANGE
  6 persist     create/write/flush, or hand the Vec back
```

### 1. Inspect

Each `.output` relation gets a differential `.inspect()` whose closure pushes
`(tuple, Ts, Diff)` into a thread-local `Rc<RefCell<Vec<_>>>`. The hot path
never touches a mutex. `.consolidate()` is applied only in incremental mode, so
in batch mode a datum emitted across several differential batches fires the
inspect more than once and lands as duplicate buffer rows.

The stored diff is the literal `1_i32` whenever `config.is_batch()`, with the
real diff bound to `_diff` and discarded. Nothing observable reads it in batch
mode: no batch file output carries a diff column and library batch drops it.
The one exception is the stderr sink, which prints `diff={:+}` unconditionally.

### 2. Flush

One mutex acquisition per worker per epoch:
`shared.lock().push(mem::take(&mut *local.borrow_mut()))`. The shared buffer is
`Arc<Mutex<Vec<Vec<(tuple, Ts, Diff)>>>>` — outer entry per flush, inner Vec that
worker's rows.

The outer Vec's order is whichever worker won the lock race. With more than one
worker and no `ORDER BY`, output row order is therefore not reproducible across
runs. That is today's behaviour and this work does not change it.

### 3. Barrier

Binary mode: all workers flush, `barrier.wait()`, then `if index == 0` runs the
merge section — still inside the timely closure. Library batch: flush, barrier,
and the drain runs on the host thread after `timely::execute` returns.

### 4. Order

Three shapes, dispatched on the sink's `(order_by(), limit())`:

| case | shape | peak memory |
|---|---|---|
| no `ORDER BY` | concatenate in outer-Vec order | streams |
| `ORDER BY` | sort each worker's Vec, then k-way merge | whole relation |
| `ORDER BY` + `LIMIT` | flatten all workers, then top-k | whole relation, one allocation |

`(None, Some(n))` is unreachable — the parser rejects `limit` without `order_by`.

Two properties this stage guarantees, and that stage 5 depends on:

- **Ordering is strictly pre-format.** Nothing sorts on output text. Comparators
  read raw slot values and compare with `Ord`; the only transformation is
  `resolve_out` on interned-string leaves, so comparison is on the string's
  bytes rather than on the intern ID.
- **Rows arrive owned.** `k_way_merge`'s sink is `FnMut(T)` and `topk` returns
  `Vec<T>`. Today's generated code immediately re-borrows (`let row = &val;`)
  only because the current formatter needs `&`.

### 5. Convert — what this work replaces

Today this stage has four independent implementations of the same recursion
over a relation's columns, three of them emitted as per-relation `quote!`:

| implementation | where | shape |
|---|---|---|
| file formatter | `gen_value_bytes` / `gen_row_bytes` | codegen |
| stderr formatter | `stderr_accessor` | codegen |
| comparator accessor | `field_accessor` | codegen |
| host conversion | `IntoUser` | runtime trait |

Only the last is a trait. The first three re-derive from `DataType` what the
Rust slot type already says.

### 6. Persist

Text: `File::create`, `BufWriter` at 1 MiB, `write_all` per row or per wave, an
explicit error-checked `flush` (relying on `BufWriter::Drop` would swallow a
failed tail write). No temp-and-rename — `write_atomic` exists in the runtime
but only the profiler uses it.

Library: the `Vec` is the result; `finish` hands it back and there is nothing
to flush.

On the hot path, stages 5 and 6 are **fused**: the bounded-streaming property
is "only one wave of formatted bytes resident at a time", so a wave must be
written before the next is formatted. Separating them would materialize the
whole relation's bytes before writing any. The fusion is confined to
`TextWriter::write_parallel`; both paths still reach `Encode`.

## Fan-out and funnel-in

The read side distributes: every worker opens its own share of the source —
a byte range, an index range, a hash ownership test — and `Reader::open`
returning `None` means "no share for this worker". Reading is parallel *in the
I/O*.

The write side gathers: rows were already distributed upstream by the dataflow,
each worker pushed into its own slot, and one worker writes. There is no share
left to derive, so `Writer::open` acquires a sink but never answers "not
mine" the way `Reader::open` does, and the write side's parallelism (rayon
waves inside worker 0) is in the *formatting*, not the I/O.

`uses_ord` collapsing reads to worker 0 is a determinism escape hatch on the
read side, not evidence that the two sides share a mechanism.

What *does* carry over is the layering. `Reader` is a cursor and the loop lives
outside it, in the `ingest` pump; that is what makes file, vec, and put uniform.
The drain keeps the same split.

## The runtime surface

Four pieces, in `flowlog-runtime/src/io/`.

### `encode/` — convert one record

```rust
/// Append one record to a destination.
///
/// The dual of `Decode`: `Src` is where a record comes from, `Dst` is where
/// it goes, and neither is the produced value. Consumes `self`, because the
/// drain owns its rows and a `String` slot should move rather than clone.
pub trait Encode<Dst: ?Sized> {
    fn encode(self, dst: &mut Dst);
}
```

Mirroring `decode/` file for file:

| | `decode/` | `encode/` |
|---|---|---|
| `text.rs` | `Line` + `DecodeCell` + `Decode<Line>` | `TextRows` + `EncodeCell` + `Encode<TextRows>` |
| `typed.rs` | `DecodeField<F>` + `Decode<(F0..)>` | `EncodeField<U>` + `Encode<Vec<(U0..)>>` |

`IntoUser` becomes `EncodeField`, which is what finally makes the two
directions share a vocabulary.

`Encode` is infallible where `Decode` is not: a well-typed slot has no form it
cannot take, and the failure that does exist belongs to the file, not the record.

**Two traits on the text side, and the reason is fixture-pinned.** A relation
row and a `FixedTuple` column lower to the same Rust type and must emit
different bytes:

| slot type | as a row | as a column |
|---|---|---|
| `(Spur, Spur)` | `p⇥q` (`tuple_pair/Back.csv`) | `(p, q)` (`tuple_pair/Out.csv`) |

`EncodeCell` and `Encode<TextRows>` disambiguate by trait, so there is no
coherence conflict — but **never** add a blanket
`impl<T: EncodeCell> Encode<TextRows> for T`: it would silently pick the cell
form for an arity-1 row and turn `p` into `(p,)` with no compile error.

`TextRows` owns its buffers (`Vec<u8>` + `itoa::Buffer` + the delimiter) rather
than borrowing them: borrowed fields cost 9–15% on the hot path, and owning
them is what removes `::itoa` from generated crates entirely. The delimiter is
one `u8`: the parser resolves `delimiter=` to a single ASCII byte, so a file
written here reads back through FlowLog's own reader.

### `writer/` — accept rows and persist them

Role for role with `reader/`, though the filenames differ: a reader is
named for where facts come from, a writer for what it emits:

| `reader/` | `writer/` |
|---|---|
| `mod.rs` — `Reader` trait | `mod.rs` — `Writer` trait |
| `file.rs` — `FileReader` | `text.rs` — `TextWriter` + `TextRows` |
| `host.rs` — `HostReader` | `vec.rs` — `VecWriter` |
| `put.rs` — `PutReader` | — no put sink — |

```rust
/// Accepts drained rows and persists them: the write-side counterpart of
/// `Reader`, pushed into rather than pulled from.
pub trait Writer<T>: Sized {
    /// What the caller gets back. `()` for a file, the rows for a host Vec.
    type Out;

    /// Acquire the sink named by the spec.
    fn open(spec: &OutputSpec<'_>) -> Result<Self, RuntimeError>;

    /// Take one row. `diff` is `None` in batch mode.
    fn push(&mut self, row: T, diff: Option<Diff>);

    /// Flush and hand over. Fallible because a dropped buffer would swallow
    /// a failed tail write.
    fn finish(self) -> Result<Self::Out, RuntimeError>;
}
```

- `TextWriter` owns a `BufWriter<File>` and a `TextRows` scratch buffer.
  `open` creates the file, `push` formats through `Encode<TextRows>` and spills,
  `finish` flushes with the error checked. `Out = ()`.
- `VecWriter<U>` owns `Vec<U>` and a parallel `Vec<i32>`. `push` converts
  through `Encode<Vec<U>>`, `finish` returns the rows, zipping the diffs into
  `Vec<(U, i32)>` in incremental mode. `Out` is that.

The diff stays out of `Encode` and lives here, which is what lets one `Encode`
serve both a trailing text column and a `Vec<(U, i32)>` pairing.

Two places this is a weaker mirror than `reader/`, both stated rather than
papered over:

- **`open` returns `Result<Self>`, not `Result<Option<Self>>`.** `None` on the
  read side means "no share for this worker"; the write side has no share to
  derive (see [Fan-out and funnel-in](#fan-out-and-funnel-in)). The worker-0
  gate stays a single `if index == 0` around the whole merge section in
  generated code — putting it in a per-relation `open` would make every worker
  run the loop just to be turned away.
- **`VecWriter::open` cannot fail**, where `TextWriter::open` creates a file.
  It returns `Result` anyway, so a caller handles both the same way — the same
  bargain `Decode` already makes for the typed source.

### `drain.rs` — the pump, mirroring `ingest`

```rust
pub fn drain_flat  <T, W: Writer<T>>(per_worker: Vec<Vec<Row<T>>>, w: W) -> Result<W::Out, RuntimeError>;
pub fn drain_sorted<T, W: Writer<T>>(per_worker: Vec<Vec<Row<T>>>, cmp: impl Fn(&Row<T>, &Row<T>) -> Ordering, w: W) -> Result<W::Out, RuntimeError>;
pub fn drain_topk  <T, W: Writer<T>>(per_worker: Vec<Vec<Row<T>>>, n: usize, cmp: impl Fn(&Row<T>, &Row<T>) -> Ordering, w: W) -> Result<W::Out, RuntimeError>;
```

The three stage-4 shapes, moved out of codegen. Each takes the writer by value
and calls `finish`, so no caller can forget the flush. Rows are handed to `push`
by value, which `k_way_merge` and `topk` already support.

### The hot path

`TextWriter` carries one inherent method the trait does not:

```rust
impl TextWriter {
    /// Bounded-streaming parallel drain: text file, arity > 0, no ORDER BY.
    pub fn write_parallel<T: Encode<TextRows> + Send>(
        self,
        per_worker: Vec<Vec<Row<T>>>,
        incremental: bool,
    ) -> Result<(), RuntimeError>;
}
```

It gives each rayon lane its own `TextRows` and its own **owned** segment, then
writes waves in worker-then-row order. Same shape as the read side —
independent accumulators, no shared state — with lanes standing in for workers.
Each lane formats through the same `Encode<TextRows>` the serial `push` uses;
only the feeding differs.

It is inherent rather than a trait method because it is an optimisation of one
sink, not a capability every sink has: a `Vec` writer has no wave to stream and
nothing to gain from formatting in parallel.

One real change it forces: the current drain borrows
`&per_worker[wi][start..end]` across lanes, so segments must become **owned**.
That removes the shared borrow rather than adding one.

### `spec.rs` — both directions

`InputSpec` and `OutputSpec` sit together, as the two things generated code
says about a relation's I/O:

```rust
pub struct OutputSpec<'a> {
    /// The `.decl` spelling, for error messages.
    pub relation: &'a str,
    pub path: &'a str,
    /// Cell separator: one ASCII byte, matching the read side.
    pub delim: u8,
}
```

`ORDER BY` and `LIMIT` are not here — they select which pump the caller uses,
and their comparator stays generated.

One real change it forces: the current drain borrows `&per_worker[wi][start..end]`
across lanes, so segments must become **owned**. That removes the shared borrow
rather than adding one.

## Data flow after the change

```
  worker N  --inspect-->  local Rc<RefCell<Vec>>
                                |
                             flush (1 lock/worker/epoch)
                                v
                    Arc<Mutex<Vec<Vec<Row<T>>>>>
                                |
                            barrier.wait()
                                |
                     worker 0, or the host thread
                                |
              +-----------------+------------------+
              |                                    |
     hot path: no ORDER BY,               general path
     arity > 0, text file
              |                                    |
   TextWriter::write_parallel          drain_flat / _sorted / _topk
              |                                    |
     rayon lanes, one                     +--------+--------+
     TextRows each                        |                 |
              |                      TextWriter        VecWriter<U>
      Encode<TextRows>                    |                 |
              |                    Writer::push      Writer::push
     waves written in order               |                 |
              |                   Encode<TextRows>   Encode<Vec<U>>
        finish -> flush                   |                 |
              |                    finish -> flush    finish -> zip
            file                          |                 |
                                        file        Vec<U> | Vec<(U, i32)>
                                                            |
                                                         the host
```

Every path ends at `Encode`; every path but the hot one reaches it through
`Writer::push`.

## What generated code becomes

The parallel file drain, in full:

```rust
{
    let out_path = format!("{}/{}_t{}{}", "out", "OutS", time_stamp, ".csv");
    let spec = ::flowlog_runtime::io::OutputSpec {
        relation: "OutS", path: &out_path, delim: b"\t", incremental: true,
    };
    let per_worker = std::mem::take(&mut *buf_OutS.lock().unwrap());
    if let Err(e) = ::flowlog_runtime::io::TextWriter::open(&spec)
        .and_then(|w| w.write_parallel(per_worker, true))
    {
        eprintln!("[flowlog] fatal: {e}");
        std::process::exit(1);
    }
}
```

`gen_drain_block` collapses from three `quote!` arms to one call plus a
comparator closure.

**What stays generated, and should:** the `ORDER BY` comparators
(`order_comparators` + `field_accessor`). "Compare column 3 descending,
resolving interned strings" needs per-column typed access into a heterogeneous
tuple; making that generic means reintroducing the indexed-accessor indirection
that `Cell`/`parse_accessor` was deleted for. That is the floor.

## What this deletes

`flowlog-compiler/src/io/output.rs` (530 → ~230 lines):

| lines | what |
|---|---|
| `:28`, `:30-35` | `OUTPUT_BUFFER_BYTES`, `PARALLEL_DRAIN_SEG_ROWS` — move to `writer/text.rs` |
| `:187-199` | `File::create` + `RuntimeError::Output` + `BufWriter` — becomes `io::create` |
| `:236-243` | the "so the two paths cannot drift" banner — drift becomes structurally impossible |
| `:265-281` | `gen_file_row_writer`'s `itoa_decl` / scratch plumbing |
| `:339-435` | `gen_parallel_file_drain` |
| `:437-497` | `gen_value_bytes`, and with it the `_ =>` integer catch-all hazard |
| `:498-530` | `gen_row_bytes` |

`flowlog-build/src/codegen/features.rs`: the `itoa` and `parallel_output` flags
and their `bool_features!` entries. `idb_buffers.rs:94-110`: the whole marking
block. `flowlog-compiler/src/scaffold.rs:167-176`: both conditional dependency
insertions.

Net: roughly 300 lines of per-relation `quote!` replaced by ~180 lines of
concrete runtime impls that are unit-testable for the first time, plus the
removal of an entire conditional-dependency mechanism that existed only to
serve them.

## Commit sequence

Each step is green on its own.

0. **Pin the bytes nothing pins yet** — a `delimiter="||"` fixture (multi-byte,
   protects against narrowing `delim` to `u8`) and a row whose string column
   contains the delimiter and a `)` (documents "raw, never escaped"). Green on
   today's code.
1. **`io/encode/{mod,text,typed}.rs`** — nothing wired; adds `itoa` to the
   runtime. Ships with byte-contract unit tests this format has never had: the
   four `Floats.csv` shapes, both `Ints.csv` extremes, `(p, (q,))`, `(p,)`,
   `alpha⇥1⇥+1`, `True`, and `String` vs `Spur` producing identical bytes.
2. **`io/spec.rs`: add `OutputSpec`**, and **`io/writer/{mod,text,vec}.rs`** —
   the trait and both impls, with a test that a writer's `finish` is the only
   thing that can report a flush failure.
3. **`io/drain.rs`** — the three pumps, with a test that each feeds a writer in
   the same order the current codegen does. Then
   `TextWriter::write_parallel`, which adds `rayon`: unit test that parallel
   output is byte-identical to a serial loop over the same multi-worker
   buffers, and that zero rows still creates the file.
4. **Route the parallel file drain** through `write_parallel`. Must land
   together with narrowing `mark_string_resolve_out`, or a file-mode program
   with string columns and no `ORDER BY` emits an unused import and fails
   `-Dwarnings`.
5. **Route the sequential sinks** through `Writer` + `drain_*`, in both modes.
6. **Delete the two feature chains.**
7. **`flowlog-runtime` 0.3.0 → 0.4.0** and the `scaffold.rs` dependency pin.

Fixtures to watch across 4–6: `output_all_types`, `output_all_types_intern`,
`tuple_{hetero,nested,pair,placeholder,str_intern}`, `delimiter_tab`,
`output_multi_worker`, `ord_multi_worker`, `rule_nullary`, `neg_nullary`,
`nullary_delta`, `recursive_nullary_delta`, `output_order_by`, `output_limit`,
`tuple_order_by`, and all of `datalog-inc/`.

## Deliberately out of scope

- **The stderr sink** stays in codegen. `-D -` has zero fixture coverage, and
  `stderr_accessor` already contains a quirk a type-directed encoder would not
  reproduce: it emits `( … )` for a one-field tuple column, so `{:?}` prints
  `"p"` where the slot type `(Spur,)` would print `("p",)`. Deleting an unpinned
  format is an unobservable behaviour change dressed as a refactor. Land a
  `-D -` fixture first; then it is a small follow-up as a third `Writer`.
  When it does move, `Writer::push` needs the timestamp — stderr is the only
  consumer of `row.1`.
- **The sqlite write path**, parked on `wip/sqlite-io-full`. It re-enters as a
  fourth `Writer` impl, `writer/sqlite.rs`, alongside a `Encode<Statement>`.
  The layout is already the seam it needs.

## Bugs surfaced while mapping, not fixed here

- Library **incremental** mode never calls `gen_drain_block`, so `order_by=`
  and `limit=` on an `.output` are silently ignored in that mode. Binary
  incremental honours them.
- The no-`ORDER BY` drain holds the shared mutex for the entire write loop, so
  formatting and I/O happen under the lock. Safe today only because each merge
  thread owns a distinct buffer.
- `topk` uses `select_nth_unstable_by`, so which of several `ORDER BY`-equal
  rows survive a `LIMIT` is arbitrary and not reproducible.
