//! Binary-mode output sink + drain codegen.
//!
//! For each IDB that is `.output`'d or `.printsize`'d, emits the post-barrier
//! code that runs on worker 0:
//!
//! - **`.output`**: drain the shared buffer, then write each row to a file
//!   (default) or stderr (`-D -`). File sinks without `ORDER BY` take the
//!   bounded-streaming parallel drain ([`gen_parallel_file_drain`]): worker
//!   buffers are resolved + formatted across cores via `rayon` and streamed to
//!   disk in worker order, byte-identical to the sequential path. The rest go
//!   through [`flowlog_build::gen_drain_block`] (which also applies `ORDER BY` /
//!   `LIMIT` and is shared with library mode via `::flowlog_runtime::sort::*`).
//! - **`.printsize`**: read the shared size cell and report it to stderr.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use flowlog_build::parser::{DataType, Relation};
use flowlog_build::{field_accessor, gen_drain_block};

use crate::{Compiler, CompilerError};

/// Capacity of the per-IDB output `BufWriter`. Large enough to amortize
/// write syscalls across the millions of small rows DOOP-scale outputs
/// produce. A handful may be live at once (relation drains run concurrently
/// in file mode), but at 1 MiB each that is negligible next to the data.
const OUTPUT_BUFFER_BYTES: usize = 1 << 20;

/// Rows formatted per parallel-drain segment. Bounds peak in-flight formatted
/// memory to roughly `rayon_threads × SEG_ROWS × row_width` per relation —
/// the drain streams segment-by-segment instead of materializing the whole
/// output, so a multi-GB relation costs only ~tens–hundreds of MiB of transient
/// buffers. 8 K rows is the knee of the speed/memory curve (near-peak format
/// throughput, ~half the footprint of larger segments). It is a *memory* bound,
/// not a parallelism knob: rayon owns the thread count and the segments still
/// follow the existing `-w N` worker partitioning.
const PARALLEL_DRAIN_SEG_ROWS: usize = 8192;

impl Compiler {
    /// The merge section spliced into `main()` after the barrier (worker 0
    /// only): empty-output touches (for `.output` relations the dataflow
    /// pruner dropped), derived `.output` drains, then `.printsize` reports.
    ///
    /// File mode runs the blocks in concurrent scoped threads — each block
    /// writes a distinct file, so distinct inode locks let the kernel
    /// genuinely parallelize the writes (≈3-4× aggregate write bandwidth on
    /// multi-relation outputs). The per-relation drain is itself bounded-
    /// streaming, so concurrent drains stay memory-safe. Stderr mode (`-D -`)
    /// stays strictly sequential: every block shares the one stderr stream and
    /// interleaved output would be garbage.
    pub(crate) fn gen_merge_section(&self) -> Result<TokenStream, CompilerError> {
        let mut blocks = Vec::new();
        if !self.config.output_to_stdout() {
            for file_name in self.program.empty_output_files() {
                blocks.push(self.gen_empty_output_touch(file_name)?);
            }
        }
        for idb in self.program.output_idbs() {
            blocks.push(self.gen_output_drain(idb)?);
        }
        for idb in self.program.printsize_idbs() {
            blocks.push(self.gen_size_report(idb)?);
        }

        if blocks.is_empty() || self.config.output_to_stdout() {
            return Ok(quote! { #(#blocks)* });
        }
        Ok(quote! {
            std::thread::scope(|merge_scope| {
                #( merge_scope.spawn(|| #blocks); )*
            });
        })
    }

    /// Resolve `-D <outdir>` or return the canonical "unset" error
    /// using `context` to disambiguate. Centralised because every
    /// file-emitting block needs the same lookup with a slightly
    /// different message.
    fn require_output_dir(&self, context: &'static str) -> Result<&str, CompilerError> {
        self.config.output_dir().ok_or_else(|| {
            CompilerError::internal(format!("binary mode {context} but `output_dir` is unset"))
        })
    }

    /// Touch `<outdir>/<file_name>` as an empty file. Used for
    /// `.output` relations the dataflow pruner dropped (no rules and
    /// no facts) — Soufflé writes the file anyway so downstream
    /// readers iterating declared outputs don't choke on a missing
    /// path. `file_name` already encodes any user-supplied `filename=`
    /// override; see [`flowlog_build::Program::empty_output_files`].
    fn gen_empty_output_touch(&self, file_name: &str) -> Result<TokenStream, CompilerError> {
        let base_dir = self.require_output_dir("touching empty `.output` file")?;
        Ok(quote! {{
            let out_path = format!("{}/{}", #base_dir, #file_name);
            std::fs::File::create(&out_path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e));
        }})
    }

    /// Drain one `.output` relation's shared buffer through its sink.
    fn gen_output_drain(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let buf_ident = Ident::new(&format!("buf_{}", idb.name()), Span::call_site());
        let string_intern = self.codegen.features().string_intern();
        let is_incremental = self.config.is_incremental();

        // File sinks without ORDER BY take the bounded-streaming parallel drain
        // — same bytes, same row order, resolve+format spread across cores and
        // streamed to disk. Nullary stays sequential (literal "True" lines), as
        // do ORDER BY/LIMIT (global sort) and stderr (one shared stream).
        if idb.uses_parallel_file_drain(self.config.output_to_stdout()) {
            let base_dir = self.require_output_dir("writing IDB output to files")?;
            let out_path_stmt =
                gen_out_path_stmt(&idb.output_file_name(), base_dir, is_incremental);
            return Ok(gen_parallel_file_drain(
                &buf_ident,
                idb,
                out_path_stmt,
                string_intern,
                is_incremental,
            ));
        }

        // Stderr is unbuffered, so only the file sink needs the explicit final
        // flush; `BufWriter::Drop` would swallow a failed tail write.
        let (sink_preamble, write_row, sink_postamble) = if self.config.output_to_stdout() {
            (
                gen_stderr_preamble(),
                gen_write_row_stderr(idb, string_intern),
                quote! {},
            )
        } else {
            let base_dir = self.require_output_dir("writing IDB output to files")?;
            (
                gen_file_preamble(&idb.output_file_name(), base_dir, is_incremental),
                gen_write_row_file(idb, string_intern, is_incremental),
                quote! { out.flush().expect("flush failed"); },
            )
        };

        Ok(gen_drain_block(
            &buf_ident,
            idb,
            sink_preamble,
            write_row,
            sink_postamble,
            string_intern,
        ))
    }

    /// Read one `.printsize` cell and either write the count to a file
    /// (`<outdir>/<RawName>.csv`, single line, Soufflé-shaped) or, when
    /// `-D -` is set, `eprintln!` the `(time, size)` pair to stderr.
    ///
    /// Mutual exclusion with `.output R` on the same relation is
    /// enforced at parse time, so we never race against an output
    /// drain over the same path.
    fn gen_size_report(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let cell = Ident::new(&format!("size_{}", idb.name()), Span::call_site());
        if self.config.output_to_stdout() {
            let prefix = idb.raw_name().to_string();
            return Ok(quote! {{
                let (t, size) = &*#cell.lock().unwrap();
                eprintln!("[size][{}] t={:?} size={}", #prefix, t, size);
            }});
        }

        let base_dir = self.require_output_dir("writing `.printsize` to a file")?;
        let file_name = idb.output_file_name();
        Ok(quote! {{
            use std::io::Write as _;
            let (_, size) = *#cell.lock().unwrap();
            let out_path = format!("{}/{}", #base_dir, #file_name);
            let mut out = std::io::BufWriter::new(
                std::fs::File::create(&out_path)
                    .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e)),
            );
            writeln!(out, "{}", size).expect("printsize write failed");
            out.flush().expect("printsize flush failed");
        }})
    }
}

// =========================================================================
// Sink preambles — emit `let mut out = ...;` + `use std::io::Write as _;`.
// The drain block's `write_row` closes over `out`.
// =========================================================================

fn gen_file_preamble(file_name: &str, base_dir: &str, is_incremental: bool) -> TokenStream {
    let out_path = gen_out_path_stmt(file_name, base_dir, is_incremental);
    quote! {
        use std::io::Write as _;
        #out_path
        let mut out = std::io::BufWriter::with_capacity(
            #OUTPUT_BUFFER_BYTES,
            std::fs::File::create(&out_path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e)),
        );
    }
}

/// Bind `let out_path = ...;` for a file sink. `file_name` is the full
/// filename (including any extension); by default `<RawName>.csv` per
/// Soufflé, overridable via the `.output Foo(filename="…")` parameter.
/// Incremental mode inserts the epoch immediately before the file extension
/// (or at the end if no extension) so each epoch gets its own file.
fn gen_out_path_stmt(file_name: &str, base_dir: &str, is_incremental: bool) -> TokenStream {
    if is_incremental {
        let (stem, ext) = split_file_extension(file_name);
        quote! {
            let out_path = format!("{}/{}_t{}{}", #base_dir, #stem, time_stamp, #ext);
        }
    } else {
        quote! { let out_path = format!("{}/{}", #base_dir, #file_name); }
    }
}

/// Split `name.ext` into `("name", ".ext")`. No dot → `(name, "")`.
/// Used by incremental-mode preambles to inject `_t<ts>` before the
/// extension instead of after it.
fn split_file_extension(file_name: &str) -> (&str, &str) {
    match file_name.rfind('.') {
        Some(idx) if idx > 0 => (&file_name[..idx], &file_name[idx..]),
        _ => (file_name, ""),
    }
}

fn gen_stderr_preamble() -> TokenStream {
    quote! {
        use std::io::Write as _;
        let mut out = std::io::stderr();
    }
}

// =========================================================================
// Row formatters — emit the per-row writes against the sink.
//
// File rows write column bytes directly (`write_all`) rather than through
// `writeln!`/`core::fmt`: string columns are the bulk of DOOP-scale output
// and their UTF-8 bytes need no formatting, so we skip the per-row format
// machinery entirely. Numeric/bool columns still format through `write!`.
//
// `gen_write_row_file` (sequential sink) and `gen_row_bytes` (parallel sink)
// MUST stay byte-identical — any change to column text, delimiter, or the
// incremental `{:+}` diff must mirror across both; both are pinned by the
// `output_all_types*` fixtures.
// =========================================================================

fn gen_write_row_file(idb: &Relation, string_intern: bool, is_incremental: bool) -> TokenStream {
    // Nullary: output is a boolean presence marker, one literal `True` line.
    if idb.arity() == 0 {
        return quote! {
            let _ = &row;
            out.write_all(b"True\n").expect("write failed");
        };
    }

    let delim = idb.output_delimiter();
    let delim_lit = Literal::byte_string(delim.as_bytes());

    let mut stmts: Vec<TokenStream> = Vec::new();
    for (i, dt) in idb.data_type().iter().enumerate() {
        if i > 0 {
            stmts.push(quote! { out.write_all(#delim_lit).expect("write failed"); });
        }
        let accessor = field_accessor(i, dt, quote! { row }, string_intern);
        stmts.push(col_write_stmt(dt, accessor));
    }

    if is_incremental {
        // Incremental lines carry the diff as a signed suffix column.
        stmts.push(quote! { out.write_all(#delim_lit).expect("write failed"); });
        stmts.push(quote! { write!(out, "{:+}", row.2).expect("write failed"); });
    }

    stmts.push(quote! { out.write_all(b"\n").expect("write failed"); });
    quote! { #(#stmts)* }
}

/// One column's byte write. String columns emit their UTF-8 bytes directly;
/// other scalar types format through `write!`.
fn col_write_stmt(dt: &DataType, accessor: TokenStream) -> TokenStream {
    if matches!(dt, DataType::String) {
        quote! { out.write_all(#accessor.as_bytes()).expect("write failed"); }
    } else {
        quote! { write!(out, "{}", #accessor).expect("write failed"); }
    }
}

fn gen_write_row_stderr(idb: &Relation, string_intern: bool) -> TokenStream {
    let prefix = idb.raw_name().to_string();
    if idb.arity() == 0 {
        return quote! {
            writeln!(out, "[tuple][{}]  t={:?}  True  diff={:+}",
                #prefix, row.1, row.2)
                .expect("write failed");
        };
    }

    let fields = data_field_accessors(idb, string_intern);
    // Stderr format shows `Debug` representations for readability; files get
    // `Display` for machine-consumable output.
    let fmt_cols = vec!["{:?}"; idb.arity()].join(", ");
    let fmt = Literal::string(&format!(
        "[tuple][{prefix}]  t={{:?}}  data=({fmt_cols})  diff={{:+}}"
    ));
    quote! {
        writeln!(out, #fmt, row.1 #(, #fields )*, row.2).expect("write failed");
    }
}

/// Token streams that read `row.0.<i>` for each data column, wrapping
/// interned-string columns in `resolve_out()` so they format as `&str`.
fn data_field_accessors(idb: &Relation, string_intern: bool) -> Vec<TokenStream> {
    idb.data_type()
        .iter()
        .enumerate()
        .map(|(i, dt)| field_accessor(i, dt, quote! { row }, string_intern))
        .collect()
}

// =========================================================================
// Bounded-streaming parallel file drain
// =========================================================================
//
// For binary-mode `.output` relations with no `ORDER BY` and arity > 0, the
// shared buffer is already partitioned one Vec per timely worker. We split
// those worker buffers into ordered fixed-size row segments and, wave by wave,
// format `rayon`-many segments into bytes in parallel (interned strings
// resolved through the flat snapshot `resolve_out` via `field_accessor`,
// integers through `::itoa`, floats/bool through `core::fmt`), then write each
// wave's bytes to the file in worker/row order. The result is byte-for-byte
// what the sequential drain produces, with the resolve+format cost spread
// across cores — but, unlike a `collect()`-everything drain, only one wave of
// formatted bytes (≈ `threads × SEG_ROWS × row_width`) is ever resident, so a
// multi-GB relation never materializes a full second copy in RAM. Scratch
// buffers are pooled and reused across waves (allocation-free, cache-hot).

/// Emit the bounded-streaming parallel drain for one `.output` file sink.
///
/// `out_path_stmt` must bind `let out_path = ...;` (the caller owns the
/// incremental `_t{epoch}` naming). The block leaves the shared buffer empty
/// (`mem::take`), preserves row order exactly (segments visited in worker
/// order, rows within a segment in order), streams each wave straight to the
/// `BufWriter`, and ends with an explicit error-checked `flush()`.
fn gen_parallel_file_drain(
    buf_ident: &Ident,
    idb: &Relation,
    out_path_stmt: TokenStream,
    string_intern: bool,
    is_incremental: bool,
) -> TokenStream {
    debug_assert!(idb.arity() > 0, "nullary outputs use the sequential path");
    let (row_writer, uses_itoa) = gen_row_bytes(idb, string_intern, is_incremental);
    let seg_rows = Literal::usize_unsuffixed(PARALLEL_DRAIN_SEG_ROWS);

    // `::itoa::Buffer` is per-segment scratch; omit when no column needs it so
    // `-Dwarnings` never sees an unused binding.
    let itoa_decl = if uses_itoa {
        quote! { let mut itoa_buf = ::itoa::Buffer::new(); }
    } else {
        quote! {}
    };

    quote! {{
        use std::io::Write as _;
        use ::rayon::prelude::*;
        #out_path_stmt
        let mut out = std::io::BufWriter::with_capacity(
            #OUTPUT_BUFFER_BYTES,
            std::fs::File::create(&out_path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e)),
        );

        // Own the per-worker buffers; the shared Vec is left empty.
        let per_worker = std::mem::take(&mut *#buf_ident.lock().unwrap());

        // Ordered (worker, start, end) row segments — worker order then row
        // order, so writing the segments in this order is byte-identical to the
        // sequential drain.
        let mut segments: Vec<(usize, usize, usize)> = Vec::new();
        for (wi, wbuf) in per_worker.iter().enumerate() {
            let mut start = 0usize;
            while start < wbuf.len() {
                let end = (start + #seg_rows).min(wbuf.len());
                segments.push((wi, start, end));
                start = end;
            }
        }

        // Stream wave by wave: format up to `lanes` segments in parallel into
        // pooled scratch buffers, write them in order, reuse. Peak resident
        // formatted bytes ≈ one wave, not the whole relation.
        let lanes = ::rayon::current_num_threads().max(1);
        let mut pool: Vec<Vec<u8>> = Vec::new();
        for wave in segments.chunks(lanes) {
            while pool.len() < wave.len() {
                pool.push(Vec::new());
            }
            pool[..wave.len()]
                .par_iter_mut()
                .zip(wave.par_iter())
                .for_each(|(bytes, &(wi, start, end))| {
                    bytes.clear();
                    #itoa_decl
                    for row in &per_worker[wi][start..end] {
                        #row_writer
                    }
                });
            for bytes in &pool[..wave.len()] {
                out.write_all(bytes).expect("write failed");
            }
        }
        out.flush().expect("flush failed");
    }}
}

/// Per-row byte-assembly statements with `row: &(tuple, Ts, i32)`,
/// `bytes: &mut Vec<u8>`, and (when `uses_itoa`) `itoa_buf` in scope. Returns
/// the tokens plus whether they reference `itoa_buf`.
///
/// Byte-fidelity contract (pinned by the `output_all_types*` fixtures, and it
/// must match [`gen_write_row_file`]): integers via `itoa` ≡ `Display`;
/// floats/bool via `write!("{}")` ≡ the sequential path; strings are raw bytes
/// either way (interned columns resolved through `resolve_out`).
fn gen_row_bytes(idb: &Relation, string_intern: bool, is_incremental: bool) -> (TokenStream, bool) {
    let delim = Literal::byte_string(idb.output_delimiter().as_bytes());
    let mut uses_itoa = false;
    let mut stmts: Vec<TokenStream> = Vec::new();

    for (i, dt) in idb.data_type().iter().enumerate() {
        if i > 0 {
            stmts.push(quote! { bytes.extend_from_slice(#delim); });
        }
        // `row.0.<i>`, wrapped in `resolve_out()` for interned string columns —
        // so `field` is `&'static str` (interned), `String` (plain), or the
        // column's scalar type.
        let field = field_accessor(i, dt, quote! { row }, string_intern);
        stmts.push(match dt {
            DataType::String => {
                quote! { bytes.extend_from_slice(#field.as_bytes()); }
            }
            DataType::Bool => {
                quote! { bytes.extend_from_slice(if #field { b"true" } else { b"false" }); }
            }
            DataType::Float32 | DataType::Float64 => {
                // OrderedFloat's Display forwards to the inner float, which is
                // the exact text the sequential `write!("{}")` produced.
                quote! { write!(bytes, "{}", #field).expect("write failed"); }
            }
            // The eight integer types.
            _ => {
                uses_itoa = true;
                quote! { bytes.extend_from_slice(itoa_buf.format(#field).as_bytes()); }
            }
        });
    }

    if is_incremental {
        // Trailing diff column, `{:+}`-shaped: explicit '+' for >= 0, itoa
        // renders the '-' itself for negatives.
        uses_itoa = true;
        stmts.push(quote! {
            bytes.extend_from_slice(#delim);
            if row.2 >= 0 {
                bytes.push(b'+');
            }
            bytes.extend_from_slice(itoa_buf.format(row.2).as_bytes());
        });
    }

    stmts.push(quote! { bytes.push(b'\n'); });
    (quote! { #(#stmts)* }, uses_itoa)
}
