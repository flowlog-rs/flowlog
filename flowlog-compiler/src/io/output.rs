//! Binary-mode output sink + drain codegen.
//!
//! For each IDB that is `.output`'d or `.printsize`'d, emits the post-barrier
//! code that runs on worker 0:
//!
//! - **`.output`**: drain the shared buffer and write each row to a file
//!   (default) or stderr (`-D -`). File sinks without `ORDER BY` use the
//!   bounded-streaming parallel drain ([`gen_parallel_file_drain`]); the rest
//!   go through [`flowlog_build::gen_drain_block`], which applies `ORDER BY` /
//!   `LIMIT` and is shared with library mode.
//! - **`.printsize`**: read the shared size cell and report it to stderr.

use flowlog_build::gen_drain_block;
use flowlog_parser::DataType;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::Literal;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;
use crate::CompilerError;

/// Capacity of each per-IDB output `BufWriter`, sized to amortize write
/// syscalls over many small rows. A few drains run concurrently in file mode,
/// but 1 MiB each is negligible next to the data.
const OUTPUT_BUFFER_BYTES: usize = 1 << 20;

/// Rows per parallel-drain segment. The drain formats and writes one segment
/// at a time instead of materializing the whole output, bounding peak
/// in-flight memory to roughly `lanes * SEG_ROWS * row_width` per relation.
/// This is a memory bound, not a parallelism knob: 8K is large enough to
/// amortize per-segment overhead while keeping the transient buffers small.
const PARALLEL_DRAIN_SEG_ROWS: usize = 8192;

impl Compiler {
    /// The merge section spliced into `main()` after the barrier (worker 0
    /// only): empty-output touches, derived `.output` drains, then `.printsize`
    /// reports.
    ///
    /// In file mode the blocks run in concurrent scoped threads — each writes a
    /// distinct file, so the kernel parallelizes the writes, and every drain is
    /// bounded-streaming so concurrent drains stay memory-safe. Stderr (`-D -`)
    /// stays sequential since all blocks share one stream.
    pub(crate) fn gen_merge_section(&self) -> Result<TokenStream, CompilerError> {
        let mut blocks = Vec::new();
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
        self.options.output_dir().ok_or_else(|| {
            CompilerError::internal(format!("binary mode {context} but `output_dir` is unset"))
        })
    }

    /// Drain one `.output` relation's shared buffer through its sink.
    fn gen_output_drain(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let buf_ident = Ident::new(&format!("buf_{}", idb.name()), Span::call_site());
        let string_intern = self.codegen.features().string_intern();
        let is_incremental = self.config.is_incremental();

        // File sinks without ORDER BY take the bounded-streaming parallel drain
        // (same bytes and row order, resolve+format spread across cores).
        // Nullary, ORDER BY/LIMIT, and stderr stay on the sequential path.
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
            let file_preamble =
                gen_file_preamble(&idb.output_file_name(), base_dir, is_incremental);
            let (scratch_decls, write_row) =
                gen_file_row_writer(idb, string_intern, is_incremental);
            (
                quote! { #file_preamble #scratch_decls },
                write_row,
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
// File row formatter.
//
// `gen_row_bytes` builds one row's bytes for both file sinks — sequential and
// parallel — so the two paths cannot drift. Strings are copied verbatim,
// integers go through `itoa`, floats/bool through `core::fmt`; the bytes match
// each value's `Display`. Pinned by the `output_all_types*` fixtures. (Stderr
// uses a separate `Debug` format; see `gen_write_row_stderr`.)
// =========================================================================

/// Sequential file sink: returns `(scratch_decls, write_row)`. The caller
/// splices `scratch_decls` into the preamble once and runs `write_row` per row,
/// which builds the row into the reused `bytes` scratch via [`gen_row_bytes`]
/// and emits it with a single `write_all`.
fn gen_file_row_writer(
    idb: &Relation,
    string_intern: bool,
    is_incremental: bool,
) -> (TokenStream, TokenStream) {
    // Nullary: output is a boolean presence marker, one literal `True` line.
    if idb.arity() == 0 {
        return (
            quote! {},
            quote! {
                let _ = &row;
                out.write_all(b"True\n").expect("write failed");
            },
        );
    }

    let (row_writer, uses_itoa) = gen_row_bytes(idb, string_intern, is_incremental);
    let itoa_decl = if uses_itoa {
        quote! { let mut itoa_buf = ::itoa::Buffer::new(); }
    } else {
        quote! {}
    };
    let scratch_decls = quote! {
        let mut bytes: Vec<u8> = Vec::new();
        #itoa_decl
    };
    let write_row = quote! {
        bytes.clear();
        #row_writer
        out.write_all(&bytes).expect("write failed");
    };
    (scratch_decls, write_row)
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
/// interned-string leaves in `resolve_out()` so they format as `&str`. Tuple
/// columns recurse into a nested tuple of resolved leaves (`(resolve_out(..), …)`),
/// which `{:?}` renders readably and, crucially, keeps `resolve_out` *used*
/// (the generated crate builds under `-Dwarnings`).
fn data_field_accessors(idb: &Relation, string_intern: bool) -> Vec<TokenStream> {
    idb.data_type()
        .iter()
        .enumerate()
        .map(|(i, dt)| {
            let idx = Literal::usize_unsuffixed(i);
            stderr_accessor(&quote! { row.0.#idx }, dt, string_intern)
        })
        .collect()
}

/// Debug-printable accessor for one value at `access`: interned-string leaves
/// resolve to `&str`; tuple columns rebuild as a nested tuple of resolved
/// leaves. Used only by the stderr sink.
fn stderr_accessor(access: &TokenStream, dt: &DataType, string_intern: bool) -> TokenStream {
    match dt {
        DataType::String if string_intern => quote! { resolve_out(#access) },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Literal::usize_unsuffixed(j);
                stderr_accessor(&quote! { (#access).#jdx }, fdt, string_intern)
            });
            quote! { ( #(#elems),* ) }
        }
        _ => access.clone(),
    }
}

// =========================================================================
// Bounded-streaming parallel file drain
// =========================================================================
//
// The shared buffer is already partitioned one Vec per timely worker. We split
// those into ordered fixed-size segments and, wave by wave, format `rayon`-many
// segments in parallel, then write each wave to the file in worker/row order.
// Output is byte-identical to the sequential drain, with the format cost spread
// across cores — but only one wave of formatted bytes is resident at a time, so
// a large relation never materializes a second full copy in RAM. Scratch
// buffers are pooled and reused across waves.

/// Emit the bounded-streaming parallel drain for one `.output` file sink.
///
/// `out_path_stmt` must bind `let out_path = ...;`. The block empties the
/// shared buffer (`mem::take`), preserves row order exactly, streams each wave
/// to the `BufWriter`, and ends with an explicit error-checked `flush()`.
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

/// Per-row byte-assembly statements for both file sinks. Expects `row`,
/// `bytes: &mut Vec<u8>`, and (when the returned flag is set) `itoa_buf` in
/// scope. Returns the tokens plus whether they use `itoa_buf`.
///
/// Byte-fidelity contract (pinned by the `output_all_types*` fixtures):
/// integers via `itoa`, floats/bool via `write!("{}")`, strings as raw bytes
/// (interned columns resolved through `resolve_out`).
/// Append the file-form bytes of a single value at `access` to `bytes`.
/// Scalars match each value's `Display` (integers via `itoa`, floats/bool via
/// `write!`, strings raw — interned columns resolved through `resolve_out`).
/// A tuple serializes in FlowLog form `(e0, e1, …)` (comma-space separator),
/// recursing into its fields. Sets `*uses_itoa` when an integer leaf is emitted.
fn gen_value_bytes(
    access: &TokenStream,
    dt: &DataType,
    string_intern: bool,
    uses_itoa: &mut bool,
) -> TokenStream {
    match dt {
        DataType::String => {
            if string_intern {
                quote! { bytes.extend_from_slice(resolve_out(#access).as_bytes()); }
            } else {
                quote! { bytes.extend_from_slice((#access).as_bytes()); }
            }
        }
        DataType::Bool => {
            quote! { bytes.extend_from_slice(if #access { b"true" } else { b"false" }); }
        }
        DataType::Float32 | DataType::Float64 => {
            // OrderedFloat's Display forwards to the inner float — the same
            // text `write!("{}")` produces.
            quote! { write!(bytes, "{}", #access).expect("write failed"); }
        }
        // Tuple column → FlowLog tuple form `(e0, e1, …)` (matches the source
        // syntax; `[ … ]` is reserved for a future array type). The `, `
        // separator is the tuple's own, independent of the column delimiter.
        DataType::FixedTuple(fields) => {
            let mut inner: Vec<TokenStream> = vec![quote! { bytes.push(b'('); }];
            for (j, fdt) in fields.iter().enumerate() {
                if j > 0 {
                    inner.push(quote! { bytes.extend_from_slice(b", "); });
                }
                let jdx = Literal::usize_unsuffixed(j);
                let sub = quote! { (#access).#jdx };
                inner.push(gen_value_bytes(&sub, fdt, string_intern, uses_itoa));
            }
            if fields.len() == 1 {
                inner.push(quote! { bytes.push(b','); });
            }
            inner.push(quote! { bytes.push(b')'); });
            quote! { #(#inner)* }
        }
        // The eight integer types.
        _ => {
            *uses_itoa = true;
            quote! { bytes.extend_from_slice(itoa_buf.format(#access).as_bytes()); }
        }
    }
}

fn gen_row_bytes(idb: &Relation, string_intern: bool, is_incremental: bool) -> (TokenStream, bool) {
    let delim = Literal::byte_string(idb.output_delimiter().as_bytes());
    let mut uses_itoa = false;
    let mut stmts: Vec<TokenStream> = Vec::new();

    for (i, dt) in idb.data_type().iter().enumerate() {
        if i > 0 {
            stmts.push(quote! { bytes.extend_from_slice(#delim); });
        }
        let idx = Literal::usize_unsuffixed(i);
        // Column value, accessed raw as `row.0.<i>`. `gen_value_bytes` applies
        // `resolve_out` at each interned-string leaf (mirroring `field_accessor`)
        // and recurses into tuple columns, which serialize as `(a, b)`.
        let access = quote! { row.0.#idx };
        stmts.push(gen_value_bytes(&access, dt, string_intern, &mut uses_itoa));
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
