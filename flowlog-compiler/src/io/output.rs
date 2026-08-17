//! Binary-mode output sink + drain codegen.
//!
//! For each IDB that is `.output`'d or `.printsize`'d, emits the post-barrier
//! code that runs on worker 0:
//!
//! - **`.output`**: drain the shared buffer and write each row to a file
//!   (default) or stderr (`-D -`). File sinks without `ORDER BY` hand the
//!   whole buffer to `flowlog_runtime::io::TextWriter`, which formats it
//!   across cores; the rest go through [`flowlog_build::gen_drain_block`],
//!   which applies `ORDER BY` / `LIMIT` and is shared with library mode.
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

impl Compiler {
    /// The merge section spliced into `main()` after the barrier (worker 0
    /// only): empty-output touches, derived `.output` drains, then
    /// `.printsize` reports.
    ///
    /// In file mode the blocks run in concurrent scoped threads: each writes
    /// a distinct file, so the kernel parallelizes the writes, and every
    /// drain is bounded-streaming so concurrent drains stay memory-safe.
    /// Stderr (`-D -`) stays sequential since all blocks share one stream.
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

    /// Resolve `-D <outdir>` or return the canonical "unset" error, using
    /// `context` to disambiguate. Centralised because every file-emitting
    /// block needs the same lookup with a slightly different message.
    fn require_output_dir(&self, context: &'static str) -> Result<&str, CompilerError> {
        self.options.output_dir().ok_or_else(|| {
            CompilerError::internal(format!("binary mode {context} but `output_dir` is unset"))
        })
    }

    /// Bind `let spec = ...;` describing one relation's output file.
    fn gen_output_spec(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let base_dir = self.require_output_dir("writing IDB output to files")?;
        let out_path_stmt = gen_out_path_stmt(
            &idb.output_file_name(),
            base_dir,
            self.config.is_incremental(),
        );
        let raw_name = idb.raw_name();
        let delim = Literal::byte_string(idb.output_delimiter().as_bytes());
        Ok(quote! {
            #out_path_stmt
            let spec = ::flowlog_runtime::io::OutputSpec {
                relation: #raw_name,
                path: &out_path,
                delim: #delim,
            };
        })
    }

    /// Drain one `.output` relation's shared buffer through its sink.
    fn gen_output_drain(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let buf_ident = Ident::new(&format!("buf_{}", idb.name()), Span::call_site());
        let string_intern = self.codegen.features().string_intern();
        let is_incremental = self.config.is_incremental();

        // A file sink with no ORDER BY hands its whole buffer over at once,
        // so the runtime can format it across cores. Nullary, ORDER BY /
        // LIMIT, and stderr go row by row.
        if idb.uses_parallel_file_drain(self.config.output_to_stdout()) {
            let spec = self.gen_output_spec(idb)?;
            return Ok(quote! {{
                #spec
                let per_worker = ::std::mem::take(
                    &mut *#buf_ident.lock().expect("output buffer poisoned"),
                );
                let written = ::flowlog_runtime::io::TextWriter::write_file(
                    &spec, per_worker, #is_incremental,
                );
                if let Err(e) = written {
                    eprintln!("[flowlog] fatal: {e}");
                    std::process::exit(1);
                }
            }});
        }

        // Stderr has nothing to open or close, so only the file sink
        // carries a preamble and a postamble.
        let (sink_preamble, write_row, sink_postamble) = if self.config.output_to_stdout() {
            (
                gen_stderr_preamble(),
                gen_write_row_stderr(idb, string_intern),
                quote! {},
            )
        } else {
            let spec = self.gen_output_spec(idb)?;
            // A nullary relation writes its presence marker and never a
            // diff column, even in an incremental epoch.
            let diff_expr = if is_incremental && idb.arity() > 0 {
                quote! { Some(diff) }
            } else {
                quote! { None }
            };
            (
                quote! {
                    #spec
                    let mut out = match ::flowlog_runtime::io::TextWriter::create(&spec) {
                        Ok(out) => out,
                        Err(e) => {
                            eprintln!("[flowlog] fatal: {e}");
                            std::process::exit(1);
                        }
                    };
                },
                quote! {
                    let _ = &time;
                    ::flowlog_runtime::io::Writer::push(&mut out, tuple, #diff_expr);
                },
                quote! {
                    if let Err(e) = out.commit() {
                        eprintln!("[flowlog] fatal: {e}");
                        std::process::exit(1);
                    }
                },
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
    /// (`<outdir>/<RawName>.csv`, one line) or, when `-D -` is set,
    /// `eprintln!` the `(time, size)` pair to stderr.
    ///
    /// Mutual exclusion with `.output R` on the same relation is enforced
    /// at parse time, so this never races an output drain over one path.
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
        let raw_name = idb.raw_name();
        // A count is a one-column row, so it goes out through the writer
        // every other file uses; the delimiter is never reached.
        Ok(quote! {{
            let (_, size) = *#cell.lock().unwrap();
            let out_path = format!("{}/{}", #base_dir, #file_name);
            let spec = ::flowlog_runtime::io::OutputSpec {
                relation: #raw_name,
                path: &out_path,
                delim: b"\t",
            };
            let written = ::flowlog_runtime::io::TextWriter::write_file(
                &spec, vec![vec![((size,), 0u32, 1)]], false,
            );
            if let Err(e) = written {
                eprintln!("[flowlog] fatal: {e}");
                std::process::exit(1);
            }
        }})
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

fn gen_write_row_stderr(idb: &Relation, string_intern: bool) -> TokenStream {
    let prefix = idb.raw_name().to_string();
    if idb.arity() == 0 {
        return quote! {
            writeln!(out, "[tuple][{}]  t={:?}  True  diff={:+}",
                #prefix, time, diff)
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
        writeln!(out, #fmt, time #(, #fields )*, diff).expect("write failed");
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
            stderr_accessor(&quote! { tuple.#idx }, dt, string_intern)
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
