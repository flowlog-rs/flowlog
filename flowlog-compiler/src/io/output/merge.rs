//! Sink selection: which drain each IDB gets, and how the blocks are ordered.

use flowlog_build::gen_drain_block;
use flowlog_common::ExecutionMode;
use flowlog_parser::OutputSink;
use flowlog_parser::Relation;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

use super::file::gen_file_preamble;
use super::file::gen_file_row_writer;
use super::file::gen_out_path_stmt;
use super::file::gen_parallel_file_drain;
use super::stdout::gen_stdout_preamble;
use super::stdout::gen_write_row_stdout;
use crate::Compiler;
use crate::CompilerError;

impl Compiler {
    /// The `.output` drains and the `.printsize` reports.
    ///
    /// Under `-D -` both land on stdout, so a relation's rows are followed by
    /// its own count, in declaration order. Nothing fans out there: one
    /// stream takes one writer at a time regardless.
    ///
    /// Writing files, the rows never reach stdout and only the counts do, so
    /// there is nothing to group them with. The drains fan out instead, each
    /// owning a distinct file the kernel can write in parallel, each
    /// bounded-streaming, though that bound is per drain: running D of them
    /// holds D times as much. The counts follow once the scope has joined,
    /// never inside it, so their order is the program's and not the
    /// scheduler's.
    pub(crate) fn gen_merge_section(&self) -> Result<TokenStream, CompilerError> {
        // One pass in declaration order: each relation's rows and its count,
        // either of which may be absent. Only the sequencing below differs by
        // sink.
        let per_relation: Vec<(Option<TokenStream>, Option<TokenStream>)> = self
            .program
            .idbs()
            .into_iter()
            .map(|idb| {
                let drain = idb
                    .has_output()
                    .then(|| self.gen_output_drain(idb))
                    .transpose()?;
                let report = idb.printsize().then(|| self.gen_size_report(idb));
                Ok((drain, report))
            })
            .collect::<Result<_, CompilerError>>()?;

        if self.config.output_to_stdout() {
            let blocks: Vec<&TokenStream> = per_relation
                .iter()
                .flat_map(|(drain, report)| drain.iter().chain(report.iter()))
                .collect();
            return Ok(quote! { #(#blocks)* });
        }

        let drains: Vec<&TokenStream> = per_relation
            .iter()
            .filter_map(|(d, _)| d.as_ref())
            .collect();
        let reports: Vec<&TokenStream> = per_relation
            .iter()
            .filter_map(|(_, r)| r.as_ref())
            .collect();
        let drain_section = if drains.is_empty() {
            quote! {}
        } else {
            quote! {
                std::thread::scope(|merge_scope| {
                    #( merge_scope.spawn(|| #drains); )*
                });
            }
        };
        Ok(quote! {
            #drain_section
            #(#reports)*
        })
    }

    /// Resolve `-D <outdir>`, or the canonical "unset" error.
    fn require_output_dir(&self) -> Result<&str, CompilerError> {
        self.options.output_dir().ok_or_else(|| {
            CompilerError::internal(
                "binary mode writing IDB output to files but `output_dir` is unset".to_string(),
            )
        })
    }

    /// Drain one `.output` relation's shared buffer through its sink.
    fn gen_output_drain(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        // Only an `.output` relation is drained, so the sink is present; a
        // missing one means an earlier stage handed over the wrong relation.
        let sink = idb.output_sink().ok_or_else(|| {
            CompilerError::internal(format!(
                "relation `{}` is drained without an `.output`",
                idb.raw_name()
            ))
        })?;
        let delim = match sink {
            OutputSink::File { delim, .. } => *delim,
            // The writer is parked, not written: the parser resolves the sink
            // so the seam exists, and this arm is what un-parking replaces.
            OutputSink::Sqlite { .. } => {
                return Err(CompilerError::internal(format!(
                    "relation `{}`: `IO=\"sqlite\"` output is not implemented",
                    idb.raw_name()
                )));
            }
        };
        let buf_ident = Ident::new(&format!("buf_{}", idb.name()), Span::call_site());
        let string_intern = self.codegen.features().string_intern();
        let is_incremental = self.config.mode() == ExecutionMode::Inc;

        // File sinks without ORDER BY take the bounded-streaming parallel drain
        // (same bytes and row order, resolve+format spread across cores).
        // Nullary, ORDER BY/LIMIT, and stdout stay on the sequential path.
        if idb.uses_parallel_file_drain(self.config.output_to_stdout()) {
            let base_dir = self.require_output_dir()?;
            let out_path_stmt = gen_out_path_stmt(sink.filename(), base_dir, is_incremental);
            return Ok(gen_parallel_file_drain(
                &buf_ident,
                idb,
                out_path_stmt,
                delim,
                string_intern,
                is_incremental,
            ));
        }

        // Stdout flushes on each newline, so only the file sink needs the
        // explicit final flush; `BufWriter::Drop` would swallow a failed tail
        // write.
        let (sink_preamble, write_row, sink_postamble) = if self.config.output_to_stdout() {
            (
                gen_stdout_preamble(),
                gen_write_row_stdout(idb, string_intern),
                quote! {},
            )
        } else {
            let base_dir = self.require_output_dir()?;
            let file_preamble = gen_file_preamble(sink.filename(), base_dir, is_incremental);
            let (scratch_decls, write_row) =
                gen_file_row_writer(idb, delim, string_intern, is_incremental);
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

    /// One `.printsize` cell as a line on stdout, in the same bracketed debug
    /// shape the `-D -` row sink uses, so a relation reads as one block where
    /// both land on the stream, and one shape everywhere else.
    ///
    /// `t` rides along because the cell holds one epoch's delta: under
    /// `--mode inc` the number is a change, and the timestamp is what says
    /// which epoch it belongs to. Souffle prints a bare `<name>\t<count>`
    /// instead; one shape is worth more here than that parity.
    fn gen_size_report(&self, idb: &Relation) -> TokenStream {
        let cell = Ident::new(&format!("size_{}", idb.name()), Span::call_site());
        let name = idb.raw_name().to_string();
        quote! {{
            let (t, size) = &*#cell.lock().unwrap();
            println!("[size][{}]  t={:?}  size={}", #name, t, size);
        }}
    }
}
