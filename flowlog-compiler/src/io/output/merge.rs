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
    /// Emits output drains and size reports with runtime sink selection.
    /// Stdout places each relation's rows before its count, in declaration
    /// order. File drains run concurrently, then counts print in declaration
    /// order. Each file drain bounds its own memory; total use scales with
    /// the number of concurrent drains.
    pub(crate) fn gen_merge_section(&self) -> Result<TokenStream, CompilerError> {
        let mut file_drains = Vec::new();
        let mut stdout_blocks = Vec::new();
        let mut reports = Vec::new();
        for idb in self.program.idbs() {
            if idb.has_output() {
                file_drains.push(self.gen_output_drain(idb, false)?);
                stdout_blocks.push(self.gen_output_drain(idb, true)?);
            }
            if idb.printsize() {
                let report = self.gen_size_report(idb);
                stdout_blocks.push(report.clone());
                reports.push(report);
            }
        }
        if file_drains.is_empty() {
            return Ok(quote! { #(#reports)* });
        }
        Ok(quote! {
            if let Some(output_dir) = &output_dir {
                std::thread::scope(|merge_scope| {
                    #( merge_scope.spawn(|| #file_drains); )*
                });
                #(#reports)*
            } else {
                #(#stdout_blocks)*
            }
        })
    }

    /// Emits one `.output` relation's drain for the selected sink.
    fn gen_output_drain(
        &self,
        idb: &Relation,
        output_to_stdout: bool,
    ) -> Result<TokenStream, CompilerError> {
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
        if idb.uses_parallel_file_drain(output_to_stdout) {
            let out_path_stmt = gen_out_path_stmt(sink.filename(), is_incremental);
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
        let (sink_preamble, write_row, sink_postamble) = if output_to_stdout {
            (
                gen_stdout_preamble(),
                gen_write_row_stdout(idb, string_intern),
                quote! {},
            )
        } else {
            let file_preamble = gen_file_preamble(sink.filename(), is_incremental);
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
