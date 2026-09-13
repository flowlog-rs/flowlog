//! Output sink selection and execution order.
//!
//! Generated code selects file paths or stdout and delegates collection,
//! formatting, and writing to runtime emitters.

use flowlog_common::ExecutionMode;
use flowlog_parser::OutputSink;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Compiler;
use crate::CompilerError;

impl Compiler {
    /// Stdout places each relation's rows before its count, in declaration
    /// order. Files are emitted concurrently, then counts print in declaration
    /// order. Workers must publish their results before this code runs.
    pub(crate) fn gen_output(&self) -> Result<TokenStream, CompilerError> {
        let mut file_outputs = Vec::new();
        let mut stdout_outputs = Vec::new();
        let mut size_reports = Vec::new();
        for idb in self.program.idbs() {
            let emitter = format_ident!("buf_{}", idb.name());
            match idb.output_sink() {
                Some(OutputSink::File { filename, .. }) => {
                    file_outputs.push(self.gen_file_output(&emitter, filename));
                    stdout_outputs.push(quote! {{
                        #emitter.emit_stdout().expect("write failed");
                    }});
                }
                Some(OutputSink::Sqlite { .. }) => {
                    return Err(CompilerError::internal(format!(
                        "relation `{}`: `IO=\"sqlite\"` output is not implemented",
                        idb.raw_name()
                    )));
                }
                None => {}
            }
            if idb.printsize() {
                let report = quote! {{ #emitter.emit_size().expect("write failed"); }};
                stdout_outputs.push(report.clone());
                size_reports.push(report);
            }
        }
        if file_outputs.is_empty() {
            return Ok(quote! { #(#size_reports)* });
        }
        Ok(quote! {
            if let Some(output_dir) = &output_dir {
                std::thread::scope(|output_scope| {
                    #( output_scope.spawn(|| #file_outputs); )*
                });
                #(#size_reports)*
            } else {
                #(#stdout_outputs)*
            }
        })
    }

    /// Resolves a filename against the runtime output directory, adding the
    /// epoch suffix in incremental mode, and maps write errors to CLI failures.
    fn gen_file_output(&self, emitter: &Ident, filename: &str) -> TokenStream {
        let is_incremental = self.config.mode() == ExecutionMode::Inc;
        let path = if is_incremental {
            let (stem, ext) = match filename.rfind('.') {
                Some(idx) if idx > 0 => (&filename[..idx], &filename[idx..]),
                Some(_) | None => (filename, ""),
            };
            quote! { output_dir.join(format!("{}_t{}{}", #stem, time_stamp, #ext)) }
        } else {
            quote! { output_dir.join(#filename) }
        };
        quote! {{
            let out_path = #path;
            if let Err(error) = #emitter.emit_file::<#is_incremental>(&out_path) {
                eprintln!("failed to write output '{}': {}", out_path.display(), error);
                std::process::exit(1);
            }
        }}
    }
}
