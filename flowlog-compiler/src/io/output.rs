//! Output sink selection and execution order.
//!
//! Generated code selects paths or stdout and delegates collection and writing
//! to runtime emitters. SQLite destinations share database transactions.

use flowlog_common::ExecutionMode;
use flowlog_parser::OutputSink;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::Compiler;

impl Compiler {
    /// Stdout places each relation's rows before its count, in declaration
    /// order. SQLite tables commit by database, text files emit concurrently,
    /// then counts print in declaration order. Workers must publish their
    /// results before this code runs.
    pub(crate) fn gen_output(&self) -> (TokenStream, TokenStream) {
        let mut file_emits = Vec::new();
        let mut stdout_emits = Vec::new();
        let mut size_emits = Vec::new();
        let mut sqlite_path_exprs = Vec::new();
        let mut sqlite_emit_arms = Vec::new();
        let is_incremental = self.config.mode() == ExecutionMode::Inc;
        for relation in self.program.idbs() {
            let emitter = format_ident!("buf_{}", relation.name());
            match relation.output_sink() {
                Some(OutputSink::File { filename, .. }) => {
                    file_emits.push(self.gen_emit_file(&emitter, filename));
                }
                Some(OutputSink::Sqlite { filename, .. }) => {
                    let columns = relation
                        .attributes()
                        .iter()
                        .map(|attribute| attribute.name());
                    let index = sqlite_path_exprs.len();
                    sqlite_path_exprs.push(quote! { output_dir.join(#filename) });
                    sqlite_emit_arms.push(quote! {
                        #index => #emitter.emit_sqlite::<#is_incremental>(transaction, &[#(#columns),*], reset),
                    });
                }
                None => {}
            }
            if relation.has_output() {
                stdout_emits.push(quote! {{
                    #emitter.emit_stdout().expect("write failed");
                }});
            }
            if relation.printsize() {
                let report = quote! {{ #emitter.emit_size().expect("write failed"); }};
                stdout_emits.push(report.clone());
                size_emits.push(report);
            }
        }
        if file_emits.is_empty() && sqlite_path_exprs.is_empty() {
            return (quote! {}, quote! { #(#size_emits)* });
        }
        let initialize_output = if sqlite_path_exprs.is_empty() {
            quote! {}
        } else {
            quote! {
                let sqlite_paths = output_dir.as_ref()
                    .map(|output_dir| vec![#(#sqlite_path_exprs),*])
                    .unwrap_or_default();
                let sqlite_writer = std::sync::Mutex::new(
                    ::flowlog_runtime::io::output::SqliteWriter::default(),
                );
            }
        };
        let emit_sqlite = (!sqlite_path_exprs.is_empty()).then(|| {
            quote! {
                let result = sqlite_writer.lock().expect("SQLite output state poisoned").write(
                    &sqlite_paths,
                    |index, transaction, reset| match index {
                        #(#sqlite_emit_arms)*
                        _ => unreachable!("SQLite destination index comes from paths"),
                    },
                );
                if let Err(error) = result {
                    eprintln!("failed to write SQLite output: {error}");
                    std::process::exit(1);
                }
            }
        });
        let emit_files = (!file_emits.is_empty()).then(|| {
            quote! {
                std::thread::scope(|output_scope| {
                    #( output_scope.spawn(|| #file_emits); )*
                });
            }
        });
        let emit_output = quote! {
            if let Some(output_dir) = &output_dir {
                #emit_sqlite
                #emit_files
                #(#size_emits)*
            } else {
                #(#stdout_emits)*
            }
        };
        (initialize_output, emit_output)
    }

    /// Resolves a filename against the runtime output directory, adding the
    /// epoch suffix in incremental mode, and maps write errors to CLI failures.
    fn gen_emit_file(&self, emitter: &Ident, filename: &str) -> TokenStream {
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
            let output_path = #path;
            if let Err(error) = #emitter.emit_file::<#is_incremental>(&output_path) {
                eprintln!("failed to write output '{}': {}", output_path.display(), error);
                std::process::exit(1);
            }
        }}
    }
}
