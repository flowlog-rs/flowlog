//! Output sink selection and execution order.
//!
//! Generated code selects paths or stdout and delegates collection and writing
//! to runtime emitters. SQLite destinations share database transactions.

use flowlog_codegen::output_emitter_ident;
use flowlog_parser::OutputSink;
use proc_macro2::Ident;
use proc_macro2::TokenStream;
use quote::quote;

use crate::Compiler;

/// The output side of the generated `main`.
#[derive(Debug)]
pub(crate) struct Output {
    /// Before the workers start: creates the output directory and the
    /// state the writers keep across epochs.
    pub initialize: TokenStream,
    /// After the workers publish: writes every relation's rows and counts.
    pub emit: TokenStream,
}

impl Compiler {
    /// Returns the output fragments. Stdout places each relation's rows
    /// before its count, in declaration order. SQLite tables commit by
    /// database, text files emit concurrently, then counts print in
    /// declaration order. Workers must publish their results before the
    /// emit fragment runs.
    pub(crate) fn gen_output(&self) -> Output {
        let mut file_emits = Vec::new();
        let mut stdout_emits = Vec::new();
        let mut size_emits = Vec::new();
        let mut sqlite_path_exprs = Vec::new();
        let mut sqlite_emit_arms = Vec::new();
        let is_incremental = self.program.is_incremental();
        // A count reports the epoch whose updates it summed. The shell has
        // advanced `time_stamp` past that epoch by the time it emits.
        let size_epoch = if is_incremental {
            quote! { &(time_stamp - 1) }
        } else {
            quote! { &() }
        };
        for relation in self.program.idbs() {
            let emitter = output_emitter_ident(relation.name());
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
                let report = quote! {{ #emitter.emit_size(#size_epoch).expect("write failed"); }};
                stdout_emits.push(report.clone());
                size_emits.push(report);
            }
        }
        if file_emits.is_empty() && sqlite_path_exprs.is_empty() {
            return Output {
                initialize: quote! {},
                emit: quote! { #(#size_emits)* },
            };
        }
        // The SQLite writer keeps per-database state across epochs, so it
        // is set up once; the paths are a few joins and resolve where they
        // are used, like the file paths.
        let sqlite_writer = (!sqlite_path_exprs.is_empty()).then(|| {
            quote! {
                let sqlite_writer = std::sync::Mutex::new(
                    ::flowlog_runtime::io::output::SqliteWriter::default(),
                );
            }
        });
        let initialize = quote! {
            if let Some(dir) = &output_dir {
                if let Err(error) = std::fs::create_dir_all(dir) {
                    eprintln!(
                        "failed to create output directory '{}': {}",
                        dir.display(), error,
                    );
                    std::process::exit(1);
                }
            }
            #sqlite_writer
        };
        let emit_sqlite = (!sqlite_path_exprs.is_empty()).then(|| {
            quote! {
                let sqlite_paths = vec![#(#sqlite_path_exprs),*];
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
        let emit = quote! {
            if let Some(output_dir) = &output_dir {
                #emit_sqlite
                #emit_files
                #(#size_emits)*
            } else {
                #(#stdout_emits)*
            }
        };
        Output { initialize, emit }
    }

    /// Resolves a filename against the runtime output directory, adding the
    /// epoch suffix in an incremental engine, and maps write errors to CLI
    /// failures.
    fn gen_emit_file(&self, emitter: &Ident, filename: &str) -> TokenStream {
        let is_incremental = self.program.is_incremental();
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
