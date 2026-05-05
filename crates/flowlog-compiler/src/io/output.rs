//! Binary-mode output sink + drain codegen.
//!
//! For each IDB that is `.output`'d or `.printsize`'d, emits the post-barrier
//! code that runs on worker 0:
//!
//! - **`.output`**: drain the shared buffer, apply `ORDER BY` / `LIMIT` via
//!   [`flowlog_build::gen_drain_block`] (shared with library mode through
//!   `::flowlog_runtime::sort::*`), then write each row to a file (default) or
//!   stderr (`-D -`).
//! - **`.printsize`**: read the shared size cell and report it to stderr.

use proc_macro2::{Ident, Literal, Span, TokenStream};
use quote::quote;

use flowlog_build::parser::Relation;
use flowlog_build::{field_accessor, gen_drain_block};

use crate::{Compiler, CompilerError};

impl Compiler {
    /// Per-IDB merge blocks spliced into `main()` after the barrier
    /// (worker 0 only). The order follows `.output` then `.printsize`.
    pub(crate) fn gen_merge_blocks(&self) -> Result<Vec<TokenStream>, CompilerError> {
        let mut blocks = Vec::new();
        for idb in self.program.output_idbs() {
            blocks.push(self.gen_output_drain(idb)?);
        }
        for idb in self.program.printsize_idbs() {
            blocks.push(gen_size_report(idb));
        }
        Ok(blocks)
    }

    /// Drain one `.output` relation's shared buffer through its sink.
    fn gen_output_drain(&self, idb: &Relation) -> Result<TokenStream, CompilerError> {
        let buf_ident = Ident::new(&format!("buf_{}", idb.name()), Span::call_site());
        let string_intern = self.codegen.features().string_intern();
        let is_incremental = self.config.is_incremental();

        let (sink_preamble, write_row) = if self.config.output_to_stdout() {
            (
                gen_stderr_preamble(),
                gen_write_row_stderr(idb, string_intern),
            )
        } else {
            let base_dir = self.config.output_dir().ok_or_else(|| {
                CompilerError::internal(
                    "binary mode writing IDB output to files but `output_dir` is unset",
                )
            })?;
            (
                gen_file_preamble(idb.name(), base_dir, is_incremental),
                gen_write_row_file(idb, string_intern, is_incremental),
            )
        };

        Ok(gen_drain_block(
            &buf_ident,
            idb,
            sink_preamble,
            write_row,
            string_intern,
        ))
    }
}

/// Read one `.printsize` cell and `eprintln!` the `(time, size)` pair.
fn gen_size_report(idb: &Relation) -> TokenStream {
    let cell = Ident::new(&format!("size_{}", idb.name()), Span::call_site());
    let prefix = idb.name().to_string();
    quote! {
        {
            let (t, size) = &*#cell.lock().unwrap();
            eprintln!("[size][{}] t={:?} size={}", #prefix, t, size);
        }
    }
}

// =========================================================================
// Sink preambles — emit `let mut out = ...;` + `use std::io::Write as _;`.
// The drain block's `write_row` closes over `out`.
// =========================================================================

fn gen_file_preamble(name: &str, base_dir: &str, is_incremental: bool) -> TokenStream {
    // Incremental mode appends `_t{timestamp}` so each epoch writes its own
    // file rather than overwriting the previous one.
    let out_path = if is_incremental {
        quote! { let out_path = format!("{}/{}_t{}", #base_dir, #name, time_stamp); }
    } else {
        quote! { let out_path = format!("{}/{}", #base_dir, #name); }
    };
    quote! {
        use std::io::Write as _;
        #out_path
        let mut out = std::io::BufWriter::new(
            std::fs::File::create(&out_path)
                .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e)),
        );
    }
}

fn gen_stderr_preamble() -> TokenStream {
    quote! {
        use std::io::Write as _;
        let mut out = std::io::stderr();
    }
}

// =========================================================================
// Row formatters — emit the `writeln!(out, ...)` that runs per row.
// =========================================================================

fn gen_write_row_file(idb: &Relation, string_intern: bool, is_incremental: bool) -> TokenStream {
    // Nullary: output is a boolean presence marker, one literal `True` line.
    if idb.arity() == 0 {
        return quote! {
            let _ = &row;
            writeln!(out, "True").expect("write failed");
        };
    }

    let fields = data_field_accessors(idb, string_intern);
    let delim = idb.output_delimiter();

    if is_incremental {
        // Incremental lines carry the diff as a signed suffix column.
        let mut parts = vec!["{}"; idb.arity()];
        parts.push("{:+}");
        let fmt = Literal::string(&parts.join(delim));
        quote! {
            writeln!(out, #fmt #(, #fields )*, row.2).expect("write failed");
        }
    } else {
        let fmt = Literal::string(&vec!["{}"; idb.arity()].join(delim));
        quote! {
            writeln!(out, #fmt #(, #fields )*).expect("write failed");
        }
    }
}

fn gen_write_row_stderr(idb: &Relation, string_intern: bool) -> TokenStream {
    let prefix = idb.name().to_string();
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
/// interned-string columns in `resolve()` so they format as `&str`.
fn data_field_accessors(idb: &Relation, string_intern: bool) -> Vec<TokenStream> {
    idb.data_type()
        .iter()
        .enumerate()
        .map(|(i, dt)| field_accessor(i, dt, quote! { row }, string_intern))
        .collect()
}
