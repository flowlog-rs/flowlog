//! Stdout sink: rows and counts in one bracketed debug shape.
//!
//! Values render through `Debug` rather than the file sink's `Display`: this
//! stream is for reading, not for machine consumption.

use flowlog_parser::DataType;
use flowlog_parser::Relation;
use proc_macro2::Literal;
use proc_macro2::TokenStream;
use quote::quote;

pub(super) fn gen_stdout_preamble() -> TokenStream {
    quote! {
        use std::io::Write as _;
        let mut out = std::io::stdout();
    }
}

pub(super) fn gen_write_row_stdout(idb: &Relation, string_intern: bool) -> TokenStream {
    let prefix = idb.raw_name().to_string();
    if idb.arity() == 0 {
        return quote! {
            writeln!(out, "[tuple][{}]  t={:?}  True  diff={:+}",
                #prefix, row.1, row.2)
                .expect("write failed");
        };
    }

    let fields = data_field_accessors(idb, string_intern);
    // The stdout format shows `Debug` representations for readability; files
    // get `Display` for machine-consumable output.
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
/// columns recurse into a nested tuple of resolved leaves, which `{:?}`
/// renders readably and, just as importantly, keeps `resolve_out` used: the
/// generated crate builds under `-Dwarnings`.
fn data_field_accessors(idb: &Relation, string_intern: bool) -> Vec<TokenStream> {
    idb.data_type()
        .iter()
        .enumerate()
        .map(|(i, dt)| {
            let idx = Literal::usize_unsuffixed(i);
            stdout_accessor(&quote! { row.0.#idx }, dt, string_intern)
        })
        .collect()
}

/// Debug-printable accessor for one value at `access`: interned-string leaves
/// resolve to `&str`; tuple columns rebuild as a nested tuple of resolved
/// leaves. Used only by the stdout sink.
fn stdout_accessor(access: &TokenStream, dt: &DataType, string_intern: bool) -> TokenStream {
    match dt {
        DataType::String if string_intern => quote! { resolve_out(#access) },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Literal::usize_unsuffixed(j);
                stdout_accessor(&quote! { (#access).#jdx }, fdt, string_intern)
            });
            quote! { ( #(#elems),* ) }
        }
        _ => access.clone(),
    }
}
