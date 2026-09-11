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

/// Preserves relation column order when collecting formatting expressions
/// from [`stdout_accessor`].
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

/// Emits expressions that borrow owned strings and resolve interned strings.
/// Tuple expressions preserve nesting and singleton tuple shape.
fn stdout_accessor(access: &TokenStream, dt: &DataType, string_intern: bool) -> TokenStream {
    match dt {
        DataType::String if string_intern => {
            quote! { ::flowlog_runtime::intern::resolve_out(#access) }
        }
        // Tuple reconstruction must borrow string leaves from the shared row.
        DataType::String => quote! { &#access },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Literal::usize_unsuffixed(j);
                stdout_accessor(&quote! { (#access).#jdx }, fdt, string_intern)
            });
            quote! { ( #(#elems,)* ) }
        }
        DataType::IntLit
        | DataType::FloatLit
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float32
        | DataType::Float64
        | DataType::Bool => access.clone(),
    }
}
