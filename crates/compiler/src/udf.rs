//! Head (scalar) UDF code generation.
//!
//! Generates the `.flat_map()` pipeline that applies a head scalar UDF declared
//! with `.extern fn` to columns in a rule head.  
//! Body UDF predicates (boolean filters) are handled separately by the planner/compiler
//! predicate pipeline.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Generate a `.flat_map()` pipeline that applies a head scalar UDF to a row.
///
/// The row has `output_arity` columns in flattened form. Columns at
/// positions `start..end` are the UDF's input arguments; they get replaced
/// by a single output column produced by `udf::fn_name(col_start, …, col_end-1)`.
#[must_use]
pub(crate) fn head_udf_map(
    fn_name: &str,
    start: usize,
    end: usize,
    output_arity: usize,
) -> TokenStream {
    let fn_ident = format_ident!("{}", fn_name);

    // Row destructuring: (x0, x1, …, x_{arity-1})
    let row_fields: Vec<_> = (0..output_arity).map(|i| format_ident!("x{}", i)).collect();
    let row_pat = match row_fields.len() {
        1 => {
            let f = &row_fields[0];
            quote! { (#f,) }
        }
        _ => quote! { (#(#row_fields),*) },
    };

    // UDF arguments: x_start, x_{start+1}, …, x_{end-1}
    let udf_args: Vec<_> = row_fields[start..end].iter().collect();

    // Output tuple: fields before start, udf result, fields after end
    let mut out_parts: Vec<TokenStream> = Vec::with_capacity(output_arity - (end - start) + 1);
    for f in &row_fields[..start] {
        out_parts.push(quote! { #f });
    }
    out_parts.push(quote! { udf_out });
    for f in &row_fields[end..] {
        out_parts.push(quote! { #f });
    }
    let out_expr = match out_parts.len() {
        1 => {
            let f = &out_parts[0];
            quote! { (#f,) }
        }
        _ => quote! { (#(#out_parts),*) },
    };

    quote! {
        .flat_map(|#row_pat| {
            let udf_out = udf::#fn_ident(#(#udf_args),*);
            std::iter::once(#out_expr)
        })
    }
}
