//! Library-mode import generation.

use generator::features::Features;
use proc_macro2::TokenStream;
use quote::{format_ident, quote};

/// Emit library-mode imports. All external crate references go through
/// `::flowlog_runtime::` so the user only needs `flowlog-runtime` in
/// `[dependencies]`.
pub(crate) fn gen_lib_imports(lib_relops: &TokenStream, features: &Features) -> TokenStream {
    let ordered_float_import = if features.ordered_float() {
        quote! { use ::flowlog_runtime::ordered_float; }
    } else {
        quote! {}
    };
    let lasso_import = if features.string_intern() {
        quote! { use ::flowlog_runtime::lasso; }
    } else {
        quote! {}
    };

    let mut out = vec![quote! {
        mod relops {
            use ::flowlog_runtime::differential_dataflow;
            #ordered_float_import
            #lasso_import
            #lib_relops
        }
        use relops::*;
        use std::sync::{Arc, Mutex};
        use std::rc::Rc;
        use std::cell::RefCell;
    }];

    // DD/timely imports — all prefixed with ::flowlog_runtime::
    out.push(lib_dd_imports(features));

    if features.timely_map() {
        out.push(quote! { use ::flowlog_runtime::timely::dataflow::operators::vec::Map; });
    }

    out.push(lib_string_intern_imports(features));
    if features.ordered_float() {
        out.push(quote! { use ::flowlog_runtime::ordered_float::OrderedFloat; });
    }

    quote! { #(#out)* }
}

fn lib_dd_imports(f: &Features) -> TokenStream {
    let mut out = Vec::new();

    if f.dd_input() {
        out.push(quote! { use ::flowlog_runtime::differential_dataflow::input::Input; });
    }
    if f.threshold_total() {
        out.push(quote! { use ::flowlog_runtime::differential_dataflow::operators::ThresholdTotal; });
    }
    if f.as_collection() {
        out.push(quote! { use ::flowlog_runtime::differential_dataflow::AsCollection; });
    }
    if f.recursive() {
        out.push(quote! {
            use ::flowlog_runtime::differential_dataflow::operators::iterate::Variable;
            use ::flowlog_runtime::timely::dataflow::Scope;
        });
    }
    if f.aggregation() {
        out.push(quote! {
            use ::flowlog_runtime::differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
        });
    }

    if f.agg_semiring() {
        // Semiring `use` statements — same as binary mode since the
        // `mod semiring` is injected by assembly.rs via `#[path]`.
        let semirings = f.agg_semirings();
        let mut entries: Vec<_> = semirings
            .iter()
            .map(|(op, dt)| {
                let mod_suffix = if dt.is_float() { "float" } else { "int" };
                (
                    format!("{}_{mod_suffix}", op.semiring_mod()),
                    format!("{}{}", op.semiring_prefix(), dt.semiring_suffix()),
                )
            })
            .collect();
        entries.sort();

        let uses: Vec<_> = entries
            .iter()
            .map(|(mod_name, ty_name)| {
                let mod_ident = format_ident!("{}", mod_name);
                let ty = format_ident!("{}", ty_name);
                quote! { use semiring::#mod_ident::#ty; }
            })
            .collect();

        out.push(quote! {
            #(#uses)*
            use ::flowlog_runtime::differential_dataflow::difference::IsZero;
        });
    }

    quote! { #(#out)* }
}

fn lib_string_intern_imports(f: &Features) -> TokenStream {
    if !f.string_intern() {
        return quote! {};
    }

    let base = quote! {
        use ::flowlog_runtime::lasso::Spur;
        use ::flowlog_runtime::intern::intern;
    };

    let resolve = if f.string_resolve() {
        quote! { use ::flowlog_runtime::intern::resolve; }
    } else {
        quote! {}
    };

    quote! { #base #resolve }
}
