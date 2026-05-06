//! `use` statements emitted into the library-mode generated file.
//!
//! Every external crate reference is funneled through `::flowlog_runtime::`
//! so the consumer only needs `flowlog-runtime` in `[dependencies]` — DD,
//! timely, `lasso`, `ordered_float`, `serde` are all re-exported from
//! there.

use proc_macro2::TokenStream;
use quote::{format_ident, quote};

use crate::codegen::Features;

/// Emit every import the generated library-mode module needs, including the
/// private `mod relops { … }` wrapper that encapsulates the input-handler
/// types.
pub(crate) fn gen_lib_imports(
    relops_body: &TokenStream,
    features: &Features,
    profile: bool,
) -> TokenStream {
    let ordered_float_import = features
        .ordered_float()
        .then(|| quote! { use ::flowlog_runtime::ordered_float; });
    let lasso_import = features
        .string_intern()
        .then(|| quote! { use ::flowlog_runtime::lasso; });

    let mut out = vec![quote! {
        mod relops {
            use ::flowlog_runtime::differential_dataflow;
            #ordered_float_import
            #lasso_import
            #relops_body
        }
        use relops::*;
        use std::sync::{Arc, Mutex};
        use std::rc::Rc;
        use std::cell::RefCell;
    }];

    out.push(dd_imports(features));

    if features.timely_map() {
        out.push(quote! { use ::flowlog_runtime::timely::dataflow::operators::vec::Map; });
    }

    out.push(string_intern_imports(features));
    if features.ordered_float() {
        out.push(quote! { use ::flowlog_runtime::ordered_float::OrderedFloat; });
    }

    out.push(profile_imports(profile));

    quote! { #(#out)* }
}

/// Items the generated `OpStats` / `DdArrangeStats` structs and their
/// loggers reference unqualified — kept conditional so non-profile builds
/// don't drag in `HashMap` / `File` / timely+DD logging for nothing.
fn profile_imports(profile: bool) -> TokenStream {
    if !profile {
        return quote! {};
    }
    quote! {
        use std::collections::HashMap;
        use std::fs::File;
        use std::io::{BufWriter, Write};
        use std::time::Duration;
        use ::flowlog_runtime::timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
        use ::flowlog_runtime::differential_dataflow::logging::{
            DifferentialEvent, DifferentialEventBuilder,
        };
    }
}

/// DD + timely `use` lines, conditioned on which features the generated
/// code actually exercised.
fn dd_imports(f: &Features) -> TokenStream {
    let mut out = Vec::new();

    if f.dd_input() {
        out.push(quote! { use ::flowlog_runtime::differential_dataflow::input::Input; });
    }
    if f.threshold_total() {
        out.push(
            quote! { use ::flowlog_runtime::differential_dataflow::operators::ThresholdTotal; },
        );
    }
    if f.as_collection() {
        out.push(quote! { use ::flowlog_runtime::differential_dataflow::AsCollection; });
    }
    if f.recursive() {
        out.push(quote! {
            use ::flowlog_runtime::differential_dataflow::operators::iterate::Variable;
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

        let uses = entries.iter().map(|(mod_name, ty_name)| {
            let mod_ident = format_ident!("{}", mod_name);
            let ty = format_ident!("{}", ty_name);
            quote! { use semiring::#mod_ident::#ty; }
        });

        out.push(quote! {
            #(#uses)*
            use ::flowlog_runtime::differential_dataflow::difference::IsZero;
        });
    }

    quote! { #(#out)* }
}

/// `intern` / `resolve` / `Spur` imports; empty when interning is off.
fn string_intern_imports(f: &Features) -> TokenStream {
    if !f.string_intern() {
        return quote! {};
    }

    let base = quote! {
        use ::flowlog_runtime::lasso::Spur;
        use ::flowlog_runtime::intern::intern;
    };

    let resolve = f
        .string_resolve()
        .then(|| quote! { use ::flowlog_runtime::intern::resolve; });

    quote! { #base #resolve }
}
