//! `use` statements emitted into the library-mode generated file.
//!
//! Every external crate reference is funneled through `::flowlog_runtime::`
//! so the consumer only needs `flowlog-runtime` in `[dependencies]`: DD,
//! timely, `lasso`, `ordered_float`, `serde` are all re-exported from
//! there.

use flowlog_parser::DataType;
use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::format_ident;
use quote::quote;

use crate::codegen::Features;

/// Emit every import the generated library-mode module needs, including the
/// private `mod relops { ... }` wrapper that encapsulates the input-handler
/// types.
pub(crate) fn gen_lib_imports(
    relops_body: &TokenStream,
    features: &Features,
    program: &Program,
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

    out.push(string_intern_imports(features, program));
    if features.ordered_float() {
        out.push(quote! { use ::flowlog_runtime::ordered_float::OrderedFloat; });
    }

    out.push(profile_imports(profile));

    quote! { #(#out)* }
}

/// Items the generated `OpMetrics` struct and its logger reference
/// unqualified; kept conditional so non-profile builds don't drag in
/// `HashMap` / timely logging for nothing. The metric tables write through
/// `flowlog_runtime::io::write_atomic`, so no `File`/`Write` import is needed.
fn profile_imports(profile: bool) -> TokenStream {
    if !profile {
        return quote! {};
    }
    quote! {
        use std::collections::HashMap;
        use std::time::Duration;
        use ::flowlog_runtime::timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
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
        // Semiring `use` statements: same as binary mode since the
        // `mod semiring` is injected by assembly.rs via `#[path]`.
        let semirings = f.agg_semirings();
        let mut entries: Vec<_> = semirings
            .iter()
            .map(|(semiring, dt)| {
                let mod_suffix = if dt.is_float() { "float" } else { "int" };
                // TODO: surface as CodegenError::internal instead of panicking.
                let suffix = dt
                    .semiring_suffix()
                    .expect("typechecker guarantees a numeric aggregation input");
                (
                    format!("{}_{mod_suffix}", semiring.module_stem()),
                    format!("{}{}", semiring.name(), suffix),
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
///
/// Bare `intern` appears where a flow builds a new string and in inline
/// string facts; loading interns inside the runtime's readers and drains
/// resolve through `EncodeField`, so each name follows its callers or the
/// generated crate's -Dwarnings build fails on it.
fn string_intern_imports(f: &Features, program: &Program) -> TokenStream {
    if !f.string_intern() {
        return quote! {};
    }

    let facts = program.facts();
    let any_inline_string = program.edbs().iter().any(|rel| {
        facts.get(rel.name()).is_some_and(|rows| !rows.is_empty())
            && rel
                .data_type()
                .iter()
                .any(|dt| matches!(dt, DataType::String))
    });

    let base = quote! { use ::flowlog_runtime::lasso::Spur; };

    let intern = (f.string_intern_calls() || any_inline_string)
        .then(|| quote! { use ::flowlog_runtime::intern::intern; });

    let resolve = f
        .string_resolve()
        .then(|| quote! { use ::flowlog_runtime::intern::resolve; });

    quote! { #base #intern #resolve }
}
