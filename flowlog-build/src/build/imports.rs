//! `use` statements emitted into the library-mode generated file.
//!
//! Every external crate reference is funneled through `::flowlog_runtime::`
//! so the consumer only needs `flowlog-runtime` in `[dependencies]`: DD,
//! timely, `lasso`, `ordered_float`, `serde` are all re-exported from
//! there.

use proc_macro2::TokenStream;
use quote::quote;

use crate::codegen::Features;

/// Emit every import the generated library-mode module needs.
pub(crate) fn gen_lib_imports(
    relops_body: &TokenStream,
    features: &Features,
    profile: bool,
) -> TokenStream {
    let mut out = vec![quote! {
        mod relops {
            #relops_body
        }
        use relops::*;
        use std::sync::{Arc, Mutex};
        use std::rc::Rc;
        use std::cell::RefCell;
    }];

    out.push(dd_imports(features));

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
    if f.recursive() {
        out.push(quote! {
            use ::flowlog_runtime::differential_dataflow::operators::iterate::Variable;
        });
    }

    quote! { #(#out)* }
}
