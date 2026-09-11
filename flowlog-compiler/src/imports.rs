//! `use` statements emitted into the generated binary's `main.rs`.
//!
//! All non-stdlib references must resolve against the dependencies declared
//! in [`crate::scaffold::render_cargo_toml`]; keep the two in sync.

use flowlog_build::Features;
use flowlog_common::Config;
use flowlog_common::ExecutionMode;
use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn gen_imports(config: &Config, features: &Features) -> TokenStream {
    let inc = config.mode() == ExecutionMode::Inc;
    let prof = config.profiling_enabled();
    let f = features;

    let mut out = Vec::<TokenStream>::new();

    out.push(quote! {
        // Mechanically generated dataflow routinely leaves intermediate
        // collection bindings unused, e.g. a relation declared (with `.input`
        // or inline facts) yet never referenced by any rule body, or a derived
        // collection whose only consumer is an output drain through a separate
        // handle. These are valid Datalog (Souffle accepts them); relax just the
        // unused-variable lint on the generated binary while `-Dwarnings` keeps
        // every other lint class fatal.
        #![allow(unused_variables)]

        // Relation names may legally begin with `_` (DOOP's `basic._MethodLookup_*`);
        // joined with their component prefix they synthesize binding idents with
        // consecutive underscores, which `non_snake_case` rejects.
        #![allow(non_snake_case)]

        mod relation;
        use relation::*;
    });

    if inc {
        out.push(quote! {
            mod cmd;
            mod prompt;
            use cmd::Cmd;
            use ::flowlog_runtime::txn::{TxnAction, TxnOp, TxnState};
            use prompt::Prompt;
            use std::sync::{Arc, RwLock};
        });
    }

    out.push(std_imports(inc, prof, f));
    out.push(dd_core_imports(f));

    if inc {
        out.push(quote! { use timely::dataflow::operators::probe::Handle as ProbeHandle; });
    }
    if prof {
        out.push(quote! {
            use std::collections::HashMap;
            use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
        });
    }

    out.push(quote! {
        use mimalloc::MiMalloc;
        #[global_allocator]
        static GLOBAL: MiMalloc = MiMalloc;
    });

    if f.ordered_float() {
        out.push(quote! { use ordered_float::OrderedFloat; });
    }
    if f.udf() {
        out.push(quote! {
            #[allow(dead_code)]
            mod udf;
        });
    }

    quote! { #(#out)* }
}

fn std_imports(inc: bool, prof: bool, f: &Features) -> TokenStream {
    if prof {
        let rc_refcell = if f.output_buffers() {
            quote! {}
        } else {
            quote! {
                use std::cell::RefCell;
                use std::rc::Rc;
            }
        };
        let output_buf = output_buffer_imports(inc, f.output_buffers());

        return quote! {
            #rc_refcell
            #output_buf
            use std::time::{Duration, Instant};
        };
    }

    let mut out = Vec::new();
    out.push(output_buffer_imports(inc, f.output_buffers()));
    out.push(quote! { use std::time::Instant; });
    quote! { #(#out)* }
}

fn dd_core_imports(f: &Features) -> TokenStream {
    let mut out = Vec::new();
    if f.dd_input() {
        out.push(quote! { use differential_dataflow::input::Input; });
    }
    if f.recursive() {
        out.push(quote! {
            use differential_dataflow::operators::iterate::Variable;
        });
    }
    quote! { #(#out)* }
}

fn output_buffer_imports(inc: bool, needed: bool) -> TokenStream {
    if !needed {
        return quote! {};
    }
    let sync = if inc {
        quote! { use std::sync::Mutex; }
    } else {
        quote! { use std::sync::{Arc, Mutex}; }
    };
    quote! {
        #sync
        use std::rc::Rc;
        use std::cell::RefCell;
    }
}
