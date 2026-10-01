//! `use` statements emitted into the generated binary's `main.rs`.
//!
//! All non-stdlib references must resolve against the dependencies declared
//! in [`crate::scaffold::render_cargo_toml`]; keep the two in sync.

use flowlog_common::Config;
use flowlog_parser::Program;
use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn gen_imports(config: &Config, program: &Program) -> TokenStream {
    let prof = config.profiling_enabled();
    let incremental = program.is_incremental();

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

    if incremental {
        out.push(quote! {
            mod cmd;
            mod prompt;
            use cmd::Cmd;
            use ::flowlog_runtime::txn::{TxnAction, TxnOp, TxnState};
            use prompt::Prompt;
            use std::sync::{Arc, RwLock};
        });
    }

    out.push(std_imports(prof));

    if incremental {
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

    if !program.udfs().is_empty() {
        out.push(quote! {
            #[allow(dead_code)]
            mod udf;
        });
    }

    quote! { #(#out)* }
}

fn std_imports(prof: bool) -> TokenStream {
    if prof {
        quote! {
            use std::cell::RefCell;
            use std::rc::Rc;
            use std::time::{Duration, Instant};
        }
    } else {
        quote! { use std::time::Instant; }
    }
}
