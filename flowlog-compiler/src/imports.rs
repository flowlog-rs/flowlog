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
        // Relation names may begin with `_` (DOOP's `_MethodLookup_*`), and
        // joined with their component prefix they synthesize binding idents
        // with consecutive underscores, which `non_snake_case` rejects.
        #![allow(non_snake_case)]
        // A comparison the program writes is emitted as written, also one
        // its column's type decides alone, such as `u >= 0` on a `uint64`,
        // which always holds.
        #![allow(unused_comparisons)]
        // Every other warning is a generator bug, and `-Dwarnings` keeps it
        // fatal (docs/dev/code.md rule 8).

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
