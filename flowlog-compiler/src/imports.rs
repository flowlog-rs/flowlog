//! `use` statements emitted into the generated binary's `main.rs`.
//!
//! All non-stdlib references must resolve against the dependencies declared
//! in [`crate::scaffold::render_cargo_toml`]; keep the two in sync.

use flowlog_build::Features;
use flowlog_common::Config;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;

pub(crate) fn gen_imports(config: &Config, features: &Features) -> TokenStream {
    let inc = config.is_incremental();
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
            use ::flowlog_txn::cmd::TxnOp;
            use ::flowlog_txn::driver::{drive, follow, Event, SharedTxn};
            use ::flowlog_txn::Prompt;
        });
    }

    out.push(std_imports(prof, f));
    out.push(dd_imports(f));

    if f.timely_map() {
        out.push(quote! { use timely::dataflow::operators::vec::Map; });
    }
    if inc {
        out.push(quote! { use timely::dataflow::operators::probe::Handle as ProbeHandle; });
    }
    if prof {
        out.push(quote! {
            use timely::logging::{StartStop, TimelyEvent, TimelyEventBuilder};
        });
    }

    out.push(quote! {
        use mimalloc::MiMalloc;
        #[global_allocator]
        static GLOBAL: MiMalloc = MiMalloc;
    });

    out.push(string_intern_imports(f));
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

/// Imports brought into the generated binary's `relation` module so the
/// shared relation codegen can reference the runtime's ingest entry points
/// unqualified.
pub(crate) fn gen_binary_relation_extras(program: &flowlog_parser::Program) -> TokenStream {
    let needs_source = program.edbs().iter().any(|r| r.arity() > 0);

    // The ingest entry points are used by every non-nullary EDB's
    // `Ingest::load_file`; a program with none (nullary-only, or inline facts only)
    // must not import them, or the generated crate's -Dwarnings build
    // fails on the unused names. Runtime types are named by full path, so
    // nothing here needs an alias.
    // Nothing to import: the generated module names the runtime's loader
    // types by full path, and the `relation!` macro reaches everything else
    // through `$crate`.
    let _ = needs_source;
    let source_imports = quote! {};

    quote! {
        #source_imports
    }
}

fn std_imports(prof: bool, f: &Features) -> TokenStream {
    if prof {
        let rc_refcell = if f.output_buffers() {
            quote! {}
        } else {
            quote! {
                use std::cell::RefCell;
                use std::rc::Rc;
            }
        };
        let output_buf = output_buffer_imports(f.output_buffers());

        return quote! {
            #rc_refcell
            #output_buf
            use std::time::{Duration, Instant};
        };
    }

    let mut out = Vec::new();
    out.push(output_buffer_imports(f.output_buffers()));
    out.push(quote! { use std::time::Instant; });
    quote! { #(#out)* }
}

fn dd_imports(f: &Features) -> TokenStream {
    let core = dd_core_imports(f);
    let semiring = agg_semiring_imports(f);
    quote! { #core #semiring }
}

fn dd_core_imports(f: &Features) -> TokenStream {
    let mut out = Vec::new();
    if f.dd_input() {
        out.push(quote! { use differential_dataflow::input::Input; });
    }
    if f.threshold_total() {
        out.push(quote! { use differential_dataflow::operators::ThresholdTotal; });
    }
    if f.as_collection() {
        out.push(quote! { use differential_dataflow::AsCollection; });
    }
    if f.recursive() {
        out.push(quote! {
            use differential_dataflow::operators::iterate::Variable;
        });
    }
    if f.aggregation() {
        out.push(quote! {
            use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
        });
    }
    quote! { #(#out)* }
}

fn output_buffer_imports(needed: bool) -> TokenStream {
    if !needed {
        return quote! {};
    }
    quote! {
        use std::sync::{Arc, Mutex};
        use std::rc::Rc;
        use std::cell::RefCell;
    }
}

fn agg_semiring_imports(f: &Features) -> TokenStream {
    if !f.agg_semiring() {
        return quote! {};
    }
    let uses = agg_semiring_uses_only(f);
    quote! { mod semiring; #uses }
}

fn agg_semiring_uses_only(f: &Features) -> TokenStream {
    if !f.agg_semiring() {
        return quote! {};
    }
    let mut entries: Vec<_> = f
        .agg_semirings()
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

    let uses: Vec<_> = entries
        .iter()
        .map(|(mod_name, ty_name)| {
            let mod_ident = Ident::new(mod_name, Span::call_site());
            let ty = Ident::new(ty_name, Span::call_site());
            quote! { use semiring::#mod_ident::#ty; }
        })
        .collect();

    quote! {
        #(#uses)*
        use differential_dataflow::difference::IsZero;
    }
}

fn string_intern_imports(f: &Features) -> TokenStream {
    if !f.string_intern() {
        return quote! {};
    }

    // The runtime owns the interner, and there must be exactly one: a
    // `Spur` only means anything against the pool that issued it. Binary
    // mode used to define its own pool, so a string a reader interned and
    // one this program computed could never compare equal.
    let resolve = if f.string_resolve() {
        quote! { use ::flowlog_runtime::intern::resolve; }
    } else {
        quote! {}
    };
    let resolve_out = if f.string_resolve_out() {
        quote! { use ::flowlog_runtime::intern::resolve_out; }
    } else {
        quote! {}
    };

    // `Spur` names the interned type in every string-typed alias; bare
    // `intern` appears only where a flow builds a new string (cat, substr,
    // to_string, string UDF results), so its import follows that marker or
    // the generated crate's -Dwarnings build fails on the unused name.
    let intern = if f.string_intern_calls() {
        quote! { use ::flowlog_runtime::intern::intern; }
    } else {
        quote! {}
    };
    quote! {
        use lasso::Spur;
        #intern
        #resolve
        #resolve_out
    }
}
