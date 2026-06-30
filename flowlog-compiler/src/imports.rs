//! `use` statements emitted into the generated binary's `main.rs`.
//!
//! All non-stdlib references must resolve against the dependencies declared
//! in [`crate::scaffold::render_cargo_toml`] — keep the two in sync.

use proc_macro2::TokenStream;
use quote::quote;

use flowlog_build::Features;
use flowlog_build::common::{Config, INTERN_MAX_RETRIES};

pub(crate) fn gen_imports(config: &Config, features: &Features) -> TokenStream {
    let inc = config.is_incremental();
    let prof = config.profiling_enabled();
    let f = features;

    let mut out = Vec::<TokenStream>::new();

    out.push(quote! {
        // Mechanically generated dataflow routinely leaves intermediate
        // collection bindings unused — e.g. a relation declared (with `.input`
        // or inline facts) yet never referenced by any rule body, or a derived
        // collection whose only consumer is an output drain through a separate
        // handle. These are valid Datalog (Soufflé accepts them); relax just the
        // unused-variable lint on the generated binary while `-Dwarnings` keeps
        // every other lint class fatal.
        #![allow(unused_variables)]

        // Relation names may legally begin with `_` (DOOP's `basic._MethodLookup_*`);
        // joined with their component prefix they synthesize binding idents with
        // consecutive underscores, which `non_snake_case` rejects.
        #![allow(non_snake_case)]

        mod relation;
        use relation::*;
        use std::collections::HashMap;
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
            use differential_dataflow::logging::{DifferentialEvent, DifferentialEventBuilder};
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

pub(crate) fn gen_worker_helpers() -> TokenStream {
    quote! {
        fn workers_from_args(args: &[String]) -> usize {
            let mut i = 0;
            while i < args.len() {
                if args[i] == "-w" && i + 1 < args.len() {
                    if let Ok(n) = args[i + 1].parse::<usize>() {
                        return n.max(1);
                    }
                    i += 2;
                    continue;
                }
                if let Some(rest) = args[i].strip_prefix("-w=") {
                    if let Ok(n) = rest.parse::<usize>() {
                        return n.max(1);
                    }
                }
                i += 1;
            }
            1
        }

        fn worker_barrier_from_args(
            args: &[String],
        ) -> std::sync::Arc<std::sync::Barrier> {
            let workers = workers_from_args(args);
            std::sync::Arc::new(std::sync::Barrier::new(workers))
        }
    }
}

/// Imports brought into the generated binary's `relation` module so the
/// shared relation codegen can reference `shard_int` / `__byte_range_reader`
/// unqualified. The actual implementations live in the `flowlog` runtime
/// crate (same crate library mode pulls in).
pub(crate) fn gen_binary_relation_extras(
    program: &flowlog_build::parser::Program,
    features: &Features,
) -> TokenStream {
    let non_nullary_edbs: Vec<&flowlog_build::parser::Relation> = program
        .edbs()
        .iter()
        .filter(|r| r.arity() > 0)
        .copied()
        .collect();
    let first_cols: Vec<flowlog_build::parser::DataType> = non_nullary_edbs
        .iter()
        .filter_map(|r| r.data_type().first().cloned())
        .collect();
    let needs_int = first_cols
        .iter()
        .any(|dt| !matches!(dt, flowlog_build::parser::DataType::String));
    let has_string = first_cols
        .iter()
        .any(|dt| matches!(dt, flowlog_build::parser::DataType::String));
    let needs_str = has_string && !features.string_intern();
    let needs_spur = has_string && features.string_intern();
    let needs_byte_range = !non_nullary_edbs.is_empty();

    let mut idents: Vec<TokenStream> = Vec::new();
    if needs_int {
        idents.push(quote! { shard_int });
    }
    if needs_str {
        idents.push(quote! { shard_str });
    }
    if needs_spur {
        idents.push(quote! { shard_spur });
    }
    let shard_imports = if idents.is_empty() {
        quote! {}
    } else {
        quote! { use ::flowlog_runtime::io::{#(#idents),*}; }
    };

    let byte_range_import = if needs_byte_range {
        quote! { use ::flowlog_runtime::io::byte_range_reader as __byte_range_reader; }
    } else {
        quote! {}
    };

    quote! {
        #shard_imports
        #byte_range_import
    }
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
            use std::fs::File;
            use std::io::{BufWriter, Write};
            #output_buf
            use std::time::{Duration, Instant};
        };
    }

    let mut out = Vec::new();
    out.push(output_buffer_imports(inc, f.output_buffers()));
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
            let mod_ident = proc_macro2::Ident::new(mod_name, proc_macro2::Span::call_site());
            let ty = proc_macro2::Ident::new(ty_name, proc_macro2::Span::call_site());
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
    let max_retries = INTERN_MAX_RETRIES;
    let base = quote! {
        use lasso::{ThreadedRodeo, Spur};
        use rustc_hash::FxBuildHasher;
        use std::sync::LazyLock;

        // FxBuildHasher (not lasso's default SipHash): interner keys are
        // program-controlled, so the per-byte cost of a HashDoS-resistant
        // hash is pure overhead on every intern/resolve.
        static INTERNER: LazyLock<ThreadedRodeo<Spur, FxBuildHasher>> =
            LazyLock::new(|| ThreadedRodeo::with_hasher(FxBuildHasher));

        #[inline(always)]
        fn intern(s: &str) -> Spur {
            for _ in 0..#max_retries {
                match INTERNER.try_get_or_intern(s) {
                    Ok(key) => return key,
                    Err(_) => std::thread::yield_now(),
                }
            }
            panic!(
                "string interner failed after {} attempts for {:?}",
                #max_retries, s
            );
        }
    };

    let resolve = if f.string_resolve() {
        quote! {
            #[inline(always)]
            fn resolve(key: Spur) -> &'static str {
                INTERNER.resolve(&key)
            }
        }
    } else {
        quote! {}
    };

    // After fixpoint, interned strings resolve through a flat `Spur`-indexed
    // snapshot instead of the concurrent `DashMap` path (`resolve`), which
    // hashes and locks on every call. Built lazily; keys interned later fall
    // back to `resolve`.
    let resolve_out = if f.string_resolve_out() {
        quote! {
            use lasso::Key as _;

            static RESOLVED: std::sync::OnceLock<Box<[&'static str]>> =
                std::sync::OnceLock::new();

            #[inline]
            fn resolve_out(key: Spur) -> &'static str {
                let table = RESOLVED.get_or_init(|| {
                    let mut table: Vec<&'static str> = vec![""; INTERNER.len()];
                    for (k, s) in INTERNER.iter() {
                        table[k.into_usize()] = s;
                    }
                    table.into_boxed_slice()
                });
                match table.get(key.into_usize()) {
                    Some(&s) => s,
                    None => INTERNER.resolve(&key),
                }
            }
        }
    } else {
        quote! {}
    };

    quote! { #base #resolve #resolve_out }
}
