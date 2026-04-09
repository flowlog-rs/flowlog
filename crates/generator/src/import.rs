//! Import code generation for the FlowLog compiler.
//!
//! Renders `use` statements and module declarations for the generated
//! program based on the active feature set in `Features`.

use common::INTERN_MAX_RETRIES;
use proc_macro2::TokenStream;
use quote::quote;

use super::Generator;
use crate::features::Features;

impl Generator {
    /// Emit all required imports as a single token stream.
    pub(crate) fn gen_imports(&self) -> TokenStream {
        let inc = self.config.is_incremental();
        let prof = self.config.profiling_enabled();
        let f = &self.features;

        let mut out = Vec::<TokenStream>::new();

        // -- relops (both modes) --
        out.push(quote! {
            mod relops;
            use relops::*;
            use std::collections::HashMap;
        });

        // -- incremental-only modules --
        if inc {
            out.push(quote! {
                mod cmd;
                mod prompt;

                use cmd::{Cmd, TxnAction, TxnOp, TxnState};
                use prompt::Prompt;

                use std::sync::{Arc, RwLock};
            });
        }

        // -- std --
        out.push(std_imports(inc, prof, f));

        // -- differential-dataflow --
        out.push(dd_imports(f));

        // -- timely --
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

        // -- allocator --
        out.push(quote! {
            use mimalloc::MiMalloc;

            #[global_allocator]
            static GLOBAL: MiMalloc = MiMalloc;
        });

        // -- library support --
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
}

// =========================================================================
// Grouped helpers
// =========================================================================

/// Standard library imports.
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

/// Differential-dataflow imports.
fn dd_imports(f: &Features) -> TokenStream {
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
            use timely::dataflow::Scope;
        });
    }
    if f.aggregation() {
        out.push(quote! {
            use differential_dataflow::trace::implementations::{ValBuilder, ValSpine};
        });
    }
    out.push(agg_semiring_imports(f));

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

/// Semiring module declaration and per-type `use` statements.
fn agg_semiring_imports(f: &Features) -> TokenStream {
    if !f.agg_semiring() {
        return quote! {};
    }
    // Sort for deterministic output order.
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
        mod semiring;
        #(#uses)*
        use differential_dataflow::difference::IsZero;
    }
}

/// String interning infrastructure (`lasso` crate).
fn string_intern_imports(f: &Features) -> TokenStream {
    if !f.string_intern() {
        return quote! {};
    }

    let max_retries = INTERN_MAX_RETRIES;
    let base = quote! {
        use lasso::{ThreadedRodeo, Spur};
        use std::sync::LazyLock;

        static INTERNER: LazyLock<ThreadedRodeo> = LazyLock::new(ThreadedRodeo::default);

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
                #max_retries,
                s
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

    quote! {
        #base
        #resolve
    }
}
