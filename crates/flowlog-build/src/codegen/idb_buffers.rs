//! Per-IDB output buffers + drain codegen.
//!
//! Pipeline: inspect → per-worker buffer → flush → drain (optional
//! ORDER BY / LIMIT) → sink.
//!
//! Buffers store `(data, time, diff)` triples. Batch mode hardcodes
//! `diff = 1` (DD uses `Present`, not `i32`). Sort operates on data only.

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::Index;

use crate::codegen::CodeGen;
use crate::codegen::ty::tuple_type;
use crate::parser::{DataType, Relation};
use crate::profiler::{Profiler, with_profiler};

// =========================================================================
// Output struct
// =========================================================================

/// Per-IDB buffer machinery spliced into the generated `main()`.
#[derive(Default)]
pub(crate) struct InspectorCodegen {
    pub buf_declarations: Vec<TokenStream>, // before timely::execute
    pub buf_clones: Vec<TokenStream>,       // closure capture
    pub local_decls: Vec<TokenStream>,      // worker body, before dataflow
    pub inspect_stmts: Vec<TokenStream>,    // inside dataflow
    pub flush_stmts: Vec<TokenStream>,      // before barrier (all workers)
    pub size_cell_decls: Vec<TokenStream>,  // `.printsize` size cells, before execute
    pub size_cell_clones: Vec<TokenStream>, // size cell closure capture
}

// =========================================================================
// Orchestration
// =========================================================================

impl CodeGen {
    /// Walk IDB relations → fill [`InspectorCodegen`].
    pub(crate) fn collect_inspectors(
        &mut self,
        profiler: &mut Option<Profiler>,
    ) -> InspectorCodegen {
        let mut cg = InspectorCodegen::default();

        with_profiler(profiler, |p| p.update_inspect_block());

        for idb in self.program.idbs() {
            let var = self.find_global_ident(idb.fingerprint());
            let name = idb.name();
            let data_type = idb.data_type();

            if idb.printsize() {
                self.features.mark_as_collection();
                self.features.mark_timely_map();
                let cell_ident = Ident::new(&format!("size_{}", name), Span::call_site());
                cg.size_cell_decls.push(quote! {
                    let #cell_ident: std::sync::Arc<std::sync::Mutex<(Ts, i32)>> =
                        std::sync::Arc::new(std::sync::Mutex::new(<(Ts, i32)>::default()));
                });
                cg.size_cell_clones.push(quote! {
                    let #cell_ident = #cell_ident.clone();
                });
                cg.inspect_stmts.push(self.gen_size_inspector(
                    &var,
                    idb.raw_name(),
                    &cell_ident,
                    profiler,
                ));
            }

            if data_type
                .iter()
                .any(|dt| matches!(dt, DataType::Float32 | DataType::Float64))
            {
                self.features.mark_ordered_float();
            }

            if idb.output() {
                if data_type.contains(&DataType::String) {
                    self.features.mark_string_resolve_out();
                }

                self.features.mark_output_buffers();

                // The parallel file drain uses `rayon` (always) and `::itoa`
                // for integer columns and the incremental `{:+}` diff. The
                // scaffold gates both deps on these marks.
                if idb.uses_parallel_file_drain(self.config.output_to_stdout()) {
                    self.features.mark_parallel_output();
                    if data_type.iter().any(DataType::is_integer) || self.config.is_incremental() {
                        self.features.mark_itoa();
                    }
                }

                // Wiring (first arg) is the collection binding feeding the
                // sink; the label (second arg) is the human-facing name.
                if self.config.output_to_stdout() {
                    with_profiler(profiler, |p| {
                        p.inspect_content_terminal_operator(
                            var.to_string(),
                            idb.raw_name().to_string(),
                        );
                    });
                } else {
                    with_profiler(profiler, |p| {
                        p.inspect_content_file_operator(
                            var.to_string(),
                            idb.raw_name().to_string(),
                        );
                    });
                }

                let (buf_decl, buf_clone, buf_ident) = self.gen_buf_declaration(name, idb);
                cg.buf_declarations.push(buf_decl);
                cg.buf_clones.push(buf_clone);

                let (local_decl, inspect, flush) =
                    self.gen_write_inspector_mem(&var, &buf_ident, idb);
                cg.local_decls.push(local_decl);
                cg.inspect_stmts.push(inspect);
                cg.flush_stmts.push(flush);
            }
        }

        cg
    }
}

// =========================================================================
// Printsize
// =========================================================================

impl CodeGen {
    /// `.printsize` — consolidate into a single key, inspect the multiplicity.
    ///
    /// Datalog-batch: `.consolidate()` dedup.  Others: `threshold_i32()` first.
    fn gen_size_inspector(
        &self,
        var: &Ident,
        display: &str,
        cell_ident: &Ident,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let maybe_probe = if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        };

        // Wiring (first arg) is the collection binding feeding the sink;
        // the label (second arg) is the human-facing name.
        with_profiler(profiler, |p| {
            p.inspect_size_operator(var.to_string(), display.to_string());
        });

        // The inspect fires once per epoch with `size` = the epoch's delta
        // (batch: single epoch → final count). Always overwrite — the cell
        // reports the most recent epoch's size-delta. Downstream consumers
        // surface it to stderr / file / typed API on their own terms.
        let dedup = if self.config.is_datalog_batch() {
            quote! {
                .consolidate()
                .inner
                .flat_map(move |(_, t, _)| std::iter::once(((), t.clone(), 1_i32)))
            }
        } else {
            quote! {
                .threshold(|_, w| if *w > 0 { 1i32 } else { 0 })
                .inner
                .flat_map(move |(_, t, d)| std::iter::once(((), t.clone(), d)))
            }
        };

        quote! {{
            let #cell_ident = #cell_ident.clone();
            #var.clone()
                #dedup
                .as_collection()
                .map(|_| ())
                .consolidate()
                .inspect(move |(_data, time, size)| {
                    *#cell_ident.lock().unwrap() = (time.clone(), *size);
                })
                #maybe_probe;
        }}
    }
}

// =========================================================================
// Buffer lifecycle
// =========================================================================

impl CodeGen {
    /// Shared buffer: `Arc<Mutex<Vec<Vec<T>>>>`.
    /// Worker 0 drains after barrier.
    fn gen_buf_declaration(&self, name: &str, idb: &Relation) -> (TokenStream, TokenStream, Ident) {
        let buf_ident = Ident::new(&format!("buf_{}", name), Span::call_site());
        let inner_ty = tuple_type(idb, self.features.string_intern());

        let declaration = quote! {
            let #buf_ident: Arc<Mutex<Vec<Vec<#inner_ty>>>> =
                Arc::new(Mutex::new(Vec::new()));
        };

        let clone_stmt = quote! {
            let #buf_ident = #buf_ident.clone();
        };

        (declaration, clone_stmt, buf_ident)
    }

    /// Local buffer: `Rc<RefCell<Vec<T>>>` — lock-free hot-path writes.
    ///
    /// Flushed into the shared buffer once at barrier via `std::mem::take`.
    /// Returns `(local_decl, inspect_stmt, flush_stmt)`.
    fn gen_write_inspector_mem(
        &self,
        var: &Ident,
        buf_ident: &Ident,
        idb: &Relation,
    ) -> (TokenStream, TokenStream, TokenStream) {
        let (maybe_consolidate, maybe_probe) = if self.config.is_incremental() {
            (
                quote! { .consolidate() },
                quote! { .probe_with(&mut probe) },
            )
        } else {
            (quote! {}, quote! {})
        };
        let local_ident = Ident::new(&format!("local_{}", idb.name()), Span::call_site());

        // The four cases below are independent: arity==0 picks the data
        // half (unit-typed key vs cloneable tuple), is_batch picks the
        // diff half (DD's `Present` is hardcoded to 1_i32 in batch mode).
        let (data_pat, data_expr) = if idb.arity() == 0 {
            (quote! { _data }, quote! { () })
        } else {
            (quote! { data }, quote! { data.clone() })
        };
        let (diff_pat, diff_expr) = if self.config.is_batch() {
            (quote! { _diff }, quote! { 1_i32 })
        } else {
            (quote! { diff }, quote! { *diff })
        };
        let inspect_pattern = quote! { (#data_pat, time, #diff_pat) };
        let push_stmt = quote! {
            #local_ident
                .borrow_mut()
                .push((#data_expr, time.clone(), #diff_expr));
        };

        let inner_ty = tuple_type(idb, self.features.string_intern());

        let local_decl = quote! {
            let #local_ident: Rc<RefCell<Vec<#inner_ty>>> =
                Rc::new(RefCell::new(Vec::new()));
        };

        let inspect_stmt = quote! {{
            let #local_ident = #local_ident.clone();
            #var
                #maybe_consolidate
                .inspect(move |#inspect_pattern| {
                    #push_stmt
                })
                #maybe_probe;
        }};

        let flush_stmt = quote! {
            #buf_ident.lock().unwrap().push(std::mem::take(&mut *#local_ident.borrow_mut()));
        };

        (local_decl, inspect_stmt, flush_stmt)
    }
}

// =========================================================================
// Drain & merge
// =========================================================================

/// Emit the per-IDB drain block: pull worker buffers, optionally sort/limit,
/// then iterate with `write_row` against the sink set up by `sink_preamble`.
///
/// `write_row` runs once per row with `row: &(tuple, Ts, i32)` in scope.
/// `sink_postamble` runs once after the last row, with the preamble's
/// bindings still in scope — file sinks use it to `flush()` explicitly
/// (relying on `BufWriter::Drop` would silently swallow flush errors).
/// The block evaluates to `()`; callers that need to capture a value (e.g.
/// library mode pushing into a typed `Vec`) should pre-declare the target
/// binding outside the block and have `write_row` mutate it.
pub fn gen_drain_block(
    buf_ident: &Ident,
    idb: &Relation,
    sink_preamble: TokenStream,
    write_row: TokenStream,
    sink_postamble: TokenStream,
    string_intern: bool,
) -> TokenStream {
    let order_by = idb.output_order_by();
    let limit = idb.output_limit();
    let elem_ty = tuple_type(idb, string_intern);

    match (order_by.as_ref(), limit) {
        (None, _) => quote! {{
            #sink_preamble
            for worker_buf in #buf_ident.lock().unwrap().drain(..) {
                for row in &worker_buf {
                    #write_row
                }
            }
            #sink_postamble
        }},

        (Some(spec), None) => {
            let cmp_body_sort = order_comparators(spec, string_intern);
            let cmp_body_merge = cmp_body_sort.clone();
            quote! {{
                let mut per_worker: Vec<Vec<#elem_ty>> =
                    std::mem::take(&mut *#buf_ident.lock().unwrap());
                for buf in per_worker.iter_mut() {
                    buf.sort_by(|a: &#elem_ty, b: &#elem_ty| {
                        #(#cmp_body_sort)*
                        std::cmp::Ordering::Equal
                    });
                }
                #sink_preamble
                ::flowlog_runtime::sort::k_way_merge(
                    per_worker,
                    |a: &#elem_ty, b: &#elem_ty| {
                        #(#cmp_body_merge)*
                        std::cmp::Ordering::Equal
                    },
                    |val| {
                        let row = &val;
                        #write_row
                    },
                );
                #sink_postamble
            }}
        }

        (Some(spec), Some(n)) => {
            let cmp_body = order_comparators(spec, string_intern);
            quote! {{
                let all: Vec<#elem_ty> = #buf_ident.lock().unwrap()
                    .drain(..).flatten().collect();
                let all = ::flowlog_runtime::sort::topk(all, #n, |a: &#elem_ty, b: &#elem_ty| {
                    #(#cmp_body)*
                    std::cmp::Ordering::Equal
                });
                #sink_preamble
                for row in &all {
                    #write_row
                }
                #sink_postamble
            }}
        }
    }
}

// =========================================================================
// Column + comparator helpers
// =========================================================================

/// Access column `col_idx` of a buffer row. `base` must evaluate to the
/// `(tuple, Ts, i32)` triple — produces `<base>.0.<col_idx>` and wraps with
/// `resolve_out()` for interned-string columns.
///
/// Output runs after fixpoint, so interned strings resolve through the flat
/// snapshot path (`resolve_out`) rather than the concurrent `DashMap`
/// (`resolve`) used while the dataflow is still interning.
pub fn field_accessor(
    col_idx: usize,
    data_type: &DataType,
    base: TokenStream,
    string_intern: bool,
) -> TokenStream {
    let idx = Index::from(col_idx);
    let inner = quote! { #base.0.#idx };
    if matches!(data_type, DataType::String) && string_intern {
        quote! { resolve_out(#inner) }
    } else {
        inner
    }
}

/// Comparator chain for ORDER BY — emits a sequence of statements suitable
/// for a `sort_by(|a, b| { ... std::cmp::Ordering::Equal })` closure body.
/// Compares by data columns only; time and diff are ignored.
pub(crate) fn order_comparators(
    spec: &[(usize, DataType, bool)],
    string_intern: bool,
) -> Vec<TokenStream> {
    spec.iter()
        .map(|(col_idx, data_type, ascending)| {
            let a_expr = field_accessor(*col_idx, data_type, quote! { a }, string_intern);
            let b_expr = field_accessor(*col_idx, data_type, quote! { b }, string_intern);
            let cmp_expr = if *ascending {
                quote! { #a_expr.cmp(&#b_expr) }
            } else {
                quote! { #b_expr.cmp(&#a_expr) }
            };
            quote! {
                let cmp = #cmp_expr;
                if cmp != std::cmp::Ordering::Equal { return cmp; }
            }
        })
        .collect()
}
