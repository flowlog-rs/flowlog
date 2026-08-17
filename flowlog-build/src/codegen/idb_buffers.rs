//! Per-IDB output buffers + drain codegen.
//!
//! Pipeline: inspect → per-worker buffer → flush → drain (optional
//! ORDER BY / LIMIT) → sink.
//!
//! Buffers store `(data, time, diff)` triples. Batch mode hardcodes
//! `diff = 1` (DD uses `Present`, not `i32`). Sort operates on data only.

use flowlog_parser::DataType;
use flowlog_parser::OutputSink;
use flowlog_parser::Relation;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::with_plan_graph;
use proc_macro2::Ident;
use proc_macro2::Span;
use proc_macro2::TokenStream;
use quote::quote;
use syn::Index;

use crate::codegen::CodeGen;
use crate::codegen::ty::tuple_type;

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
        plan_graph: &mut Option<PlanGraph>,
    ) -> InspectorCodegen {
        let mut cg = InspectorCodegen::default();

        with_plan_graph(plan_graph, |p| p.update_inspect_block());

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
                    plan_graph,
                ));
            }

            // Leaf-aware checks: a tuple column's float / string / integer
            // sub-fields need the same feature flags as a scalar column would.
            if data_type
                .iter()
                .any(|dt| dt.any_scalar(&DataType::is_float))
            {
                self.features.mark_ordered_float();
            }

            if idb.output() {
                self.features.mark_output_buffers();

                // Every file sink formats inside the runtime now, so the
                // only generated code still resolving interned strings is
                // the stderr sink and the ORDER BY comparators.
                if (self.config.output_to_stdout()
                    || idb.output_sink().is_some_and(|s| s.order_by().is_some()))
                    && data_type
                        .iter()
                        .any(|dt| dt.any_scalar(&|l| matches!(l, DataType::String)))
                {
                    self.features.mark_string_resolve_out();
                }

                // Wiring (first arg) is the collection binding feeding the
                // sink; the label (second arg) is the human-facing name.
                if self.config.output_to_stdout() {
                    with_plan_graph(plan_graph, |p| {
                        p.inspect_content_terminal_operator(
                            var.to_string(),
                            idb.raw_name().to_string(),
                        );
                    });
                } else {
                    with_plan_graph(plan_graph, |p| {
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
        plan_graph: &mut Option<PlanGraph>,
    ) -> TokenStream {
        let maybe_probe = if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        };

        // Wiring (first arg) is the collection binding feeding the sink;
        // the label (second arg) is the human-facing name.
        with_plan_graph(plan_graph, |p| {
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

/// Emit the per-IDB drain block: take the worker buffers and hand every
/// row to `write_row`, in whatever order the relation asked for.
///
/// `write_row` runs once per row with `tuple`, `time`, and `diff` in scope.
/// `sink_preamble` runs before the first row and `sink_postamble` after the
/// last, with the preamble's bindings still live; a file sink uses the
/// postamble to finish its writer, because letting `Drop` do it would
/// discard a failed tail write.
///
/// The ordering itself lives in the runtime. Only the comparator is
/// generated, because comparing column 3 descending needs the column's
/// type; how the sorted runs are then merged does not.
///
/// The block evaluates to `()`; a caller that needs a value (library mode
/// filling a typed `Vec`) declares the binding outside and has `write_row`
/// mutate it.
pub fn gen_drain_block(
    buf_ident: &Ident,
    idb: &Relation,
    sink_preamble: TokenStream,
    write_row: TokenStream,
    sink_postamble: TokenStream,
    string_intern: bool,
) -> TokenStream {
    let sink = idb.output_sink();
    let order_by = sink.and_then(OutputSink::order_by);
    let limit = sink.and_then(OutputSink::limit);
    let elem_ty = tuple_type(idb, string_intern);

    let take = quote! {
        let per_worker: Vec<Vec<#elem_ty>> =
            std::mem::take(&mut *#buf_ident.lock().expect("output buffer poisoned"));
    };
    let each = quote! { |tuple, time, diff| { #write_row } };

    let pump = match (order_by.as_ref(), limit) {
        (None, _) => quote! {
            ::flowlog_runtime::io::for_each_flat(per_worker, #each);
        },
        (Some(spec), limit) => {
            let cmp_body = order_comparators(spec, string_intern);
            let cmp = quote! {
                |a: &#elem_ty, b: &#elem_ty| {
                    #(#cmp_body)*
                    std::cmp::Ordering::Equal
                }
            };
            match limit {
                None => quote! {
                    ::flowlog_runtime::io::for_each_sorted(per_worker, #cmp, #each);
                },
                Some(n) => quote! {
                    ::flowlog_runtime::io::for_each_topk(per_worker, #n, #cmp, #each);
                },
            }
        }
    };

    quote! {{
        #take
        #sink_preamble
        #pump
        #sink_postamble
    }}
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
    // Resolve interned-string leaves so comparisons/formatting see the actual
    // strings. For a tuple column this descends every leaf: ORDER BY on a tuple
    // must compare resolved strings, not their (run-dependent) intern IDs.
    if string_intern && data_type.any_scalar(&|l| matches!(l, DataType::String)) {
        resolve_string_leaves(&inner, data_type)
    } else {
        inner
    }
}

/// Rebuild `access` with every interned-string leaf wrapped in `resolve_out`,
/// recursing through tuple columns. Non-string leaves pass through unchanged.
fn resolve_string_leaves(access: &TokenStream, data_type: &DataType) -> TokenStream {
    match data_type {
        DataType::String => quote! { resolve_out(#access) },
        DataType::FixedTuple(fields) => {
            let elems = fields.iter().enumerate().map(|(j, fdt)| {
                let jdx = Index::from(j);
                resolve_string_leaves(&quote! { (#access).#jdx }, fdt)
            });
            quote! { ( #(#elems,)* ) }
        }
        _ => access.clone(),
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
