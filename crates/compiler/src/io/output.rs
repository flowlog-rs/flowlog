//! Inspector codegen — buffer, merge, and write IDB output.
//!
//! Both stdout and file output use the same pipeline:
//!   inspect → per-worker buffer → flush → merge (ORDER BY / LIMIT) → write
//!
//! Buffers store `(data, time, diff)` triples. Batch mode hardcodes
//! `diff = 1` (DD uses `Present`, not `i32`). Sort operates on data only.

use crate::ty::data::data_type_tokens;
use crate::Compiler;

use parser::{DataType, Relation};
use profiler::{with_profiler, Profiler};

use proc_macro2::{Ident, Span, TokenStream};
use quote::quote;
use syn::{Index, LitStr};

// =========================================================================
// Output struct
// =========================================================================

/// Code fragments spliced into the generated `main()`.
pub(crate) struct InspectorCodegen {
    pub buf_declarations: Vec<TokenStream>, // before timely::execute
    pub buf_clones: Vec<TokenStream>,       // closure capture
    pub local_decls: Vec<TokenStream>,      // worker body, before dataflow
    pub inspect_stmts: Vec<TokenStream>,    // inside dataflow
    pub flush_stmts: Vec<TokenStream>,      // before barrier (all workers)
    pub merge_stmts: Vec<TokenStream>,      // after barrier (worker 0 only)
}

// =========================================================================
// Orchestration
// =========================================================================

impl Compiler {
    /// Walk IDB relations → fill [`InspectorCodegen`].
    pub(crate) fn collect_inspectors(
        &mut self,
        profiler: &mut Option<Profiler>,
    ) -> InspectorCodegen {
        let mut cg = InspectorCodegen {
            buf_declarations: Vec::new(),
            buf_clones: Vec::new(),
            local_decls: Vec::new(),
            inspect_stmts: Vec::new(),
            flush_stmts: Vec::new(),
            merge_stmts: Vec::new(),
        };

        with_profiler(profiler, |p| p.update_inspect_block());

        for idb in self.program.idbs() {
            let var = self.find_global_ident(idb.fingerprint());
            let name = idb.name();

            if idb.printsize() {
                self.features.mark_as_collection();
                self.features.mark_timely_map();
                cg.inspect_stmts
                    .push(self.gen_size_inspector(&var, name, profiler));
            }

            if idb.data_type().contains(&DataType::Float32)
                || idb.data_type().contains(&DataType::Float64)
            {
                self.features.mark_ordered_float();
            }

            if idb.output() {
                if idb.data_type().contains(&DataType::String) {
                    self.features.mark_string_resolve();
                }

                self.features.mark_output_buffers();

                let to_stdout = self.config.output_to_stdout();

                if to_stdout {
                    with_profiler(profiler, |p| {
                        p.inspect_content_terminal_operator(name.to_string(), name.to_string());
                    });
                } else {
                    with_profiler(profiler, |p| {
                        p.inspect_content_file_operator(name.to_string(), name.to_string());
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

                let (sink_preamble, write_row) = if to_stdout {
                    (
                        self.gen_stderr_preamble(),
                        self.gen_write_row_stderr(name, idb),
                    )
                } else {
                    let parent_dir = self.config.output_dir().expect(
                        "Compiler error: output directory must be provided when writing IDB output to files",
                    );
                    (
                        self.gen_file_preamble(name, parent_dir),
                        self.gen_write_row_file(idb),
                    )
                };

                cg.merge_stmts.push(self.gen_merge_from_memory(
                    &buf_ident,
                    idb,
                    sink_preamble,
                    write_row,
                ));
            }
        }

        cg
    }
}

// =========================================================================
// Printsize
// =========================================================================

impl Compiler {
    /// `.printsize` — consolidate into a single key, inspect the multiplicity.
    ///
    /// Datalog-batch: `.consolidate()` dedup.  Others: `threshold_i32()` first.
    fn gen_size_inspector(
        &self,
        var: &Ident,
        name: &str,
        profiler: &mut Option<Profiler>,
    ) -> TokenStream {
        let prefix = name.to_string();
        let maybe_probe = self.maybe_probe_incremental();

        with_profiler(profiler, |p| {
            p.inspect_size_operator(prefix.clone(), prefix.clone());
        });

        if self.config.is_datalog_batch() {
            quote! {{
                #var.clone()
                    .consolidate()
                    .inner
                    .flat_map(move |(_, t, _)| std::iter::once(((), t.clone(), 1_i32)))
                    .as_collection()
                    .map(|_| ())
                    .consolidate()
                    .inspect(|(_data, time, size)| eprintln!("[size][{}] t={:?} size={:?}", #prefix, time, size))
                    #maybe_probe;
            }}
        } else {
            quote! {{
                #var.clone()
                    .threshold(|_, w| if *w > 0 { 1i32 } else { 0 })
                    .inner
                    .flat_map(move |(_, t, d)| std::iter::once(((), t.clone(), d)))
                    .as_collection()
                    .map(|_| ())
                    .consolidate()
                    .inspect(|(_data, time, diff)| eprintln!("[size][{}] t={:?} size_diff={:?}", #prefix, time, diff))
                    #maybe_probe;
            }}
        }
    }
}

// =========================================================================
// Buffer lifecycle
// =========================================================================

impl Compiler {
    /// Shared buffer: `Arc<Mutex<Vec<Vec<T>>>>`.
    /// Worker 0 drains after barrier.
    fn gen_buf_declaration(&self, name: &str, idb: &Relation) -> (TokenStream, TokenStream, Ident) {
        let buf_ident = Ident::new(&format!("buf_{}", name), Span::call_site());
        let inner_ty = self.buf_element_type(idb);

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
        let maybe_probe = self.maybe_probe_incremental();
        let maybe_consolidate = self.maybe_consolidate_incremental_output();
        let local_ident = Ident::new(&format!("local_{}", idb.name()), Span::call_site());

        // Batch DD uses `Present` for diff — hardcode 1_i32.
        let (inspect_pattern, push_stmt) = match (idb.arity() == 0, self.config.is_batch()) {
            (true, true) => (
                quote! { (_data, time, _diff) },
                quote! { #local_ident.borrow_mut().push(((), time.clone(), 1_i32)); },
            ),
            (true, false) => (
                quote! { (_data, time, diff) },
                quote! { #local_ident.borrow_mut().push(((), time.clone(), *diff)); },
            ),
            (false, true) => (
                quote! { (data, time, _diff) },
                quote! { #local_ident.borrow_mut().push((data.clone(), time.clone(), 1_i32)); },
            ),
            (false, false) => (
                quote! { (data, time, diff) },
                quote! { #local_ident.borrow_mut().push((data.clone(), time.clone(), *diff)); },
            ),
        };

        let inner_ty = self.buf_element_type(idb);

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
// Merge & sort
// =========================================================================

impl Compiler {
    /// Drain worker buffers → optional ORDER BY / LIMIT → write to sink.
    ///
    /// - No ORDER BY: stream buffers directly.
    /// - ORDER BY: sort per-worker, k-way merge (k = workers, linear scan).
    /// - ORDER BY + LIMIT: flatten, `select_nth_unstable_by` O(n), sort top-k.
    fn gen_merge_from_memory(
        &self,
        buf_ident: &Ident,
        idb: &Relation,
        sink_preamble: TokenStream,
        write_row: TokenStream,
    ) -> TokenStream {
        let order_by = idb.output_order_by();
        let limit = idb.output_limit();

        match (order_by.as_ref(), limit) {
            (None, _) => {
                quote! {{
                    #sink_preamble
                    {
                        use std::io::Write as _;
                        for worker_buf in #buf_ident.lock().unwrap().drain(..) {
                            for row in &worker_buf {
                                #write_row
                            }
                        }
                    }
                }}
            }

            (Some(spec), None) => {
                let comparators = self.gen_order_comparators(spec);
                let elem_ty = self.buf_element_type(idb);
                quote! {{
                    let mut bufs = #buf_ident.lock().unwrap();
                    for buf in bufs.iter_mut() {
                        buf.sort_by(|a, b| {
                            #(#comparators)*
                            std::cmp::Ordering::Equal
                        });
                    }
                    #sink_preamble
                    {
                        use std::io::Write as _;
                        let cmp_fn = |a: &#elem_ty, b: &#elem_ty| -> std::cmp::Ordering {
                            #(#comparators)*
                            std::cmp::Ordering::Equal
                        };
                        let k = bufs.len();
                        let mut pos = vec![0usize; k];
                        loop {
                            let mut best: Option<usize> = None;
                            for i in 0..k {
                                if pos[i] < bufs[i].len() {
                                    if let Some(bi) = best {
                                        if cmp_fn(&bufs[i][pos[i]], &bufs[bi][pos[bi]]).is_lt() {
                                            best = Some(i);
                                        }
                                    } else {
                                        best = Some(i);
                                    }
                                }
                            }
                            match best {
                                Some(i) => {
                                    let row = &bufs[i][pos[i]];
                                    #write_row
                                    pos[i] += 1;
                                }
                                None => break,
                            }
                        }
                    // Clear inner per-worker buffers for reuse in incremental mode.
                    // The outer `bufs` Vec is fixed-size (one slot per worker) and does not grow.
                    for buf in bufs.iter_mut() { buf.clear(); }
                    }
                }}
            }

            (Some(spec), Some(n)) => {
                let comparators = self.gen_order_comparators(spec);
                let comparators2 = comparators.clone();
                quote! {{
                    let mut all: Vec<_> = #buf_ident.lock().unwrap()
                        .drain(..).flatten().collect();
                    if all.len() > #n {
                        all.select_nth_unstable_by(#n, |a, b| {
                            #(#comparators)*
                            std::cmp::Ordering::Equal
                        });
                        all.truncate(#n);
                    }
                    all.sort_by(|a, b| {
                        #(#comparators2)*
                        std::cmp::Ordering::Equal
                    });
                    #sink_preamble
                    {
                        use std::io::Write as _;
                        for row in &all {
                            #write_row
                        }
                    }
                }}
            }
        }
    }

    /// Comparator chain for ORDER BY — compares data fields only (not time/diff).
    fn gen_order_comparators(&self, spec: &[(usize, DataType, bool)]) -> Vec<TokenStream> {
        spec.iter()
            .map(|(col_idx, data_type, ascending)| {
                let idx = Index::from(*col_idx);
                let a_expr = self.field_accessor(&idx, data_type, quote! { a });
                let b_expr = self.field_accessor(&idx, data_type, quote! { b });
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
}

// =========================================================================
// Row formatters & sink preambles
// =========================================================================

impl Compiler {
    /// File sink: `BufWriter<File>`. Incremental filenames include `_t{timestamp}`.
    fn gen_file_preamble(&self, name: &str, base_path: &str) -> TokenStream {
        let base_dir = base_path.to_string();
        let rel_name = name.to_string();

        let out_path = if self.config.is_incremental() {
            quote! { let out_path = format!("{}/{}_t{}", #base_dir, #rel_name, time_stamp); }
        } else {
            quote! { let out_path = format!("{}/{}", #base_dir, #rel_name); }
        };

        quote! {
            #out_path
            let mut out = std::io::BufWriter::new(
                std::fs::File::create(&out_path)
                    .unwrap_or_else(|e| panic!("failed to create {}: {}", out_path, e))
            );
        }
    }

    /// Stderr sink.
    fn gen_stderr_preamble(&self) -> TokenStream {
        quote! {
            let mut out = std::io::stderr();
        }
    }

    /// File format — batch: data only. Incremental: data + diff.
    fn gen_write_row_file(&self, idb: &Relation) -> TokenStream {
        if idb.arity() == 0 {
            return quote! {
                let _ = &row;
                writeln!(out, "True").expect("write failed");
            };
        }

        let data_accessors: Vec<TokenStream> = idb
            .data_type()
            .iter()
            .enumerate()
            .map(|(i, dt)| self.field_accessor(&Index::from(i), dt, quote! { row }))
            .collect();

        if self.config.is_incremental() {
            let mut parts = vec!["{}"; idb.arity()];
            parts.push("{:+}");
            let fmt = parts.join(idb.output_delimiter());
            let fmt = LitStr::new(&fmt, Span::call_site());
            quote! {
                writeln!(out, #fmt #(, #data_accessors )*, row.2).expect("write failed");
            }
        } else {
            let fmt = vec!["{}"; idb.arity()].join(idb.output_delimiter());
            let fmt = LitStr::new(&fmt, Span::call_site());
            quote! {
                writeln!(out, #fmt #(, #data_accessors )*).expect("write failed");
            }
        }
    }

    /// Stderr format — always includes time and diff.
    fn gen_write_row_stderr(&self, name: &str, idb: &Relation) -> TokenStream {
        let prefix = name.to_string();

        if idb.arity() == 0 {
            return quote! {
                writeln!(out, "[tuple][{}]  t={:?}  True  diff={:+}",
                    #prefix, row.1, row.2)
                    .expect("write failed");
            };
        }

        let field_displays: Vec<TokenStream> = idb
            .data_type()
            .iter()
            .enumerate()
            .map(|(i, dt)| self.field_accessor(&Index::from(i), dt, quote! { row }))
            .collect();
        let fmt_str = (0..idb.arity())
            .map(|_| "{:?}")
            .collect::<Vec<_>>()
            .join(", ");
        let fmt_full = format!(
            "[tuple][{}]  t={{:?}}  data=({})  diff={{:+}}",
            prefix, fmt_str
        );
        let fmt_lit = LitStr::new(&fmt_full, Span::call_site());
        quote! {
            writeln!(out, #fmt_lit, row.1 #(, #field_displays )*, row.2)
                .expect("write failed");
        }
    }
}

// =========================================================================
// Type helpers
// =========================================================================

impl Compiler {
    /// Incremental: `.consolidate()` before inspect to show net delta.
    fn maybe_consolidate_incremental_output(&self) -> TokenStream {
        if self.config.is_incremental() {
            quote! { .consolidate() }
        } else {
            quote! {}
        }
    }

    /// Incremental: `.probe_with(&mut probe)` for progress tracking.
    fn maybe_probe_incremental(&self) -> TokenStream {
        if self.config.is_incremental() {
            quote! { .probe_with(&mut probe) }
        } else {
            quote! {}
        }
    }

    /// Buffer element: `(data_tuple, timestamp, diff)`.
    fn buf_element_type(&self, idb: &Relation) -> TokenStream {
        let tuple_ty = data_type_tokens(&idb.data_type(), self.features.string_intern());
        let time_ty = self.outer_time_type();
        quote! { (#tuple_ty, #time_ty, i32) }
    }

    /// Access column `idx` from buffer row's data tuple at `var.0`.
    /// String-interned columns are wrapped in `resolve()`.
    fn field_accessor(&self, idx: &Index, data_type: &DataType, var: TokenStream) -> TokenStream {
        let base = quote! { #var.0.#idx };
        if matches!(data_type, DataType::String) && self.features.string_intern() {
            quote! { resolve(#base) }
        } else {
            base
        }
    }
}
