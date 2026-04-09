//! Code fragment collection for the FlowLog code generator.
//!
//! Runs all code-generation passes and collects the resulting fragments
//! into [`AssemblyParts`].  This struct is the sole output of the
//! generator and contains everything a downstream compiler or build tool
//! needs to assemble a complete Rust program.

use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::HashSet;

use planner::StratumPlanner;
use profiler::{with_profiler, Profiler};

use crate::io::InspectorCodegen;
use crate::Generator;

/// Token-stream fragments and rendered source files produced by
/// [`Generator::generate`].
///
/// Every field is public so that downstream crates (`flowlog-compiler`,
/// `flowlog-build`, …) can freely consume or ignore individual pieces.
pub struct AssemblyParts {
    // -- dataflow graph --
    /// `let (handle, collection) = scope.new_collection::<_, Diff>();` per EDB.
    pub edb_inputs: Vec<TokenStream>,
    /// LHS pattern for destructuring the dataflow return value.
    pub handle_binding: TokenStream,
    /// Expression returned from `worker.dataflow(|scope| { ... })`.
    pub dataflow_return: TokenStream,
    /// Per-stratum transformation rules.
    pub flows: Vec<TokenStream>,

    // -- output pipeline --
    /// Shared output buffer declarations (one `Arc<Mutex<Vec>>` per output relation).
    pub output_bufs: Vec<TokenStream>,
    /// Clones of shared buffers moved into the worker closure.
    pub output_buf_clones: Vec<TokenStream>,
    /// Per-worker local buffer declarations (`Rc<RefCell<Vec>>`).
    pub local_bufs: Vec<TokenStream>,
    /// `inspect()` calls that push into local buffers.
    pub inspectors: Vec<TokenStream>,
    /// Drain local → shared buffer at end of each epoch.
    pub flush: Vec<TokenStream>,
    /// Merge shared buffers → sort → write to sink.
    pub merge: Vec<TokenStream>,

    // -- relops ingestion --
    /// `rels.insert(name, Box::new(RelXxx::new(handle)));` per EDB.
    pub registry_inserts: Vec<TokenStream>,
    /// `rels.get_mut(name).unwrap().apply_file(path, ...)` per file-backed EDB.
    pub file_ingests: Vec<TokenStream>,
    /// Conditional `let peers = worker.peers();` (only when file-backed EDBs exist).
    pub maybe_peers: TokenStream,
    /// Preload epoch block (file + inline facts, advance, flush, merge).
    pub preload: TokenStream,

    // -- profiling --
    /// Struct definitions for profiling (emitted at file level).
    pub profile_structs: TokenStream,
    /// Logger registration code (emitted inside worker closure).
    pub profile_init: TokenStream,
    /// Time profiling write-out code for batch mode.
    pub time_profile_write_batch: TokenStream,
    /// Time profiling write-out code for incremental mode.
    pub time_profile_write_incremental: TokenStream,
    /// Memory profiling write-out code for batch mode.
    pub memory_profile_write_batch: TokenStream,
    /// Memory profiling write-out code for incremental mode.
    pub memory_profile_write_incremental: TokenStream,

    // -- runtime helpers --
    /// `workers_from_args` + `worker_barrier_from_args` helper functions.
    pub worker_helpers: TokenStream,

    // -- file-level boilerplate --
    /// All `use` / `mod` statements for the generated `main.rs`.
    pub imports: TokenStream,
    /// Type aliases and constants for the `(Data, Diff, Time)` triple.
    pub type_declarations: TokenStream,

    // -- rendered project files --
    /// Content of `src/relops.rs`.
    pub relops_rs: String,
    /// Content of `Cargo.toml` for the generated crate.
    pub cargo_toml: String,
    /// Content of `.cargo/config.toml`.
    pub cargo_config: String,
    /// Rendered semiring module files: `(relative_path, content)`.
    /// Paths are relative to the `src/` directory (e.g. `"semiring/min_int.rs"`).
    pub semiring_modules: Vec<(String, String)>,
}

impl Generator {
    /// Run all code-generation passes and collect the resulting fragments
    /// into an [`AssemblyParts`].
    pub(crate) fn collect_parts(
        &mut self,
        strata: &[StratumPlanner],
        profiler: &mut Option<Profiler>,
    ) -> AssemblyParts {
        // Record entering main dataflow scope in profiler if enabled
        with_profiler(profiler, |profiler| {
            profiler.enter_scope();
        });

        // Static sections.
        let input_decls = self.gen_input_decls(profiler);
        let (lhs_binding, ret_expr) = self.gen_handle_binding();
        let time_profile_struct = self.gen_time_profile_struct();
        let memory_profile_struct = self.gen_memory_profile_struct();
        let time_profile_init = self.gen_time_profile_init();
        let memory_profile_init = self.gen_memory_profile_init();

        let worker_sync_helpers = quote! {
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
        };

        // Flow generation per stratum.
        let mut flows: Vec<TokenStream> = Vec::new();
        let mut calculated_output_fps: HashSet<u64> = HashSet::new();

        for (idx, stratum) in strata.iter().enumerate() {
            with_profiler(profiler, |profiler| {
                profiler.update_stratum_block(idx);
            });

            let (core_flows, non_recursive_arranged_map) =
                self.gen_non_recursive_core_flows(stratum, profiler);
            flows.extend(core_flows);

            if stratum.is_recursive() {
                flows.push(self.gen_recursive_block(
                    &non_recursive_arranged_map,
                    stratum,
                    profiler,
                ));
            } else {
                flows.extend(self.gen_non_recursive_post_flows(
                    &calculated_output_fps,
                    stratum,
                    profiler,
                ));
            }

            calculated_output_fps.extend(stratum.output_relations());
        }

        // Output inspectors.
        let InspectorCodegen {
            buf_declarations: output_bufs,
            buf_clones: output_buf_clones,
            local_decls: local_bufs,
            inspect_stmts: inspectors,
            flush_stmts: flush,
            merge_stmts: merge,
        } = self.collect_inspectors(profiler);

        // Relation registry.
        let registry_inserts: Vec<TokenStream> = self
            .program
            .edbs()
            .iter()
            .map(|rel| {
                let rel_name = rel.name().to_ascii_lowercase();
                let handle_ident =
                    Ident::new(&format!("h{rel_name}"), proc_macro2::Span::call_site());
                let ops_ty_ident =
                    Ident::new(&format!("Rel{rel_name}"), proc_macro2::Span::call_site());
                quote! {
                    rels.insert(
                        #rel_name.to_string(),
                        Box::new(#ops_ty_ident::new(#handle_ident)),
                    );
                }
            })
            .collect();

        // File ingestion.
        let has_file_backed_edbs = self
            .program
            .edbs()
            .iter()
            .any(|rel| rel.is_file_backed() && rel.arity() > 0);

        let has_inline_facts = !self.program.facts().is_empty();
        let needs_preload = has_file_backed_edbs || has_inline_facts;

        let maybe_peers = if has_file_backed_edbs {
            quote! { let peers = worker.peers(); }
        } else {
            quote! {}
        };

        let file_ingests: Vec<TokenStream> = self
            .program
            .edbs()
            .iter()
            .filter(|rel| rel.is_file_backed() && rel.arity() > 0)
            .map(|rel| {
                let rel_name = rel.name().to_ascii_lowercase();
                let file_name = rel.input_file_name();
                let path = self
                    .config
                    .fact_dir()
                    .map(|dir| {
                        std::path::Path::new(dir)
                            .join(&file_name)
                            .to_string_lossy()
                            .into_owned()
                    })
                    .unwrap_or_else(|| file_name);
                quote! {
                    rels.get_mut(#rel_name).unwrap()
                        .apply_file(std::path::Path::new(#path), SEMIRING_ONE, peers, index);
                }
            })
            .collect();

        // Preload epoch (only when there's data to preload).
        let preload = if needs_preload {
            quote! {
                #(#file_ingests)*
                for (_, r) in rels.iter_mut() {
                    r.apply_inline(index);
                }
                time_stamp += 1;
                for (_, r) in rels.iter_mut() {
                    r.advance_to(time_stamp);
                    r.flush();
                }
                while probe.less_than(&time_stamp) {
                    worker.step();
                }
                #(#flush)*
                barrier.wait();
                if index == 0 {
                    #(#merge)*
                }
                barrier.wait();
            }
        } else {
            quote! {}
        };

        let profile_structs = quote! {
            #time_profile_struct
            #memory_profile_struct
        };

        let profile_init = quote! {
            #time_profile_init
            #memory_profile_init
        };

        // -- Profile write code (mode-specific) --
        let time_profile_write_batch = self.gen_time_profile_write_batch();
        let time_profile_write_incremental = self.gen_time_profile_write_incremental();
        let memory_profile_write_batch = self.gen_memory_profile_write_batch();
        let memory_profile_write_incremental = self.gen_memory_profile_write_incremental();

        // -- File-level boilerplate (depends on features populated above) --
        let imports = self.gen_imports();
        let type_declarations = self.gen_type_declarations();

        // -- Rendered project files --
        let relops_rs = self.render_relops(self.program.edbs());
        let cargo_toml = self.render_cargo_toml();
        let cargo_config = self.render_cargo_config();
        let semiring_modules = self.render_semiring_modules();

        AssemblyParts {
            edb_inputs: input_decls,
            handle_binding: lhs_binding,
            dataflow_return: ret_expr,
            flows,
            output_bufs,
            output_buf_clones,
            local_bufs,
            inspectors,
            flush,
            merge,
            registry_inserts,
            file_ingests,
            maybe_peers,
            preload,
            profile_structs,
            profile_init,
            time_profile_write_batch,
            time_profile_write_incremental,
            memory_profile_write_batch,
            memory_profile_write_incremental,
            worker_helpers: worker_sync_helpers,
            imports,
            type_declarations,
            relops_rs,
            cargo_toml,
            cargo_config,
            semiring_modules,
        }
    }
}
