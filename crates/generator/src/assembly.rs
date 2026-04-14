//! Bundles per-pass fragments into [`AssemblyParts`].

use proc_macro2::TokenStream;
use quote::quote;
use std::collections::HashSet;

use planner::StratumPlanner;
use profiler::{with_profiler, Profiler};

use crate::io::InspectorCodegen;
use crate::Generator;

/// Token-stream fragments and rendered source files produced by
/// [`Generator::generate`]. All fields are `pub` so consumers can
/// pick the subset they need.
pub struct AssemblyParts {
    // -- dataflow graph --
    /// `let (handle, collection) = scope.new_collection::<_, Diff>();` per EDB.
    pub edb_decls: Vec<TokenStream>,
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
    /// `.printsize` size cell decls (`Arc<Mutex<i32>>`) before `timely::execute`.
    pub size_cell_decls: Vec<TokenStream>,
    /// Size cell clones moved into the worker closure.
    pub size_cell_clones: Vec<TokenStream>,

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

    /// Type aliases and constants for the `(Data, Diff, Time)` triple.
    pub type_declarations: TokenStream,

    /// Rendered semiring module files: `(relative_path, content)`.
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
        let edb_decls = self.gen_edb_decls(profiler);
        let (handle_binding, dataflow_return) = self.gen_handle_binding();
        let time_profile_struct = self.gen_time_profile_struct();
        let memory_profile_struct = self.gen_memory_profile_struct();
        let time_profile_init = self.gen_time_profile_init();
        let memory_profile_init = self.gen_memory_profile_init();

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
            size_cell_decls,
            size_cell_clones,
        } = self.collect_inspectors(profiler);

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

        let type_declarations = self.gen_type_declarations();
        let semiring_modules = self.render_semiring_modules();

        AssemblyParts {
            edb_decls,
            handle_binding,
            dataflow_return,
            flows,
            output_bufs,
            output_buf_clones,
            local_bufs,
            inspectors,
            flush,
            size_cell_decls,
            size_cell_clones,
            profile_structs,
            profile_init,
            time_profile_write_batch,
            time_profile_write_incremental,
            memory_profile_write_batch,
            memory_profile_write_incremental,
            type_declarations,
            semiring_modules,
        }
    }
}
