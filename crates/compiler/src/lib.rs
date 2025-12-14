//! FlowLog Compiler Library
//! Generates executable Rust code from planned strata.

// =========================================================================
// Module Declarations
// =========================================================================
mod aggregation;
mod arg;
mod comm;
mod data_type;
mod flow;
mod fs_utils;
mod ident;
mod import;
mod inspect;
mod io_utils;
mod scaffold;
mod transformation;

// =========================================================================
// Imports
// =========================================================================
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use std::collections::{HashMap, HashSet};
use syn::{parse2, File};

use common::{Config, ExecutionMode};
use parser::{DataType, Program};
use planner::StratumPlanner;

use import::ImportTracker;
use inspect::gen_merge_partitions;
use inspect::{gen_print_inspector, gen_size_inspector, gen_write_inspector};

pub struct Compiler {
    /// Configuration provided to the compiler.
    config: Config,

    /// The parsed FlowLog program.
    program: Program,

    /// Global map from relation fingerprint to its identifier.
    global_fp_to_ident: HashMap<u64, Ident>,

    /// Global map from relation fingerprint to its key-value data type.
    global_fp_to_type: HashMap<u64, (Vec<DataType>, Vec<DataType>)>,

    imports: ImportTracker,
}

impl Compiler {
    /// Create a new Compiler instance from Config and Program.
    pub fn new(config: Config, program: Program) -> Self {
        let mut compiler = Compiler {
            config,
            program,
            global_fp_to_ident: HashMap::new(),
            global_fp_to_type: HashMap::new(),
            imports: ImportTracker::default(),
        };
        compiler.make_global_ident_map();
        compiler.make_global_type_map();

        compiler
    }

    /// Create executable from the strata plan.
    pub fn generate_executable_at(&mut self, strata: &[StratumPlanner]) -> std::io::Result<()> {
        let main = self.generate_main(strata);
        self.write_project(&main)
    }
}

// =========================================================================
// Project Generation Utilities
// =========================================================================
impl Compiler {
    /// Generate the text for a standalone `main.rs` program executing the provided strata.
    fn generate_main(&mut self, strata: &[StratumPlanner]) -> String {
        self.imports.reset();

        // Static sections of the generated program.
        let input_decls = self.gen_input_decls();
        let (lhs_binding, ret_expr) = self.build_handle_binding();
        let ingest_stmts = self.gen_ingest_stmts();
        let close_stmts = self.gen_close_stmts();

        // Flow generation per stratum.
        let mut flow_stmts: Vec<TokenStream> = Vec::new();
        let mut calculated_output_fps: HashSet<u64> = HashSet::new();

        for stratum in strata {
            let (core_flows, non_recursive_arranged_map) =
                self.gen_non_recursive_core_flows(stratum.non_recursive_transformations());
            flow_stmts.extend(core_flows);

            if stratum.is_recursive() {
                self.imports.mark_recursive();
                flow_stmts.push(self.gen_iterative_block(&non_recursive_arranged_map, stratum));
            } else {
                flow_stmts
                    .extend(self.gen_non_recursive_post_flows(&calculated_output_fps, stratum));
            }

            calculated_output_fps.extend(stratum.output_relations());
        }

        let (inspect_stmts, merge_stmts) = self.collect_inspectors();

        // Imports block (conditional on recursion for Variable).
        let imports = self.imports.render();

        let diff_type = match self.config.mode() {
            ExecutionMode::Incremental => quote! { isize },
            ExecutionMode::Batch => quote! { differential_dataflow::difference::Present },
        };

        let semiring_one_value = match self.config.mode() {
            ExecutionMode::Incremental => quote! { 1 },
            ExecutionMode::Batch => quote! { differential_dataflow::difference::Present },
        };

        let timestamp_type = match self.config.mode() {
            ExecutionMode::Incremental => quote! { u32 },
            ExecutionMode::Batch => quote! { () },
        };

        let iter_type = if self.imports.is_recursive() {
            quote! { type Iter = u16; }
        } else {
            quote! {}
        };

        let file_ts: TokenStream = quote! {
            #imports

            type Diff = #diff_type;
            #iter_type
            const SEMIRING_ONE: Diff = #semiring_one_value;

            fn main() {
                timely::execute_from_args(std::env::args(), |worker| {
                    // --- Runtime setup -------------------------------------------------
                    let timer = Instant::now();
                    let peers = worker.peers();
                    let index = worker.index();

                    // --- Build dataflow graph -----------------------------------------
                    let #lhs_binding =
                        worker.dataflow::<#timestamp_type, _, _>(|scope| {
                            #(#input_decls)*

                            // === Transformation flows ===
                            #(#flow_stmts)*

                            // === Inspect IDB sizes ===
                            #(#inspect_stmts)*

                            #ret_expr
                        });

                    if index == 0 {
                        println!("{:?}:\tDataflow assembled", timer.elapsed());
                    }

                    // --- Data ingestion -----------------------------------------------
                    #(#ingest_stmts)*
                    #(#close_stmts)*

                    // --- Execute to fixpoint -------------------------------------------
                    while worker.step() {}

                    if index == 0 {
                        println!("{:?}:\tDataflow executed", timer.elapsed());
                        // === Merge per-worker output partitions (if any) ===
                        #(#merge_stmts)*
                    }
                }).unwrap();
            }
        };

        let ast: File = parse2(file_ts).expect("valid token stream");
        prettyplease::unparse(&ast)
    }

    /// Assemble inspection and partition-merge statements for IDB relations based on CLI args.
    fn collect_inspectors(&mut self) -> (Vec<TokenStream>, Vec<TokenStream>) {
        let mut inspect_stmts = Vec::new();
        let mut merge_stmts = Vec::new();

        for idb in self.program.idbs() {
            let var = self.find_global_ident(idb.fingerprint());
            let name = idb.name();

            if idb.printsize() {
                self.imports.mark_threshold_total();
                self.imports.mark_as_collection();
                self.imports.mark_timely_map();
                inspect_stmts.push(gen_size_inspector(&var, name));
            }

            if idb.output() {
                if self.config.output_to_stdout() {
                    inspect_stmts.push(gen_print_inspector(&var, name, idb.arity()));
                } else {
                    let parent_dir = self.config.output_dir().expect(
                        "output directory must be provided when writing IDB output to files",
                    );
                    inspect_stmts.push(gen_write_inspector(&var, name, parent_dir, idb.arity()));
                    merge_stmts.push(gen_merge_partitions(name, parent_dir));
                }
            }
        }

        (inspect_stmts, merge_stmts)
    }
}
