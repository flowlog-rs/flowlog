//! FlowLog Generator Library
//! Generates executable Rust code from planned strata.

// =========================================================================
// Module Declarations
// =========================================================================

mod aggregation;
mod arg;
mod data_type;
mod flow;
mod fs_utils;
mod ident;
mod inspect;
mod io_utils;
mod scaffold;
mod transformation;

// =========================================================================
// Imports
// =========================================================================

use std::collections::{HashMap, HashSet};
use std::path::Path;

use common::{Args, ExecutionMode};
use parser::Program;
use planner::StratumPlanner;
use proc_macro2::{Ident, TokenStream};
use quote::quote;
use syn::{parse2, File};

use crate::ident::find_ident;
use data_type::make_type_map;
use flow::{gen_iterative_block, gen_non_recursive_core_flows, gen_non_recursive_post_flows};
use ident::make_ident_map;
use inspect::gen_merge_partitions;
use inspect::{gen_print_inspector, gen_size_inspector, gen_write_inspector};
use io_utils::{build_handle_binding, gen_close_stmts, gen_ingest_stmts, gen_input_decls};
use scaffold::write_project;

// =========================================================================
// Generator Main Entry Point
// =========================================================================

/// Create a project directory and write Cargo.toml + src/main.rs generated from the strata plan.
pub fn generate_project_at(
    args: &Args,
    out_dir: &Path,
    package_name: &str,
    program: &Program,
    strata: &[StratumPlanner],
) -> std::io::Result<()> {
    let main = generate_main(args, program, strata);
    write_project(out_dir, package_name, &main)
}

// =========================================================================
// Project Generation Utilities
// =========================================================================

/// Generate the `use` imports for the generated `main.rs`.
/// - Always imports: std IO, differential Input, and operators::*.
/// - Imports iterate::Variable only when the stratum is recursive.
fn gen_imports(is_recursive: bool) -> TokenStream {
    let rec_imports = if is_recursive {
        quote! {
            use differential_dataflow::operators::iterate::SemigroupVariable;
            use timely::dataflow::Scope;
        }
    } else {
        quote! {}
    };

    quote! {
        use std::fs::File;
        use std::io::{BufRead, BufReader};
        use std::time::Instant;

        use differential_dataflow::input::Input;
        use differential_dataflow::operators::*;
        use differential_dataflow::operators::arrange::ArrangeByKey;
        use differential_dataflow::operators::reduce::*;
        use differential_dataflow::trace::implementations::*;
        use differential_dataflow::AsCollection;
        use timely::dataflow::operators::core::*;
        #rec_imports
    }
}

/// Generate the text for a standalone `main.rs` program executing the provided strata.
fn generate_main(args: &Args, program: &Program, strata: &[StratumPlanner]) -> String {
    let mut fp_to_type = make_type_map(program.edbs(), program.idbs());
    let fp_to_ident = make_ident_map(program.edbs(), program.idbs());
    let input_order = program.edb_names();

    // Static sections of the generated program.
    let input_decls = gen_input_decls(program.edbs());
    let (lhs_binding, ret_expr) = build_handle_binding(&input_order);
    let ingest_stmts = gen_ingest_stmts(args.fact_dir(), program.edbs());
    let close_stmts = gen_close_stmts(&input_order);

    let is_recursive = strata.iter().any(|s| s.is_recursive());

    // Flow generation per stratum.
    let mut flow_stmts: Vec<TokenStream> = Vec::new();
    let mut calculated_output_fps: HashSet<u64> = HashSet::new();

    for stratum in strata {
        let (core_flows, non_recursive_arranged_map) = gen_non_recursive_core_flows(
            &fp_to_ident,
            &mut fp_to_type,
            stratum.non_recursive_transformations(),
        );
        flow_stmts.extend(core_flows);

        if stratum.is_recursive() {
            flow_stmts.push(gen_iterative_block(
                &fp_to_ident,
                &mut fp_to_type,
                &non_recursive_arranged_map,
                stratum,
                stratum.output_to_aggregation_map(),
            ));
        } else {
            flow_stmts.extend(gen_non_recursive_post_flows(
                &fp_to_ident,
                &mut fp_to_type,
                &calculated_output_fps,
                stratum.output_to_idb_map(),
                stratum.output_to_aggregation_map(),
            ));
        }

        calculated_output_fps.extend(stratum.output_relations());
    }

    let (inspect_stmts, merge_stmts) = collect_inspectors(args, program, &fp_to_ident);

    // Imports block (conditional on recursion for Variable).
    let imports = gen_imports(is_recursive);

    let diff_type = match args.mode() {
        ExecutionMode::Incremental => quote! { isize },
        ExecutionMode::Batch => quote! { differential_dataflow::difference::Present },
    };

    let semiring_one_value = match args.mode() {
        ExecutionMode::Incremental => quote! { 1 },
        ExecutionMode::Batch => quote! { differential_dataflow::difference::Present },
    };

    let file_ts: TokenStream = quote! {
        #imports

        type Diff = #diff_type;
        type Iter = u16;
        const SEMIRING_ONE: Diff = #semiring_one_value;

        fn main() {
            timely::execute_from_args(std::env::args(), |worker| {
                // --- Runtime setup -------------------------------------------------
                let timer = Instant::now();
                let peers = worker.peers();
                let index = worker.index();

                // --- Build dataflow graph -----------------------------------------
                #lhs_binding =
                    worker.dataflow::<(), _, _>(|scope| {
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
fn collect_inspectors(
    args: &Args,
    program: &Program,
    fp_to_ident: &HashMap<u64, Ident>,
) -> (Vec<TokenStream>, Vec<TokenStream>) {
    let mut inspect_stmts = Vec::new();
    let mut merge_stmts = Vec::new();

    for idb in program.idbs() {
        let var = find_ident(fp_to_ident, idb.fingerprint());
        let name = idb.name();

        if idb.printsize() {
            inspect_stmts.push(gen_size_inspector(&var, name));
        }

        if idb.output() {
            if args.output_to_stdout() {
                inspect_stmts.push(gen_print_inspector(&var, name));
            } else {
                let parent_dir = args
                    .output_dir()
                    .expect("output directory must be provided when writing IDB output to files");
                inspect_stmts.push(gen_write_inspector(&var, name, parent_dir, idb.arity()));
                merge_stmts.push(gen_merge_partitions(name, parent_dir));
            }
        }
    }

    (inspect_stmts, merge_stmts)
}
