//! Macaron Generator Library
//! Generates execution plans and code from planned strata and rules produced by the optimizer.
//!
//! At this stage, this crate exposes a minimal facade and logs the generation
//! process. Later it can be extended to generate optimized execution code.

mod arg;
mod data_type;
mod ident;
mod ingest;
mod inspect;
mod io;
mod non_recursive_flow;
mod recursive_flow;
mod scaffold;
mod transformation;

use common::Args;
use parser::Program;
use planner::StratumPlanner;
use proc_macro2::TokenStream;
use quote::quote;
use std::collections::HashSet;
use std::path::Path;
use syn::{parse2, File};

use data_type::make_type_map;
use ident::make_ident_map;
use ingest::{build_handle_binding, gen_close_stmts, gen_ingest_stmts, gen_input_decls};
use inspect::gen_merge_partitions;
use inspect::{gen_print_inspector, gen_size_inspector, gen_write_inspector};
use non_recursive_flow::{gen_non_recursive_core_flows, gen_non_recursive_post_flows};
use recursive_flow::gen_iterative_block;
use scaffold::write_project;

use crate::ident::find_ident;

/// =========================================================================
/// Generator Main Entry Point
/// =========================================================================

/// Create a project directory and write Cargo.toml + src/main.rs generated from the `Stratum`.
pub fn generate_project_at(
    args: &Args,
    out_parent: &Path,
    package_name: &str,
    program: &Program,
    strata: &Vec<StratumPlanner>,
) -> std::io::Result<()> {
    let out_dir = out_parent.join(package_name);
    let main = generate_main(args, program, strata);
    write_project(&out_dir, package_name, &main)
}

/// =========================================================================
/// Project Generation Utilities
/// =========================================================================

/// Generate the `use` imports for the generated `main.rs`.
/// - Always imports: std IO, differential Input, and operators::*.
/// - Imports iterate::Variable only when the stratum is recursive.
fn gen_imports(is_recursive: bool) -> TokenStream {
    let rec_imports = if is_recursive {
        quote! {
            use differential_dataflow::operators::iterate::Variable;
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
        #rec_imports
    }
}

/// Generate the text for a standalone `main.rs` program executing the provided strata.
fn generate_main(args: &Args, program: &Program, strata: &Vec<StratumPlanner>) -> String {
    let mut fp_to_type = make_type_map(program.edbs());
    let fp_to_ident = make_ident_map(program.edbs(), program.idbs());
    let input_order = program.edb_names();

    // static pieces
    let input_decls = gen_input_decls(program.edbs());
    let (lhs_binding, ret_expr) = build_handle_binding(&input_order);
    let ingest_stmts = gen_ingest_stmts(program.edbs());
    let close_stmts = gen_close_stmts(&input_order);

    let is_recursive = strata.iter().any(|s| s.is_recursive());

    // Process strata in order and collect their token streams
    let mut flow_stmts = Vec::<TokenStream>::new();
    let mut inspect_stmts = Vec::<TokenStream>::new();
    let mut merge_stmts = Vec::<TokenStream>::new();
    // Fingerprints of output relations already materialized in prior strata.
    let mut calculated_output_fps: HashSet<u64> = HashSet::new();

    for stratum in strata {
        // Core (non-recursive) transformations for the stratum.
        let static_core = gen_non_recursive_core_flows(
            &fp_to_ident,
            &mut fp_to_type,
            stratum.static_transformations(),
        );
        flow_stmts.extend(static_core);

        // If recursive, build iterative block; else emit post-processing unions.
        if stratum.is_recursive() {
            let dyn_block = gen_iterative_block(&fp_to_ident, &mut fp_to_type, stratum);
            flow_stmts.push(dyn_block);
        } else {
            let post_flows = gen_non_recursive_post_flows(
                &fp_to_ident,
                &mut fp_to_type,
                &calculated_output_fps,
                stratum.output_to_idb_map(),
            );
            flow_stmts.extend(post_flows);
        }

        // Record output relation fingerprints for incremental unions in later strata.
        for fp in stratum.output_relations() {
            calculated_output_fps.insert(fp);
        }
    }

    // After constructing flows, add inspectors for all IDB collections.
    for idb in program.idbs() {
        let var = find_ident(&fp_to_ident, idb.fingerprint());
        let name = idb.name();
        if idb.printsize() {
            inspect_stmts.push(gen_size_inspector(&var, name));
        }

        if idb.output() {
            if args.output_to_stdout() {
                inspect_stmts.push(gen_print_inspector(&var, name));
            } else {
                let parent_dir = args.output_dir().unwrap();
                inspect_stmts.push(gen_write_inspector(&var, name, parent_dir));
                merge_stmts.push(gen_merge_partitions(name, parent_dir));
            }
        }
    }

    // Imports block (conditional on recursion for Variable).
    let imports = gen_imports(is_recursive);

    let file_ts: TokenStream = quote! {
        #imports

        type Diff = isize;

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
