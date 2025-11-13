use parser::Program;
use planner::StratumPlanner;
use proc_macro2::TokenStream;
use quote::quote;
use std::path::Path;
use syn::{parse2, File};

use super::comm::{make_ident_map, make_type_map, ordered_input_names};
use super::ingest::{build_handle_binding, gen_close_stmts, gen_ingest_stmts, gen_input_decls};
use super::non_recursive_flow::gen_non_recursive_flows;
use super::recursive_flow::gen_iterative_block;
use crate::scaffold::write_project;

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

/// Generate the text for a standalone `main.rs` program that executes the given `Stratum`.
pub fn generate_main(program: &Program, stratums: Vec<&StratumPlanner>) -> String {
    let fp2ident = make_ident_map(program.edbs(), program.idbs());
    let mut fp2type = make_type_map(program.edbs());
    let input_order = ordered_input_names(program.edbs());

    // static pieces
    let input_decls = gen_input_decls(program.edbs());
    let (lhs_binding, ret_expr) = build_handle_binding(&input_order);
    let ingest_stmts = gen_ingest_stmts(program.edbs());
    let close_stmts = gen_close_stmts(&input_order);

    let is_recursive = stratums.iter().any(|s| s.is_recursive());

    // Process strata in order and collect their token streams
    let mut flow_stmts = Vec::<TokenStream>::new();

    for stratum in &stratums {
        // Each stratum always has non-recursive transformations
        let static_flows =
            gen_non_recursive_flows(&fp2ident, &mut fp2type, stratum.static_transformations());
        flow_stmts.extend(static_flows);

        // Recursive strata also need iterative blocks for dynamic transformations
        if stratum.is_recursive() {
            // let recursive_block = gen_iterative_block(&fp2ident, stratum);
            // flow_stmts.push(recursive_block);
            todo!("Generate iterative block for recursive rules");
        }
    }

    // Imports block (conditional on recursion for Variable).
    let imports = gen_imports(is_recursive);

    let file_ts: TokenStream = quote! {
        #imports

        type Diff = i64;

        fn main() {
            timely::execute_from_args(std::env::args(), |worker| {
                // --- Runtime setup -------------------------------------------------
                let start = Instant::now();
                let peers = worker.peers();
                let index = worker.index();

                // --- Build dataflow graph -----------------------------------------
                #lhs_binding =
                    worker.dataflow::<(), _, _>(|scope| {
                        #(#input_decls)*

                        // === Transformation flows ===
                        #(#flow_stmts)*

                        #ret_expr
                    });

                if index == 0 {
                    println!("{:?}:\tDataflow assembled", start.elapsed());
                }

                // --- Data ingestion -----------------------------------------------
                #(#ingest_stmts)*
                #(#close_stmts)*

                if index == 0 {
                    println!("{:?}:\tData loaded", start.elapsed());
                }

                // --- Execute to fixpoint -------------------------------------------
                while worker.step() {}

                if index == 0 {
                    println!("{:?}:\tFixpoint reached", start.elapsed());
                }
            }).unwrap();
        }
    };

    let ast: File = parse2(file_ts).expect("valid token stream");
    prettyplease::unparse(&ast)
}

/// Create a project directory and write Cargo.toml + src/main.rs generated from the `Stratum`.
pub fn generate_project_at(
    out_parent: &Path,
    package_name: &str,
    program: &Program,
    stratum: Vec<&StratumPlanner>,
) -> std::io::Result<()> {
    let out_dir = out_parent.join(package_name);
    let main = generate_main(program, stratum);
    write_project(&out_dir, package_name, &main)
}
