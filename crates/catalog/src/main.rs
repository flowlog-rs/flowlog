use std::process;

use args::{get_example_files, Args};
use catalog::rule::Catalog;
use clap::Parser;
use parser::Program;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize tracing similar to parser/stratifier mains
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let args = Args::parse();

    if args.should_process_all() {
        run_all_examples();
        return;
    }

    let program = Program::parse(args.program());
    info!(
        "Parsed program: {} rules ({} EDBs, {} IDBs)",
        program.rules().len(),
        program.edbs().len(),
        program.idbs().len()
    );
    print_catalogs(&program);
}

fn print_catalogs(program: &Program) {
    for (i, rule) in program.rules().iter().enumerate() {
        info!("{}", "-".repeat(80));
        info!("[Rule {}] {}", i, rule.head().name());
        let catalog = Catalog::from_rule(rule);
        info!("{}", catalog);
    }
}

fn run_all_examples() {
    let example_files = get_example_files();

    info!("Running catalog on {} example files...", example_files.len());

    let mut successful = 0usize;
    let mut failed = 0usize;

    for file_path in example_files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(_program) => {
                successful += 1;
                println!("SUCCESS: {}", file_name);
            }
            Err(_panic_info) => {
                failed += 1;
                println!("FAILED: {}", file_name);
            }
        }
    }

    println!("\nSUMMARY:");
    println!("  Total files: {}", example_files.len());
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);

    if failed > 0 {
        // Non-zero exit so CI can catch failures
        process::exit(1);
    } else {
        println!("\nAll example files processed successfully!");
    }
}
