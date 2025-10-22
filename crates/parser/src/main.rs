use std::process;

use args::{get_example_files, Args};
use clap::Parser;
use parser::program::Program;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    // Parse command line arguments
    let args = Args::parse();

    if args.should_process_all() {
        // Run parser on all example files
        run_all_examples();
    } else {
        // Parse single program file
        let program = Program::parse(args.program());
        info!("Success parse program\n{program}");
    }
}

fn run_all_examples() {
    let example_files = get_example_files();

    info!("Running parser on {} example files...", example_files.len());
    println!("{}", "=".repeat(80));

    let mut successful = 0;
    let mut failed = 0;

    for (i, file_path) in example_files.iter().enumerate() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        println!(
            "\n[{}/{}] Processing: {}",
            i + 1,
            example_files.len(),
            file_name
        );
        println!("{}", "-".repeat(40));

        match std::panic::catch_unwind(|| Program::parse(file_path.to_str().unwrap())) {
            Ok(program) => {
                successful += 1;
                info!("  SUCCESS: {}", file_name);

                // Print basic statistics
                let edbs = program.edbs();
                let idbs = program.idbs();
                let rules = program.rules();

                println!("    Statistics:");
                println!("     EDB relations: {}", edbs.len());
                println!("     IDB relations: {}", idbs.len());
                println!("     Rules: {}", rules.len());
            }
            Err(panic_info) => {
                failed += 1;
                eprintln!("  FAILED: {}", file_name);

                // Try to extract panic message
                if let Some(s) = panic_info.downcast_ref::<String>() {
                    eprintln!("  Error: {}", s);
                } else if let Some(s) = panic_info.downcast_ref::<&str>() {
                    eprintln!("  Error: {}", s);
                } else {
                    eprintln!("  Error: Unknown panic occurred");
                }
            }
        }
    }

    // Summary
    println!("\n{}", "=".repeat(80));
    println!("SUMMARY:");
    println!("  Total files: {}", example_files.len());
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);

    if failed > 0 {
        println!("\nSome files failed to parse. Check the errors above for details.");
        process::exit(1);
    } else {
        println!("\nAll example files parsed successfully!");
    }
}
