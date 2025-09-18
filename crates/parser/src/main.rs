use std::env;
use std::fs;
use std::path::Path;
use std::process;

use parser::program::Program;
use tracing::info;
use tracing_subscriber::EnvFilter;

fn main() {
    // Initialize simple tracing
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    // Parse command line arguments
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: {} <program_file>", args[0]);
        eprintln!("       {} --all", args[0]);
        eprintln!();
        eprintln!("Examples:");
        eprintln!("  {} ./example/reach.dl", args[0]);
        eprintln!("  {} --all", args[0]);
        process::exit(1);
    }

    let argument = &args[1];

    if argument == "--all" {
        // Run parser on all example files
        run_all_examples();
    } else {
        // Parse single program file
        let program = Program::parse(argument).unwrap_or_else(|e| {
            eprintln!("Error parsing file '{}': {}", argument, e);
            process::exit(1);
        });
        info!("Success parse program\n{program}");
    }
}

fn run_all_examples() {
    let example_dir = "example";

    // Check if example directory exists
    if !Path::new(example_dir).exists() {
        eprintln!("Error: example directory '{}' not found", example_dir);
        process::exit(1);
    }

    // Read all .dl files from example directory
    let entries = match fs::read_dir(example_dir) {
        Ok(entries) => entries,
        Err(e) => {
            eprintln!("Error reading example directory: {}", e);
            process::exit(1);
        }
    };

    let mut example_files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("dl") {
            example_files.push(path);
        }
    }

    // Sort files for consistent output
    example_files.sort();

    if example_files.is_empty() {
        eprintln!("No .dl files found in {} directory", example_dir);
        process::exit(1);
    }

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

        match Program::parse(file_path.to_str().unwrap()) {
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
            Err(parser_error) => {
                failed += 1;
                eprintln!("  FAILED: {}", file_name);
                eprintln!("    Error: {}", parser_error);
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
