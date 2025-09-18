use catalog::rule::Catalog;
use parser::{Program, error::ParserError};
use std::env;
use std::fs;
use std::path::Path;
use std::process;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

fn main() -> Result<(), ParserError> {
    // Initialize tracing similar to parser/stratifier mains
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("info"))
        .init();

    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <program_file>", args[0]);
        eprintln!("       {} --all", args[0]);
        eprintln!("Examples:");
        eprintln!("  {} ./example/reach.dl", args[0]);
        eprintln!("  {} --all", args[0]);
        process::exit(1);
    }

    let argument = &args[1];
    if argument == "--all" {
        run_all_examples()?;
        return Ok(());
    }

    let program = Program::parse(argument)?;
    info!(
        "Parsed program: {} rules ({} EDBs, {} IDBs)",
        program.rules().len(),
        program.edbs().len(),
        program.idbs().len()
    );
    print_catalogs(&program);
    Ok(())
}

fn print_catalogs(program: &Program) {
    for (i, rule) in program.rules().iter().enumerate() {
        info!("{}", "-".repeat(80));
        info!("[Rule {}] {}", i, rule.head().name());
        let catalog = Catalog::from_rule(rule);
        info!("{}", catalog);
    }
}

fn run_all_examples() -> Result<(), ParserError> {
    let example_dir = "example";
    if !Path::new(example_dir).exists() {
        error!("Error: example directory '{}' not found", example_dir);
        process::exit(1);
    }

    let entries = fs::read_dir(example_dir).map_err(|e| ParserError::Io(e.to_string()))?;

    let mut files = Vec::new();
    for entry in entries.flatten() {
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("dl") {
            files.push(path);
        }
    }
    files.sort();
    if files.is_empty() {
        error!("No .dl files found in {example_dir} directory");
        process::exit(1);
    }

    info!("Running catalog on {} example files...", files.len());

    let mut successful = 0usize;
    let mut failed = 0usize;

    for file_path in files.iter() {
        let file_name = file_path.file_name().unwrap().to_str().unwrap();

        match Program::parse(file_path.to_str().unwrap()) {
            Ok(_program) => { successful += 1; println!("SUCCESS: {}", file_name); }
            Err(err) => { failed += 1; println!("FAILED: {} ({err})", file_name); }
        }
    }

    println!("\nSUMMARY:");
    println!("  Total files: {}", files.len());
    println!("  Successful: {}", successful);
    println!("  Failed: {}", failed);

    if failed > 0 {
        // Non-zero exit so CI can catch failures
        process::exit(1);
    } else {
        println!("\nAll example files processed successfully!");
    }
    Ok(())
}
