use std::env;
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
        eprintln!("Example: {} ./examples/programs/greater_equal.dl", args[0]);
        process::exit(1);
    }

    let program_source = &args[1];

    // Parse the FlowLog program
    let program = Program::parse(program_source);
    info!("Success parse program\n{program}");
}
