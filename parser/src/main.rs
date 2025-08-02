use std::env;
use std::process;
use std::str::FromStr;

use parser::program::Program;
use tracing::{error, info};
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
    match Program::from_str(program_source) {
        Ok(program) => {
            info!("Success parse program\n{program}");
        }
        Err(e) => {
            error!("Failed to parse program from '{program_source}': {e}");
        }
    }
}
