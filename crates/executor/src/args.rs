use clap::Parser;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Args {
    /// Path of the Datalog program
    #[arg(short, long, value_name = "PATH")]
    pub program: String,

    /// Specify directory for fact files.
    #[arg(short = 'F', long, value_name = "DIR")]
    pub fact_dir: String,

    /// Specify directory for output files. If <DIR> is `-` then stdout is used.
    #[arg(short = 'D', long, value_name = "DIR")]
    pub output_dir: String,

    /// Run interpreter/compiler in parallel using N threads (default: system CPU count).
    #[arg(short = 'j', long, value_name = "N")]
    pub jobs: Option<usize>,
}

impl Args {
    pub fn program(&self) -> &str {
        &self.program
    }

    pub fn program_name(&self) -> String {
        std::path::Path::new(&self.program)
            .file_stem()
            .and_then(|stem| stem.to_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| "unknown_program".into())
    }

    pub fn fact_dir(&self) -> &str {
        &self.fact_dir
    }

    pub fn output_dir(&self) -> &str {
        &self.output_dir
    }

    pub fn output_to_stdout(&self) -> bool {
        self.output_dir == "-"
    }

    /// Returns the number of threads to use. If not provided, returns the CPU count.
    pub fn jobs(&self) -> usize {
        self.jobs.unwrap_or_else(|| {
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(1)
        })
    }

    pub fn timely_args(&self) -> Vec<String> {
        vec![String::from("-w"), self.jobs().to_string()]
    }
}
