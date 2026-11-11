//! Clap handles executable options and help. Worker and networking values
//! pass through to Timely for configuration and validation.

use std::path::Path;
use std::path::PathBuf;

use clap::Arg;
use clap::ArgAction;
use clap::Args;
use clap::CommandFactory;
use clap::FromArgMatches;
use clap::Parser;
use clap::error::ErrorKind;

/// Resolved startup settings with runtime directory overrides applied.
#[derive(Debug)]
pub struct RuntimeArgs {
    pub config: timely::Config,
    pub fact_dir: PathBuf,
    /// `None` selects stdout.
    pub output_dir: Option<PathBuf>,
}

impl RuntimeArgs {
    /// Parses process arguments, using the supplied directories as defaults.
    /// Relative paths resolve from the working directory.
    pub fn from_env(default_fact_dir: &str, default_output_dir: &str) -> Self {
        let mut command = Cli::command()
            .mut_arg("fact_dir", |arg| {
                arg.default_value(default_fact_dir.to_owned())
                    .required(false)
            })
            .mut_arg("output_dir", |arg| {
                arg.default_value(default_output_dir.to_owned())
                    .required(false)
            })
            // Timely reserves -h for its host file.
            .arg(
                Arg::new("help")
                    .long("help")
                    .help("Print help")
                    .help_heading("Options")
                    .action(ArgAction::Help),
            );
        let cli =
            Cli::from_arg_matches(&command.get_matches_mut()).unwrap_or_else(|error| error.exit());
        let config = timely::Config::from_args(cli.timely.into_args().into_iter())
            .unwrap_or_else(|error| command.error(ErrorKind::ValueValidation, error).exit());

        Self {
            config,
            fact_dir: cli.fact_dir,
            output_dir: if cli.output_dir == Path::new("-") {
                None
            } else {
                Some(cli.output_dir)
            },
        }
    }
}

#[derive(Debug, Parser)]
#[command(disable_help_flag = true, next_help_heading = "FlowLog options")]
struct Cli {
    /// Input fact directory.
    #[arg(short = 'F', long, value_name = "DIR")]
    fact_dir: PathBuf,

    /// Output directory; '-' selects stdout.
    #[arg(short = 'D', long, value_name = "DIR")]
    output_dir: PathBuf,

    #[command(flatten, next_help_heading = "Timely options (passed through)")]
    timely: TimelyArgs,
}

#[derive(Debug, Args)]
struct TimelyArgs {
    /// Number of worker threads per process.
    #[arg(short = 'w', long, value_name = "NUM")]
    threads: Option<String>,

    /// Identity of this process.
    #[arg(short = 'p', long, value_name = "IDX")]
    process: Option<String>,

    /// Number of processes.
    #[arg(short = 'n', long, value_name = "NUM")]
    processes: Option<String>,

    /// File containing one process address per line.
    #[arg(short = 'h', long, value_name = "FILE")]
    hostfile: Option<String>,

    /// Report connection progress.
    #[arg(short = 'r', long)]
    report: bool,

    /// Enable zero-copy communication within a process.
    #[arg(short = 'z', long)]
    zerocopy: bool,

    /// Progress tracking mode: eager or demand.
    #[arg(long, value_name = "MODE")]
    progress_mode: Option<String>,

    /// Microseconds an idle worker polls before parking.
    #[arg(long, value_name = "MICROS")]
    idle_spin: Option<String>,
}

impl TimelyArgs {
    /// Returns the supplied Timely options without adding defaults or
    /// interpreting their values.
    fn into_args(self) -> Vec<String> {
        let mut args = Vec::new();
        for (flag, value) in [
            ("--threads", self.threads),
            ("--process", self.process),
            ("--processes", self.processes),
            ("--hostfile", self.hostfile),
            ("--progress-mode", self.progress_mode),
            ("--idle-spin", self.idle_spin),
        ] {
            if let Some(value) = value {
                args.extend([flag.to_owned(), value]);
            }
        }
        if self.report {
            args.push("--report".to_owned());
        }
        if self.zerocopy {
            args.push("--zerocopy".to_owned());
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use timely::CommunicationConfig;

    use super::*;

    /// Every option the executable passes through reaches Timely's parser
    /// under a name Timely knows, with its value. The literal names every
    /// field, so an option added later stops this test from compiling until
    /// it gets a value here.
    #[test]
    fn every_passthrough_option_reaches_timely() {
        let mut hostfile = tempfile::NamedTempFile::new().unwrap();
        writeln!(hostfile, "host-a:2101\nhost-b:2101").unwrap();
        let args = TimelyArgs {
            threads: Some("3".to_owned()),
            process: Some("1".to_owned()),
            processes: Some("2".to_owned()),
            hostfile: Some(hostfile.path().to_str().unwrap().to_owned()),
            report: true,
            zerocopy: true,
            progress_mode: Some("eager".to_owned()),
            idle_spin: Some("50".to_owned()),
        };
        let config = timely::Config::from_args(args.into_args().into_iter()).unwrap();
        let CommunicationConfig::Cluster {
            threads,
            process,
            addresses,
            report,
            zerocopy,
        } = config.communication
        else {
            panic!("two processes make a cluster: {:?}", config.communication);
        };
        assert_eq!((threads, process, report, zerocopy), (3, 1, true, true));
        assert_eq!(addresses, ["host-a:2101", "host-b:2101"]);
    }
}
