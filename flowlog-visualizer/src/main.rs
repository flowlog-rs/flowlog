use std::fs;
use std::io;
use std::path::Path;

use anyhow::Context;
use anyhow::bail;
use clap::Parser;
use flowlog_profiler::PlanGraph;
use flowlog_profiler::metrics;
use tracing_subscriber::EnvFilter;

mod render;
mod view;

pub type Result<T> = anyhow::Result<T>;

#[derive(Parser)]
#[command(name = "flowlog-visualizer")]
#[command(about = "FlowLog profile visualizer", long_about = None)]
struct Cli {
    /// Path to the ops.json spec (`<stem>_log/ops.json`).
    #[arg(short = 'p', long)]
    ops: String,

    /// Path to the folder of per-worker metrics logs (`<stem>_log/metrics/`,
    /// an `operators_worker_*`/`channels_worker_*` table pair per worker).
    #[arg(short = 'm', long)]
    metrics: String,

    /// Output HTML file.
    #[arg(short = 'o', long)]
    out: String,
}

fn main() -> Result<()> {
    // Warnings to stderr (default `warn`; override via `RUST_LOG`).
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .with_writer(io::stderr)
        .without_time()
        .init();

    let Cli {
        ops,
        metrics: metrics_dir,
        out,
    } = Cli::parse();

    // Load the plan graph, read the run's metrics against it, then shape
    // each snapshot's facts into a rendered report.
    let ops_text = fs::read_to_string(&ops).with_context(|| format!("read ops file {ops}"))?;
    let plan: PlanGraph =
        serde_json::from_str(&ops_text).with_context(|| format!("parse ops file {ops}"))?;

    let runs = metrics::read(&plan, Path::new(&metrics_dir))
        .with_context(|| format!("read metrics for {ops} from {metrics_dir}"))?;
    if runs.is_empty() {
        bail!("no .log files found in metrics folder {metrics_dir}");
    }

    let labels: Vec<&str> = runs.iter().map(|r| r.label.as_str()).collect();
    let reports: Vec<view::Report> = runs.iter().map(|r| view::build(&plan, r)).collect();
    let html = render::render_html_report(&reports)?;
    fs::write(&out, html).with_context(|| format!("write output file {out}"))?;
    println!(
        "Wrote {out} ({} snapshot(s): {})",
        labels.len(),
        labels.join(", ")
    );

    Ok(())
}
