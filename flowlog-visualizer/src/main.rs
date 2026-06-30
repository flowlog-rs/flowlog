use anyhow::Context;
use clap::Parser;
use std::collections::BTreeMap;
use std::fs;

mod log;
mod ops;
mod render;
mod stats;
mod view;

pub type Result<T> = anyhow::Result<T>;

#[derive(Parser)]
#[command(name = "flowlog-visualizer")]
#[command(about = "FlowLog profile visualizer", long_about = None)]
struct Cli {
    /// Path to the ops.json spec (`<stem>_log/ops.json`).
    #[arg(short = 'p', long)]
    ops: String,

    /// Path to the folder of unified per-worker metrics logs
    /// (`<stem>_log/metrics/`, files `metrics_worker_t{t}_{index}.log`).
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
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .with_writer(std::io::stderr)
        .without_time()
        .init();

    let Cli { ops, metrics, out } = Cli::parse();

    // 1) Parse + validate ops.json.
    let ops_text = fs::read_to_string(&ops).with_context(|| format!("read ops file {}", ops))?;
    let profiler: flowlog_profiler::Profiler =
        serde_json::from_str(&ops_text).with_context(|| format!("parse ops file {}", ops))?;
    let validated = ops::validate_and_build(&profiler)?;

    let ops::ValidatedOps {
        nodes,
        roots: root_ids,
        rules,
        fingerprint_to_node,
    } = validated;

    let mut nodes_by_name = BTreeMap::new();
    for (id, node) in nodes {
        nodes_by_name.insert(id.to_string(), node);
    }

    let roots: Vec<String> = root_ids.iter().map(|id| id.to_string()).collect();

    let fingerprint_to_node: BTreeMap<String, String> = fingerprint_to_node
        .into_iter()
        .map(|(fp, id)| (fp, id.to_string()))
        .collect();

    // 2) Parse the unified metrics folder (auto-detects batch vs per-transaction
    //    snapshots from the `_t{N}_` in each filename).
    let metric_snapshots = log::parse_metrics_folder(&metrics)?;

    // 3) Build one ReportData per snapshot.
    let mut snapshot_labels: Vec<String> = Vec::new();
    let mut snapshots: Vec<view::ReportData> = Vec::new();

    for snap in &metric_snapshots {
        snapshot_labels.push(snap.label.clone());
        snapshots.push(view::build_report_data(
            &nodes_by_name,
            &roots,
            &rules,
            &fingerprint_to_node,
            &snap.time,
            &snap.memory,
        )?);
    }

    // 4) Render HTML.
    let html = render::render_html_report(&snapshot_labels, &snapshots)?;
    fs::write(&out, html).with_context(|| format!("write output file {}", out))?;
    println!(
        "Wrote {} ({} snapshot(s): {})",
        out,
        snapshot_labels.len(),
        snapshot_labels.join(", ")
    );

    Ok(())
}
