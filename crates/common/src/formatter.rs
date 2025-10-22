//! Unified output formatting for processing all examples.

use std::process;
use tracing::{error, info};

/// Unified output formatting for processing all examples
pub struct AllResultsFormatter {
    tool_name: String,
    total_files: usize,
    successful: usize,
    failed: usize,
}

impl AllResultsFormatter {
    pub fn new(tool_name: &str, total_files: usize) -> Self {
        info!("Running {} on {} example files...", tool_name, total_files);
        info!("{}", "=".repeat(80));

        Self {
            tool_name: tool_name.to_string(),
            total_files,
            successful: 0,
            failed: 0,
        }
    }

    pub fn report_success(&mut self, file_name: &str, stats: Option<&str>) {
        self.successful += 1;
        if let Some(stats) = stats {
            info!("SUCCESS: {} ({})", file_name, stats);
        } else {
            info!("SUCCESS: {}", file_name);
        }
    }

    pub fn report_failure(&mut self, file_name: &str, error: Option<&str>) {
        self.failed += 1;
        if let Some(error) = error {
            error!("FAILED: {} - {}", file_name, error);
        } else {
            error!("FAILED: {}", file_name);
        }
    }

    pub fn finish(self) {
        info!("");
        info!("{}", "=".repeat(80));
        info!("SUMMARY:");
        info!("  Total files: {}", self.total_files);
        info!("  Successful: {}", self.successful);
        info!("  Failed: {}", self.failed);

        if self.failed > 0 {
            error!(
                "Some files failed to process with {}. Check the errors above for details.",
                self.tool_name
            );
            process::exit(1);
        } else {
            info!(
                "All example files processed successfully with {}!",
                self.tool_name
            );
        }
    }
}
