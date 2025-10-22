//! Shared common utilities for Macaron Datalog programs.

pub mod args;
pub mod formatter;

// Re-export main types for backwards compatibility
pub use args::{get_example_files, Args};
pub use formatter::TestResult;
