//! Shared primitives for the FlowLog pipeline crates.

mod config;
mod fmt;
mod hash;

pub use config::Config;
pub use config::ExecutionMode;
pub use config::program_stem;
pub use fmt::SECTION_BAR;
pub use fmt::SUBSECTION_BAR;
pub use fmt::pretty_print;
pub use hash::compute_fp;
