//! Generator module root: re-exports the public API and wires submodules.

mod arg;
mod build;
mod comm;
mod ingest;
mod recursion;
mod static_flow;

pub use build::{generate_main, generate_project_at};
