//! Input ingestion and output writing for the FlowLog compiler.

pub(crate) mod input;
pub(crate) mod output;
pub(crate) mod relops;

pub(crate) use output::InspectorCodegen;
