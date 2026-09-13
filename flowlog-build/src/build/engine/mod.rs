//! Library engine generation and scheduling.
//!
//! Batch and incremental engines stage host rows and control workers.
//! Runtime loaders ingest inputs; emitters construct typed output results.

mod batch;
mod incremental;

pub(crate) use batch::gen_lib_engine;
pub(crate) use incremental::gen_lib_incremental_engine;
