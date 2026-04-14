//! Dataflow I/O boundaries: EDB input handles and IDB output buffers.

pub(crate) mod edb_handles;
pub(crate) mod idb_buffers;

pub(crate) use idb_buffers::InspectorCodegen;
