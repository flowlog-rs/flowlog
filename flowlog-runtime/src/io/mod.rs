//! Reading relations into the engine and writing them back out.
//!
//! Split by direction, and each direction reads the same way:
//!
//! | | [`input`] | [`output`] |
//! |---|---|---|
//! | the API | [`Ingest`] | [`drain`](output::drain) |
//! | the resource | `reader` | [`Writer`] |
//! | one record | [`Decode`] | [`Encode`] |
//!
//! Two things sit above the split, because both directions use them:
//! [`spec`], what generated code says a relation is, and [`atomic`], the
//! one way a file gets written.
//!
//! Generated code names only what this module re-exports, so the layout
//! below is free to move without touching an emitted line.

pub mod atomic;
pub mod input;
pub mod output;
pub mod spec;

pub use atomic::AtomicFile;
pub use input::decode::Decode;
pub use input::decode::typed::DecodeField;
pub use input::decode::untyped::DecodeCell;
pub use input::decode::untyped::TextRow;
pub use input::ingest::Ingest;
pub use input::session::Session;
pub use output::drain_flat;
pub use output::drain_sorted;
pub use output::drain_topk;
pub use output::encode::Encode;
pub use output::encode::text::EncodeCell;
pub use output::encode::text::TextRows;
pub use output::encode::typed::EncodeField;
pub use output::for_each_flat;
pub use output::for_each_sorted;
pub use output::for_each_topk;
pub use output::writer::Writer;
pub use output::writer::text::TextWriter;
pub use output::writer::vec::DeltaWriter;
pub use output::writer::vec::VecWriter;
pub use spec::Format;
pub use spec::InputSpec;
pub use spec::OutputSpec;
pub use spec::RelationSpec;
pub use spec::ShardKey;
