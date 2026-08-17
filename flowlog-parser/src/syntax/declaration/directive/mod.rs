//! `.input`, `.output`, and `.printsize` directives.
//!
//! One leaf per directive, each also holding the resolved form its
//! parameters take, which the enclosing [`Relation`](super::Relation)
//! adopts: [`InputSource`] for `.input`, [`OutputSink`] for `.output`.

mod input;
mod output;
mod params;
mod printsize;

pub(crate) use input::InputDirective;
pub use input::InputSource;
pub use output::OrderKey;
pub(crate) use output::OutputDirective;
pub use output::OutputSink;
pub(crate) use params::parse_io_params;
pub(crate) use printsize::PrintSizeDirective;
