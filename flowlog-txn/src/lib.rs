//! The interactive shell a compiled FlowLog program reads transactions at.
//!
//! Three modules, three jobs. [`cmd`] is what a line means: a control
//! word, or an op to stage, with no terminal so it is exercised without
//! one. [`driver`] is how a commit runs: the epoch choreography every
//! worker coordinates through. [`prompt`] is the terminal, holding only
//! what needs one, behind the default `prompt` feature.
//!
//! Incremental mode alone has a shell, so a batch program depends on none
//! of this. A library-mode host takes the crate without default features:
//! its generated engine speaks [`driver`]'s wire state but never opens a
//! prompt, so `rustyline` stays out of its tree.

pub mod cmd;
pub mod driver;
#[cfg(feature = "prompt")]
pub mod prompt;

pub use cmd::Cmd;
pub use cmd::TxnOp;
pub use driver::Event;
pub use driver::SharedTxn;
pub use driver::drive;
pub use driver::follow;
#[cfg(feature = "prompt")]
pub use prompt::Prompt;
