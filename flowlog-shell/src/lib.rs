//! The interactive shell a compiled FlowLog program reads transactions at.
//!
//! Two halves, and the split is testability: [`cmd`] is the command language
//! and nothing else, so it is exercised without a terminal; [`prompt`] is the
//! terminal, and holds only what needs one.
//!
//! Incremental mode alone has a shell, so a batch program depends on none of
//! this and carries neither this crate nor `rustyline`.

pub mod cmd;
pub mod prompt;

pub use cmd::Cmd;
pub use prompt::Prompt;
