//! Update weights, one per relation update class.
//!
//! Operators dispatch on the weight, so each class selects its own
//! implementation: [`Static`] and [`Append`] carry presence, [`Mutable`]
//! a signed count. Code names them with the module prefix
//! (`diff::Static`), since the bare words are too common to read alone.

mod append;
mod mutable;
mod r#static;

pub use append::Append;
pub use mutable::Mutable;
pub use r#static::Static;
