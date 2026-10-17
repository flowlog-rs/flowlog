//! Expressions inside operator closures, from a rule's arguments and filters:
//!
//! - [`param`]: the closure's parameters.
//! - [`projection`]: the key and value tuples it emits.
//! - [`compare`] and [`constraint`]: the predicates it filters on.
//! - [`term`]: the terms all of the above are built from.

pub(crate) mod compare;
pub(crate) mod constraint;
pub(crate) mod param;
pub(crate) mod projection;
pub(crate) mod term;
