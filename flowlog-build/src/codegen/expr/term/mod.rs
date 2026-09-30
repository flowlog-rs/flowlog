//! Terms: a rule's arguments (variables, constants, arithmetic, built-in and
//! UDF calls) as Rust expressions, shared by the projections, comparisons,
//! and constraints above.

pub(crate) mod arithmetic;
pub(crate) mod builtin;
pub(crate) mod constant;
pub(crate) mod udf;
