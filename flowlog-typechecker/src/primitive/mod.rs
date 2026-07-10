//! Pass 1: primitive `DataType` checking and literal pinning. Subtype-blind —
//! [`crate::subtype`] is Pass 2.
//!
//! Per-construct bricks over `ty` (the `LitKind` lattice): `expr` infers and
//! pins expressions; `atom`, `fact`, `compare`, and `rule` (the composer)
//! check and pin each construct. Names read `<verb>_<node>` — `infer`, `check`,
//! `pin`, or `check_and_pin` for both. `check_program` calls
//! `rule::check_and_pin_rules`, `builtin::check_ord`, and
//! `fact::check_and_pin_facts` directly.

mod atom;
pub(crate) mod builtin;
mod compare;
mod expr;
pub(crate) mod fact;
pub(crate) mod rule;
mod ty;

use std::collections::HashMap;

use flowlog_common::Span;
use flowlog_parser::DataType;

/// Var -> (first-seen type, first-seen span). Later uses must agree.
/// Private, so it stays confined to the `primitive` subtree — the subtype
/// pass keeps its own `TypeId`-keyed map.
type Bindings = HashMap<String, (DataType, Span)>;
