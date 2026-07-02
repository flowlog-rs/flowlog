//! FlowLog rule-level type checker with constant-type inference.
//!
//! Runs after parse, before stratification. Binds variable types from
//! positive-atom columns, checks every body and head site against the
//! binding map, **and pins every polymorphic literal** to the concrete
//! width derived from its surrounding context. Spans come from the AST,
//! so diagnostics point at the offending expression rather than the
//! enclosing rule.
//!
//! # Two jobs
//!
//! - **Check.** Reject programs whose types don't line up.
//! - **Infer const types.** Every `ConstType::Int(_)` / `Float(_)`
//!   placeholder from the parser is rewritten in place to its concrete
//!   counterpart via [`ConstType::pin`]. After [`check_program`] returns
//!   `Ok`, no polymorphic literal survives anywhere in the program —
//!   catalog, planner, and codegen can call `data_type()` unconditionally.
//!
//! # What we reject
//!
//! - A variable bound to one type but reused as another
//!   (`A(x, _), B(x, _)` where `A` and `B` disagree on column 0).
//! - Arithmetic or comparison between two concrete types that differ
//!   (e.g. `Int32 + Float64`, `x = s` where `x: Int32, s: String`).
//! - Operators applied to an incompatible type: `+-*/%` on `Bool` or
//!   `String`, `cat` on anything non-string, `<`/`>` on `Bool`.
//! - A constant whose family doesn't match the column (`5.0` into
//!   `Int32`, `"x"` into `Bool`).
//! - Calls to undeclared UDFs, wrong arity, or arg of the wrong family.
//! - `sum`/`avg`/`min`/`max` over a non-numeric input, or declared with
//!   an output type that contradicts the op.
//! - A head arity or column type that doesn't match the relation's
//!   `.decl`.
//!
//! # What we allow
//!
//! - Integer literals match any integer column (`Int8`..`UInt64`); float
//!   literals match any float column (`Float32`/`Float64`). The width is
//!   fixed by context and written back by [`ConstType::pin`].
//! - We do **not** range-check integer literals: `300` into a `UInt8`
//!   column passes here and is caught later by the Rust compiler on the
//!   generated code.
//! - Unbound variables in negated atoms, comparisons, or UDF calls —
//!   reported separately by the range-restriction pass, not here.

mod env;
mod error;
mod primitive;
mod subtype;

use flowlog_common::Config;
use flowlog_parser::Program;

pub use error::TypeCheckError;

use crate::env::PrimitiveEnv;

/// Type-check every rule and pin each polymorphic literal to its concrete
/// width. Stops at the first failure; on `Ok(())` the program's literals are
/// fully concrete and subtype rules have been enforced.
///
/// Runs two passes: Pass 1 ([`primitive`]) checks primitive `DataType`s and
/// pins literals; Pass 2 ([`subtype`]) enforces subtype rules and lowers
/// `as()` casts. `config` is consulted for config-gated builtins.
pub fn check_program(program: &mut Program, config: &Config) -> Result<(), TypeCheckError> {
    let env = PrimitiveEnv::from_program(program);

    primitive::check_and_pin_rules(program, &env)?;
    primitive::check_builtin_config_requirements(program, config)?;
    primitive::check_and_pin_facts(program.facts_mut(), &env)?;

    subtype::check_and_lower(program)
}
