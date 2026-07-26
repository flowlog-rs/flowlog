//! Type-checks every rule and pins every polymorphic literal to the concrete
//! width its context implies.
//! # Two jobs
//!
//! - **Check.** Reject programs whose types don't line up.
//! - **Pin literals.** Every `Constant::Int(_)` / `Float(_)`
//!   placeholder from the parser is rewritten in place to its concrete
//!   counterpart via [`Constant::pin`]. After [`check_program`] returns
//!   `Ok`, no polymorphic literal survives anywhere in the program:
//!   catalog, planner, and codegen can call `data_type()` unconditionally.
//!
//! # What we reject
//!
//! Primitive checks (Pass 1):
//! - A variable bound to one type then reused as another (`A(x, _), B(x, _)`
//!   disagreeing on column 0).
//! - Mixing concrete types in arithmetic or comparison (`Int32 + Float64`;
//!   `x = s` for `x: Int32, s: String`).
//! - An operator on an incompatible type: `+-*/%` on `Bool`/`String`, ordering
//!   `<`/`>` on `Bool`, `match`/`contains` on non-strings, or a built-in arg
//!   outside its allowed set (`cat(1, 2)`).
//! - A constant whose family doesn't match the column (`5.0` into `Int32`,
//!   `"x"` into `Bool`), or whose value doesn't fit the width its context
//!   pins it to (`300` into an `Int8` column).
//! - UDF calls that are undeclared, wrong-arity, or of the wrong arg family.
//! - `ord(s)` used without `--str-intern`.
//! - `sum`/`avg`/`min`/`max` over a non-numeric input, or an output type that
//!   contradicts the op.
//! - Tuple misuse: `_` in a construct (`(a, _)`), wrong field count or type,
//!   or destructuring a non-tuple.
//! - A head whose arity or column type doesn't match the relation's `.decl`.
//!
//! Subtype and cast checks (Pass 2):
//! - Sibling subtypes joined at one variable or compared (`x: UserId` with
//!   `x: ProductId`, both `<: number`).
//! - Narrowing parent -> subtype in a head without `as()` (a `number` into a
//!   `UserId` column).
//! - `as(e, T)` where `T` is undeclared, or `e` and `T` have different
//!   primitive roots.
//!
//! # What we allow
//!
//! - Integer literals fit any integer column (`Int8`..`UInt64`), floats any
//!   float column; the width is fixed by context and written back by
//!   [`Constant::pin`].
//! - Implicit widening subtype -> parent in a head column, no `as()` needed
//!   (`UserId` into a `number` column).
//! - `as(e, T)` between types sharing a primitive root (two `<: number`
//!   subtypes); the cast is stripped after checking.
//! - Unbound variables in negated atoms, comparisons, or UDF calls: reported
//!   separately by the range-restriction pass, not here.

mod env;
mod primitive;
mod subtype;

use flowlog_common::Config;

use self::env::PrimitiveEnv;
use crate::ParseError;
use crate::Program;

/// Type-check every rule and pin each polymorphic literal to its concrete
/// width. Stops at the first failure; on `Ok(())` the program's literals are
/// fully concrete and subtype rules have been enforced.
///
/// Runs two passes: Pass 1 (`primitive`) checks primitive `DataType`s and
/// pins literals; Pass 2 (`subtype`) enforces subtype rules and lowers
/// `as()` casts. `config` gates config-dependent builtins (`ord`) and is
/// updated with `ord`'s serial-load requirement.
pub fn check_program(program: &mut Program, config: &mut Config) -> Result<(), ParseError> {
    let env = PrimitiveEnv::from_program(program);
    primitive::check_and_pin(program, &env, config)?;
    subtype::check_and_lower(program)
}
