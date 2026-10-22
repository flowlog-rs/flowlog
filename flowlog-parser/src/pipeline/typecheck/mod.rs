//! Type checking, and the lowering that depends on it. [`check_program`]
//! runs four steps, each valid only after the one before:
//!
//! - `infer` types every rule's variables by unification (on `solver`),
//!   over the rules as written, and rejects a program whose types do not
//!   line up.
//! - `substitute` replaces each variable an equality defines by its value,
//!   now that the definition has been checked as written: an assignment's
//!   variable by its expression, a destructure's components by projections
//!   of its tuple. Later stages read a variable's value only from body
//!   atoms and aggregates.
//! - `pin` rewrites each polymorphic `Constant::Int(_)` / `Float(_)`
//!   placeholder in place to its concrete width via [`Constant::pin`], once
//!   per use a value was substituted into, and gives each aggregate's
//!   variable its type. After it, no polymorphic literal survives anywhere
//!   in the program: catalog, planner, and codegen can call `data_type()`
//!   unconditionally.
//! - `subtype` enforces the subtype rules the erased primitive types cannot
//!   express, and lowers `as()` casts.
//!
//! # What we reject
//!
//! Inference:
//! - A variable bound to one type then reused as another (`A(x, _), B(x, _)`
//!   disagreeing on column 0).
//! - Mixing concrete types in arithmetic or comparison (`Int32 + Float64`;
//!   `x = s` for `x: Int32, s: String`).
//! - An operator on an incompatible type: `+-*/%` on `Bool`/`String`, ordering
//!   `<`/`>` on `Bool`, `match`/`contains` on non-strings, or a built-in arg
//!   outside its allowed set (`cat(1, 2)`).
//! - A constant whose family doesn't match the column (`5.0` into `Int32`,
//!   `"x"` into `Bool`).
//! - UDF calls that are undeclared, wrong-arity, or of the wrong arg family.
//! - `ord(s)` used without `--str-intern`.
//! - `sum`/`mean`/`min`/`max` over a non-numeric input, or an output type that
//!   contradicts the op.
//! - Tuple misuse: `_` in a construct (`(a, _)`), wrong field count or type,
//!   or destructuring a non-tuple or with a pattern of another width than
//!   the tuple (`(a, b, c) = p` for a 2-field `p`).
//! - A body atom whose arity doesn't match the relation's `.decl`.
//! - A head whose arity or column type doesn't match the relation's `.decl`.
//!
//! Pinning:
//! - A literal whose value doesn't fit the width its context pins it to
//!   (`300` into an `Int8` column).
//!
//! Subtype and cast checks:
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
//! - A variable defined as a value with no variables (`t = 1`) fills columns
//!   of different widths in one rule; each use is typed on its own.
//! - Implicit widening subtype -> parent in a head column, no `as()` needed
//!   (`UserId` into a `number` column).
//! - `as(e, T)` between types sharing a primitive root (two `<: number`
//!   subtypes); the cast is stripped after checking.
//! - Unbound variables in negated atoms, comparisons, or UDF calls: reported
//!   separately by the range-restriction pass, not here.

mod declarations;
mod infer;
mod pin;
mod solver;
mod substitute;
mod subtype;

use flowlog_common::Config;

use crate::ParseError;
use crate::Program;

/// Type-checks an assembled program and lowers it: on `Ok(())` every
/// literal is pinned to a concrete width, every aggregate variable is
/// typed, every `as()` cast is lowered, and no rule body defines a
/// variable with `=`. Stops at the first error. `config` gates
/// config-dependent builtins (`ord`) and is updated with `ord`'s
/// serial-load requirement.
pub fn check_program(program: &mut Program, config: &mut Config) -> Result<(), ParseError> {
    let types = infer::infer(program, config)?;
    substitute::substitute_definitions(program.rules_mut())?;
    pin::pin(program, &types)?;
    subtype::check_and_lower(program)
}
