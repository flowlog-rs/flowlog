# Code Conventions

How to shape FlowLog code. Each rule stands alone and applies to every
crate in the workspace.

## Rules

1. **Display for users, Debug for developers.** `Display` renders a value
   the way its author wrote it (surface spelling, source syntax); it is
   what diagnostics show. `Debug` may add the internal identity a
   developer needs (canonical forms, cached hashes). User-visible output
   must never depend on `Debug`. Derive `Debug` by default; hand-write it
   only when the derived output would obscure identity or drown it in
   noise.

2. **Match closed sets exhaustively.** When a match covers a closed set of
   variants, name every arm instead of using `_`, so adding a variant is
   a compile error that forces a decision rather than a silent
   fall-through.

3. **A helper earns existence by owning a contract.** Split functions at
   contract boundaries (a distinct precondition, invariant, or
   transformation), not by size. A wrapper that merely renames a call is
   noise: inline it and call the real thing.

4. **Let each crate do its own job.** A concept lives in the crate whose
   domain owns it, as its own type; a type never carries knowledge for
   another layer. If a doc has to explain why some other crate needs a
   method, the method is in the wrong crate.

5. **Visibility follows role, not the current caller list.** A crate
   whose product is data (a parsed AST, a plan) exists to be read, so
   read accessors on the result it hands another crate are `pub` by
   design, even with no external caller yet. Keep the power to *build or
   change* that state internal: constructors, setters, `*_mut` accessors,
   and build-time helpers (name lookups, resolution) default to
   `pub(crate)` or narrower, so only the owning code shapes the result.
   The question is not "does someone call it yet" but "does this read the
   result (`pub`) or shape it (internal)". Three refinements the simple
   version misses:

   - **The result is what crosses a crate boundary, not every readable
     type.** An intermediate that only a sibling module reads (a plan's
     `nodes`, feeding a report builder in the same crate) stays
     `pub(crate)` along with its read accessors; `pub` is for the type a
     *downstream crate* consumes.
   - **Producer and owner can differ.** When another crate is the
     legitimate producer (the compiler populating a plan the profiler
     crate defines), that record/build API is `pub` for it. "Internal"
     means "not offered to consumers", not "never crosses a crate
     boundary".
   - **An invariant shaped from a sibling module uses `pub(in path)` /
     `pub(super)`, not `pub(crate)`.** When the recording methods live in
     a neighbor module, scope the fields to the parent module that owns
     the invariant, so the rest of the crate still cannot break it.

6. **Group a file by type, not by kind.** A type's definition, inherent
   impl, and trait impls sit together in one section (separated by the
   rulers of comments.md rule 9); a free helper sits next to its only
   caller. Definitions interleaved with other types' implementations
   force the reader to jump.

7. **Module entries may define their central contract.** A module entry
   file (`foo.rs` or `foo/mod.rs`) may define its central traits and
   types alongside submodule declarations and re-exports. Substantial
   implementations belong in child modules. Prefer `foo.rs` with a
   `foo/` directory for its children; do not create a separate file solely
   to hold a small central contract. `lib.rs` remains limited to module
   declarations and re-exports.

8. **Generated code allows only the lints the user's program causes.**
   Every `allow` on generated code names one lint, and that lint must be
   one codegen cannot satisfy because it is driven by user input:
   `non_snake_case` and `non_camel_case_types` on identifiers derived from
   user-chosen relation names (DOOP's `_MethodLookup_*`), and
   `unused_comparisons` (with clippy's `absurd_extreme_comparisons`) on a
   comparison the program writes that its column's type decides alone
   (`u >= 0` on a `uint64`). Each such allow
   is scoped as narrowly as possible and carries a comment naming the
   cause. Every other warning, rustc or clippy, is a generator bug: an
   `unused_variables` is a binding nothing reads, a `dead_code` is a
   lifecycle method emitted for a relation that does not need it, a
   `clone_on_copy` is a clone emitted without consulting the column's
   type. Fix the generator, never widen the allow, and never emit a
   blanket group such as `clippy::all` or `unused`.

9. **Unique ownership before `Rc` and `Arc`.** Shared ownership adds a
   refcount, interior-mutability plumbing, and a graph the reader must
   hold in their head. Reach for it only when the borrowing structure
   that avoids it would be unreasonably complex to write or maintain,
   and say so where it is introduced; that is a review discussion, not
   a default. Cloning an `Rc` or `Arc` is the intended way to use one
   and is not the deep copy the next rule is about.

10. **Clone to copy, not to dodge the borrow checker.** `.clone()` is for
    a value that genuinely needs its own copy. When a borrow fails, fix
    the ownership shape first; fall back to a clone only when the
    borrowing relationships would be excessively complex, and weigh the
    allocation against where the code runs.

11. **Functions before `macro_rules!`, `macro_rules!` before code
    generation, both before proc macros.** A macro creates a dialect:
    IDEs, search, and the next reader all see less through it, and it
    costs more to refactor across. Reach for the next rung only when the
    previous one cannot express the thing. This rule is about hand-written
    source; flowlog-codegen emitting a whole program is its job, and
    code.md rule 8 governs what it emits.

12. **No glob imports.** `use foo::*` hides where a name comes from and
    breaks under any automated rename. Two exceptions: `use super::*`
    inside a test module, and a third-party prelude the crate is built
    around (`rayon::prelude::*`), which a reviewer may allow. Do not
    define preludes.

13. **An iterator chain carries logic; a loop carries control flow.**
    Use a combinator when it names the whole operation (`any`, `all`,
    `filter_map` into `collect`). Write a `for` loop when the chain would
    branch, mutate state outside itself, or run long enough to need a
    comment. Never build a chain in a `for` header: bind it to a named
    variable first, so the name documents the chain. `for_each` needs a
    reason a `for` loop cannot give.

14. **Write the type where inference would make the reader guess.** A
    `.into()`, a `.parse()`, or the result of a long chain gets its type
    on the `let` binding, not as a turbofish; a turbofish the compiler
    insists on may use `_` for the parts it can infer. Anyone familiar
    with the codebase should be able to name a binding's type without a
    tool.

15. **`format!` and `write!` for concatenation and substitution.** Three
    or more pieces go through `format!("{a}{b}{c}")`, not `+`, which
    allocates per step; a literal with holes goes through a format string,
    not `str::replace` over a home-grown template.

16. **Stable Rust only.** No `#![feature]`. The nightly toolchain is for
    rustfmt's unstable options and nothing else.
