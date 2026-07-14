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
   read accessors on its public result types are `pub` by design, even
   with no external caller yet. Keep the power to *build or change* that
   state internal: constructors, setters, `*_mut` accessors, and
   build-time helpers (name lookups, resolution) default to `pub(crate)`
   or private, so only the owning crate shapes the result. The question
   is not "does someone call it yet" but "does this read the result
   (`pub`) or shape it (internal)".

6. **Group a file by type, not by kind.** A type's definition, inherent
   impl, and trait impls sit together in one section (separated by the
   rulers of comments.md rule 9); a free helper sits next to its only
   caller. Definitions interleaved with other types' implementations
   force the reader to jump.

7. **The crate root defines nothing.** `lib.rs` declares modules and
   re-exports the public surface; every item, impl, and test lives in a
   module. An item with no obvious module is a missing module, not a
   crate-root resident.
