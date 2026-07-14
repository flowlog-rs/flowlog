# Comment and Doc Style

How to write comments and rustdoc in FlowLog. Each rule stands alone and
applies to every crate in the workspace. Follow them in everything you
write or modify, new code and edits alike; when you touch existing code
that violates one, fix it in place. Examples come from
`flowlog-parser/src/types/data_type.rs`, but the rules are not
parser-specific.

## Rules

1. **Code first.** Before writing a comment, try to make the code
   self-explaining: a better name, a smaller function, a clearer shape.
   A comment supplements clear code; it never substitutes for it.

2. **Never restate the code.** A comment that repeats what the reader can
   already see is noise. Delete it on sight. Signatures count: do not
   restate types or visibility (`pub(crate)` already says crate-internal).
   Rustdoc links every type in a signature, so pointing a variant or field
   doc at its own wrapped type (`(see [`Type`])`) is noise; reserve
   cross-references for items the signature does not already show.

   ```rust
   // Bad: the variant name already says this.
   /// 8-bit signed integer type.
   Int8,

   // Good: adds a fact the name does not carry.
   /// Fixed-arity tuple column; fields may nest but not recurse.
   FixedTuple(Vec<DataType>),
   ```

3. **`///` states the contract.** A doc comment says what an item does,
   what it guarantees, and what it requires: inputs, outputs, invariants,
   and a `# Panics` section when it can panic. It does not walk through
   the implementation. State invariants, not how they are maintained;
   the write sites carry that. Do not name an item's callers either
   ("used by the X pass"): the caller list is grep's job and drifts;
   state what the item guarantees that its siblings do not.

4. **`//` explains why, never what.** A body comment records only what
   the code cannot show: a constraint, an ordering requirement, a
   rejected alternative, a cross-module assumption.

   ```rust
   // Good: explains why this order is required.
   // Phase 1: reserve id + name before resolving fields, so a self-typed
   // field resolves to this tuple and the recursion check below sees it.
   ```

5. **`//!` module docs are a map.** A few lines on what lives in the file
   and how the pieces relate. Not a tutorial, not a changelog.

6. **One source of truth.** Document a fact once, at the item that defines
   it; everywhere else points there. The same list written in two docs
   means one of them is already going stale.

   ```rust
   // Good: FromStr does not re-list the spellings; PRIM_NAMES owns them.
   /// Parses a [`DataType`] from its grammar spelling, accepting any
   /// canonical name or alias listed in the `PRIM_NAMES` table.
   ```

7. **ASCII only.** Everything in a source file, comments and docs
   included, is plain ASCII. When non-ASCII data is itself under test,
   write it with escapes (`"\u{e9}"`) so the source stays ASCII.

   | Instead of              | Write                                        |
   |-------------------------|----------------------------------------------|
   | em dash                 | restructure with `:`, `;`, or a new sentence |
   | right arrow             | prose ("maps to", "becomes") or `->`         |
   | ellipsis character      | `...`                                        |
   | math symbols            | `join`, `intersect`, `not`, `>=`, `!=`       |
   | box-drawing separators  | `-` and `=` (see rule 9)                     |

8. **One voice per kind of item.** The same shape of item opens with the
   same words. In this codebase: boolean predicates read "Returns `true`
   for/if ..."; anything that can panic carries a `# Panics` section;
   registration functions start "Registers ...". Docs are declarative
   statements, never questions.

9. **Plain separators.** Top-level file sections use the full-width `=`
   ruler; subsections are a short `// --- Title ---` with no trailing
   padding to maintain.

   ```rust
   // =============================================================================
   // TypeRegistry
   // =============================================================================

   // --- Compatibility predicates used by the typechecker ---
   ```

10. **Tests: the name is the doc.** Name a test after the behavior it pins
    (`self_referential_tuple_is_rejected`). Use `///` on a test only to
    state the invariant or why the case matters; use `//` inside only for
    setup that is not obvious.

11. **Plain words over jargon.** When a name is a term of art (`meet`,
    `stratify`), the doc says what it does in plain language; a reader
    should not need the theory to use the function.

## Enforcement

A rule a tool can check belongs in the tool, not in this file (rustfmt,
the clippy deny-list, and `_typos.toml` already work this way). The ASCII
rule will become a CI grep once existing files are cleaned; until then it
applies to all new and touched code.
