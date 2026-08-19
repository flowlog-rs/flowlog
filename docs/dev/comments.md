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
   the code cannot show. One that fits none of these kinds is almost
   always rule 2 noise:

   - **Constraint or ordering.** Why a step sits where it does, and what
     breaks if it moves.
   - **Load-bearing choice.** The distant assumption a line is holding
     up: "collect into a `BTreeSet`; the merge below assumes iteration is
     ordered".
   - **Why not the obvious thing.** When the code passes over the
     idiomatic shape, name the alternative and why it loses here, so the
     next reader does not re-derive it and change it back.
   - **Correctness argument.** The informal proof that a non-obvious step
     is right; an assertion pairs well with one. If the argument stops
     holding halfway through writing it, the bug is in the code.
   - **Hard-won fix.** The incantation that took an afternoon to find,
     and the symptom it cures, so nobody pays that afternoon twice.
   - **Constant rationale.** What a literal measures, how the value was
     picked, and what changing it costs. "Picked arbitrarily, nothing
     measured" is a real answer and worth the line.
   - **External reference.** A permalink to the paper, issue, or
     implementation being followed, and every point where this code
     diverges from it.
   - **Algorithm outline.** For a long routine, the abstract steps
     interleaved with the code implementing them. Skip it when the code
     already reads as those steps.

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

11. **Comments wrap at 80 columns.** Code lines follow rustfmt's width;
    comment and doc lines hold to 80 columns. Enforcement belongs to
    rustfmt (`wrap_comments` + `comment_width`) once the one-time
    workspace reflow lands as its own change; until then the rule applies
    to all new and touched comments.

12. **Plain words over jargon.** When a name is a term of art (`meet`,
    `stratify`), the doc says what it does in plain language; a reader
    should not need the theory to use the function.

13. **A `TODO` says enough to act on.** Name what is missing and what
    unblocks it. A keyword is a bookmark for its author, not a note for
    whoever finds it a year later.

    ```rust
    // Bad: unactionable by anyone else.
    // TODO: consider cardinalities

    // Good: states the change and its precondition.
    // TODO: compute the fingerprint here and go pub(crate) once
    // flowlog-build's catalog stops hand-constructing atoms.
    ```

14. **Record a trade-off where it was made.** A choice that had a live
    alternative carries four parts: the situation, the option taken, what
    it buys, and what it costs. Next to the code it stays discoverable
    and gets revised with it; in a review thread it has to be relitigated
    every year.

    ```rust
    // Good: situation, option, gain, cost.
    // Interner keys come from the program (`.dl` literals and input
    // facts), never from an adversary, so we take `FxBuildHasher` over
    // the default SipHash for the per-byte win, accepting that the pool
    // would be wide open to HashDoS if untrusted input ever reached it.
    ```

15. **Write for a reader without your context.** Full sentences and real
    punctuation; expand all but the most obvious abbreviations. Cut
    "obviously", "simply", "just", and "trivially": each one asserts the
    reader already knows the thing the comment exists to tell them. Name
    variables and functions exactly, since a comment that misnames what
    it describes sends the reader somewhere worse than silence would.

16. **A comment travels with the code it describes.** When you change a
    line, the comment above it is part of that diff. Prose describing an
    earlier version of the code outranks the code in the reader's head,
    and they pay for the contradiction before they can dismiss it.

## Enforcement

A rule a tool can check belongs in the tool, not in this file (rustfmt,
the clippy deny-list, and `_typos.toml` already work this way). The ASCII
rule will become a CI grep once existing files are cleaned; until then it
applies to all new and touched code.

There is no target ratio of comments to code, and "needs a comment" is
not a review finding by itself: name the rule and the fact that is
missing, or let the code ship.
