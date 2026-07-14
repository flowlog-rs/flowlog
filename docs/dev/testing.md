# Unit Test Style

How to write unit tests in FlowLog. Each rule stands alone and applies to
every crate in the workspace. Comments inside tests follow
`docs/dev/comments.md` (rule 10 there). Examples come from
`flowlog-parser/src/types/data_type.rs`, but the rules are not
parser-specific.

## Rules

1. **The name states the behavior pinned.** Reading the name alone must
   say what broke when the test fails: `self_referential_tuple_is_rejected`,
   not `test_tuple_2`.

2. **One invariant per test.** A failure should point at exactly one
   broken behavior. Several assertions are fine when they pin the same
   invariant from both directions (`meet(s, n)` and `meet(n, s)`).

3. **Hardcode the expectations.** Expected values are written out, never
   re-derived from the code under test; a bug in the implementation must
   not be able to compute itself into the expectation.

4. **Table-drive the matrices.** When one property must hold across many
   variants, use an rstest case table with one row per variant, and
   bundle related predicates per row so they cannot drift apart:

   ```rust
   //    type                is_integer  is_float  is_numeric  is_tuple
   #[case(DataType::Float32, (false,     true,     true,       false))]
   ```

5. **Guard single-source tables by iterating them.** A test that loops
   over the table itself (`from_str_accepts_every_prim_name` walks
   `PRIM_NAMES`) covers future rows automatically.

6. **Pin the exact error.** An input that must fail asserts which error
   it produces (`assert_err!(..., ParseError::RecursiveTuple { .. })`),
   never just that something failed.

7. **Test contracts through the API.** Prefer the public (or crate)
   surface; reach into a private helper directly only when layering makes
   a path unreachable from outside, and say why in the test's doc.

8. **Close the loop where one exists.** Anything with paired directions
   (`Display` and `FromStr`, register and lookup) gets a roundtrip test.

9. **Comprehensive, not exhaustive.** Cover every contract behavior:
   each documented guarantee, each error, each boundary the code branches
   on. Do not chase line coverage or a 100% mutation score; a missed
   mutant in defensive or unreachable code is an acceptable finding, not
   a to-do. `cargo mutants --file <file>` is a probe for forgotten
   behaviors, not a target to satisfy.

10. **Keep setup visible.** Small local helpers that do real work
    (`fields()` turning literal pairs into owned tuples) beat fixtures
    and frameworks, but a helper that merely renames a call
    (`fn reg() { TypeRegistry::new() }`) is noise: call the real thing.
    A reader should see a test's whole story inside the test.

11. **Tests are layered like the code.** Each module's tests verify its
    own block and treat lower layers as already-correct; never re-check a
    lower layer through a full pipeline run. Drive a unit through the
    smallest entry that can produce its input (in flowlog-parser,
    `test_util::parse_node` runs the grammar from a single rule), and pin
    each behavior at the lowest layer that can observe it.

12. **Cover the negative space.** A contract is what it accepts and
    what it refuses: an `Option` gets `Some` and `None` tests, a
    `Result` gets `Ok` and `Err`, a predicate gets `true` and `false`
    inputs, a matcher gets a non-match. A suite that exercises only the
    happy rows proves acceptance but never rejection
    (`semiring_suffix_covers_every_numeric_type` is paired with
    `semiring_suffix_is_none_for_non_numeric`).
