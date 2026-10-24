# Error Handling

How FlowLog code signals failure. Each rule stands alone and applies to
every crate in the workspace.

## Rules

1. **User mistakes get a diagnostic.** An error in the user's program
   maps to a dedicated error variant carrying a span, rendered as a
   source diagnostic. Never a panic, never a bare string.

2. **Detected invariant failures are internal errors.** When an operation
   can discover that an earlier stage produced an impossible state, report
   it through the crate's internal-error constructor, which wraps
   `flowlog_common::InternalError` and its bug-report URL. Do not add
   fallible lookups solely to recheck values created and contained by a
   private type; make its constructor establish the invariant instead.

3. **Queries return facts; absence is `Option`.** When "no answer" is a
   legitimate fact about the input, return `Option` and leave the policy
   to the caller. A fact is not a failure.

4. **Commands return `Result` when failure is always a bug.** An
   operation whose failure no caller can meaningfully branch on builds
   the internal error itself; callers propagate it with `?`.

5. **Build the error where the context lives.** Whichever side holds the
   information for a good message constructs it: the callee when it sees
   the whole story, the caller (via `ok_or_else`) when only it knows the
   situation.

6. **Never cross error domains.** A crate returns its own error type or
   a bare fact; a caller in another crate wraps the fact in its own
   error currency.

7. **`debug_assert!` is for soft self-checks.** Use it to re-verify a
   contract already guaranteed elsewhere: free in release, loud under
   test. It must never be the only guard on a real invariant.

8. **Never panic on outside values.** A function that accepts a
   caller-controlled index or user-derived value must return `Option` or
   `Result` when that value may be invalid. Direct access is acceptable
   for indices created and contained by a private structure when its
   constructor guarantees their bounds. Where a trait signature offers
   no error channel (e.g. `fmt::Display`, where returning a spurious
   `Err` makes `format!` panic anyway), make the implementation infallible
   by construction or degrade to a visible placeholder. Tests are
   exempt; `unwrap` in a test is the failure mechanism.

9. **Messages state the violated expectation with the offending values.**
   What was attempted, on what, and which rule broke. User diagnostics
   label every relevant span, not just the site of the report.

10. **`expect` over `unwrap` where a panic must stay.** When a value is
    unreachable-as-`None` by an invariant the type system cannot state,
    `expect` names that invariant ("column index was bounds-checked at
    plan time"), so the panic message says what broke, not just that
    something did. `unwrap` is for tests (rule 8) and for the rare case
    where the surrounding line already says why it cannot fail.

11. **Prove it to the compiler; do not check and then unwrap.** `if let`,
    `let ... else`, and `match` make the `Some` arm the only place the
    value exists. `if x.is_some() { x.unwrap() }` asks the reader to
    verify by hand what the compiler could have verified, and drifts
    into a real panic the first time someone edits the condition.

12. **An assertion's message says something the condition does not.**
    Use the format-string argument to state why the condition holds
    ("populated from a BTreeSet, so iteration is ordered"), to log the
    offending value, or to say whether a failure is caller misuse or an
    internal bug. A message that restates the condition
    (`assert!(v.is_sorted(), "v should be sorted")`) is noise; a comment
    above an assertion belongs in the message instead, where it reaches
    the log.
