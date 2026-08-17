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

10. **Runtime input failures return the runtime's error currency.** A file
    the user named that is missing, unreadable, or malformed has no span
    to point at, so it is neither a source diagnostic nor an internal
    error: `flowlog-runtime` returns `RuntimeError`, which the generated
    driver prints with the relation and path before exiting 1. Resource
    exhaustion is the exception, and panics: an interner out of keys is
    the allocation failure it looks like, and no caller can act on it. A
    variant earns its place when a caller acts on it; today the acting is
    structural: a row that fails to decode is skipped and reported, and a
    cursor that fails stops the load.

11. **Every error type implements `FlowlogError`.** The trait lives in
    `flowlog-error`, carries `is_internal`, and is what lets a caller read
    a message and tell our bug from the user's mistake without knowing
    which stage produced it. `Diagnostic` extends it for errors that have
    a span to render; `RuntimeError` implements it plainly, because a data
    file is not the program's source. The crate stays light because
    compiled programs carry it.
