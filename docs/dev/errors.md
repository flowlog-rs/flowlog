# Error Handling

How FlowLog code signals failure. Each rule stands alone and applies to
every crate in the workspace.

## Rules

1. **`Result` for facts, panic for bugs.** A failure is one of two things:
   a fact about the world (the user's program, a file, a row, a database,
   a thread) or a bug in FlowLog (a state an earlier stage promised could
   not occur). A fact comes back as `Result` or `Option` and reaches the
   user as a message. A bug panics: no caller can act on it, and carrying
   it through `Result` taxes every signature on the path for nothing.
   When the code runs decides what a panic costs, and so what counts as
   acceptable (rules 3 and 4): the same crate can hold both kinds.

2. **User mistakes get a diagnostic.** An error in the user's program
   maps to a dedicated error variant carrying a span, rendered as a
   source diagnostic. Never a panic, never a bare string.

3. **Code that runs in the user's program returns every environment
   failure.** That is flowlog-runtime and the text inside every `quote!`
   in flowlog-build and flowlog-compiler: generated once, then compiled
   into the user's binary and called when their program runs, on their
   timely workers, where a panic takes their program down. A failed
   read, write, load, or database call, and a poisoned lock, are facts,
   so each is a `RuntimeError` all the way to the user: the generated
   engine API (`new`, `run`, `commit`) returns `Result`, and the
   standalone binary's `main` reports it and exits nonzero. A template
   never `expect`s a `Result`. A worker thread's panic is joined and
   reported, never left hanging. What stays a panic is an invariant no
   caller can break, and the site says which.

4. **Code that runs while FlowLog compiles panics on a bug, through
   `bug!`.** That is the parser, planner, codegen, the compiler driver,
   and `Builder::build` in a `build.rs`: everything around the `quote!`,
   up to the moment the generated source is written out. It runs in
   `flowlog-compiler` or in cargo's build script, where going on past a
   broken invariant only produces wrong output and a panic costs the
   user one failed build. Such a state panics with
   `bug!("stage", "detail ...")`, and the binary's panic hook renders it
   as an internal compiler error with the bug-report link. Do not add a
   fallible lookup or an `Internal` error variant to carry a bug to the
   top; the hook does that for every panic. A state the grammar or an
   earlier pass rules out by construction is still a bug when it shows
   up, so it takes `bug!` (or `unreachable!`), not `Result`.

5. **Queries return facts; absence is `Option`.** When "no answer" is a
   legitimate fact about the input, return `Option` and leave the policy
   to the caller. A fact is not a failure.

6. **Build the error where the context lives.** Whichever side holds the
   information for a good message constructs it: the callee when it sees
   the whole story, the caller (via `ok_or_else`) when only it knows the
   situation.

7. **Never cross error domains.** A crate returns its own error type or
   a bare fact; a caller in another crate wraps the fact in its own
   error currency.

8. **`debug_assert!` is for soft self-checks.** Use it to re-verify a
   contract already guaranteed elsewhere: free in release, loud under
   test. It must never be the only guard on a real invariant.

9. **Never panic on outside values.** A function that accepts a
   caller-controlled index or user-derived value must return `Option` or
   `Result` when that value may be invalid. Direct access is acceptable
   for indices created and contained by a private structure when its
   constructor guarantees their bounds. Where a trait signature offers
   no error channel (e.g. `fmt::Display`, where returning a spurious
   `Err` makes `format!` panic anyway), make the implementation infallible
   by construction or degrade to a visible placeholder. Tests are
   exempt; `unwrap` in a test is the failure mechanism.

10. **Messages state the violated expectation with the offending values.**
    What was attempted, on what, and which rule broke. User diagnostics
    label every relevant span, not just the site of the report. A `bug!`
    names its stage and the state it found, so a report is actionable
    before anyone asks for a backtrace.
