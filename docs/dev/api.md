# API Shape

How to shape signatures, types, traits, and names in FlowLog. Each rule
stands alone and applies to every crate in the workspace.

FlowLog is application code, not a library: each crate solves one
specific problem for the crates beside it in this workspace, so an API
favors simplicity and directness over reusability. Do not generalize a
signature ahead of a second caller.

## Function inputs and outputs

1. **Borrow the data, not its container.** A parameter or return type
   names the data a caller works with, never the box it happens to live
   in: `&[T]` over `&Vec<T>`, `&str` over `&String`, `Option<&T>` over
   `&Option<T>`, `&T` over `&Box<T>`. Clippy's `ptr_arg` and `ref_option`
   catch the parameter cases; return types (`fn items(&self) -> &[T]`)
   are on the author. The exception is a type whose container-level API
   the caller needs (`Vec::capacity`), and that is rare.

2. **Return what the function builds; accept what the caller has.**
   Library code reaches for `impl Iterator` over `Vec` in return types
   and `impl IntoIterator` over `&[T]` in parameters to keep every
   caller's options open; application code does not need that. Return a
   `Vec` when the function builds one anyway, the caller will index it,
   or the caller collects it regardless; return `impl Iterator` when the
   caller only walks the result and the allocation would be pure waste.
   Accept `&[T]` unless a real caller holds something that is not a
   slice.

3. **A bool parameter is named at the call site or replaced by an enum.**
   A bare `true` in an argument list says nothing. When the method name
   carries the meaning (a builder's `profile(enabled: bool)`), a bool is
   fine. When two or more bools travel together, or the two states have
   names of their own (`Polarity::Positive`), use a two-variant enum. A
   positional bool that stays carries an inline `/* profile */` at the
   call site.

4. **`impl Into<T>` parameters need a measured ergonomics win.** The
   generic costs a monomorphized copy per call type, a harder signature,
   and occasional inference failures, to save a `.into()` at the call
   site. Application code spells the `.into()`.

## Types

5. **Newtype when the meaning differs; alias when only the spelling does.**
   A newtype (`FileId(u32)`) is for a value with its own semantics or
   invariants, one that must not be confused with a same-shaped sibling,
   or one whose representation may change. A type alias is a shorthand
   for a long spelling and adds no safety; it is fine for a private
   `Bindings = HashMap<..>` that one module reads.

6. **Named fields over tuples.** A tuple with more than one field, and
   especially an alias over one (`type OrderKey = (usize, DataType,
   bool)`), hides what `.0` and `.2` mean from every reader. Give the
   positions names in a struct. Exceptions: single-field newtypes, and a
   tuple that is built and destructured within a few lines.

7. **An option bag implements `Default`.** A struct of public fields that
   exists to configure a call implements `Default`, so a caller can write
   `Options { depth: 3, ..Default::default() }` and new fields do not
   break existing call sites.

## Traits

8. **A trait exists for a bound, a `dyn`, or an extension; never to
   organize code.** Define a trait to write `T: Trait`, to accept or
   return `impl Trait` or `dyn Trait`, or to add methods to a foreign
   type. Everything else is an inherent `impl`. A trait with exactly one
   implementor and no generic user is a module that lost its way.

9. **Generics over `dyn Trait` in signatures.** `impl Trait` and `T:
   Trait` parameters inline and monomorphize; `dyn` adds a fat pointer and
   a vtable call. Reach for `dyn` only to store heterogeneous values or
   where a closure parameter is called once and monomorphization buys
   nothing (`&mut dyn Write` handed to a one-shot writer).

10. **Implement the standard trait instead of its look-alike method.**
    `impl FromStr`, not `fn from_str`; `impl Display`, not `fn
    to_string`; `impl Default` when `new()` takes no arguments; `Hash` and
    `Eq` on anything that becomes a map key; `Ord` where a total order is
    real. Derive when a derive says it; hand-write only when the derived
    semantics are wrong (see code.md rule 1 for `Debug`). Add other
    impls when a caller needs them, not ahead of time.

11. **Implement expected API sets together.** `len` comes with
    `is_empty`; `PartialOrd` with `PartialEq`; a `From` that is a primary
    way to build a type comes with a named constructor (`from_spec`) so
    the construction reads at the call site.

12. **An extension trait is named `FooExt` and owned by the type's
    owner.** A trait whose only job is to add methods to a type from
    another crate carries the `Ext` suffix and lives with whoever owns
    the type, so two extensions cannot collide on a method name. This
    workspace has none today; the rule is here so the first one is
    shaped right.

## Naming

13. **Constructors and accessors follow the standard library's names.**

    | Role                                  | Name                       |
    |---------------------------------------|----------------------------|
    | infallible constructor                | `new`, `new_with_foo`      |
    | fallible constructor                  | `try_new`                  |
    | value-to-value conversion             | `from_foo` (+ `From` impl) |
    | read accessor (`&T`, or `T: Copy`)    | `foo`                      |
    | mutable accessor                      | `foo_mut`                  |
    | setter                                | `set_foo`                  |
    | setter returning the old value        | `replace_foo`              |
    | clear and return the old value        | `take_foo`                 |
    | scoped access through a closure       | `with_foo`, `with_mut_foo` |

    Conversion methods use the `as_` (free view), `to_` (expensive copy),
    `into_` (consumes self) prefixes with the matching receiver; clippy's
    `wrong_self_convention` checks the receiver. Visibility of each row
    is governed by code.md rule 5, not by this table.

14. **Abbreviate only domain terms.** `expr`, `pred`, `rel`, `agg`,
    `src`, and `fmt` are vocabulary this project's docs and grammar use,
    and they stay. Never shorten a word by deleting letters (`mngr`,
    `cnt`), and never coin a project-private acronym; a reader from
    outside the project must be able to read every identifier. Docs
    spell the full term on first use (comments.md rule 15).

15. **Lifetimes and generic parameters say what they bind.** A lifetime
    on a type is descriptive (`'src`, `'reg`); `'_` wherever it works;
    `'a` only to tie a method's output to one input, and at most two such
    letters per signature. A generic parameter is short but indicative
    (`K`, `V`, `R` for a closure's return); `T` and `U` are for genuinely
    unbound positions.

## Attributes

16. **`#[must_use]` where ignoring the result is a bug.** Pure queries
    (`lookup`, `root_primitive`), lazy values that do nothing until read,
    and builders consumed by value all carry it. A function already
    returning `Result` does not need it; `Result` is `must_use` itself.
    `Option` is not, so a lookup returning `Option` still carries the
    attribute.

17. **`#[non_exhaustive]` only on error enums.** Each crate's error enum
    is an open set by design: callers match the variants they can act on
    and `?` the rest, so a new variant must not break them. Every other
    enum in the workspace is a closed set matched exhaustively (code.md
    rule 2), and `non_exhaustive` would silently convert that compile
    error into a fall-through.

18. **`const fn` is a promise.** Turning a `const fn` back into a plain
    function breaks every caller that used it in a constant. Mark a
    function `const` only when its body will stay const-evaluable as
    the type grows.

19. **Operators keep their mathematical meaning.** Overload `Add`,
    `Mul`, `Index`, and friends for a type where the operator means what
    it means on numbers and slices. Do not overload for brevity; a named
    method reads better than a `+` that merges two plans.
