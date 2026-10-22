# Types

Status: implemented. The vocabulary lives in `flowlog-parser/src/types/`,
the checking in `flowlog-parser/src/pipeline/typecheck/`.

FlowLog is statically typed: every relation declares its column types,
every rule is checked against them before any code is generated, and the
generated Rust works in concrete machine types only. This note records the
type vocabulary, where types come from, how a rule is typed, and what the
stages after type checking may assume.

## The vocabulary

A column type is one of:

| Type | Spelling | Values |
|---|---|---|
| Signed integer | `int8`, `int16`, `int32` (alias `number`), `int64` | two's complement of that width |
| Unsigned integer | `uint8`, `uint16`, `uint32` (alias `unsigned`), `uint64` | |
| Float | `float` (alias `f32`), `f64` | IEEE 754 |
| String | `string` (alias `symbol`) | UTF-8 |
| Boolean | `bool` | `True`, `False` |
| Tuple | `(a: T0, b: T1, ...)`, or a `.type` name for one | fixed-width, fields may nest |

Every scalar is its own type: `int8` and `int16` do not unify, convert, or
promote. The width a column declares is the width its values have in
memory and in arithmetic. There is no `any`, no union, and no implicit
conversion; a conversion is a call (`to_string`, `to_number`).

A `.type` declaration names a type:

- `.type X = Y` makes `X` another spelling of `Y`. It adds no identity:
  a column of `X` and a column of `Y` are the same type everywhere.
- `.type X <: Y` makes `X` a **subtype** of `Y`, a scalar or another
  subtype. `X` is a distinct identity for the type checker and the same
  machine type as `Y`'s root for everything else: a value of `X` may flow
  into a `Y` column, a `Y` value needs an explicit `as(e, X)` to flow into
  an `X` column, and two subtypes of one root that are not in a chain
  (`UserId` and `ProductId`, both `<: number`) are siblings that may not
  meet. Subtypes are nominal, zero-cost, and erased after type checking.
  A tuple cannot be subtyped.
- `.type T = (a: T0, ...)` names a tuple type. Fields may be tuples, but
  no tuple may contain itself, directly or through another: recursive
  types are not supported.

A **lattice column**, `d: min<int32>`, is not a type but a property of the
column: its type is `int32`, and the relation keeps the least (or
greatest) value per key. The wrapped type must be a number or a string
(`docs/design/aggregation.md`).

A **literal** has no width of its own. `5` is an integer literal that
becomes whatever integer type its context asks for, `1.5` a float literal
likewise; `"x"`, `True`, and `False` are a `string` and `bool`. In the
parser, an unpinned literal has the type `IntLit` or `FloatLit`, a
*literal family*; type checking replaces every one with a concrete width,
so no literal family survives into the planner or the generated code.

## Where types come from

- **Declarations.** `.decl R(x: T, ...)` fixes every column; `.extern fn
  f(a: T, ...) -> R` fixes a UDF's parameters and result. Built-ins have
  fixed signatures (`strlen: string -> int32`, `cat: string, string ->
  string`, `substr: string, int32, int32 -> string`, `ord: string ->
  int32`, `to_number: string -> int32`); `to_string` takes any scalar but
  a string.
- **The registry.** `.type` declarations are interned, in order, into a
  per-program `TypeRegistry` that maps each name to a `TypeId` and each
  `TypeId` to its root scalar, its parent subtype, or its tuple fields.
  Aliases map to their target's `TypeId`; subtypes and tuples get their
  own. The registry is the only place subtype identity exists.
- **Inference.** A rule's variables and literals have no declared type;
  inference works them out from the columns, calls, and operators they
  meet, below.

## The pipeline

```
assemble  ->  typecheck  ->  fold  ->  prune  ->  validate  ->  planner  ->  codegen
              |
              infer  ->  substitute  ->  pin  ->  subtype
```

`typecheck::check_program` runs four steps, each valid only after the one
before:

- **infer** (`infer.rs`, on `solver.rs`) types each rule's variables as the
  rule is written, by unification, and reports every type error.
- **substitute** (`substitute.rs`) replaces each variable an equality
  defines by its value: an assignment's variable by its expression, a
  destructure's components by projections of its tuple. After it, later
  stages read a variable's value only from body atoms and aggregates.
- **pin** (`pin.rs`) writes the inference back: every literal is pinned to
  the width its context asks, and every aggregate's variable is given its
  type. It runs after substitution, so a literal that a definition carried
  into several uses is pinned once per use.
- **subtype** (`subtype.rs`) enforces the nominal rules the erased scalars
  cannot express, over the substituted program, then lowers every `as()`
  cast.

What the stages after it may assume: every `Constant` has a concrete
`DataType`; every aggregate carries its answer's type; no `=` defines a
variable; no `as()` remains; a tuple is a `DataType::FixedTuple` of
concrete fields; and subtypes have been checked and erased, so a column of
`UserId` is a column of `int32` to the planner and to codegen.

## Inference

Every construct states one kind of fact, that two types are equal, and a
solver enforces all of a rule's facts together. A *type term* is a
concrete scalar, a tuple of terms, or a type variable: a type not known
yet. A type variable has a *kind*, what its type must turn out to be:
anything, an ordered type (a number or a string), a number, an integer, or
a float. The kinds form a lattice; unifying two variables meets their
kinds, and integer and float do not meet.

| Construct | Constraint |
|---|---|
| `A(x, y)`, positive or negated | `x ~ col0`, `y ~ col1` |
| `lhs = rhs`, `lhs != rhs` | `lhs ~ rhs`; a tuple literal field by field, its `_` a field of any type |
| `lhs < rhs` and the other orderings | `lhs ~ rhs`, of kind ordered |
| `match(a, b)`, `contains(a, b)` | `a ~ string`, `b ~ string` |
| `a + b` and the other numeric operators | `a ~ b`, of kind number; the expression is that type |
| `a band b` and the other bitwise operators | `a ~ b`, of kind integer |
| `f(a, b)`, a UDF | `a ~ param0`, `b ~ param1`; the call is the return type |
| a built-in call | each argument its parameter's type, or one of them for `to_string` |
| `as(e, T)` | the term of `e`; the subtype step checks the cast |
| `5`, `1.5` | a fresh variable of kind integer or float |
| `count : { ... }` | a fresh integer variable, default `int32` |
| `mean e : { ... }` | `e` of kind number; a fresh float variable, default `float64` |
| `sum e`, `min e`, `max e` | `e` of kind number; the answer is `e`'s type |
| `Out(a, b) :- ...` | `a ~ col0`, `b ~ col1` |
| `P(1, "x").` | each column's family fits its `.decl` |

A rule variable is a fresh variable of kind anything on first mention. One
that nothing constrains has no type at the end; the range-restriction pass
reports it, since such a variable is unsafe whatever its type.

Variables of one rule are solved together, so a width reaches a variable
through any chain of constraints. `s = sum t : { ..., c = count : ...,
t = c + 1 }` with `s` in an `int64` column makes `c` an `int64`: the head
fixes `s`, `s` is `t`, `t` is `c + 1`, and `c + 1` is `c`'s type.

### Defaults

A variable of kind integer or float that nothing fixes takes a default:
`int32` or `float32` for a literal, `int32` for a `count`, `float64` for a
`mean`. When two unfixed variables merge, the wider default wins, so
`mean y : ... > 1.5` answers a `float64`. A variable of another kind that
nothing fixes has no type.

### Definitions with no variables

An equality `t = e` defines `t` when nothing else binds `t` and every
variable of `e` is bound. When `e` has no variables at all, or only
variables defined this way (`t = 1 + 2`, then `u = t * 2`), `t` names a
value rather than a typed cell. Its term is kept, and each use of `t`
instantiates it with fresh literal variables, so `Out(t, t) :- ..., t = 1.`
fills an `int8` column and an `int64` one in the same rule. Substitution
then inlines the value into each use, and pinning gives each copy its own
width. This is let-polymorphism with the usual value restriction: the
value must be closed.

A definition over a typed variable, `t = x + 1`, has one type; its uses
must agree.

### Order and diagnostics

Constraints are generated in reading order: the body's predicates left to
right, an aggregate's body before its target, and the head last. The first
constraint that disagrees with those before it is the one reported, with
the site where the disagreeing variable's type was first fixed when the
error carries one. The set of accepted programs does not depend on the
order; only the choice of which site a rejection names does.

Every error names the construct the user wrote. A destructure of the wrong
width names the destructure; a head argument of the wrong type names the
argument; a literal that does not fit the width its context pins it to
names the literal (the one error pinning can raise).

## Tuples

A tuple literal `(e0, e1, ...)` beside `=` is a **destructure** when the
other side is a tuple: `(a, _) = p` binds `a` to `p`'s first field and
ignores the second. The pattern must have exactly the tuple's width, and
`_` is allowed only there. Anywhere else a tuple literal **constructs** a
value: `p = (x, y)` defines `p`, `Out((x, y))` fills a tuple column, and a
`_` in it is an error. A one-field tuple needs its trailing comma,
`(x,)`, to be a tuple rather than a parenthesized `x`.

Inference types a destructure by unifying the pattern with the tuple field
by field, so an out-of-order chain (`(a, b) = inner, (x, inner) = o`)
needs no special order. Substitution then expands it: a fresh component
becomes a projection `(p).0`, a bound component or an expression becomes a
filter `(p).i = component`, and `_` leaves nothing behind. Projections
exist only from then on.

Tuples are compared and joined by value: `p = q` on two tuple variables
compares all fields, and a tuple column is a join key like any other.
Arithmetic on a tuple is an error.

## Subtypes and casts

The subtype step runs after substitution and works in `TypeId`s. Each
variable's identity is the meet of the identities of the columns that bind
it: two columns of one chain meet at the more specific one, two siblings
do not meet and are an error. A head argument must widen into its column:
a subtype into its parent needs nothing, a parent into a subtype needs
`as(e, Sub)`. A cast's target must share the operand's root; `as(x,
Symbol)` on a number is an error. A tuple's fields are checked the same
way. After the checks, every `as()` is removed, and downstream sees roots
only.

## Strings

Strings are values like any other to the type checker. With `--str-intern`
the loader interns them to integer keys, and `ord(s)` exposes a string's
key as an `int32`; without the flag `ord` is an error, since there is no
key to expose. `ord` is the only construct whose typing depends on
configuration, and it is checked by inference.

## Differences from Soufflé

- **Scalars.** Soufflé has `number`, `unsigned`, `float`, and `symbol`,
  each one machine width; FlowLog has every width, and the spelling
  `number` is `int32`, `symbol` is `string`.
- **Literals.** In Soufflé a numeric literal is a `number`; in FlowLog it
  takes its context's width, and a value defined from literals alone may
  take several.
- **Inference.** Soufflé types by a lattice of type sets; FlowLog by
  unification over concrete scalars, with subtypes checked by a separate
  nominal step. The programs both accept agree on everything but union
  types, which FlowLog does not have.
- **Records.** Soufflé's `[a, b]` is nullable and heap-allocated, and may
  be recursive; FlowLog's `(a, b)` is an inline fixed tuple, never null,
  never recursive. Soufflé also allows a record pattern as an atom
  argument, `L([_, x])`; FlowLog has only the equality form.
- **`.type`.** Soufflé's `.type X = Y` is a type *equivalence* too, and
  `.type X <: Y` a subtype as here; Soufflé's union types `.type U = A | B`
  have no counterpart.

## Open questions

- **Patterns in atom arguments.** `L((_, x))` as Soufflé allows would make
  a destructure a binding site by construction. It is a grammar change,
  and would type as an atom argument does.
- **Lattice-valued inference.** The subtype step could be folded into the
  solver by unifying over a lattice of `TypeId`s rather than equality on
  scalars. The two stay separate until a need arises.
- **User-defined lattice types** for lattice columns, and **arrays**
  (`[...]` is reserved in the grammar), are not designed.
