# flowlog-codegen

Code generation for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler. Takes a typechecked program and its stratified plan and emits the dataflow that evaluates it: an input collection per input relation, one operator chain (map, filter, join, antijoin, aggregation) per rule body, a loop to fixpoint per recursive stratum, and an emitter per output relation; each collection gets its `(Data, Diff, Time)` types from the relation's declaration and mutability. The result is a `Skeleton` of fragments that a frontend splices into its own engine. Internal dependency of the other FlowLog crates; you typically don't depend on it directly.

## Layout

- `skeleton` — `Skeleton` and the order its fragments are filled in.
- `io` — `relation` (the `Relation` impls and `Inputs`), `input` (input collections and handles), `output` (emitters, inspectors, flush).
- `stratum` — strata in order; `non_recursive` and `recursive` for the two kinds.
- `rule` — `head` (binding a rule's result, outside or inside a loop) and `body` (the operator chain).
- `expr` — closure pieces: `param`, `projection`, `compare`, `constraint`, `aggregation`, and the `term`s they are built from.
- `ty` — `data`, `diff`, and `time` for the triple above.
- `ident`, `features`, `profile`, `error` — binding names, the features a program needs, the profiler's side of the engine, and the internal error type.
