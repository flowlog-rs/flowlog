# flowlog-planner

Semantic analysis and query planning for [FlowLog](https://github.com/flowlog-rs/flowlog), a Datalog-to-[differential-dataflow](https://crates.io/crates/differential-dataflow) compiler. Sits between the parser and code generation (`flowlog-build`); internal dependency of the other FlowLog crates, you typically don't depend on it directly.

## Layout

- `catalog` — per-rule metadata for the planner (signatures, pushdown filters, range checks).
- `stratifier` — groups rules into dependency-ordered strata; a stratum with a cycle recurses to fixpoint.
- `planner` — lowers rules to a Differential Dataflow plan, sharing sub-plans to reuse arrangements.
- `optimizer` — cardinality-based join ordering and worst-case optimal joins (WIP).
