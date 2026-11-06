# Layout DP quicktest

This prototype turns a Datalog program and a fact directory into an executable
that uses the cutset-DP choice of physical field orders and binary-join input
direction. It keeps the plan's existing join tree and key/value field split.
Its objective is the phase-prioritized Boolean-record inversion score, not
measured execution time.

Run the complete pipeline from the repository root:

```bash
tools/layout-quicktest/run.sh \
  tools/layout-quicktest/examples/path/path.dl \
  tools/layout-quicktest/examples/path/facts \
  /tmp/flowlog-layout-quicktest 4
```

The arguments are `QUERY.dl FACT_DIR OUTPUT_DIR [WORKERS]`. The fact directory
must contain the files named by the program's `.input` declarations. The
default worker count is four. Rust dependencies must be available in Cargo's
cache; the script builds offline by default. Set `CARGO_NET_OFFLINE=false` if
the dependencies need to be downloaded. Set
`FLOWLOG_LAYOUT_QUICKTEST_TARGET_DIR` to reuse a different Cargo target
directory across queries.

| Step | Input | Output |
| --- | --- | --- |
| Build tools | This branch's compiler and codegen source | `flowlog-compiler` and `export_plan` in the Cargo target directory |
| Export plan | `QUERY.dl` | `OUTPUT_DIR/plans/QUERY.json` |
| Optimize | Plan JSON | `OUTPUT_DIR/selected/QUERY.json`, including `selected`, phase scores, and search details |
| Extract selection | Result's `selected` field | `OUTPUT_DIR/layout.json` |
| Generate and build | Same `QUERY.dl`, `layout.json`, this branch's runtime | `OUTPUT_DIR/generated/` and `OUTPUT_DIR/optimized` |
| Execute | `optimized`, fact directory, worker count | Console output and `OUTPUT_DIR/results/` |

The compiler reads `layout.json` through `FLOWLOG_EXPERIMENT_LAYOUT_PLAN`.
The plan exporter and compiler must come from the same source revision so
transformation fingerprints in the selection match the compiled program.
Rerun the script after changing the query or the optimizer; an existing
executable cannot read a new selection at runtime.

For the bundled example, `Path` contains six tuples. The selected physical
row order is `(y, x)` and the recursive join reads `Arc` on the left. The
logical relation and the expected output tuples remain unchanged. Run
`tools/layout-quicktest/smoke.sh` to check the layout, join direction, and
all six output tuples end to end.

The optimizer's legal choices are field permutations within the plan's
existing relation/index layouts and swapping operands of a positive binary
join. It does not change which fields belong to an arrangement key, choose a
different multiway join tree, or remove operators. Explicit aggregate plans
are currently rejected by the optimizer. This is an experiment entry point,
not part of the normal FlowLog compiler invocation.

The Python solver and its model dependencies are stored in this directory.
`layout-dp-benchmark-20261021/optimizer.py` is the optimization entry point;
`layout-dp-feasibility-20261021/` provides factor analysis and cutset search;
`optimizer-source/` provides the plan model and inversion cost. The compiler
integration is in `flowlog-codegen/src/experiment_layout.rs` and
`flowlog-planner/src/planner/experiment_layout.rs`.
