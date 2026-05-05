# souffle-programs/

Canonical Souffle (`.dl`) implementations of every benchmark program in
`tools/benchmark/config.txt`. Used by `compare.sh` when invoked with
`--baseline=souffle` (or `--baseline=interpreter,souffle`) — Souffle is
treated as an external Datalog oracle, both for cross-validation
(`Crosscheck_Souffle` column in the perf CSV) and as a perf baseline.

## Naming

Every file here is `<stem>.dl` where `<stem>` matches the filename in
`config.txt` (without the `<subdir>/` prefix). When `compare.sh` runs
the Souffle baseline for `program_analysis/eclipse.dl=eclipse`, it
reaches for `souffle-programs/eclipse.dl`.

## Programs without a Souffle counterpart

A handful of `config.txt` entries deliberately have no canonical `.dl`
here. They are tagged `[souffle:skip]` in `config.txt` so the Souffle
baseline cleanly records `N/A` for those rows instead of warning on
every sweep:

| Program            | Why no Souffle version                                                                                          |
|--------------------|-----------------------------------------------------------------------------------------------------------------|
| `cc` (graph)       | The reference connected-components implementation we benchmark uses a worklist pattern that doesn't translate cleanly to Souffle's stratified model. |
| `sssp` (graph)     | Same — single-source shortest path with semi-naïve evaluation in FlowLog has no idiomatic Souffle equivalent.   |

When you add a new entry to `config.txt`, either drop a matching `.dl`
here or tag the row `[souffle:skip]` to keep sweep output clean.

## Programs not yet referenced from `config.txt`

A few `.dl` files are present but unused by the current `config.txt`.
They are kept for two reasons:

1. **Future config additions** — borrow-checker, points-to, and CRDT
   stress variants we may want to fold into the regular sweep once the
   matching FlowLog programs and datasets are in place.
2. **Stress-testing the Souffle baseline harness itself** —
   `crdt_slow.dl` deliberately exercises a slower fixed-point pattern,
   which is useful when validating timing-sensitive changes to
   `run_souffle()` in `compare.sh`.

| File             | Origin / purpose                                                              |
|------------------|--------------------------------------------------------------------------------|
| `borrow.dl`      | Borrow-checker analysis (Rust-style). Pairs with future borrow benchmark.     |
| `crdt_slow.dl`   | Slow variant of `crdt.dl` for timing harness validation.                      |
| `pointsto.dl`    | Minimal points-to analysis. Educational; not yet wired to a benchmark dataset. |

If you decide one of these is dead weight, removing it is safe — it
just narrows the harness's coverage for that particular pattern.
