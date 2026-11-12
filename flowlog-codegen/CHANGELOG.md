# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0](https://github.com/flowlog-rs/flowlog/compare/flowlog-codegen-v0.1.0...flowlog-codegen-v0.2.0) - 2026-11-12

### Added

- *(planner)* carry physical field orders in relation bindings ([#524](https://github.com/flowlog-rs/flowlog/pull/524))
- *(profiler)* report heap bytes per timely operator ([#544](https://github.com/flowlog-rs/flowlog/pull/544))

### Fixed

- *(planner)* plan a body without rows against a one-row unit ([#539](https://github.com/flowlog-rs/flowlog/pull/539))
- *(codegen)* spell out a feedback variable's row type ([#506](https://github.com/flowlog-rs/flowlog/pull/506))
- *(codegen)* cast a compound substr index in one pair of parentheses ([#500](https://github.com/flowlog-rs/flowlog/pull/500))
- merge `min<string>` and `max<string>` columns by their text ([#485](https://github.com/flowlog-rs/flowlog/pull/485))
- build a program with no input, or with every output pruned ([#484](https://github.com/flowlog-rs/flowlog/pull/484))
- define integer arithmetic in the runtime, allow type-limit comparisons ([#483](https://github.com/flowlog-rs/flowlog/pull/483))
- *(codegen)* test an atom's constants before the rule's comparisons ([#481](https://github.com/flowlog-rs/flowlog/pull/481))
- give a literal in `as()` its target's width ([#487](https://github.com/flowlog-rs/flowlog/pull/487))

### Other

- *(runtime)* fold mutable integer aggregates in and out of one weight per group ([#596](https://github.com/flowlog-rs/flowlog/pull/596))
- *(runtime)* cancel a growing antijoin by its arms' counts ([#599](https://github.com/flowlog-rs/flowlog/pull/599))
- *(runtime)* answer append aggregates from one weight per group ([#578](https://github.com/flowlog-rs/flowlog/pull/578))
- *(runtime)* fold presence aggregates over the arranged matches ([#577](https://github.com/flowlog-rs/flowlog/pull/577))
- *(codegen)* count .printsize batches in place ([#565](https://github.com/flowlog-rs/flowlog/pull/565))
- *(runtime)* [**breaking**] order the weights and read presence as a count of one ([#575](https://github.com/flowlog-rs/flowlog/pull/575))
- *(profiler)* record metrics in flowlog-runtime, not generated code ([#564](https://github.com/flowlog-rs/flowlog/pull/564))
- *(codegen)* count .printsize rows on each worker ([#563](https://github.com/flowlog-rs/flowlog/pull/563))
- *(codegen)* name an arrangement in one place ([#561](https://github.com/flowlog-rs/flowlog/pull/561))
- *(runtime)* use timely's tuple timestamp for LexLoop ([#552](https://github.com/flowlog-rs/flowlog/pull/552))
- *(runtime)* replace lasso with an in-crate string table ([#501](https://github.com/flowlog-rs/flowlog/pull/501))
- *(planner)* generate logical dedup/aggregation on head in planning phase ([#502](https://github.com/flowlog-rs/flowlog/pull/502))

## [0.1.0](https://github.com/flowlog-rs/flowlog/releases/tag/flowlog-codegen-v0.1.0) - 2026-10-19

### Added

- [**breaking**] aggregate expressions and lattice relations ([#431](https://github.com/flowlog-rs/flowlog/pull/431))
- aggregate per rule, so a relation unions its rules' answers ([#427](https://github.com/flowlog-rs/flowlog/pull/427))
- append relations accept inserts across epochs and never delete ([#407](https://github.com/flowlog-rs/flowlog/pull/407))
- *(runtime)* [**breaking**] a mutable input is a set, not a bag ([#405](https://github.com/flowlog-rs/flowlog/pull/405))

### Fixed

- *(runtime)* [**breaking**] report a no-change epoch's count as zero at its own epoch ([#429](https://github.com/flowlog-rs/flowlog/pull/429))
- *(planner)* keep the copying step of a rule that reads one relation as it is ([#428](https://github.com/flowlog-rs/flowlog/pull/428))

### Other

- *(codegen)* merge a lattice relation's union without a dedup ([#433](https://github.com/flowlog-rs/flowlog/pull/433))
- *(codegen)* reuse presence membership for printsize ([#419](https://github.com/flowlog-rs/flowlog/pull/419))
- generate lint-clean code and allow only user-caused lints ([#402](https://github.com/flowlog-rs/flowlog/pull/402))
- *(codegen)* one fragment struct per side, and strata that compose their own head steps ([#401](https://github.com/flowlog-rs/flowlog/pull/401))
- split codegen into its own flowlog-codegen crate ([#398](https://github.com/flowlog-rs/flowlog/pull/398))
