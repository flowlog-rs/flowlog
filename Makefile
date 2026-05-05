# FlowLog convenience targets.
#
# Thin wrappers around the test/benchmark scripts so contributors don't
# need to remember the full paths. See tests/README.md for the layered
# regression model and tools/sweep/README.md for runner details.

.PHONY: help build test smoke sweep sweep-no-perf perf clean-sweep test-safety doctor bench-one

help:
	@echo "FlowLog convenience targets"
	@echo "  make build           cargo build --release --workspace"
	@echo "  make test            cargo test  --release --workspace  (L0 only)"
	@echo "  make test-safety     run shell-level safety regression tests (<1s)"
	@echo "  make doctor          environment health check (deps, caches, FLOWLOG_*) — <1s"
	@echo "  make smoke           full regression sweep, smoke subset    (~5 min)"
	@echo "  make sweep           full regression sweep                  (hours)"
	@echo "  make sweep-no-perf   correctness layers only (skip L3)"
	@echo "  make perf            L3 perf compare in isolation"
	@echo "  make bench-one PROG=<file> DATASET=<name>"
	@echo "                       single-pair perf gate (closed-loop tooling integration)"
	@echo "  make clean-sweep     remove result/sweep/ history"
	@echo
	@echo "See tests/README.md for the four-layer model + costs."

build:
	cargo build --release --workspace

test:
	cargo test --release --workspace

test-safety:
	bash tests/safety/cleanup_dataset_test.sh

doctor:
	bash tools/env_check.sh

# Single-pair perf gate. PROG is the .dl path (relative to example/ or
# absolute); DATASET is the directory name under facts/. Used by external
# closed-loop tools (e.g. groomer's perf gate, regression CI workflows).
# See tools/benchmark/bench_one.sh header for the stable stdout contract.
bench-one:
	@test -n "$(PROG)"    || { echo "usage: make bench-one PROG=<.dl> DATASET=<name>" >&2; exit 2; }
	@test -n "$(DATASET)" || { echo "usage: make bench-one PROG=<.dl> DATASET=<name>" >&2; exit 2; }
	bash tools/benchmark/bench_one.sh $(PROG) $(DATASET)

smoke:
	bash tools/sweep/run_full_sweep.sh --smoke

sweep:
	bash tools/sweep/run_full_sweep.sh

sweep-no-perf:
	bash tools/sweep/run_full_sweep.sh --skip-l3

perf:
	bash tools/benchmark/compare.sh

clean-sweep:
	rm -rf result/sweep
