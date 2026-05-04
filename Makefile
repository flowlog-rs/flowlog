# FlowLog convenience targets.
#
# Thin wrappers around the test/benchmark scripts so contributors don't
# need to remember the full paths. See tests/README.md for the layered
# regression model and tools/sweep/README.md for runner details.

.PHONY: help build test smoke sweep sweep-no-perf perf clean-sweep

help:
	@echo "FlowLog convenience targets"
	@echo "  make build           cargo build --release --workspace"
	@echo "  make test            cargo test  --release --workspace  (L0 only)"
	@echo "  make smoke           full regression sweep, smoke subset    (~5 min)"
	@echo "  make sweep           full regression sweep                  (hours)"
	@echo "  make sweep-no-perf   correctness layers only (skip L3)"
	@echo "  make perf            L3 perf compare in isolation"
	@echo "  make clean-sweep     remove result/sweep/ history"
	@echo
	@echo "See tests/README.md for the four-layer model + costs."

build:
	cargo build --release --workspace

test:
	cargo test --release --workspace

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
