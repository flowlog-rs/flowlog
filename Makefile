# FlowLog convenience targets.
#
# Thin wrappers around the test/benchmark scripts so contributors don't
# need to remember the full paths. See tests/README.md for the layered
# regression model and tools/sweep/README.md for runner details.
#
# ----------------------------------------------------------------------
# Closed-loop integration surface (stable; consumed by external tools
# like the groomer agent):
#
#     make doctor                         env / cache health probe
#     make bench-one PROG=... DATASET=... single-pair perf, stable stdout
#     make oracle    CONFIG=... [MODE=...] full L2 oracle on a config file
#     make perf-compare BASE=... HEAD=... LIST=...   two-sha drift gate
#
# Plus two env vars: FLOWLOG_KEEP_DATASETS, FLOWLOG_SOUFFLE_REF_CACHE.
# Everything else below is dev-facing.
# ----------------------------------------------------------------------

.PHONY: help build test smoke sweep sweep-no-perf perf clean-sweep \
        test-safety doctor bench-one oracle perf-compare

help:
	@echo "FlowLog convenience targets"
	@echo
	@echo "  Dev workflow:"
	@echo "    make build           cargo build --release --workspace"
	@echo "    make test            cargo test  --release --workspace  (L0 only)"
	@echo "    make test-safety     run shell-level safety regression tests (<1s)"
	@echo "    make smoke           full regression sweep, smoke subset    (~5 min)"
	@echo "    make sweep           full regression sweep                  (hours)"
	@echo "    make sweep-no-perf   correctness layers only (skip L3)"
	@echo "    make perf            L3 perf compare in isolation"
	@echo "    make clean-sweep     remove result/sweep/ history"
	@echo
	@echo "  Closed-loop integration (stable; for external tooling):"
	@echo "    make doctor          environment health check                  — <1s"
	@echo "    make bench-one PROG=<.dl> DATASET=<name>"
	@echo "                         single-pair perf gate; honours WORKERS, NUM_RUNS"
	@echo "    make oracle CONFIG=<file> [MODE=compiler|lib|both]"
	@echo "                         L2 Souffle oracle on a config; default MODE=both"
	@echo "                         honours WORKERS, RAYON_NUM_THREADS, FLOWLOG_KEEP_DATASETS"
	@echo "    make perf-compare BASE=<sha> HEAD=<sha> LIST=<file>"
	@echo "                         two-sha wall+RSS drift; honours PERF_COMPARE_*"
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

# ----------------------------------------------------------------------
# Closed-loop integration targets — kept thin and stable.
# ----------------------------------------------------------------------

# Single-pair perf gate. PROG is the .dl path (relative to example/ or
# absolute); DATASET is the directory name under facts/. Used by external
# closed-loop tools (e.g. groomer's perf gate, regression CI workflows).
# See tools/benchmark/bench_one.sh header for the stable stdout contract.
bench-one:
	@test -n "$(PROG)"    || { echo "usage: make bench-one PROG=<.dl> DATASET=<name>" >&2; exit 2; }
	@test -n "$(DATASET)" || { echo "usage: make bench-one PROG=<.dl> DATASET=<name>" >&2; exit 2; }
	bash tools/benchmark/bench_one.sh $(PROG) $(DATASET)

# L2 Souffle-oracle suite on a single CONFIG file. MODE selects which
# lowering path(s) to exercise: compiler (binary path), lib (library
# path), or both (default — matches `make sweep` semantics).
#
# Both runners honour the same env vars they would when called directly
# (WORKERS, RAYON_NUM_THREADS, FLOWLOG_KEEP_DATASETS, FLOWLOG_SOUFFLE_REF_CACHE).
oracle:
	@test -n "$(CONFIG)" || { echo "usage: make oracle CONFIG=<file> [MODE=compiler|lib|both]" >&2; exit 2; }
	@case "$(or $(MODE),both)" in \
	    compiler|lib|both) ;; \
	    *) echo "MODE must be one of: compiler|lib|both" >&2; exit 2 ;; \
	 esac
	@if [ "$(or $(MODE),both)" = "compiler" ] || [ "$(or $(MODE),both)" = "both" ]; then \
	    bash tests/oracle/run_compiler.sh $(CONFIG) ; \
	 fi
	@if [ "$(or $(MODE),both)" = "lib" ]      || [ "$(or $(MODE),both)" = "both" ]; then \
	    bash tests/oracle/run_lib.sh      $(CONFIG) ; \
	 fi

# Two-commit perf+RSS drift gate. Times each (program, dataset) pair in
# LIST at BASE and HEAD, fails if any pair regresses past
# PERF_COMPARE_TIME_PCT (default 10) or PERF_COMPARE_RSS_PCT (default 20).
# See tools/perf_compare.sh for the full env contract.
perf-compare:
	@test -n "$(BASE)" -a -n "$(HEAD)" -a -n "$(LIST)" || \
	    { echo "usage: make perf-compare BASE=<sha> HEAD=<sha> LIST=<file>" >&2; exit 2; }
	bash tools/perf_compare.sh $(BASE) $(HEAD) $(LIST)

# ----------------------------------------------------------------------
# Dev-facing sweep/perf entry points.
# ----------------------------------------------------------------------

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
