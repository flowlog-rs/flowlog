# FlowLog convenience targets.
#
# This repo is the **correctness** surface for FlowLog. Performance work
# lives in the sibling `flowlog-bench` repo (see ../AGENTS.md for the
# split rationale).
#
# ----------------------------------------------------------------------
# Closed-loop integration surface (stable; consumed by external tools):
#
#     make doctor                         env / cache health probe
#     make oracle    CONFIG=... [MODE=...] full Soufflé oracle on a config file
#
# Plus two env vars that influence those targets:
#     FLOWLOG_KEEP_DATASETS               truthy → don't rm -rf datasets after each pair
#     FLOWLOG_SOUFFLE_REF_CACHE           reuse cached Soufflé ref tarballs
#
# Everything else below is dev-facing.
# ----------------------------------------------------------------------

.PHONY: help build test test-safety doctor oracle

help:
	@echo "FlowLog convenience targets — correctness only."
	@echo "(perf benches live in the flowlog-bench sibling repo; see AGENTS.md)"
	@echo
	@echo "  Dev workflow:"
	@echo "    make build           cargo build --release --workspace"
	@echo "    make test            cargo test  --release --workspace"
	@echo "    make test-safety     run shell-level safety regression test (<1s)"
	@echo "    make doctor          environment health check                  — <1s"
	@echo
	@echo "  Soufflé oracle:"
	@echo "    make oracle CONFIG=<file> [MODE=compiler|lib|both]"
	@echo "                         full byte-diff against pre-baked Soufflé refs"
	@echo "                         honours WORKERS, RAYON_NUM_THREADS,"
	@echo "                         FLOWLOG_KEEP_DATASETS, FLOWLOG_SOUFFLE_REF_CACHE"
	@echo
	@echo "  See tests/README.md for the per-suite contracts."

build:
	cargo build --release --workspace

test:
	cargo test --release --workspace

test-safety:
	bash tests/safety/cleanup_dataset_test.sh

doctor:
	bash tools/env_check.sh

# Soufflé-oracle suite on a single CONFIG file. MODE selects which
# lowering path to exercise: compiler (binary path), lib (library
# path), or both (default).
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
