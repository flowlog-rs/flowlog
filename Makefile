# FlowLog convenience targets.
#
# This repo is the **correctness** surface for FlowLog. Performance work
# lives in the sibling `flowlog-bench` repo (see AGENTS.md for the
# split rationale).

.PHONY: help build test oracle

help:
	@echo "FlowLog convenience targets — correctness only."
	@echo "(perf benches live in the flowlog-bench sibling repo; see AGENTS.md)"
	@echo
	@echo "  Dev workflow:"
	@echo "    make build           cargo build --release --workspace"
	@echo "    make test            cargo test  --release --workspace"
	@echo
	@echo "  Soufflé oracle:"
	@echo "    make oracle CONFIG=<file> [MODE=compiler|lib|both] [ARGS=...]"
	@echo "                         full byte-diff against pre-baked Soufflé refs"
	@echo "                         ARGS forwards to the runner; common flags:"
	@echo "                           --keep-datasets, --workers <n>,"
	@echo "                           --souffle-ref-cache <path>, --sip"
	@echo
	@echo "  See tests/README.md for the per-suite contracts."

build:
	cargo build --release --workspace

test:
	cargo test --release --workspace

# Soufflé-oracle suite on a single CONFIG file. MODE selects which
# lowering path to exercise: compiler (binary path), lib (library
# path), or both (default). ARGS is forwarded verbatim to the runner.
oracle:
	@test -n "$(CONFIG)" || { echo "usage: make oracle CONFIG=<file> [MODE=compiler|lib|both] [ARGS=\"--flag ...\"]" >&2; exit 2; }
	@case "$(or $(MODE),both)" in \
	    compiler|lib|both) ;; \
	    *) echo "MODE must be one of: compiler|lib|both" >&2; exit 2 ;; \
	 esac
	@if [ "$(or $(MODE),both)" = "compiler" ] || [ "$(or $(MODE),both)" = "both" ]; then \
	    bash tests/oracle/run_compiler.sh $(CONFIG) $(ARGS) ; \
	 fi
	@if [ "$(or $(MODE),both)" = "lib" ]      || [ "$(or $(MODE),both)" = "both" ]; then \
	    bash tests/oracle/run_lib.sh      $(CONFIG) $(ARGS) ; \
	 fi
