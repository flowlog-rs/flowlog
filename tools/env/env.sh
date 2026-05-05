#!/usr/bin/env bash
#
# tools/env/env.sh — one-time machine setup for the FlowLog correctness
# stack on Linux / macOS.
#
# Run this **once** on a fresh dev box / runner image. It:
#   1. Installs rustup + a stable toolchain (if missing).
#   2. Installs the OS packages tests need (apt on Linux, brew on macOS).
#   3. Runs `cargo check --workspace` as a smoke test of the install.
#
# It is intentionally not idempotent in the *minimal* sense: re-running
# is safe, but it will check/refresh package lists. It is **not** the
# right thing to call on every test run — that's `make doctor` (read-only
# health probe) and the per-suite scripts.
#
# This script targets the **flowlog** repo only (correctness). Perf-side
# deps (souffle, duckdb, GNU time) live with the sibling `flowlog-bench`
# repo and its own env.sh — see ../../AGENTS.md for the split.
#
# Exit codes:
#   0  bootstrap completed; `cargo check --workspace` succeeded
#   1  bootstrap failed (missing dependency, network failure, build break)

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
SCRIPT_NAME="$(basename "$0")"

log()  { printf '\033[1;34m[%s]\033[0m %s\n' "$SCRIPT_NAME" "$*"; }
warn() { printf '\033[1;33m[%s WARN]\033[0m %s\n' "$SCRIPT_NAME" "$*" >&2; }
die()  { printf '\033[1;31m[%s ERROR]\033[0m %s\n' "$SCRIPT_NAME" "$*" >&2; exit 1; }

OS="$(uname -s)"

# ----------------------------------------------------------------------
# 1. rustup + stable toolchain
# ----------------------------------------------------------------------
install_rust() {
    if command -v rustup >/dev/null 2>&1; then
        log "rustup already installed: $(rustup --version 2>&1 | head -1)"
    else
        log "installing rustup (stable)…"
        if command -v curl >/dev/null 2>&1; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
                | sh -s -- -y --default-toolchain stable --profile minimal
        elif command -v wget >/dev/null 2>&1; then
            wget -qO- https://sh.rustup.rs \
                | sh -s -- -y --default-toolchain stable --profile minimal
        else
            die "neither curl nor wget found; install one and re-run"
        fi
        # rustup put cargo at $HOME/.cargo/bin; source the env file for this shell.
        [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
    fi
    log "ensuring stable toolchain is installed and default…"
    rustup install stable
    rustup default stable
    command -v cargo >/dev/null 2>&1 || die "rustup install completed but cargo still not on PATH; open a new shell and re-run"
}

# ----------------------------------------------------------------------
# 2. OS packages
# ----------------------------------------------------------------------
# Required for the correctness suites:
#   - protobuf-compiler / protobuf : crates depend on prost-build
#   - bsdmainutils / util-linux    : `script -qefc` used by tests/fixtures
#   - python3                      : diff math in fixture/oracle helpers
#   - wget, unzip, tar             : oracle reference fetch + extract
#   - build-essential / xcode CLT  : C toolchain for native crate builds
APT_PACKAGES=(build-essential pkg-config protobuf-compiler bsdmainutils python3 wget unzip tar)
BREW_PACKAGES=(protobuf util-linux python3 wget gnu-tar coreutils)

install_packages_apt() {
    log "installing apt packages: ${APT_PACKAGES[*]}"
    if [[ $EUID -ne 0 ]]; then
        if ! command -v sudo >/dev/null 2>&1; then
            die "need root or sudo to install apt packages"
        fi
        sudo apt-get update -qq
        sudo apt-get install -y -qq "${APT_PACKAGES[@]}"
    else
        apt-get update -qq
        apt-get install -y -qq "${APT_PACKAGES[@]}"
    fi
}

install_packages_brew() {
    if ! command -v brew >/dev/null 2>&1; then
        log "installing Homebrew…"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        # Add Homebrew to PATH for current session (Apple Silicon path).
        [[ -x /opt/homebrew/bin/brew ]] && eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
    log "installing brew packages: ${BREW_PACKAGES[*]}"
    brew update >/dev/null
    brew install "${BREW_PACKAGES[@]}"
}

install_packages() {
    case "$OS" in
        Linux)
            if command -v apt-get >/dev/null 2>&1; then
                install_packages_apt
            else
                warn "non-apt Linux detected; install equivalents of: ${APT_PACKAGES[*]} manually"
            fi
            ;;
        Darwin)
            install_packages_brew
            ;;
        *)
            warn "unsupported OS '$OS'; skipping package install"
            ;;
    esac
}

# ----------------------------------------------------------------------
# 3. Smoke test
# ----------------------------------------------------------------------
smoke_check() {
    log "running 'cargo check --workspace' as smoke test…"
    (cd "$ROOT_DIR" && cargo check --workspace) || die "cargo check --workspace failed"
    log "cargo check --workspace OK"
}

# ----------------------------------------------------------------------
main() {
    log "bootstrapping FlowLog correctness env on $OS …"
    install_rust
    install_packages
    smoke_check
    log "done. Next:  make doctor   then   make test"
}

main "$@"
