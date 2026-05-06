#!/usr/bin/env bash
#
# env/env.sh — one-time machine setup for FlowLog development on Linux
# (apt) / macOS (brew). Installs rustup + stable toolchain, the OS
# packages the repo needs, then runs `cargo check --workspace` as a
# smoke test.
#
# Run once on a fresh dev box.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

log()  { printf '\033[1;34m[env.sh]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[env.sh WARN]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[env.sh ERROR]\033[0m %s\n' "$*" >&2; exit 1; }

# OS packages this repo needs:
#   protobuf-compiler / protobuf : prost-build (build-time codegen)
#   bsdmainutils / util-linux    : `script -qefc` for pty wrappers
#   python3                      : Python helpers in this repo
#   wget, unzip, tar             : fetch + extract dataset / reference archives
#   build-essential / xcode CLT  : C toolchain for native crate builds
APT_PACKAGES=(build-essential pkg-config protobuf-compiler bsdmainutils python3 wget unzip tar)
BREW_PACKAGES=(protobuf util-linux python3 wget gnu-tar coreutils)

install_rust() {
    if command -v rustup >/dev/null 2>&1; then
        log "rustup already installed: $(rustup --version 2>&1 | head -1)"
    else
        log "installing rustup…"
        if command -v curl >/dev/null 2>&1; then
            curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
                | sh -s -- -y --default-toolchain stable --profile minimal
        elif command -v wget >/dev/null 2>&1; then
            wget -qO- https://sh.rustup.rs \
                | sh -s -- -y --default-toolchain stable --profile minimal
        else
            die "neither curl nor wget found; install one and re-run"
        fi
        # rustup writes to $HOME/.cargo/bin but doesn't touch this shell's PATH.
        [[ -f "$HOME/.cargo/env" ]] && source "$HOME/.cargo/env"
    fi
    rustup install stable
    rustup default stable
}

install_apt() {
    log "apt install: ${APT_PACKAGES[*]}"
    local sudo=""
    if [[ $EUID -ne 0 ]]; then
        command -v sudo >/dev/null 2>&1 || die "need root or sudo for apt-get"
        sudo="sudo"
    fi
    $sudo apt-get update -qq
    $sudo apt-get install -y -qq "${APT_PACKAGES[@]}"
}

install_brew() {
    if ! command -v brew >/dev/null 2>&1; then
        log "installing Homebrew…"
        /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
        [[ -x /opt/homebrew/bin/brew ]] && eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
    log "brew install: ${BREW_PACKAGES[*]}"
    brew update >/dev/null
    brew install "${BREW_PACKAGES[@]}"
}

install_packages() {
    case "$(uname -s)" in
        Linux)
            command -v apt-get >/dev/null 2>&1 \
                || { warn "non-apt Linux; install manually: ${APT_PACKAGES[*]}"; return; }
            install_apt
            ;;
        Darwin) install_brew ;;
        *)      warn "unsupported OS '$(uname -s)'; skipping package install" ;;
    esac
}

log "bootstrapping FlowLog dev env on $(uname -s)…"
install_rust
install_packages
log "running cargo check --workspace…"
(cd "$ROOT_DIR" && cargo check --workspace) || die "cargo check --workspace failed"
log "done."
