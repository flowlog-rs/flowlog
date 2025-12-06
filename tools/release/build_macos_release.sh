#!/usr/bin/env bash
set -euo pipefail

# 1) Build the release binary
cargo build --release

# 2) Variables for packaging
VERSION=${VERSION:-v0.1.0-internal}
APP_NAME=${APP_NAME:-flowlog_compile}
MACOS_VERSION=$(sw_vers -productVersion | cut -d. -f1-2)
ARCH=$(uname -m)
TARGET_DIR="${APP_NAME}-${VERSION}-macos${MACOS_VERSION}-${ARCH}"

# 3) Create a clean release directory
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

# 4) Copy and rename the binary: compiler -> flowlog_compile
cp target/release/compiler "$TARGET_DIR/$APP_NAME"

# Optionally include README / LICENSE
[ -f README.md ] && cp README.md "$TARGET_DIR/"
[ -f LICENSE ] && cp LICENSE "$TARGET_DIR/"

# 5) Create the tar.gz
tar -czf "${TARGET_DIR}.tar.gz" "$TARGET_DIR"

printf 'Created %s\n' "${TARGET_DIR}.tar.gz"
