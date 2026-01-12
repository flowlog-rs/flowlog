#!/usr/bin/env bash
set -euo pipefail

# Require explicit version argument (and disallow fallback to env/default).
if [ $# -lt 1 ]; then
  echo "Usage: $0 <version>" >&2
  exit 1
fi
VERSION="$1"

# 1) Build the release binary
cargo build --release

# 2) Variables for packaging
APP_NAME="flowlog"
MACOS_VERSION=$(sw_vers -productVersion | cut -d. -f1-2)
ARCH=$(uname -m)
TARGET_DIR="${APP_NAME}-${VERSION}-macos${MACOS_VERSION}-${ARCH}"

# 3) Create a clean release directory
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

# 4) Copy the binary
cp "target/release/${APP_NAME}" "$TARGET_DIR/${APP_NAME}"

# Optionally include README / LICENSE
[ -f README.md ] && cp README.md "$TARGET_DIR/"
[ -f LICENSE ] && cp LICENSE "$TARGET_DIR/"

# 5) Create the tar.gz
ARCHIVE="${TARGET_DIR}.tar.gz"
tar -czf "$ARCHIVE" "$TARGET_DIR"

# 6) Remove the staging directory, keep only the archive
rm -rf "$TARGET_DIR"

printf 'Created %s\n' "$ARCHIVE"
