# 1) Build the release binary (still named `generator`)
cargo build --release

# 2) Variables for packaging
VERSION=v0.1.0-internal
APP_NAME=flowlog_compile
TARGET_DIR="${APP_NAME}-${VERSION}-ubuntu22.04-x86_64"

# 3) Create a clean release directory
rm -rf "$TARGET_DIR"
mkdir -p "$TARGET_DIR"

# 4) Copy and rename the binary: generator -> flowlog_compile
cp target/release/generator "$TARGET_DIR/$APP_NAME"

# Optionally include README / LICENSE
[ -f README.md ] && cp README.md "$TARGET_DIR/"
[ -f LICENSE ] && cp LICENSE "$TARGET_DIR/"

# 5) Create the tar.gz
tar -czf "${TARGET_DIR}.tar.gz" "$TARGET_DIR"
