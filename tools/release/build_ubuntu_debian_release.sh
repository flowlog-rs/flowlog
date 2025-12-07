# 0) Detect OS information for packaging suffix
if [ -r /etc/os-release ]; then
	# shellcheck disable=SC1091
	. /etc/os-release
	OS_ID=${ID:-}
	OS_VERSION_ID=${VERSION_ID:-}
else
	echo "Error: Cannot read /etc/os-release; unsupported platform." >&2
	exit 1
fi

case "$OS_ID" in
	ubuntu|debian)
		;;
	*)
		echo "Error: Detected unsupported OS '$OS_ID'. This release script only targets Ubuntu/Debian." >&2
		exit 1
		;;
esac

OS_TAG=$OS_ID
if [ -n "$OS_VERSION_ID" ]; then
	OS_TAG="${OS_TAG}${OS_VERSION_ID}"
else
	OS_TAG="${OS_TAG}unknown"
fi

# Require version argument.
if [ $# -lt 1 ]; then
	echo "Usage: $0 <version>" >&2
	exit 1
fi
VERSION="$1"

# 1) Build the release binary
cargo build --release

# 2) Variables for packaging
APP_NAME=flowlog_compile
TARGET_DIR="${APP_NAME}-${VERSION}-${OS_TAG:-unknown}-x86_64"

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
