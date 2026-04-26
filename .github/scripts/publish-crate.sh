#!/usr/bin/env bash
# Publish a workspace crate to crates.io only if the local version is newer
# than the version currently published. Skips (exit 0) when already up to date.
#
# Usage: publish-crate.sh <crate-name> <crate-path>

set -euo pipefail

CRATE_NAME="${1:?crate name required}"
CRATE_PATH="${2:?crate path required}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "::group::Resolve versions for ${CRATE_NAME}"

LOCAL_VERSION="$("${SCRIPT_DIR}/crate-version.sh" "${CRATE_NAME}")"

curl -sSf \
    -H 'User-Agent: flowlog-publish-workflow (https://github.com/flowlog-rs/flowlog)' \
    "https://crates.io/api/v1/crates/${CRATE_NAME}" \
    -o /tmp/crates-resp.json
PUBLISHED_VERSION="$(jq -r '.crate.max_stable_version // .crate.max_version // ""' /tmp/crates-resp.json)"

echo "Local version:     ${LOCAL_VERSION}"
echo "Published version: ${PUBLISHED_VERSION:-<none>}"
echo "::endgroup::"

if [[ "${LOCAL_VERSION}" == "${PUBLISHED_VERSION}" ]]; then
    echo "✅ ${CRATE_NAME} ${LOCAL_VERSION} is already on crates.io — skipping."
    exit 0
fi

echo "🚀 Publishing ${CRATE_NAME} ${LOCAL_VERSION} to crates.io"
cargo publish -p "${CRATE_NAME}" --manifest-path "${CRATE_PATH}/Cargo.toml"
