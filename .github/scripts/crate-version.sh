#!/usr/bin/env bash
# Print the version of a workspace crate as declared in Cargo.toml.
#
# Usage: crate-version.sh <crate-name>

set -euo pipefail

CRATE_NAME="${1:?crate name required}"

VERSION="$(cargo metadata --no-deps --format-version 1 \
    | jq -r --arg name "${CRATE_NAME}" '.packages[] | select(.name == $name) | .version')"

if [[ -z "${VERSION}" || "${VERSION}" == "null" ]]; then
    echo "Could not resolve version for ${CRATE_NAME}" >&2
    exit 1
fi

printf '%s\n' "${VERSION}"
