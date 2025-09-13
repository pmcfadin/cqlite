#!/bin/bash

set -euo pipefail

# Publish canonical datasets to a GitHub release using fixed asset names (no version in filename).
#
# Usage:
#   test-data/scripts/publish_datasets.sh --type refs --tag datasets-v2 [--base-name cassandra5-small]
#   test-data/scripts/publish_datasets.sh --type full --tag datasets-v2 [--base-name cassandra5-small]
#
# Notes:
# - Filenames are FIXED by type:
#     <base>-refs-only.tar.gz  (e.g., cassandra5-small-refs-only.tar.gz)
#     <base>-full.tar.gz       (e.g., cassandra5-small-full.tar.gz)
# - Versioning is done in the release TAG only (no version in filename) to avoid CI/test drift.
# - The script will create the release if it doesn't exist, and upload with --clobber to replace assets.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

TYPE=""
TAG=""
BASE_NAME="cassandra5-small"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --type)
      TYPE="$2"; shift 2 ;;
    --tag)
      TAG="$2"; shift 2 ;;
    --base-name)
      BASE_NAME="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $(basename "$0") --type <refs|full> --tag <release-tag> [--base-name <name>]"; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$TYPE" || -z "$TAG" ]]; then
  echo "❌ Missing required arguments: --type and --tag are required" >&2
  exit 1
fi

case "$TYPE" in
  refs)
    ASSET_NAME="${BASE_NAME}-refs-only.tar.gz" ;;
  full)
    ASSET_NAME="${BASE_NAME}-full.tar.gz" ;;
  *)
    echo "❌ Invalid --type: $TYPE (expected 'refs' or 'full')" >&2
    exit 1 ;;
esac

echo "📦 Building dataset archive ($TYPE) with fixed name: $ASSET_NAME"

# Build the archive using the unified packager with fixed filename and explicit type
ASSET_PATH="${REPO_ROOT}/../${ASSET_NAME}"
DATASET_TYPE="$TYPE" \
  ASSET_NAME="$ASSET_NAME" \
  "$REPO_ROOT/test-data/scripts/package_datasets.sh" --type "$TYPE" --asset-name "$ASSET_NAME" --tag "$TAG"

if [[ ! -f "$ASSET_PATH" ]]; then
  echo "❌ Expected asset not found: $ASSET_PATH" >&2
  exit 1
fi

SHA256_PATH="${ASSET_PATH}.sha256"
if [[ ! -f "$SHA256_PATH" ]]; then
  echo "❌ Expected sha256 not found: $SHA256_PATH" >&2
  exit 1
fi

echo "🚀 Publishing to GitHub release: $TAG"
if ! gh release view "$TAG" >/dev/null 2>&1; then
  echo "ℹ️ Release $TAG does not exist; creating"
  gh release create "$TAG" --title "$TAG" --notes "Canonical Cassandra 5 datasets ($TYPE)."
fi

# Upload or replace assets using --clobber
gh release upload "$TAG" "$ASSET_PATH" "$SHA256_PATH" --clobber

echo "✅ Published $ASSET_NAME to release $TAG"

cat <<EOF

CI configuration hints:
- DATASET_TAG=$TAG
- DATASET_ASSET=$ASSET_NAME
- DATASET_SHA256=(value from ${SHA256_PATH})

Download example in CI:
  gh release download "$DATASET_TAG" --pattern "$DATASET_ASSET" --dir /tmp
  echo "\${DATASET_SHA256}  /tmp/\${DATASET_ASSET}" | sha256sum -c -
  tar -xzf /tmp/"\${DATASET_ASSET}" -C .

EOF


