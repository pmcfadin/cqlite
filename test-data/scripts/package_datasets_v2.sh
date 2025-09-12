#!/bin/bash

set -euo pipefail

# Determine repo root and dataset dir
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATASETS_DIR="$ROOT/datasets"

# Parameters (override via flags or env):
#   --asset-name NAME   → explicit archive filename (overrides composed name)
#   --base-name NAME    → base name before labels/versions (default: cassandra5-small)
#   --label LABEL       → optional descriptive label pre-extension (e.g., with-refs)
#   --suffix SUFFIX     → optional version suffix pre-extension (e.g., v2)
#   --tag TAG           → optional GitHub release tag to suggest in next steps
# Env fallbacks: ASSET_NAME, BASE_NAME, LABEL_SUFFIX, VERSION_SUFFIX, RELEASE_TAG
ASSET_NAME=${ASSET_NAME:-}
BASE_NAME=${BASE_NAME:-cassandra5-small}
LABEL_SUFFIX=${LABEL_SUFFIX:-}
VERSION_SUFFIX=${VERSION_SUFFIX:-}
RELEASE_TAG=${RELEASE_TAG:-}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --asset-name)
      ASSET_NAME="$2"; shift 2 ;;
    --base-name)
      BASE_NAME="$2"; shift 2 ;;
    --label)
      LABEL_SUFFIX="$2"; shift 2 ;;
    --suffix)
      VERSION_SUFFIX="$2"; shift 2 ;;
    --tag)
      RELEASE_TAG="$2"; shift 2 ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--asset-name NAME] [--base-name NAME] [--label LABEL] [--suffix SUFFIX] [--tag TAG]"; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Compose asset name if not explicitly set: <BASE_NAME>[-<LABEL>][-<SUFFIX>].tar.gz
if [[ -z "$ASSET_NAME" ]]; then
  NAME="$BASE_NAME"
  if [[ -n "$LABEL_SUFFIX" ]]; then NAME+="-$LABEL_SUFFIX"; fi
  if [[ -n "$VERSION_SUFFIX" ]]; then NAME+="-$VERSION_SUFFIX"; fi
  ASSET_NAME="$NAME.tar.gz"
fi

ARCHIVE_PATH=${ARCHIVE_PATH:-"$ROOT/../$ASSET_NAME"}

if [ ! -d "$DATASETS_DIR" ]; then
  echo "❌ Datasets directory not found: $DATASETS_DIR" >&2
  echo "💡 Generate datasets first (start-clean → generate → export)" >&2
  exit 1
fi

if [ ! -f "$DATASETS_DIR/metadata.yml" ]; then
  echo "❌ metadata.yml not found in $DATASETS_DIR" >&2
  exit 1
fi

if [ ! -f "$DATASETS_DIR/references.yml" ]; then
  echo "⚠️ references.yml not found; ensure export.sh generated it before packaging" >&2
fi

echo "📦 Creating AppleDouble-safe tarball: $ARCHIVE_PATH"
export COPYFILE_DISABLE=1

# Ensure parent dir exists
mkdir -p "$(dirname "$ARCHIVE_PATH")"

# Stage into a temporary directory with desired layout: test-data/datasets
STAGING_DIR="$(mktemp -d)"
trap 'rm -rf "$STAGING_DIR"' EXIT
mkdir -p "$STAGING_DIR/test-data"
cp -R "$DATASETS_DIR" "$STAGING_DIR/test-data/datasets"

# Create tar.gz from staging root (BSD tar compatible)
tar --exclude '._*' --exclude '.DS_Store' \
  -C "$STAGING_DIR" -czf "$ARCHIVE_PATH" \
  test-data/datasets

echo "✅ Archive created: $ARCHIVE_PATH"

echo "🔐 Computing SHA256..."
if command -v shasum >/dev/null 2>&1; then
  shasum -a 256 "$ARCHIVE_PATH" | tee "${ARCHIVE_PATH}.sha256"
else
  sha256sum "$ARCHIVE_PATH" | tee "${ARCHIVE_PATH}.sha256"
fi

echo "✅ SHA256 written to ${ARCHIVE_PATH}.sha256"

cat <<EOF

Next steps:
1) Create GitHub release and upload asset:
   gh release create ${RELEASE_TAG:-<release-tag>} "$ARCHIVE_PATH" \
     --title "
${RELEASE_TAG:-<release-tag>}" \
     --notes "Canonical Cassandra 5 datasets with precomputed refs (JSONL, Statistics).\n\nSHA256:\n\n$(cat "${ARCHIVE_PATH}.sha256")"

2) Update CI env values:
   DATASET_TAG=${RELEASE_TAG:-<release-tag>}
   DATASET_ASSET=$(basename "$ARCHIVE_PATH")
   DATASET_SHA256=<paste sha256>
EOF


