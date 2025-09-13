#!/bin/bash

set -euo pipefail

# Determine repo root and dataset dir
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATASETS_DIR="$ROOT/datasets"

# Parameters (override via flags or env):
#   --asset-name NAME   → explicit archive filename (overrides composed name)
#   --base-name NAME    → base name before labels/versions (default: cassandra5-small)
#   --label LABEL       → optional descriptive label pre-extension (e.g., refs, full)
#   --suffix SUFFIX     → optional version suffix pre-extension (e.g., v2)
#   --tag TAG           → optional GitHub release tag to suggest in next steps
#   --type TYPE         → full | refs (default: full). refs = refs-only (JSONL + Statistics)
#   --refs-only         → shorthand for --type refs
#   --full              → shorthand for --type full
# Env fallbacks: ASSET_NAME, BASE_NAME, LABEL_SUFFIX, VERSION_SUFFIX, RELEASE_TAG, DATASET_TYPE
ASSET_NAME=${ASSET_NAME:-}
BASE_NAME=${BASE_NAME:-cassandra5-small}
LABEL_SUFFIX=${LABEL_SUFFIX:-}
VERSION_SUFFIX=${VERSION_SUFFIX:-}
RELEASE_TAG=${RELEASE_TAG:-}
DATASET_TYPE=${DATASET_TYPE:-full}

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
    --type)
      DATASET_TYPE="$2"; shift 2 ;;
    --refs-only)
      DATASET_TYPE="refs"; shift 1 ;;
    --full)
      DATASET_TYPE="full"; shift 1 ;;
    -h|--help)
      echo "Usage: $(basename "$0") [--asset-name NAME] [--base-name NAME] [--label LABEL] [--suffix SUFFIX] [--tag TAG] [--type full|refs] [--refs-only] [--full]"; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

# Compose asset name if not explicitly set: <BASE_NAME>[-<LABEL>][-<SUFFIX>].tar.gz
if [[ -z "$ASSET_NAME" ]]; then
  NAME="$BASE_NAME"
  # Default label from type if not explicitly given
  if [[ -z "$LABEL_SUFFIX" ]]; then
    if [[ "$DATASET_TYPE" == "refs" ]]; then LABEL_SUFFIX="refs-only"; else LABEL_SUFFIX="full"; fi
  fi
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
# Copy datasets tree to staging
cp -R "$DATASETS_DIR" "$STAGING_DIR/test-data/datasets"

# Enforce content policy based on DATASET_TYPE
if [[ "$DATASET_TYPE" == "refs" ]]; then
  echo "🔧 Producing refs-only archive (JSONL + Statistics). Removing .db files..."
  find "$STAGING_DIR/test-data/datasets/sstables" -type f \
    \( -name "*-Data.db" -o -name "*-Index.db" -o -name "*-Summary.db" -o -name "*-Filter.db" -o -name "*-CompressionInfo.db" -o -name "*-TOC.txt" -o -name "*-Digest.crc32" \) \
    -print -delete || true
else
  echo "🔧 Producing full archive (includes .db files and references if present)"
fi

# Validate expected contents
echo "🔎 Validating staged contents for $DATASET_TYPE"
DB_COUNT=$(find "$STAGING_DIR/test-data/datasets" -name "*-Data.db" | wc -l | tr -d ' ')
IDX_COUNT=$(find "$STAGING_DIR/test-data/datasets" -name "*-Index.db" | wc -l | tr -d ' ')
JSONL_COUNT=$(find "$STAGING_DIR/test-data/datasets" -name "*-Data.db.jsonl" | wc -l | tr -d ' ')
STATS_COUNT=$(find "$STAGING_DIR/test-data/datasets" -name "*-Statistics.db.txt" | wc -l | tr -d ' ')
echo "   Data.db=$DB_COUNT Index.db=$IDX_COUNT JSONL=$JSONL_COUNT Statistics=$STATS_COUNT"

if [[ "$DATASET_TYPE" == "refs" ]]; then
  if [[ "$DB_COUNT" != "0" || "$IDX_COUNT" != "0" ]]; then
    echo "❌ Refs-only archive contains .db files (Data.db=$DB_COUNT, Index.db=$IDX_COUNT)" >&2
    exit 1
  fi
  if [[ "$JSONL_COUNT" == "0" || "$STATS_COUNT" == "0" ]]; then
    echo "❌ Refs-only archive missing reference files" >&2
    exit 1
  fi
else
  if [[ "$DB_COUNT" == "0" || "$IDX_COUNT" == "0" ]]; then
    echo "❌ Full archive missing .db files (Data.db=$DB_COUNT, Index.db=$IDX_COUNT)" >&2
    exit 1
  fi
fi

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
     --notes "Canonical Cassandra 5 datasets.\n\nType: $DATASET_TYPE\n\n- full: includes SSTable .db files (+ refs if present)\n- refs-only: includes JSONL + Statistics only (no .db)\n\nSHA256:\n\n$(cat "${ARCHIVE_PATH}.sha256")"

2) Update CI env values:
   DATASET_TAG=${RELEASE_TAG:-<release-tag>}
   DATASET_ASSET=$(basename "$ARCHIVE_PATH")
   DATASET_SHA256=<paste sha256>
EOF


