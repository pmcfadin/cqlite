#!/usr/bin/env bash
set -euo pipefail

# Build the cqlite workspace with optional feature flags
# Usage:
#   scripts/validation/build_cqlite.sh [--release] [--features <features>]

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

RELEASE_FLAG=""
FEATURES=""
PACKAGE="cqlite-cli"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --release)
      RELEASE_FLAG="--release"
      shift
      ;;
    -p|--package)
      PACKAGE="$2"
      shift 2
      ;;
    --features)
      FEATURES="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

echo "==> Building package: $PACKAGE (release=${RELEASE_FLAG:+yes}${RELEASE_FLAG:+no})"

FEATURE_ARGS=()
if [[ -n "$FEATURES" ]]; then
  FEATURE_ARGS=(--features "$FEATURES")
  echo "==> Using features: $FEATURES"
fi

RUSTFLAGS="${RUSTFLAGS:-}" \
  cargo build ${RELEASE_FLAG} -p "$PACKAGE" "${FEATURE_ARGS[@]}"

echo "==> Build complete"


