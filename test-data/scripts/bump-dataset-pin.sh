#!/usr/bin/env bash
# bump-dataset-pin.sh — repoint every CI workflow at a newly-published dataset asset.
#
# Issue #1099 follow-up (Path A): after regenerating + publishing a new
# `cassandra5-small-full` tarball that INCLUDES the Epic #970 `test_comp` +
# `corruption/test_comp_corrupt` fixtures, run this to update the dataset pin
# (asset filename + SHA256, and optionally the release tag) across all
# `.github/workflows/*.yml` in one shot, then verify no stale references remain.
#
# The CI workflows reference the pin in two styles — env vars
# (DATASET_ASSET/DATASET_SHA256) and inline literals (coverage.yml, m1-ci.yml).
# Both styles embed the exact asset filename and SHA256 string, so a literal
# string replacement across the workflow dir updates every site safely.
#
# Usage:
#   test-data/scripts/bump-dataset-pin.sh --new-sha <sha256> \
#     [--new-asset cassandra5-small-full-v3.3.tar.gz] \
#     [--new-tag datasets-v3] \
#     [--old-asset cassandra5-small-full-v3.2.tar.gz] \
#     [--old-sha bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa] \
#     [--old-tag datasets-v3]
#
# Only --new-sha is required. Defaults below match the current (v3.2) pin and a
# v3.3 asset uploaded to the SAME release tag (datasets-v3 can hold multiple
# assets). If you cut a NEW release tag instead, pass --new-tag (and --old-tag
# if it differs).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WF_DIR="$REPO_ROOT/.github/workflows"

# Current (to-be-replaced) pin — keep in sync with the committed workflows.
OLD_ASSET="cassandra5-small-full-v3.2.tar.gz"
OLD_SHA="bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa"
OLD_TAG="datasets-v3"

NEW_ASSET="cassandra5-small-full-v3.3.tar.gz"
NEW_SHA=""
NEW_TAG="datasets-v3"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --new-sha)   NEW_SHA="$2"; shift 2 ;;
    --new-asset) NEW_ASSET="$2"; shift 2 ;;
    --new-tag)   NEW_TAG="$2"; shift 2 ;;
    --old-asset) OLD_ASSET="$2"; shift 2 ;;
    --old-sha)   OLD_SHA="$2"; shift 2 ;;
    --old-tag)   OLD_TAG="$2"; shift 2 ;;
    -h|--help)
      sed -n '2,33p' "$0"; exit 0 ;;
    *)
      echo "Unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$NEW_SHA" ]]; then
  echo "❌ --new-sha <sha256> is required (the SHA256 of the newly-published asset)." >&2
  echo "   It is printed by package_datasets.sh and written to <archive>.sha256." >&2
  exit 2
fi
if [[ ! "$NEW_SHA" =~ ^[0-9a-f]{64}$ ]]; then
  echo "❌ --new-sha must be a 64-char lowercase hex SHA256, got: $NEW_SHA" >&2
  exit 2
fi
if [[ ! -d "$WF_DIR" ]]; then
  echo "❌ workflow dir not found: $WF_DIR" >&2
  exit 2
fi

echo "Repointing dataset pin"
echo "  asset: $OLD_ASSET -> $NEW_ASSET"
echo "  sha:   $OLD_SHA -> $NEW_SHA"
if [[ "$OLD_TAG" != "$NEW_TAG" ]]; then
  echo "  tag:   $OLD_TAG -> $NEW_TAG"
fi

# Files that CONSUME the pin (CI workflows + the canonical fetch helper, which
# carries its own DATASET_ASSET/SHA256/TAG defaults). This is an explicit list
# rather than a repo-wide grep so we never rewrite the OLD_* constants in THIS
# script or the example pin in the #1099 runbook.
FETCH_HELPER="$REPO_ROOT/test-data/scripts/fetch-datasets.sh"
SELF_PATH="$REPO_ROOT/test-data/scripts/bump-dataset-pin.sh"

FILES=()
for f in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
  [[ -e "$f" ]] && FILES+=("$f")
done
[[ -e "$FETCH_HELPER" ]] && FILES+=("$FETCH_HELPER")

# Portable in-place sed (BSD/macOS vs GNU).
sed_inplace() {
  if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi
}

changed=0
for f in "${FILES[@]}"; do
  [[ "$f" == "$SELF_PATH" ]] && continue
  before=$(cat "$f")
  sed_inplace "s|$OLD_ASSET|$NEW_ASSET|g; s|$OLD_SHA|$NEW_SHA|g" "$f"
  if [[ "$OLD_TAG" != "$NEW_TAG" ]]; then
    # Replace the tag only in its pin-bearing contexts so unrelated mentions
    # (e.g. cache keys built from ${{ env.DATASET_TAG }}) are untouched:
    #   - env decl:                DATASET_TAG: <tag>
    #   - gh release download:     gh release download <tag>
    #   - release URL path:        releases/download/<tag>/
    #   - fetch-datasets default:  DATASET_TAG:-<tag>
    sed_inplace \
      "s|DATASET_TAG: $OLD_TAG|DATASET_TAG: $NEW_TAG|g; \
       s|release download $OLD_TAG|release download $NEW_TAG|g; \
       s|releases/download/$OLD_TAG/|releases/download/$NEW_TAG/|g; \
       s|DATASET_TAG:-$OLD_TAG|DATASET_TAG:-$NEW_TAG|g" "$f"
  fi
  if [[ "$(cat "$f")" != "$before" ]]; then
    echo "  ✓ updated ${f#"$REPO_ROOT"/}"
    changed=$((changed + 1))
  fi
done

echo "Updated $changed file(s)."

# Verify: no stale OLD asset/sha remains in any pin-consuming file.
stale=$(grep -lF -e "$OLD_ASSET" -e "$OLD_SHA" "${FILES[@]}" 2>/dev/null || true)
if [[ -n "$stale" ]]; then
  echo "❌ stale OLD pin still present in:" >&2
  echo "$stale" >&2
  exit 1
fi
# If the tag changed, no OLD tag may remain in a pin context in those files.
if [[ "$OLD_TAG" != "$NEW_TAG" ]]; then
  tag_stale=$(grep -lE "DATASET_TAG: ?-?$OLD_TAG|release download $OLD_TAG|releases/download/$OLD_TAG/" "${FILES[@]}" 2>/dev/null || true)
  if [[ -n "$tag_stale" ]]; then
    echo "❌ stale OLD tag ($OLD_TAG) still present in a pin context in:" >&2
    echo "$tag_stale" >&2
    exit 1
  fi
fi

n_asset=$(grep -lF "$NEW_ASSET" "${FILES[@]}" 2>/dev/null | wc -l | tr -d ' ')
echo "NEW asset present in $n_asset pin-consuming file(s)."

# Advisory: surface any OTHER tracked files that still NAME the old asset/sha
# (e.g. website docs). These are descriptive, not pin-consuming, so they are NOT
# auto-edited — update by hand if you want the docs to match the new pin.
others=$(cd "$REPO_ROOT" && git grep -lF -e "$OLD_ASSET" -e "$OLD_SHA" 2>/dev/null \
  | grep -vE '^(test-data/scripts/bump-dataset-pin\.sh|docs/runbooks/1099-dataset-republish\.md|\.github/workflows/|test-data/scripts/fetch-datasets\.sh)$' || true)
if [[ -n "$others" ]]; then
  echo "ℹ️  doc-only references to the old pin (not auto-edited — review manually):"
  echo "$others" | sed 's/^/     /'
fi

echo "✅ Pin bump complete. Review 'git diff', then open a PR."
