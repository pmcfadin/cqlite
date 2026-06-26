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
#     [--new-asset cassandra5-small-full-v3.2.tar.gz] \
#     [--new-tag datasets-v3] \
#     [--old-asset cassandra5-small-full-v3.1.tar.gz] \
#     [--old-sha f5fa0b6599a27c1c493d7c6c063194d55d031cab417396947313e7245afc5ceb] \
#     [--old-tag datasets-v3]
#
# Only --new-sha is required. Defaults below match the current (v3.1) pin and a
# v3.2 asset uploaded to the SAME release tag (datasets-v3 can hold multiple
# assets). If you cut a NEW release tag instead, pass --new-tag (and --old-tag
# if it differs).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WF_DIR="$REPO_ROOT/.github/workflows"

# Current (to-be-replaced) pin — keep in sync with the committed workflows.
OLD_ASSET="cassandra5-small-full-v3.1.tar.gz"
OLD_SHA="f5fa0b6599a27c1c493d7c6c063194d55d031cab417396947313e7245afc5ceb"
OLD_TAG="datasets-v3"

NEW_ASSET="cassandra5-small-full-v3.2.tar.gz"
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

echo "Repointing dataset pin in $WF_DIR"
echo "  asset: $OLD_ASSET -> $NEW_ASSET"
echo "  sha:   $OLD_SHA -> $NEW_SHA"
if [[ "$OLD_TAG" != "$NEW_TAG" ]]; then
  echo "  tag:   $OLD_TAG -> $NEW_TAG"
fi

# Portable in-place sed (BSD/macOS vs GNU).
sed_inplace() {
  if sed --version >/dev/null 2>&1; then sed -i "$@"; else sed -i '' "$@"; fi
}

changed=0
for f in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
  [[ -e "$f" ]] || continue
  if grep -qF -e "$OLD_ASSET" -e "$OLD_SHA" "$f" \
     || { [[ "$OLD_TAG" != "$NEW_TAG" ]] && grep -qF "DATASET_TAG: $OLD_TAG" "$f"; }; then
    sed_inplace "s|$OLD_ASSET|$NEW_ASSET|g; s|$OLD_SHA|$NEW_SHA|g" "$f"
    if [[ "$OLD_TAG" != "$NEW_TAG" ]]; then
      sed_inplace "s|DATASET_TAG: $OLD_TAG|DATASET_TAG: $NEW_TAG|g" "$f"
    fi
    echo "  ✓ updated $(basename "$f")"
    changed=$((changed + 1))
  fi
done

echo "Updated $changed workflow file(s)."

# Verify: no stale references to the OLD asset/sha remain anywhere.
stale=$(grep -rIlF -e "$OLD_ASSET" -e "$OLD_SHA" "$WF_DIR" 2>/dev/null || true)
if [[ -n "$stale" ]]; then
  echo "❌ stale OLD pin still present in:" >&2
  echo "$stale" >&2
  exit 1
fi

# Sanity: the NEW asset+sha should now appear in the expected number of files.
n_asset=$(grep -rIlF "$NEW_ASSET" "$WF_DIR" 2>/dev/null | wc -l | tr -d ' ')
n_sha=$(grep -rIlF "$NEW_SHA" "$WF_DIR" 2>/dev/null | wc -l | tr -d ' ')
echo "NEW asset present in $n_asset file(s); NEW sha present in $n_sha file(s)."
echo "✅ Pin bump complete. Review 'git diff .github/workflows', then open a PR."
