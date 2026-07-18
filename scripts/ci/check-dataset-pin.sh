#!/usr/bin/env bash
# check-dataset-pin.sh — assert every workflow (and pin-consuming helper) agrees
# with the tracked dataset pin (issue #2646, epic #2636).
#
# The canonical dataset SHA256/asset/tag lives in ONE tracked file,
# test-data/dataset-pin.env. It is embedded across ~11 workflows plus several
# helper scripts, a silent-drift surface. This check REDS if:
#   - the tracked pin file is missing or malformed;
#   - any `.github/workflows/*.yml` declares a `DATASET_SHA256:` env that does
#     not equal the tracked sha;
#   - the fetch helper / bump script / restore action / pre-merge / provenance
#     guard defaults disagree with the tracked sha, asset, or tag;
#   - ANY 64-char hex token in a pin-consuming file differs from the tracked
#     sha (catches a stray literal that skipped the env decl);
#   - the GENERATED test-data/datasets/.dataset-pin has become git-tracked.
#
# Exit 0 = all agree; non-zero = drift (prints every disagreement).
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
PIN_FILE="$REPO_ROOT/test-data/dataset-pin.env"
WF_DIR="$REPO_ROOT/.github/workflows"

errors=0
err() { echo "❌ $*" >&2; errors=$((errors + 1)); }

if [[ ! -f "$PIN_FILE" ]]; then
  echo "❌ tracked pin file not found: $PIN_FILE" >&2
  exit 1
fi

# Parse the tracked pin (KEY=value, no expansion beyond the file itself).
DATASET_TAG=""; DATASET_ASSET=""; DATASET_SHA256=""
# shellcheck disable=SC1090
source "$PIN_FILE"

[[ -n "$DATASET_TAG" ]]    || err "test-data/dataset-pin.env: DATASET_TAG is empty/unset"
[[ -n "$DATASET_ASSET" ]]  || err "test-data/dataset-pin.env: DATASET_ASSET is empty/unset"
if [[ ! "$DATASET_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  err "test-data/dataset-pin.env: DATASET_SHA256 must be 64-char lowercase hex, got '${DATASET_SHA256}'"
fi
if [[ "$errors" -ne 0 ]]; then
  echo "" >&2
  echo "Tracked pin file is malformed; cannot validate. Fix test-data/dataset-pin.env." >&2
  exit 1
fi

echo "Tracked dataset pin (test-data/dataset-pin.env):"
echo "  tag=${DATASET_TAG}  asset=${DATASET_ASSET}  sha256=${DATASET_SHA256}"

# 1) Every workflow that declares a `DATASET_SHA256:` env must match the pin.
shopt -s nullglob
wf_checked=0
for f in "$WF_DIR"/*.yml "$WF_DIR"/*.yaml; do
  # Only the literal env declaration (`  DATASET_SHA256: <hex>`), not the many
  # `${{ env.DATASET_SHA256 }}` references that derive from it.
  while IFS= read -r decl; do
    wf_checked=$((wf_checked + 1))
    val="$(printf '%s' "$decl" | sed -E 's/.*DATASET_SHA256:[[:space:]]*//; s/[[:space:]].*$//')"
    if [[ "$val" != "$DATASET_SHA256" ]]; then
      err "${f#"$REPO_ROOT"/}: DATASET_SHA256 env '${val}' != tracked pin '${DATASET_SHA256}'"
    fi
  done < <(grep -E '^[[:space:]]*DATASET_SHA256:[[:space:]]*[0-9a-f]{64}' "$f" || true)
done

# 2) Every 64-hex token in a pin-consuming file must equal the tracked sha.
#    Catches inline literals (e.g. coverage.yml, m1-ci.yml) and helper-script
#    defaults that skipped the env-decl form. The tracked pin file and THIS
#    script legitimately contain the sha; the bump script carries an OLD_SHA
#    constant by design, so it is validated separately below.
PIN_CONSUMERS=(
  "$WF_DIR"/*.yml
  "$WF_DIR"/*.yaml
  "$REPO_ROOT/.github/actions/restore-canonical-datasets/action.yml"
  "$REPO_ROOT/scripts/local/pre-merge.sh"
  "$REPO_ROOT/scripts/ci/ensure_real_dataset.sh"
  "$REPO_ROOT/test-data/scripts/fetch-datasets.sh"
)
for f in "${PIN_CONSUMERS[@]}"; do
  [[ -e "$f" ]] || continue
  while IFS= read -r tok; do
    if [[ "$tok" != "$DATASET_SHA256" ]]; then
      err "${f#"$REPO_ROOT"/}: stray dataset-like sha '${tok}' != tracked pin '${DATASET_SHA256}'"
    fi
  done < <(grep -hoE '[0-9a-f]{64}' "$f" || true)
done

# 3) The fetch helper + restore action + pre-merge defaults must name the pin's
#    asset and tag (the sha is covered by check 2).
declare -A NAMED_DEFAULTS=(
  ["$REPO_ROOT/test-data/scripts/fetch-datasets.sh"]="fetch-datasets.sh"
  ["$REPO_ROOT/.github/actions/restore-canonical-datasets/action.yml"]="restore-canonical-datasets/action.yml"
  ["$REPO_ROOT/scripts/local/pre-merge.sh"]="pre-merge.sh"
)
for f in "${!NAMED_DEFAULTS[@]}"; do
  [[ -e "$f" ]] || continue
  label="${NAMED_DEFAULTS[$f]}"
  grep -qF "$DATASET_ASSET" "$f" || err "${label}: does not reference tracked asset '${DATASET_ASSET}'"
  grep -qF "$DATASET_TAG"   "$f" || err "${label}: does not reference tracked tag '${DATASET_TAG}'"
done

# NOTE: bump-dataset-pin.sh is intentionally NOT sha-checked here. Its `OLD_*`
# are loaded from this tracked pin at runtime; the hardcoded literals in its
# body are only a fallback for a missing pin file and legitimately go stale
# after a bump (the bump skips its own body), so asserting them would false-red.
# It is excluded from the PIN_CONSUMERS 64-hex sweep above for the same reason.

# 4) The GENERATED dataset pin must NOT be tracked (it is produced per-fetch).
if git -C "$REPO_ROOT" ls-files --error-unmatch test-data/datasets/.dataset-pin >/dev/null 2>&1; then
  err "test-data/datasets/.dataset-pin is git-tracked; it is GENERATED by fetch-datasets.sh and must stay untracked (use test-data/dataset-pin.env)"
fi

echo "Checked ${wf_checked} workflow DATASET_SHA256 env declaration(s) and all pin-consuming files."
if [[ "$errors" -ne 0 ]]; then
  echo "" >&2
  echo "❌ dataset pin drift: ${errors} disagreement(s) with test-data/dataset-pin.env." >&2
  echo "   Fix via: test-data/scripts/bump-dataset-pin.sh --new-sha <sha> (see dev-cookbook)." >&2
  exit 1
fi
echo "✅ all workflows and pin-consuming files agree with the tracked dataset pin."
