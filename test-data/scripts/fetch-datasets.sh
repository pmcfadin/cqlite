#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v3 DATASET_ASSET=cassandra5-small-full-v3.5.tar.gz DATASET_SHA256=414195074f6df446a7381aad051af84158e9a021a6e2cd21cbc6c3ad0be1ba16 ./test-data/scripts/fetch-datasets.sh

# The canonical asset/tag/sha live in ONE tracked file (issue #2646):
# test-data/dataset-pin.env. Load it for the defaults so this helper never
# drifts from the workflows. Precedence: explicit DATASET_* env (CI passes the
# workflow-declared pin) > tracked pin file > historical hardcoded fallback
# (older checkouts without the pin file). We capture the incoming env FIRST so
# sourcing the pin file cannot clobber a real override.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIN_ENV="${SCRIPT_DIR}/../dataset-pin.env"
_ENV_TAG="${DATASET_TAG:-}"
_ENV_ASSET="${DATASET_ASSET:-}"
_ENV_SHA="${DATASET_SHA256:-}"
_PIN_TAG=""; _PIN_ASSET=""; _PIN_SHA=""
if [ -f "${PIN_ENV}" ]; then
  # shellcheck disable=SC1090
  . "${PIN_ENV}"
  _PIN_TAG="${DATASET_TAG:-}"
  _PIN_ASSET="${DATASET_ASSET:-}"
  _PIN_SHA="${DATASET_SHA256:-}"
fi

TAG="${_ENV_TAG:-${_PIN_TAG:-datasets-v3}}"
ASSET="${_ENV_ASSET:-${_PIN_ASSET:-cassandra5-small-full-v3.5.tar.gz}}"
SHA256_EXPECTED="${_ENV_SHA:-${_PIN_SHA:-414195074f6df446a7381aad051af84158e9a021a6e2cd21cbc6c3ad0be1ba16}}"
DATASET_ROOT="${CQLITE_DATASETS_ROOT:-test-data/datasets}"
ARCHIVE_DATASET_ROOT="test-data/datasets"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
ASSET_PATH="/tmp/${ASSET}"
WIDE_PARTITION_DIR="${DATASET_ROOT}/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
WIDE_PARTITION_GOLDEN="${WIDE_PARTITION_DIR}/nb-2-big-Data.db.jsonl"
# Exact promoted wide_partition reference binaries the byte_for_byte parity
# scenarios + the digest strict test require (mirrors REQUIRED_BINARY_COMPONENTS
# in cqlite-core/tests/issue_993_wide_partition_promoted_index_parity.rs). A
# dataset missing any of these must force a re-fetch (issue #1185 fail-closed).
WIDE_PARTITION_REQUIRED_COMPONENTS=(
  "nb-2-big-Data.db"
  "nb-2-big-Index.db"
  "nb-2-big-Digest.crc32"
  "nb-2-big-CompressionInfo.db"
)

if [ -z "${DATASET_ROOT}" ] || [ "${DATASET_ROOT}" = "/" ]; then
  echo "ERROR: unsafe CQLITE_DATASETS_ROOT='${DATASET_ROOT}'" >&2
  exit 1
fi

fail_unsafe_dataset_root() {
  echo "ERROR: unsafe CQLITE_DATASETS_ROOT='${DATASET_ROOT}': $1" >&2
  exit 1
}

canonicalize_dataset_root() {
  local raw_root="$1"
  local abs_root parent base parent_abs repo_root

  case "${raw_root}" in
    ""|"/"|".")
      fail_unsafe_dataset_root "must point at a dataset directory, not a filesystem root"
      ;;
    /*) abs_root="${raw_root}" ;;
    *) abs_root="${PWD}/${raw_root}" ;;
  esac

  parent="$(dirname "${abs_root}")"
  base="$(basename "${abs_root}")"

  [ "${base}" = "datasets" ] \
    || fail_unsafe_dataset_root "final path component must be 'datasets'"

  mkdir -p "${parent}"
  parent_abs="$(cd "${parent}" && pwd -P)"
  [ "${parent_abs}" != "/" ] \
    || fail_unsafe_dataset_root "refusing to replace a top-level /datasets directory"

  abs_root="${parent_abs}/${base}"

  if repo_root="$(git rev-parse --show-toplevel 2>/dev/null)"; then
    repo_root="$(cd "${repo_root}" && pwd -P)"
    [ "${abs_root}" != "${repo_root}" ] \
      || fail_unsafe_dataset_root "refusing to replace the repository root"
  fi

  [ "${abs_root}" != "${HOME:-}" ] \
    || fail_unsafe_dataset_root "refusing to replace HOME"
  [ "${abs_root}" != "${TMPDIR:-}" ] \
    || fail_unsafe_dataset_root "refusing to replace TMPDIR"
  [ "${abs_root}" != "/tmp" ] \
    || fail_unsafe_dataset_root "refusing to replace /tmp"
  [ "${abs_root}" != "/private/tmp" ] \
    || fail_unsafe_dataset_root "refusing to replace /private/tmp"

  printf '%s\n' "${abs_root}"
}

DATASET_ROOT="$(canonicalize_dataset_root "${DATASET_ROOT}")"
export CQLITE_DATASETS_ROOT="${DATASET_ROOT}"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
WIDE_PARTITION_DIR="${DATASET_ROOT}/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294"
WIDE_PARTITION_GOLDEN="${WIDE_PARTITION_DIR}/nb-2-big-Data.db.jsonl"

write_pin() {
  mkdir -p "${DATASET_ROOT}"
  {
    echo "tag=${TAG}"
    echo "asset=${ASSET}"
    echo "sha256=${SHA256_EXPECTED}"
  } > "${PIN_FILE}"
}

# ---------------------------------------------------------------------------
# Tracked-fixture guard (issue #2878)
#
# `rm -rf "${DATASET_ROOT}"` below is unconditionally destructive, and the pinned
# archive is NOT a superset of the checkout: ~875 files under test-data/datasets
# are git-TRACKED (162 JSONL sstabledump goldens, force-added byte-parity *.db
# references, the 4 commitlog fixtures from #2389) and several of them ship in no
# archive at all. Deleting them and extracting does not bring them back: the gate
# then FAILs core/cli tests on a pristine main and leaves the checkout dirty with
# stageable deletions of tracked fixtures (data loss by accident).
#
# So we CAPTURE the tracked-file list from git BEFORE the destructive step and
# restore exactly that captured list afterwards. Two rules follow from the
# pre-#2878 defect, whose restore path never ran:
#   * NOT CI-gated. The local/agent box (CI unset) is the destructive arm.
#   * NEVER a silent bail. If we cannot determine whether the dataset dir holds
#     tracked files, we REFUSE to run `rm -rf` rather than guess — see
#     refuse_unprotected_dataset_root.
# ---------------------------------------------------------------------------

# State published by capture_tracked_dataset_files, consumed by
# restore_tracked_dataset_files. TRACKED_GUARD_STATE is one of:
#   in-repo      dataset dir lives in a git work tree; LIST/REPO/REL/COUNT valid
#   out-of-repo  dataset dir provably outside any git work tree; nothing to protect
#   unset        capture has not run (restore is then a no-op)
TRACKED_GUARD_STATE=unset
TRACKED_GUARD_REPO=""
TRACKED_GUARD_REL=""
TRACKED_GUARD_LIST=""
TRACKED_GUARD_COUNT=0

# Escape hatch for an environment that genuinely cannot run the guard (no git
# binary, exotic mount). Loud, opt-in, never the default.
TRACKED_GUARD_ALLOW_UNPROTECTED="${CQLITE_DATASETS_ALLOW_UNPROTECTED:-}"

cleanup_tracked_guard_list() {
  [ -n "${TRACKED_GUARD_LIST}" ] && rm -f "${TRACKED_GUARD_LIST}"
  TRACKED_GUARD_LIST=""
  return 0
}

refuse_unprotected_dataset_root() {
  local reason="$1"
  if [ "${TRACKED_GUARD_ALLOW_UNPROTECTED}" = "1" ]; then
    echo "WARNING: cannot protect git-tracked files under '${DATASET_ROOT}': ${reason}" >&2
    echo "WARNING: proceeding anyway because CQLITE_DATASETS_ALLOW_UNPROTECTED=1; tracked fixtures may be DELETED" >&2
    TRACKED_GUARD_STATE=out-of-repo
    return 0
  fi
  echo "ERROR: refusing to replace '${DATASET_ROOT}': ${reason}" >&2
  echo "ERROR: this script deletes that directory, and it may contain git-tracked reference files" >&2
  echo "ERROR: (issue #2878). Point CQLITE_DATASETS_ROOT at a directory outside any git checkout," >&2
  echo "ERROR: install git so the tracked files can be captured and restored, or set" >&2
  echo "ERROR: CQLITE_DATASETS_ALLOW_UNPROTECTED=1 to accept the data loss." >&2
  exit 1
}

# Physical absolute path of a target that need not exist yet: resolve the nearest
# EXISTING ancestor with `cd -P`/`pwd -P` (which collapses symlinks and `..`),
# then re-append the not-yet-existing remainder. A raw string prefix compare
# against an unresolved path is exactly what silently skipped the pre-#2878
# restore.
physical_path() {
  local target="$1" suffix="" head resolved
  case "${target}" in
    /*) ;;
    "") return 1 ;;
    *) target="${PWD}/${target}" ;;
  esac
  head="${target}"
  while [ ! -e "${head}" ]; do
    case "${head}" in
      /|.|"") return 1 ;;
    esac
    suffix="/$(basename "${head}")${suffix}"
    head="$(dirname "${head}")"
  done
  if [ -d "${head}" ]; then
    resolved="$(cd -P "${head}" 2>/dev/null && pwd -P)" || return 1
  else
    local head_parent head_base
    head_parent="$(dirname "${head}")"
    head_base="$(basename "${head}")"
    resolved="$(cd -P "${head_parent}" 2>/dev/null && pwd -P)" || return 1
    resolved="${resolved%/}/${head_base}"
  fi
  printf '%s\n' "${resolved%/}${suffix}"
}

# Nearest existing DIRECTORY ancestor of an absolute path (the `git -C` probe
# point: DATASET_ROOT itself may not exist yet).
nearest_existing_dir() {
  local head="$1"
  while [ ! -d "${head}" ]; do
    case "${head}" in
      /|.|"") printf '/\n'; return 0 ;;
    esac
    head="$(dirname "${head}")"
  done
  printf '%s\n' "${head}"
}

# Component-wise containment: prints the child's path RELATIVE to the parent when
# the child is the parent or lives under it, else fails. Both sides must already
# be physical absolute paths. Component-wise (not string-prefix) so
# /repo/test-data-foo is NOT treated as inside /repo/test-data.
path_relative_inside() {
  local parent="${1%/}" child="${2%/}"
  [ -n "${parent}" ] || parent="/"
  if [ "${child}" = "${parent}" ]; then
    printf '.\n'
    return 0
  fi
  case "${parent}" in
    /) printf '%s\n' "${child#/}"; return 0 ;;
  esac
  case "${child}" in
    "${parent}"/*) printf '%s\n' "${child#"${parent}/"}"; return 0 ;;
    *) return 1 ;;
  esac
}

# True when some ancestor of the path (or the path itself) holds a .git entry —
# a git-free filesystem probe used only to decide whether an unprotectable
# dataset dir is plausibly inside a checkout (i.e. whether to refuse).
ancestor_has_git_dir() {
  local head="$1"
  while :; do
    [ -e "${head}/.git" ] && return 0
    case "${head}" in
      /|.|"") return 1 ;;
    esac
    head="$(dirname "${head}")"
  done
}

# Capture the git-tracked files under DATASET_ROOT. MUST be called before any
# destructive step; safe to call repeatedly (re-captures).
capture_tracked_dataset_files() {
  cleanup_tracked_guard_list
  TRACKED_GUARD_STATE=unknown
  TRACKED_GUARD_REPO=""
  TRACKED_GUARD_REL=""
  TRACKED_GUARD_COUNT=0

  local dataset_abs probe repo_root repo_root_phys rel
  if ! dataset_abs="$(physical_path "${DATASET_ROOT}")"; then
    refuse_unprotected_dataset_root "could not resolve a physical path for it"
    return 0
  fi

  if ! command -v git >/dev/null 2>&1; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      refuse_unprotected_dataset_root "git is not installed but '${dataset_abs}' is inside a git checkout"
      return 0
    fi
    TRACKED_GUARD_STATE=out-of-repo
    return 0
  fi

  probe="$(nearest_existing_dir "${dataset_abs}")"
  if ! repo_root="$(git -C "${probe}" rev-parse --show-toplevel 2>/dev/null)" || [ -z "${repo_root}" ]; then
    if ancestor_has_git_dir "${dataset_abs}"; then
      refuse_unprotected_dataset_root "'${dataset_abs}' looks like it is inside a git checkout, but 'git -C ${probe} rev-parse --show-toplevel' reported no work tree"
      return 0
    fi
    TRACKED_GUARD_STATE=out-of-repo
    return 0
  fi

  if ! repo_root_phys="$(physical_path "${repo_root}")"; then
    refuse_unprotected_dataset_root "could not resolve a physical path for the enclosing repository '${repo_root}'"
    return 0
  fi

  # The probe IS inside this work tree, and dataset_abs only appends components
  # to it — so a containment failure here means the two paths disagree in a way
  # we do not understand. Refusing is mandatory: a silent skip is the #2878 bug.
  if ! rel="$(path_relative_inside "${repo_root_phys}" "${dataset_abs}")"; then
    refuse_unprotected_dataset_root "could not decide whether '${dataset_abs}' is inside the git work tree at '${repo_root_phys}' (probed from '${probe}')"
    return 0
  fi

  TRACKED_GUARD_LIST="$(mktemp "${TMPDIR:-/tmp}/cqlite-tracked-datasets.XXXXXX")"
  if ! git -C "${repo_root_phys}" ls-files -z -- ":(literal)${rel}" >"${TRACKED_GUARD_LIST}"; then
    cleanup_tracked_guard_list
    refuse_unprotected_dataset_root "'git ls-files' failed for '${rel}' in '${repo_root_phys}'"
    return 0
  fi

  # Count NUL-terminated entries exactly (a filename may contain a newline).
  local count=0 rel_path
  while IFS= read -r -d '' rel_path; do
    count=$((count + 1))
  done <"${TRACKED_GUARD_LIST}"

  TRACKED_GUARD_REPO="${repo_root_phys}"
  TRACKED_GUARD_REL="${rel}"
  TRACKED_GUARD_COUNT="${count}"
  TRACKED_GUARD_STATE=in-repo
  return 0
}

# Restore the captured tracked files from HEAD.
#   missing-only  only files that are absent on disk (pre-flight repair of a
#                 previous run's damage; never clobbers a local modification)
#   all           every captured file (post-extraction, after rm -rf)
# In `all` mode the tracked subtree MUST end clean, else this is a hard error:
# a restore that silently no-ops is otherwise indistinguishable from one that
# worked (issue #2878 acceptance oracle).
restore_tracked_dataset_files() {
  local mode="$1"
  [ "${TRACKED_GUARD_STATE}" = "in-repo" ] || return 0
  [ -n "${TRACKED_GUARD_LIST}" ] && [ -s "${TRACKED_GUARD_LIST}" ] || return 0

  local -a pathspecs=()
  local rel_path
  while IFS= read -r -d '' rel_path; do
    if [ "${mode}" = "missing-only" ] && [ -e "${TRACKED_GUARD_REPO}/${rel_path}" ]; then
      continue
    fi
    pathspecs+=( ":(literal)${rel_path}" )
  done <"${TRACKED_GUARD_LIST}"

  if [ "${#pathspecs[@]}" -gt 0 ]; then
    echo "Restoring ${#pathspecs[@]} git-tracked file(s) under ${TRACKED_GUARD_REL} (of ${TRACKED_GUARD_COUNT} tracked; mode=${mode})"
    # Batch so a very large fixture set cannot hit ARG_MAX.
    local batch=400 i
    for (( i = 0; i < ${#pathspecs[@]}; i += batch )); do
      # Restore from the INDEX (not HEAD): the index is what `git ls-files`
      # enumerated, so this reproduces the checkout's intended content and cannot
      # clobber a developer's STAGED edit to a tracked reference file.
      if ! git -C "${TRACKED_GUARD_REPO}" restore --worktree -- "${pathspecs[@]:i:batch}"; then
        echo "ERROR: failed to restore git-tracked files under ${TRACKED_GUARD_REL} in ${TRACKED_GUARD_REPO} (issue #2878)" >&2
        exit 1
      fi
    done
  fi

  # Post-condition (only meaningful for the post-destruction restore): no tracked
  # file under the dataset dir may be left deleted or modified relative to the
  # index. `git diff` isolates exactly that — untracked extracted content is
  # expected and is .gitignore's business, and a pre-existing STAGED change is
  # not damage this script caused.
  if [ "${mode}" = "all" ]; then
    local dirty
    if ! dirty="$(git -C "${TRACKED_GUARD_REPO}" diff --name-status -- ":(literal)${TRACKED_GUARD_REL}")"; then
      echo "ERROR: could not verify tracked-file integrity under ${TRACKED_GUARD_REL} (issue #2878)" >&2
      exit 1
    fi
    if [ -n "${dirty}" ]; then
      echo "ERROR: dataset fetch left git-tracked files under ${TRACKED_GUARD_REL} deleted or modified (issue #2878):" >&2
      printf '%s\n' "${dirty}" | head -20 >&2
      exit 1
    fi
  fi
  return 0
}

# Validates the dataset CONTENT only (no pin tag/asset/sha checks). A freshly
# extracted archive is in exactly this state — content present but the pin not
# yet stamped — so the post-extraction validation uses this to confirm the
# archive is complete (incl. the exact wide_partition reference binaries) before
# write_pin runs.
has_required_content() {
  [ -f "${DATASET_ROOT}/metadata.yml" ] || return 1
  [ -s "${WIDE_PARTITION_GOLDEN}" ] || return 1

  local core_fixture
  core_fixture="$(find "${DATASET_ROOT}/sstables/test_basic" -path '*simple_table-*-Data.db' -print -quit 2>/dev/null || true)"
  [ -n "${core_fixture}" ] || return 1

  local data_count index_count summary_count statistics_count
  data_count="$(find "${DATASET_ROOT}" -name '*-Data.db' 2>/dev/null | wc -l | tr -d ' ')"
  index_count="$(find "${DATASET_ROOT}" -name '*-Index.db' 2>/dev/null | wc -l | tr -d ' ')"
  summary_count="$(find "${DATASET_ROOT}" -name '*-Summary.db' 2>/dev/null | wc -l | tr -d ' ')"
  statistics_count="$(find "${DATASET_ROOT}" -name '*-Statistics.db' 2>/dev/null | wc -l | tr -d ' ')"

  [ "${data_count}" -gt 0 ] || return 1
  [ "${index_count}" -gt 0 ] || return 1
  [ "${summary_count}" -gt 0 ] || return 1
  [ "${statistics_count}" -gt 0 ] || return 1

  # The global counts above can be satisfied by other tables; explicitly require
  # the EXACT promoted wide_partition reference binaries so a partial/stale
  # dataset (other tables present, wide_partition absent) cannot be accepted and
  # stamped as the new pin (issue #1185 fail-closed).
  local component
  for component in "${WIDE_PARTITION_REQUIRED_COMPONENTS[@]}"; do
    [ -f "${WIDE_PARTITION_DIR}/${component}" ] || return 1
  done
}

# Strict fast-path check: content present AND pinned to the expected v3.5 tag/
# asset/sha. The skip-download path requires BOTH so a stale/unpinned dataset
# forces a re-fetch (issue #1185 fail-closed).
has_required_dataset() {
  has_required_content || return 1

  # The pin file MUST exist and match. A missing pin is NOT acceptable — it would
  # otherwise let an unverified dataset fall through and be re-stamped (#1185).
  [ -f "${PIN_FILE}" ] || return 1
  grep -qx "tag=${TAG}" "${PIN_FILE}" || return 1
  grep -qx "asset=${ASSET}" "${PIN_FILE}" || return 1
  grep -qx "sha256=${SHA256_EXPECTED}" "${PIN_FILE}" || return 1
}

EXTRACT_TMP=""
# One EXIT trap for both temporaries. Guarded so a failing command inside the
# trap can never turn a successful run's exit status non-zero under `set -e`
# (BSD `rm -rf ""` is not silent everywhere).
cleanup_fetch_temporaries() {
  [ -n "${EXTRACT_TMP}" ] && rm -rf "${EXTRACT_TMP}"
  cleanup_tracked_guard_list
  return 0
}
trap cleanup_fetch_temporaries EXIT

# Pre-flight repair: a previous run (or a pre-#2878 run) may have left tracked
# fixtures deleted. Restore only the MISSING ones so an intentional local edit to
# a tracked reference file survives, then let has_required_dataset judge.
capture_tracked_dataset_files
restore_tracked_dataset_files missing-only

if has_required_dataset; then
  write_pin
  echo "Dataset ${ASSET} (tag ${TAG}) already present in ${DATASET_ROOT}; skipping download"
  exit 0
fi

echo "Fetching dataset ${ASSET} (tag ${TAG})"
mkdir -p "${DATASET_ROOT}"
curl -fsSL -o "${ASSET_PATH}" "https://github.com/pmcfadin/cqlite/releases/download/${TAG}/${ASSET}"

if command -v sha256sum >/dev/null 2>&1; then
  echo "${SHA256_EXPECTED}  ${ASSET_PATH}" | sha256sum -c -
elif command -v shasum >/dev/null 2>&1; then
  ACTUAL="$(shasum -a 256 "${ASSET_PATH}" | awk '{print $1}')"
  test "${ACTUAL}" = "${SHA256_EXPECTED}" || { echo "SHA256 mismatch"; exit 1; }
else
  # Fail closed in CI (issue #1024): a missing checksum tool must NOT let an
  # unverified asset through where parity is gated. When CQLITE_PARITY_REQUIRE_DATASETS
  # (exported by the parity workflow) or the generic CI signal is set, an
  # unverifiable dataset is a gate failure — there is no safe way to admit the
  # asset without confirming its provenance against the pinned SHA256.
  #
  # On a local/dev machine that simply lacks both tools, fall through with a
  # loud warning so iteration is not blocked; the operator can opt back into
  # fail-closed behavior with CQLITE_PARITY_REQUIRE_DATASETS=1.
  if [ "${CQLITE_PARITY_REQUIRE_DATASETS:-}" = "1" ] || [ -n "${CI:-}" ]; then
    echo "ERROR: no sha256 checker found (need sha256sum or shasum) — cannot verify dataset provenance" >&2
    exit 1
  else
    echo "WARNING: no sha256 tool; skipping checksum verification (set CQLITE_PARITY_REQUIRE_DATASETS=1 to enforce)" >&2
  fi
fi

# Re-capture immediately before the destructive step so the restored list is
# exactly what existed at deletion time (issue #2878 AC1: capture, never
# re-derive after the fact). capture_* exits rather than returning when it cannot
# determine whether tracked files are at risk.
capture_tracked_dataset_files

rm -rf "${DATASET_ROOT}"
if [ "${DATASET_ROOT}" = "${ARCHIVE_DATASET_ROOT}" ]; then
  tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
else
  EXTRACT_TMP="$(mktemp -d)"
  tar -xzf "${ASSET_PATH}" -C "${EXTRACT_TMP}" --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'

  if [ ! -d "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" ]; then
    echo "ERROR: dataset archive did not contain ${ARCHIVE_DATASET_ROOT}" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${DATASET_ROOT}")"
  mv "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" "${DATASET_ROOT}"
fi

# Undo the rm -rf for every captured tracked file, and hard-fail if the tracked
# subtree is not clean afterwards (issue #2878).
restore_tracked_dataset_files all

# Remove macOS AppleDouble shadow files (`._*`). The archive may contain them
# when produced on macOS, and they break test helpers that scan for files by
# suffix (e.g., `*-Data.db` matches both the real file and `._..-Data.db`).
find "${DATASET_ROOT}" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

# Validate the freshly extracted CONTENT (not the pin — package_datasets.sh does
# not embed a .dataset-pin in the archive). A bad/partial archive still fails
# loudly here; the pin is stamped only after content is confirmed.
if ! has_required_content; then
  echo "ERROR: dataset extraction did not produce required Cassandra SSTable components in ${DATASET_ROOT}" >&2
  exit 1
fi

write_pin
echo "Dataset extracted to ${DATASET_ROOT}"
