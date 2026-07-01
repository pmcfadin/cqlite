#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v3 DATASET_ASSET=cassandra5-small-full-v3.4.tar.gz DATASET_SHA256=3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33 ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v3}"
ASSET="${DATASET_ASSET:-cassandra5-small-full-v3.4.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-3cae644360e0142a6bb5e96ddab445ff18e3478e7058104842ce1a455fba8a33}"
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

restore_ci_tracked_dataset_files() {
  [ -n "${CI:-}" ] || return 0
  command -v git >/dev/null 2>&1 || return 0
  git rev-parse --is-inside-work-tree >/dev/null 2>&1 || return 0

  local repo_root dataset_abs dataset_rel tracked_files
  repo_root="$(git rev-parse --show-toplevel)"
  case "${DATASET_ROOT}" in
    /*) dataset_abs="${DATASET_ROOT}" ;;
    *) dataset_abs="${PWD}/${DATASET_ROOT}" ;;
  esac

  case "${dataset_abs}" in
    "${repo_root}"/*) dataset_rel="${dataset_abs#"${repo_root}/"}" ;;
    *) return 0 ;;
  esac

  tracked_files="$(git -C "${repo_root}" ls-files -- "${dataset_rel}")"
  if [ -n "${tracked_files}" ]; then
    echo "Restoring git-tracked dataset reference files under ${dataset_rel}"
    git -C "${repo_root}" restore --source=HEAD -- "${dataset_rel}" 2>/dev/null || true
  fi
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

# Strict fast-path check: content present AND pinned to the expected v3.4 tag/
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

restore_ci_tracked_dataset_files

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

rm -rf "${DATASET_ROOT}"
if [ "${DATASET_ROOT}" = "${ARCHIVE_DATASET_ROOT}" ]; then
  tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
else
  EXTRACT_TMP="$(mktemp -d)"
  trap 'rm -rf "${EXTRACT_TMP:-}"' EXIT
  tar -xzf "${ASSET_PATH}" -C "${EXTRACT_TMP}" --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'

  if [ ! -d "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" ]; then
    echo "ERROR: dataset archive did not contain ${ARCHIVE_DATASET_ROOT}" >&2
    exit 1
  fi

  mkdir -p "$(dirname "${DATASET_ROOT}")"
  mv "${EXTRACT_TMP}/${ARCHIVE_DATASET_ROOT}" "${DATASET_ROOT}"
fi
restore_ci_tracked_dataset_files

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
