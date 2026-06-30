#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v2 DATASET_ASSET=cassandra5-small-full.tar.gz DATASET_SHA256=<sha> ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v3}"
ASSET="${DATASET_ASSET:-cassandra5-small-full-v3.3.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-44a3dc5d1fdb918dbf9d7568676cf906ccad7881991aaadcf96a7993627951ac}"
DATASET_ROOT="${CQLITE_DATASETS_ROOT:-test-data/datasets}"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
ASSET_PATH="/tmp/${ASSET}"
WIDE_PARTITION_GOLDEN="${DATASET_ROOT}/sstables/test_big/wide_partition-ffe2ee50733111f19e8f6d08b8e7a294/nb-2-big-Data.db.jsonl"

if [ -z "${DATASET_ROOT}" ] || [ "${DATASET_ROOT}" = "/" ]; then
  echo "ERROR: unsafe CQLITE_DATASETS_ROOT='${DATASET_ROOT}'" >&2
  exit 1
fi

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

has_required_dataset() {
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

  if [ -f "${PIN_FILE}" ]; then
    grep -qx "tag=${TAG}" "${PIN_FILE}" || return 1
    grep -qx "asset=${ASSET}" "${PIN_FILE}" || return 1
    grep -qx "sha256=${SHA256_EXPECTED}" "${PIN_FILE}" || return 1
  fi
}

restore_ci_tracked_dataset_files

if has_required_dataset; then
  write_pin
  echo "Dataset ${ASSET} (tag ${TAG}) already present in ${DATASET_ROOT}; skipping download"
  exit 0
fi

echo "Fetching dataset ${ASSET} (tag ${TAG})"
mkdir -p test-data/datasets
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
tar -xzf "${ASSET_PATH}" -C . --exclude='*/._*' --exclude='._*' --exclude='*/.DS_Store' --exclude='.DS_Store'
restore_ci_tracked_dataset_files

# Remove macOS AppleDouble shadow files (`._*`). The archive may contain them
# when produced on macOS, and they break test helpers that scan for files by
# suffix (e.g., `*-Data.db` matches both the real file and `._..-Data.db`).
find "${DATASET_ROOT}" \( -name '._*' -o -name '.DS_Store' \) -delete 2>/dev/null || true

if ! has_required_dataset; then
  echo "ERROR: dataset extraction did not produce required Cassandra SSTable components in ${DATASET_ROOT}" >&2
  exit 1
fi

write_pin
echo "Dataset extracted to ${DATASET_ROOT}"
