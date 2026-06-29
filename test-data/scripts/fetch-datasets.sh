#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v2 DATASET_ASSET=cassandra5-small-full.tar.gz DATASET_SHA256=<sha> ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v3}"
ASSET="${DATASET_ASSET:-cassandra5-small-full-v3.2.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-bebc763752c8d68c7fb0483a1b31294b4d1d21343d3f7d124da069e5073202fa}"
DATASET_ROOT="${CQLITE_DATASETS_ROOT:-test-data/datasets}"
PIN_FILE="${DATASET_ROOT}/.dataset-pin"
ASSET_PATH="/tmp/${ASSET}"

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

has_required_dataset() {
  [ -f "${DATASET_ROOT}/metadata.yml" ] || return 1

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
