#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v2 DATASET_ASSET=cassandra5-small-full.tar.gz DATASET_SHA256=<sha> ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v3}"
ASSET="${DATASET_ASSET:-cassandra5-small-full-v3.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-69950feaaf45854e38c467087911d2d9772eeb459b6131adb676a342d7dfa983}"

echo "Fetching dataset ${ASSET} (tag ${TAG})"
mkdir -p test-data/datasets
curl -fsSL -o /tmp/${ASSET} "https://github.com/pmcfadin/cqlite/releases/download/${TAG}/${ASSET}"

if command -v sha256sum >/dev/null 2>&1; then
  echo "${SHA256_EXPECTED}  /tmp/${ASSET}" | sha256sum -c -
elif command -v shasum >/dev/null 2>&1; then
  ACTUAL="$(shasum -a 256 /tmp/${ASSET} | awk '{print $1}')"
  test "${ACTUAL}" = "${SHA256_EXPECTED}" || { echo "SHA256 mismatch"; exit 1; }
else
  echo "Warning: no sha256 checker found; skipping verification" >&2
fi

tar -xzf /tmp/${ASSET} -C .

# Remove macOS AppleDouble shadow files (`._*`). The archive may contain them
# when produced on macOS, and they break test helpers that scan for files by
# suffix (e.g., `*-Data.db` matches both the real file and `._..-Data.db`).
find test-data/datasets -name '._*' -delete 2>/dev/null || true

echo "Dataset extracted to test-data/datasets"

