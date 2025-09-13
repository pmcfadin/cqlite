#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v2 DATASET_ASSET=cassandra5-small-full.tar.gz DATASET_SHA256=<sha> ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v2}"
ASSET="${DATASET_ASSET:-cassandra5-small-full.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-4eb496339a5f4bc62c3490b75e57966317d7e5c216bbffe8161a659e87cbb56b}"

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

tar -xzf /tmp/${ASSET} -C test-data
echo "Dataset extracted to test-data/datasets"


