#!/usr/bin/env bash
set -euo pipefail

# Fetch canonical Cassandra 5 datasets into test-data/datasets
# Usage: DATASET_TAG=datasets-v1 DATASET_ASSET=cassandra5-small.tar.gz ./test-data/scripts/fetch-datasets.sh

TAG="${DATASET_TAG:-datasets-v1}"
ASSET="${DATASET_ASSET:-cassandra5-small.tar.gz}"
SHA256_EXPECTED="${DATASET_SHA256:-313763f28a4de71870c80346818dfa1656f4d9db564b4dce2ddd79f4e00a44dd}"

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


