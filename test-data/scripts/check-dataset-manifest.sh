#!/usr/bin/env bash
#
# check-dataset-manifest.sh (issue #1230)
#
# Fail-closed CI sanity check: assert that the fetched dataset asset contains a
# Data.db for EVERY expected (keyspace, table) in the 33-table validation corpus,
# not just test_basic/simple_table. A partial extraction or a dropped table reds
# CI here instead of silently turning the dataset-dependent test lanes green by
# letting them skip on absence.
#
# Usage: check-dataset-manifest.sh [DATASETS_ROOT]
#   DATASETS_ROOT defaults to test-data/datasets (the parent of sstables/).
#
set -euo pipefail

ROOT="${1:-test-data/datasets}"
SSTABLES="${ROOT}/sstables"

# Expected user-keyspace tables (33 total). Keep in sync with the validation
# matrix (test-data/validation-matrix.md) and the test_basic manifest in
# cqlite-core/tests/reader_compression_tests.rs.
EXPECTED=(
  # test_basic (8)
  "test_basic/composite_key_table"
  "test_basic/compression_test_table"
  "test_basic/counters"
  "test_basic/multi_partition_table"
  "test_basic/simple_table"
  "test_basic/static_columns_table"
  "test_basic/ttl_test_table"
  "test_basic/uncompressed_table"
  # test_collections (8)
  "test_collections/collection_clustering_table"
  "test_collections/collection_table"
  "test_collections/collections_with_udts"
  "test_collections/empty_collections_table"
  "test_collections/frozen_collections_table"
  "test_collections/large_collections_table"
  "test_collections/nested_collections_table"
  "test_collections/typed_collections_table"
  # test_timeseries (9)
  "test_timeseries/app_metrics"
  "test_timeseries/event_store"
  "test_timeseries/log_entries"
  "test_timeseries/sensor_data"
  "test_timeseries/stock_prices"
  "test_timeseries/tick_data"
  "test_timeseries/time_bucketed_counters"
  "test_timeseries/user_activity"
  "test_timeseries/user_sessions"
  # test_wide_rows (8)
  "test_wide_rows/chat_messages"
  "test_wide_rows/document_versions"
  "test_wide_rows/large_blob_table"
  "test_wide_rows/many_columns_table"
  "test_wide_rows/multi_metric_timeseries"
  "test_wide_rows/product_catalog"
  "test_wide_rows/sparse_data_table"
  "test_wide_rows/wide_partition_table"
)

if [ ! -d "$SSTABLES" ]; then
  echo "❌ dataset manifest check: sstables dir missing: $SSTABLES" >&2
  exit 1
fi

missing=0
found=0
for entry in "${EXPECTED[@]}"; do
  table="${entry#*/}"
  # Match <table>-<uuid>/<prefix>-Data.db; -path keeps us pipefail-safe.
  data_db=$(find "$SSTABLES/$(dirname "$entry")" -path "*${table}-*-Data.db" 2>/dev/null | head -1 || true)
  if [ -z "$data_db" ]; then
    echo "❌ missing Data.db for expected table: $entry" >&2
    missing=$((missing + 1))
  else
    found=$((found + 1))
  fi
done

echo "dataset manifest: ${found}/${#EXPECTED[@]} expected tables present"
if [ "$missing" -ne 0 ]; then
  echo "❌ dataset manifest check FAILED: $missing expected table(s) missing — partial extraction or dropped table?" >&2
  exit 1
fi
echo "✅ dataset manifest check passed (all ${#EXPECTED[@]} expected tables present)"
