#!/usr/bin/env bash
# capture-issue-4204-sstablemetadata.sh — how test-data/oracles/issue_4204/*.sstablemetadata.txt were produced.
#
# For each fixture generation: copy its components (NOT the .jsonl/.txt sidecars) into a scratch dir,
# run the pinned Cassandra 5.0.8 `sstablemetadata` (cheap tier, default flags) in a throwaway
# container, record the wall-clock epoch of the run (the tool evaluates 'Estimated droppable
# tombstones' at gcBefore = now - 0), and prefix three comment lines naming the tool image.
# Run on the Cassandra VM (docker available), e.g. inside `incus exec wintermute:cassandra --`.
set -euo pipefail
IMAGE="${CQLITE_CASSANDRA_IMAGE:-cassandra:5.0.8}"
for dir in "$@"; do   # each: a directory holding one generation's components
  for data in "$dir"/*-Data.db; do
    base=$(basename "$data" -Data.db)
    epoch=$(date +%s)
    docker run --rm -v "$dir:/d" "$IMAGE" /opt/cassandra/tools/bin/sstablemetadata "/d/$base-Data.db" \
      > "$dir.$base.sstablemetadata.txt"
    echo "# captured_epoch: $epoch" >> "$dir.$base.sstablemetadata.epoch"
  done
done
