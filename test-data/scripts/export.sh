#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"
DATASETS_DIR="$ROOT/datasets"
META="$DATASETS_DIR/metadata.yml"

# Ensure a clean export directory
rm -rf "$DATASETS_DIR"
mkdir -p "$DATASETS_DIR"

# Flush to SSTables
docker compose -f "$COMPOSE" exec -T cassandra-5-0 nodetool flush

# Produce metadata.yml with counts and columns using generator container (has pyyaml + cassandra-driver)
# Write YAML to stdout and redirect to host file
docker compose -f "$COMPOSE" run --rm --no-deps data-generator python3 - <<'PY' > "$META"
import yaml
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

cluster = Cluster(["cassandra-5-0"], port=9042)
session = cluster.connect()

keyspaces = ["test_basic","test_collections","test_timeseries","test_wide_rows"]
info = {"keyspaces": []}

for ks in keyspaces:
    try:
        ks_meta = session.cluster.metadata.keyspaces.get(ks)
        if not ks_meta:
            continue
        rows = session.execute(SimpleStatement(
            f"SELECT table_name FROM system_schema.tables WHERE keyspace_name='{ks}' ALLOW FILTERING;"
        ))
        tables = [r.table_name for r in rows]
        ks_rec = {"name": ks, "tables": []}
        for t in tables:
            cols_rows = session.execute(SimpleStatement(
                f"SELECT column_name, kind, type FROM system_schema.columns WHERE keyspace_name='{ks}' AND table_name='{t}' ALLOW FILTERING;"
            ))
            columns = [{"column_name": r.column_name, "kind": r.kind, "type": r.type} for r in cols_rows]
            try:
                cnt_row = session.execute(SimpleStatement(
                    f"SELECT count(*) AS c FROM {ks}.{t} ALLOW FILTERING;"
                )).one()
                count_val = int(cnt_row.c) if cnt_row and cnt_row.c is not None else 0
            except Exception:
                count_val = 0
            ks_rec["tables"].append({"name": t, "row_count": count_val, "columns": columns})
        info["keyspaces"].append(ks_rec)
    except Exception:
        continue

print(yaml.safe_dump(info, sort_keys=False))
PY

# Destructive export of data directory tree via tar stream (more reliable than docker cp)
rm -rf "$DATASETS_DIR/sstables" "$DATASETS_DIR/data"
# Stream from container /var/lib/cassandra/data → host $DATASETS_DIR/data, then rename to sstables
if docker compose -f "$COMPOSE" exec -T cassandra-5-0 bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$DATASETS_DIR" -xf -; then
  mv "$DATASETS_DIR/data" "$DATASETS_DIR/sstables"
  echo "[export] Exported SSTables to $DATASETS_DIR/sstables and wrote $META"
else
  echo "[export] ERROR: Failed to export SSTables via tar stream" >&2
  exit 1
fi

# Generate sstabledump reference JSONs for Index/Summary/Statistics for each Data.db
SSTDUMP_WRAPPER="$ROOT/../scripts/sstabledump-docker.sh"
if [ ! -x "$SSTDUMP_WRAPPER" ]; then
  echo "[export] ERROR: sstabledump wrapper not found or not executable: $SSTDUMP_WRAPPER" >&2
  exit 1
fi

echo "[export] Generating sstabledump reference JSONs (Index/Summary/Statistics) ..."

# Iterate all *-Data.db files under exported sstables
while IFS= read -r -d '' DATA_FILE; do
  DIR="$(dirname "$DATA_FILE")"
  BASE="$(basename "$DATA_FILE")"
  PREFIX_NAME="${BASE%-Data.db}"

  INDEX_JSON="$DIR/${PREFIX_NAME}-Index.db.json"
  SUMMARY_JSON="$DIR/${PREFIX_NAME}-Summary.db.json"
  STATS_JSON="$DIR/${PREFIX_NAME}-Statistics.db.json"

  echo "[export]  • $BASE → ${PREFIX_NAME}-{Index,Summary,Statistics}.db.json"

  # Index: include keys and index info, JSON output
  if ! "$SSTDUMP_WRAPPER" "$DATA_FILE" -k -i -j > "${INDEX_JSON}.tmp"; then
    echo "[export] ERROR: Failed to generate Index reference for $DATA_FILE" >&2
    exit 1
  fi
  mv "${INDEX_JSON}.tmp" "$INDEX_JSON"

  # Summary: decoded summary with JSON output
  if ! "$SSTDUMP_WRAPPER" "$DATA_FILE" -d -s -j > "${SUMMARY_JSON}.tmp"; then
    echo "[export] ERROR: Failed to generate Summary reference for $DATA_FILE" >&2
    exit 1
  fi
  mv "${SUMMARY_JSON}.tmp" "$SUMMARY_JSON"

  # Statistics: JSON output (statistics included by default in JSON)
  if ! "$SSTDUMP_WRAPPER" "$DATA_FILE" -j > "${STATS_JSON}.tmp"; then
    echo "[export] ERROR: Failed to generate Statistics reference for $DATA_FILE" >&2
    exit 1
  fi
  mv "${STATS_JSON}.tmp" "$STATS_JSON"

  # Basic sanity: ensure files are non-empty
  for f in "$INDEX_JSON" "$SUMMARY_JSON" "$STATS_JSON"; do
    if [ ! -s "$f" ]; then
      echo "[export] ERROR: Reference JSON is empty or missing: $f" >&2
      exit 1
    fi
  done
done < <(find "$DATASETS_DIR/sstables" -type f -name "*-Data.db" -print0)

echo "[export] Completed generating sstabledump reference JSONs."
