#!/bin/bash

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE="$ROOT/docker/docker-compose-cassandra5.yml"
DATASETS_DIR="$ROOT/datasets"
META="$DATASETS_DIR/metadata.yml"

. "$ROOT/scripts/container_env.sh"

# Export for compose providers that read COMPOSE_FILE env
export COMPOSE_FILE="$COMPOSE"

# Ensure a clean export directory
rm -rf "$DATASETS_DIR"
mkdir -p "$DATASETS_DIR"

# Flush to SSTables
compose_exec_nontty cassandra-5-0 nodetool flush

# Produce metadata.yml with counts and columns using generator container (has pyyaml + cassandra-driver)
# Write YAML to stdout and redirect to host file
compose_run_nontty data-generator python3 - <<'PY' > "$META"
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

# Destructive export of data directory tree via tar stream (more reliable than container cp)
rm -rf "$DATASETS_DIR/sstables" "$DATASETS_DIR/data"
# Stream from container /var/lib/cassandra/data → host $DATASETS_DIR/data, then rename to sstables
if compose_exec_nontty cassandra-5-0 bash -lc 'tar -C /var/lib/cassandra -cf - data' | tar -C "$DATASETS_DIR" -xf -; then
  mv "$DATASETS_DIR/data" "$DATASETS_DIR/sstables"
  echo "[export] Exported SSTables to $DATASETS_DIR/sstables and wrote $META"
else
  echo "[export] ERROR: Failed to export SSTables via tar stream" >&2
  exit 1
fi

# Generate Cassandra tool references for each Data.db
echo "[export] Generating Cassandra tool references (Data dump, Summary, Statistics) ..."

# Initialize references manifest (refs_version v2)
REF_MANIFEST="$DATASETS_DIR/references.yml"
{
  echo "refs_version: 2"
  echo "generated_at: $(date -u +"%Y-%m-%dT%H:%M:%SZ")"
  echo "tables:"
} > "$REF_MANIFEST"

# Detect tool availability in image
SUMMARY_TOOL="/opt/cassandra/tools/bin/sstablesummary"
SUMMARY_AVAILABLE="$($ENGINE_CMD run --rm docker.io/library/cassandra:5.0 bash -lc "test -x '$SUMMARY_TOOL' && echo yes || echo no" || true)"
if [ "$SUMMARY_AVAILABLE" != "yes" ]; then
  echo "[export] NOTE: sstablesummary not found in image; Summary generation will be skipped"
fi

# Mount root once to avoid per-dir mount issues (ensure absolute path for Docker)
# Prefer python abspath for portability
MOUNT_ROOT="$(python3 - <<'PY'
import os,sys
path = os.path.join(os.environ.get('DATASETS_DIR',''), 'sstables')
print(os.path.abspath(path))
PY
)"

# Validate mount root exists and is absolute
if [ ! -d "$MOUNT_ROOT" ] || [[ "$MOUNT_ROOT" != /* ]]; then
  echo "[export] ERROR: Invalid mount root: $MOUNT_ROOT" >&2
  exit 1
fi

# Podman rootless often needs UID/GID remap for bind mounts
VOLUME_FLAGS=""
if [ "${ENGINE_CMD:-}" = "podman" ]; then
  VOLUME_FLAGS=":U"
fi

# Iterate all *-Data.db files under exported sstables
while IFS= read -r -d '' DATA_FILE; do
  DIR="$(dirname "$DATA_FILE")"
  BASE="$(basename "$DATA_FILE")"
  PREFIX_NAME="${BASE%-Data.db}"

  # Derive keyspace and table from directory structure: sstables/<keyspace>/<table>-<hash>
  KEYSPACE="$(basename "$(dirname "$DIR")")"
  TABLE_DIR_NAME="$(basename "$DIR")"
  TABLE="${TABLE_DIR_NAME%%-*}"

  DATA_JSONL="$DIR/${PREFIX_NAME}-Data.db.jsonl"
  SUMMARY_TXT="$DIR/${PREFIX_NAME}-Summary.db.txt"
  STATS_TXT="$DIR/${PREFIX_NAME}-Statistics.db.txt"

  echo "[export]  • $BASE → ${PREFIX_NAME}-{Data.db.jsonl, Summary.db.txt, Statistics.db.txt}"

  # Compute relative path from mount root for stable container pathing
  REL_PATH=$(python3 - "$DATA_FILE" "$MOUNT_ROOT" <<'PY'
import os, sys
print(os.path.relpath(sys.argv[1], sys.argv[2]))
PY
)

  # Data dump (JSON lines by partition)
  CMD_OUTPUT=$($ENGINE_CMD run --rm -v "$MOUNT_ROOT:/data$VOLUME_FLAGS" docker.io/library/cassandra:5.0 \
    bash -lc '"/opt/cassandra/tools/bin/sstabledump" "/data/'"$REL_PATH"'" -l') || true
  if [ -z "$CMD_OUTPUT" ]; then
    echo "[export] ERROR: Failed to generate Data dump for $DATA_FILE" >&2
    exit 1
  fi
  printf "%s" "$CMD_OUTPUT" > "$DATA_JSONL"

  # Summary (text) if available
  if [ "$SUMMARY_AVAILABLE" = "yes" ]; then
    CMD_OUTPUT=$($ENGINE_CMD run --rm -v "$MOUNT_ROOT:/data$VOLUME_FLAGS" docker.io/library/cassandra:5.0 \
      bash -lc '"/opt/cassandra/tools/bin/sstablesummary" "/data/'"$REL_PATH"'"') || true
    if [ -z "$CMD_OUTPUT" ]; then
      echo "[export] ERROR: Failed to generate Summary for $DATA_FILE" >&2
      exit 1
    fi
    printf "%s" "$CMD_OUTPUT" > "$SUMMARY_TXT"
  else
    : # Skip summary
  fi

  # Statistics (text)
  CMD_OUTPUT=$($ENGINE_CMD run --rm -v "$MOUNT_ROOT:/data$VOLUME_FLAGS" docker.io/library/cassandra:5.0 \
    bash -lc '"/opt/cassandra/tools/bin/sstablemetadata" "/data/'"$REL_PATH"'"') || true
  if [ -z "$CMD_OUTPUT" ]; then
    echo "[export] ERROR: Failed to generate Statistics for $DATA_FILE" >&2
    exit 1
  fi
  printf "%s" "$CMD_OUTPUT" > "$STATS_TXT"

  # Basic sanity: ensure files are non-empty
  # Sanity: ensure outputs are non-empty (Summary optional)
  if [ ! -s "$DATA_JSONL" ]; then
    echo "[export] ERROR: Data dump output is empty: $DATA_JSONL" >&2
    exit 1
  fi
  if [ ! -s "$STATS_TXT" ]; then
    echo "[export] ERROR: Statistics output is empty: $STATS_TXT" >&2
    exit 1
  fi
  if [ "$SUMMARY_AVAILABLE" = "yes" ] && [ ! -s "$SUMMARY_TXT" ]; then
    echo "[export] ERROR: Summary output is empty: $SUMMARY_TXT" >&2
    exit 1
  fi

  # Append entry to references manifest
  GEN_TS=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  printf -- "  - keyspace: %s\n" "$KEYSPACE" >> "$REF_MANIFEST"
  printf -- "    table: %s\n" "$TABLE" >> "$REF_MANIFEST"
  printf -- "    sstable_dir: \"%s\"\n" "$DIR" >> "$REF_MANIFEST"
  printf -- "    prefix: %s\n" "$PREFIX_NAME" >> "$REF_MANIFEST"
  printf -- "    references:\n" >> "$REF_MANIFEST"
  printf -- "      data_jsonl:\n        present: %s\n        path: \"%s\"\n" "true" "$DATA_JSONL" >> "$REF_MANIFEST"
  printf -- "      statistics_txt:\n        present: %s\n        path: \"%s\"\n" "true" "$STATS_TXT" >> "$REF_MANIFEST"
  if [ "$SUMMARY_AVAILABLE" = "yes" ]; then
    SUM_PRESENT=true
  else
    SUM_PRESENT=false
  fi
  printf -- "      summary_txt:\n        present: %s\n        path: \"%s\"\n" "$SUM_PRESENT" "$SUMMARY_TXT" >> "$REF_MANIFEST"
  printf -- "    generated_at: %s\n" "$GEN_TS" >> "$REF_MANIFEST"
done < <(find "$DATASETS_DIR/sstables" -type f -name "*-Data.db" -print0)

echo "[export] Completed generating sstabledump reference JSONs."
